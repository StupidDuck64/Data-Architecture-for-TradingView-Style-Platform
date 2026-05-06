"""
Klines API — real-time and recent OHLCV candle data.

Thin route handler that delegates business logic to candle_service.
"""

from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, HTTPException, Query

from backend.core.constants import INTERVAL_SECONDS, INFLUX_1M_RETENTION_DAYS, MAX_RAW_CANDLES, LIVE_MAX_BASE_ROWS, MAX_BACKFILL_PAGES
from backend.core.database import get_redis
from backend.core.redis_sentinel import get_redis_master
from backend.services.candle_service import (
    validate_symbol,
    validate_interval,
    aggregate,
    merge_unique,
    collect_base_1m_candles,
    query_influx_candles,
    query_trino_hourly,
)

router = APIRouter(prefix="/api", tags=["klines"])


@router.get("/klines")
async def get_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = Query(200, ge=1, le=1500),
    endTime: int | None = Query(None, description="End timestamp in milliseconds (exclusive). If provided, returns candles before this time."),
):
    """
    Historical OHLCV candles.

    If endTime is provided, returns `limit` candles ending before endTime (useful for scroll loading).
    """
    symbol = validate_symbol(symbol)
    interval, target_sec = validate_interval(interval)

    r = await get_redis()
    cache_key = f"klines_cache:{symbol}:{interval}:{limit}"

    # Check Redis cache (skip for scroll queries)
    if not endTime:
        cached = await r.get(cache_key)
        if cached:
            return json.loads(cached)

    candles = []
    now_ms = int(time.time() * 1000)
    influx_cutoff_ms = now_ms - (INFLUX_1M_RETENTION_DAYS * 24 * 3600 * 1000)

    if interval == "1s":
        candles = await _fetch_1s_candles(r, symbol, limit, endTime, now_ms)
    else:
        candles = await _fetch_1m_plus_candles(
            r, symbol, interval, target_sec, limit, endTime, now_ms, influx_cutoff_ms,
        )

    # Aggregate for intervals above 1-minute resolution
    if interval not in ("1s", "1m") and candles:
        candles = aggregate(candles, target_sec * 1000)

    # Build final result
    if endTime:
        candles = [c for c in candles if c["openTime"] < endTime]
        result = candles[-limit:] if candles else []
    else:
        if interval not in ("1s", "1m"):
            candles = await _enrich_with_live_ticker(r, symbol, target_sec, candles)
        result = candles[-limit:]

    # Cache result (skip for scroll queries)
    if not endTime:
        ttl_ms = 200 if interval == "1s" else 1500
        # Use master for write operations
        r_master = await get_redis_master()
        pipe = r_master.pipeline()
        pipe.set(cache_key, json.dumps(result))
        pipe.pexpire(cache_key, ttl_ms)
        await pipe.execute()

    return result


async def _fetch_1s_candles(r, symbol: str, limit: int, end_time: int | None, now_ms: int) -> list[dict]:
    """Fetch 1-second candles exclusively from KeyDB (speed layer)."""
    needed_1s = min(limit + 2, MAX_RAW_CANDLES)
    live_lookback_ms = max(needed_1s * 1000, 120_000)
    score_min = (end_time - needed_1s * 1000) if end_time else str(now_ms - live_lookback_ms)
    score_max = (end_time - 1) if end_time else "+inf"
    raw = await r.zrangebyscore(f"candle:1s:{symbol}", score_min, score_max)
    if not raw and not end_time:
        raw = await r.zrevrange(f"candle:1s:{symbol}", 0, needed_1s - 1)

    best_by_time: dict[int, dict] = {}
    for item in raw if raw else []:
        c = json.loads(item)
        t = int(c["t"])
        if t not in best_by_time or c["v"] > best_by_time[t]["v"]:
            best_by_time[t] = c

    candles = []
    for t, c in best_by_time.items():
        candles.append({
            "openTime": t,
            "open": c["o"], "high": c["h"],
            "low": c["l"], "close": c["c"],
            "volume": c["v"],
        })
    candles.sort(key=lambda x: x["openTime"])
    return candles


async def _fetch_1m_plus_candles(
    r, symbol: str, interval: str, target_sec: int, limit: int,
    end_time: int | None, now_ms: int, influx_cutoff_ms: int,
) -> list[dict]:
    """Fetch 1m+ candles from InfluxDB with Trino fallback."""
    candles: list[dict] = []

    if end_time is not None:
        backfilled = await asyncio.to_thread(
            collect_base_1m_candles,
            symbol, target_sec, limit, end_time, now_ms, influx_cutoff_ms,
            MAX_BACKFILL_PAGES, True,
        )
        candles = merge_unique(candles, backfilled)
    else:
        raw_needed = min((limit * max(target_sec // 60, 1)) + 2, MAX_RAW_CANDLES)
        live_limit = min(max(raw_needed, limit), LIVE_MAX_BASE_ROWS)
        live_range_h = min(max((live_limit * 60) // 3600 + 2, 1), INFLUX_1M_RETENTION_DAYS * 24)
        live_rows = await asyncio.to_thread(
            query_influx_candles, symbol, "1m", live_limit, live_range_h, None,
        )
        candles = merge_unique(candles, live_rows)

    # Fallback to legacy hourly for 1h+ when 1m data is sparse
    if end_time is not None and interval in ("1h", "4h", "1d", "1w"):
        target_h = max(target_sec // 3600, 1)
        hourly_needed = min((limit * target_h) + target_h, 5000)
        if len(candles) < max(limit, target_h * 8):
            hourly_rows = await asyncio.to_thread(
                query_trino_hourly, symbol, end_time or now_ms, hourly_needed,
            )
            candles = merge_unique(candles, hourly_rows)

    return candles


async def _enrich_with_live_ticker(r, symbol: str, target_sec: int, candles: list[dict]) -> list[dict]:
    """Enrich the latest candle with live ticker price for 5m+ intervals.

    Only enriches if ticker is fresher than the latest sub-candle data.
    """
    ticker = await r.hgetall(f"ticker:latest:{symbol}")
    if not (ticker.get("price") and ticker.get("event_time")):
        return candles

    target_ms = target_sec * 1000
    live_price = float(ticker["price"])
    live_ts = int(ticker["event_time"])
    aligned_time = (live_ts // target_ms) * target_ms
    latest_candle_ts = candles[-1]["openTime"] if candles else 0
    window_is_open = int(time.time() * 1000) < (aligned_time + target_ms)

    # Check ticker freshness against sub-candle data
    # For aggregated intervals (5m+), verify ticker is newer than source data
    if candles and candles[-1]["openTime"] == aligned_time and window_is_open:
        # Query the latest sub-candle timestamp for this window
        source_interval = "1m"  # 5m+ intervals aggregate from 1m
        source_key = f"candle:{source_interval}:{symbol}"
        latest_sub = await r.zrevrange(source_key, 0, 0, withscores=True)

        latest_sub_ts = 0
        if latest_sub:
            latest_sub_ts = int(latest_sub[0][1])  # score = kline_start timestamp

        # Only enrich if ticker is fresher than latest sub-candle
        if live_ts > max(latest_candle_ts, latest_sub_ts):
            candles[-1]["close"] = live_price
            candles[-1]["high"] = max(candles[-1]["high"], live_price)
            candles[-1]["low"] = min(candles[-1]["low"], live_price)
        return candles

    # Create new candle if ticker is for a newer window
    if not candles or aligned_time > candles[-1]["openTime"]:
        candles.append({
            "openTime": aligned_time,
            "open": live_price,
            "high": live_price,
            "low": live_price,
            "close": live_price,
            "volume": 0.0,
        })

    return candles
