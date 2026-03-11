import asyncio
import json
import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from serving.config import INFLUX_BUCKET
from serving.connections import get_influx, get_redis

router = APIRouter(prefix="/api", tags=["klines"])

INTERVAL_SECONDS = {
    "1s": 1, "1m": 60, "5m": 300, "15m": 900,
    "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800,
}

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}$")


def _validate(symbol: str) -> str:
    s = symbol.strip().upper()
    if not _SYMBOL_RE.match(s):
        raise HTTPException(400, "Invalid symbol")
    return s


def _ms_to_rfc3339(ms: int) -> str:
    """Convert epoch milliseconds to RFC3339 string for InfluxDB Flux queries."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _base_interval(interval: str) -> str:
    """Pick the finest stored interval to serve as aggregation base."""
    if interval == "1s":
        return "1s"
    return "1m" if interval in ("1m", "5m", "15m") else "1h"


def _aggregate(candles: list[dict], target_ms: int) -> list[dict]:
    """Re-sample OHLCV candles into larger-interval buckets."""
    if not candles:
        return []
    buckets: dict[int, dict] = {}
    for c in candles:
        key = (c["openTime"] // target_ms) * target_ms
        if key not in buckets:
            buckets[key] = {
                "openTime": key,
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c["volume"],
            }
        else:
            b = buckets[key]
            b["high"] = max(b["high"], c["high"])
            b["low"] = min(b["low"], c["low"])
            b["close"] = c["close"]
            b["volume"] = round(b["volume"] + c["volume"], 8)
    return sorted(buckets.values(), key=lambda x: x["openTime"])


def _query_influx_sync(symbol: str, interval: str, limit: int, range_h: int,
                       end_ms: int | None = None) -> list[dict]:
    if end_ms:
        # For historical scroll queries: query a window ending at end_ms
        # Go back enough time to cover `limit` candles
        interval_sec = INTERVAL_SECONDS.get(interval, 60)
        start_ms = end_ms - (limit * interval_sec * 1000) - (interval_sec * 1000)
        start_rfc = _ms_to_rfc3339(start_ms)
        stop_rfc = _ms_to_rfc3339(end_ms)
        query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {start_rfc}, stop: {stop_rfc})
  |> filter(fn: (r) => r._measurement == "candles")
  |> filter(fn: (r) => r.symbol == "{symbol}")
  |> filter(fn: (r) => r.interval == "{interval}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
  |> tail(n: {limit})
'''
    else:
        query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{range_h}h)
  |> filter(fn: (r) => r._measurement == "candles")
  |> filter(fn: (r) => r.symbol == "{symbol}")
  |> filter(fn: (r) => r.interval == "{interval}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
  |> tail(n: {limit})
'''
    tables = get_influx().query_api().query(query)
    out: list[dict] = []
    for table in tables:
        for rec in table.records:
            out.append({
                "openTime": int(rec.get_time().timestamp() * 1000),
                "open": float(rec.values.get("open", 0)),
                "high": float(rec.values.get("high", 0)),
                "low": float(rec.values.get("low", 0)),
                "close": float(rec.values.get("close", 0)),
                "volume": float(rec.values.get("volume", 0)),
            })
    return out


@router.get("/klines")
async def get_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = Query(200, ge=1, le=1500),
    endTime: int | None = Query(None, description="End timestamp in milliseconds (exclusive). If provided, returns candles before this time."),
):
    """
    Historical OHLCV candles.

    Response matches the shape expected by the React frontend:
    ``[{"openTime": ms, "open": float, "high": float, "low": float, "close": float, "volume": float}]``
    
    If endTime is provided, returns `limit` candles ending before endTime (useful for scroll loading).
    """
    symbol = _validate(symbol)
    target_sec = INTERVAL_SECONDS.get(interval)
    if target_sec is None:
        raise HTTPException(400, f"Unsupported interval: {interval}")

    # Check Redis cache first (0.1 second TTL for ultra-low latency)
    # Skip cache for endTime queries (historical scroll loading)
    r = await get_redis()
    cache_key = f"klines_cache:{symbol}:{interval}:{limit}"
    if not endTime:
        cached = await r.get(cache_key)
        if cached:
            return json.loads(cached)

    base = _base_interval(interval)
    base_sec = INTERVAL_SECONDS[base]
    mult = max(target_sec // base_sec, 1)
    needed = limit * mult + mult  # small buffer
    range_h = min(max(needed * base_sec // 3600 + 1, 1), 2160)  # cap at 90 days

    candles = []

    # For endTime (scroll-left) queries, skip KeyDB and go directly to InfluxDB
    # because KeyDB only keeps recent data (1s: 8h, 1m: 7d).
    if not endTime:
        # PRIORITY 1: Try KeyDB first for real-time data (0.5s latency)
        # Try the resolution that matches the requested interval first
        if interval == "1s":
            key_order = (f"candle:1s:{symbol}", f"candle:1m:{symbol}")
        else:
            key_order = (f"candle:1m:{symbol}", f"candle:1s:{symbol}")
        raw = None
        for keydb_key in key_order:
            raw = await r.zrangebyscore(keydb_key, "-inf", "+inf")
            if raw:
                break
        # Deduplicate by timestamp — keep the entry with the highest volume
        # (most complete aggregation) for each timestamp
        best_by_time: dict[int, dict] = {}
        for item in raw if raw else []:
            c = json.loads(item)
            t = int(c["t"])
            if t not in best_by_time or c["v"] > best_by_time[t]["v"]:
                best_by_time[t] = c
        for t, c in best_by_time.items():
            candles.append({
                "openTime": t,
                "open": c["o"], "high": c["h"],
                "low": c["l"], "close": c["c"],
                "volume": c["v"],
            })
        candles.sort(key=lambda x: x["openTime"])

    # PRIORITY 2: Query InfluxDB for historical data
    # For endTime (scroll-left) queries, always go to InfluxDB.
    # For regular queries, only if KeyDB doesn't have enough.
    if endTime or len(candles) < limit:
        influx_candles = await asyncio.to_thread(
            _query_influx_sync, symbol, base, needed, range_h, endTime,
        )
        # Fallback: if requesting >=1h data but no aggregated 1h rows exist yet,
        # re-query InfluxDB for 1m data and aggregate server-side.
        if not influx_candles and base == "1h":
            base = "1m"
            mult = max(target_sec // 60, 1)
            needed = limit * mult + mult
            range_h = min(max(needed // 60 + 1, 1), 168)  # 7 days max for 1m
            influx_candles = await asyncio.to_thread(
                _query_influx_sync, symbol, "1m", needed, range_h, endTime,
            )
        # Merge InfluxDB data with KeyDB data (avoiding duplicates)
        existing_times = {c["openTime"] for c in candles}
        for c in influx_candles:
            if c["openTime"] not in existing_times:
                candles.append(c)
        candles.sort(key=lambda x: x["openTime"])

    if base != interval and candles:
        candles = _aggregate(candles, target_sec * 1000)

    # When endTime is provided (scroll loading), skip live ticker merge
    # and filter candles to only those before endTime
    if endTime:
        candles = [c for c in candles if c["openTime"] < endTime]
        result = candles[-limit:] if candles else []
    else:
        # Merge the live in-progress candle using the real-time ticker price
        # so the latest bar reflects current price even when Flink lags.
        # Only use ticker if it's NEWER than the latest candle's timestamp.
        ticker = await r.hgetall(f"ticker:latest:{symbol}")
        if ticker.get("price") and ticker.get("event_time"):
            target_ms = target_sec * 1000
            live_price = float(ticker["price"])
            live_ts = int(ticker["event_time"])
            aligned_time = (live_ts // target_ms) * target_ms
            latest_candle_ts = candles[-1]["openTime"] if candles else 0
            if candles and candles[-1]["openTime"] == aligned_time and live_ts > latest_candle_ts:
                # Same period and ticker is fresher — update close / extend high-low
                candles[-1]["close"] = live_price
                candles[-1]["high"] = max(candles[-1]["high"], live_price)
                candles[-1]["low"] = min(candles[-1]["low"], live_price)
            elif not candles or aligned_time > candles[-1]["openTime"]:
                candles.append({
                    "openTime": aligned_time,
                    "open": live_price,
                    "high": live_price,
                    "low": live_price,
                    "close": live_price,
                    "volume": 0,
                })
        result = candles[-limit:]
    
    # Cache result in Redis (0.1 second TTL for ultra-low latency)
    # Skip caching for endTime queries (historical scroll loading)
    if not endTime:
        import time
        cache_data = json.dumps(result)
        pipe = r.pipeline()
        pipe.set(cache_key, cache_data)
        pipe.pexpire(cache_key, 100)  # 100ms TTL
        await pipe.execute()
    
    return result
