import asyncio
import json
import re

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


def _query_influx_sync(symbol: str, interval: str, limit: int, range_h: int) -> list[dict]:
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
    limit: int = Query(1500, ge=1, le=4320),  # Max 3 days (1m) or 180 days (1h)
):
    """
    Historical OHLCV candles.

    Response matches the shape expected by the React frontend:
    ``[{"openTime": ms, "open": float, "high": float, "low": float, "close": float, "volume": float}]``
    """
    symbol = _validate(symbol)
    target_sec = INTERVAL_SECONDS.get(interval)
    if target_sec is None:
        raise HTTPException(400, f"Unsupported interval: {interval}")

    base = _base_interval(interval)
    base_sec = INTERVAL_SECONDS[base]
    mult = max(target_sec // base_sec, 1)
    needed = limit * mult + mult  # small buffer
    # InfluxDB retention: 90 days for 1m candles
    # Cap range_h to avoid timeout with large queries
    if base == "1m":
        range_h = min(max(needed * base_sec // 3600 + 1, 1), 2160)  # cap at 90 days for 1m
    else:
        range_h = min(max(needed * base_sec // 3600 + 1, 1), 4320)  # cap at 180 days for 1h+

    candles = await asyncio.to_thread(
        _query_influx_sync, symbol, base, needed, range_h,
    )

    # Fallback: if requesting >=1h data but no aggregated 1h rows exist yet,
    # re-query InfluxDB for 1m data and aggregate server-side.
    if not candles and base == "1h":
        mult = max(target_sec // 60, 1)
        needed = limit * mult + mult
        range_h = min(max(needed // 60 + 1, 1), 720)  # 30 days max for 1m fallback
        candles = await asyncio.to_thread(
            _query_influx_sync, symbol, "1m", needed, range_h,
        )

    # Fallback: try KeyDB candle sorted sets
    if not candles:
        r = await get_redis()
        # Try 1m first, then 1s
        raw = None
        for keydb_key in (f"candle:1m:{symbol}", f"candle:1s:{symbol}"):
            raw = await r.zrangebyscore(keydb_key, "-inf", "+inf")
            if raw:
                break
        seen_times: set[int] = set()
        for item in raw if raw else []:
            c = json.loads(item)
            t = int(c["t"])
            if t in seen_times:
                continue  # deduplicate by timestamp
            seen_times.add(t)
            candles.append({
                "openTime": t,
                "open": c["o"], "high": c["h"],
                "low": c["l"], "close": c["c"],
                "volume": c["v"],
            })
        candles.sort(key=lambda x: x["openTime"])

    if base != interval and candles:
        candles = _aggregate(candles, target_sec * 1000)

    # Merge the live in-progress candle from KeyDB so the latest bar
    # reflects real-time price changes (agitation) between interval boundaries.
    r = await get_redis()
    live = await r.hgetall(f"candle:latest:{symbol}")
    if live:
        target_ms = target_sec * 1000
        kline_start = int(live["kline_start"])
        aligned_time = (kline_start // target_ms) * target_ms
        live_candle = {
            "openTime": aligned_time,
            "open": float(live["open"]),
            "high": float(live["high"]),
            "low": float(live["low"]),
            "close": float(live["close"]),
            "volume": float(live["volume"]),
        }
        if candles and candles[-1]["openTime"] == aligned_time:
            # Same period — replace with the live version (has latest OHLCV)
            candles[-1] = live_candle
        elif not candles or aligned_time > candles[-1]["openTime"]:
            candles.append(live_candle)

    return candles[-limit:]
