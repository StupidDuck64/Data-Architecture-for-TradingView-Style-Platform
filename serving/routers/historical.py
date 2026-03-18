import asyncio
import os
import re
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from serving.config import INFLUX_BUCKET
from serving.connections import get_influx, get_trino_connection

router = APIRouter(prefix="/api", tags=["historical"])

_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}$")

INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}

MAX_RAW_ROWS = 200_000
HOURLY_INTERVALS = {"1h", "4h", "1d", "1w"}
INFLUX_1M_RETENTION_DAYS = int(os.environ.get("INFLUX_1M_RETENTION_DAYS", "90"))


def _validate(symbol: str) -> str:
    s = symbol.strip().upper()
    if not _SYMBOL_RE.match(s):
        raise HTTPException(400, "Invalid symbol")
    return s


def _to_candle_rows(rows: list[tuple]) -> list[dict]:
    return [
        {
            "openTime": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]),
        }
        for r in rows
    ]


def _merge_unique(existing: list[dict], incoming: list[dict]) -> list[dict]:
    if not incoming:
        return existing
    merged: dict[int, dict] = {int(c["openTime"]): c for c in existing}
    for c in incoming:
        merged[int(c["openTime"])] = c
    return sorted(merged.values(), key=lambda x: x["openTime"])


def _ms_to_rfc3339(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _aggregate(candles: list[dict], target_ms: int) -> list[dict]:
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


def _query_trino_1m(
    symbol: str, start_ms: int, end_ms: int, limit: int,
) -> list[dict]:
    """Query coin_klines 1m candles via Trino for a date range.

    Rows are fetched from the right edge of the range (DESC + LIMIT) so when
    the requested window is huge, the API still returns the most relevant
    recent segment for chart rendering.
    """
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                kline_start AS open_time,
                open, high, low, close, volume
            FROM crypto_lakehouse.coin_klines
            WHERE symbol = ?
              AND interval = '1m'
              AND is_closed = true
              AND kline_start >= ?
              AND kline_start < ?
            ORDER BY kline_start DESC
            LIMIT ?
            """,
            (symbol, start_ms, end_ms, limit),
        )
        rows = cur.fetchall()
        rows.reverse()
        return _to_candle_rows(rows)
    finally:
        conn.close()


def _query_trino_historical(
    symbol: str, start_ms: int, end_ms: int, limit: int,
) -> list[dict]:
    """Fallback: query the historical_hourly table (backfilled from Binance)."""
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                open_time,
                open, high, low, close, volume
            FROM crypto_lakehouse.historical_hourly
            WHERE symbol = ?
              AND open_time >= ?
              AND open_time < ?
            ORDER BY open_time DESC
            LIMIT ?
            """,
            (symbol, start_ms, end_ms, limit),
        )
        rows = cur.fetchall()
        rows.reverse()
        return _to_candle_rows(rows)
    finally:
        conn.close()


def _query_influx_1m_range(
    symbol: str, start_ms: int, end_ms: int, limit: int,
) -> list[dict]:
    start_rfc = _ms_to_rfc3339(start_ms)
    stop_rfc = _ms_to_rfc3339(end_ms)
    query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {start_rfc}, stop: {stop_rfc})
  |> filter(fn: (r) => r._measurement == "candles")
  |> filter(fn: (r) => r.symbol == "{symbol}")
  |> filter(fn: (r) => r.interval == "1m")
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


@router.get("/klines/historical")
async def get_historical_klines(
    symbol: str,
    interval: str = Query("1h", description="Target interval (1m/5m/15m/1h/4h/1d/1w)"),
    startTime: int = Query(..., description="Range start in epoch milliseconds"),
    endTime: int = Query(..., description="Range end in epoch milliseconds"),
    limit: int = Query(500, ge=1, le=5000),
):
    """
    Query Iceberg cold storage via Trino for historical OHLCV candles
    within a specific date range. All higher intervals are derived from 1m.

    Response shape matches ``/api/klines``:
    ``[{"openTime": ms, "open": float, "high": float, "low": float, "close": float, "volume": float}]``
    """
    symbol = _validate(symbol)
    interval = interval.strip().lower()

    target_sec = INTERVAL_SECONDS.get(interval)
    if target_sec is None:
        raise HTTPException(400, f"Unsupported interval: {interval}")

    if endTime <= startTime:
        raise HTTPException(400, "endTime must be greater than startTime")

    # Cap range to 1 year
    max_range_ms = 365 * 24 * 3600 * 1000
    if endTime - startTime > max_range_ms:
        raise HTTPException(400, "Date range cannot exceed 1 year")

    # Query 1m base candles from hot (Influx) and cold (Iceberg) layers.
    # This keeps small/recent ranges responsive while still supporting deep history.
    mult = max(target_sec // 60, 1)
    raw_limit = min((limit * mult) + mult, MAX_RAW_ROWS)
    candles: list[dict] = []

    now_ms = int(time.time() * 1000)
    influx_cutoff_ms = now_ms - (INFLUX_1M_RETENTION_DAYS * 24 * 3600 * 1000)

    # Recent overlap -> Influx 1m
    influx_start = max(startTime, influx_cutoff_ms)
    influx_end = endTime
    if influx_end > influx_start:
        try:
            influx_rows = await asyncio.to_thread(
                _query_influx_1m_range, symbol, influx_start, influx_end, raw_limit,
            )
            candles = _merge_unique(candles, influx_rows)
        except Exception:
            pass

    # Older overlap -> Iceberg 1m
    trino_start = startTime
    trino_end = min(endTime, influx_cutoff_ms)
    if trino_end > trino_start:
        try:
            trino_rows = await asyncio.to_thread(
                _query_trino_1m, symbol, trino_start, trino_end, raw_limit,
            )
            candles = _merge_unique(candles, trino_rows)
        except Exception:
            pass

    # If no 1m base rows are available, fallback to hourly cold table for 1h+.
    if not candles:
        if interval in ("1m", "5m", "15m"):
            return []
        hourly_limit = min(max(limit * max(target_sec // 3600, 1), limit), 5000)
        candles = await asyncio.to_thread(
            _query_trino_historical, symbol, startTime, endTime, hourly_limit,
        )
        if candles and interval in ("4h", "1d", "1w"):
            candles = _aggregate(candles, target_sec * 1000)
        candles = [c for c in candles if startTime <= c["openTime"] < endTime]
        return candles[-limit:]

    if interval != "1m":
        candles = _aggregate(candles, target_sec * 1000)
    candles = [c for c in candles if startTime <= c["openTime"] < endTime]
    candles = candles[-limit:]

    return candles
