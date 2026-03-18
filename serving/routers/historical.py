import asyncio
import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from serving.connections import get_trino_connection

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

    # Query 1m base candles from coin_klines and aggregate server-side.
    # For very large 1h+ ranges, prefer hourly source to avoid over-fetching 1m.
    range_ms = endTime - startTime
    max_1m_span_ms = MAX_RAW_ROWS * 60 * 1000
    use_hourly_first = interval in HOURLY_INTERVALS and range_ms > max_1m_span_ms

    mult = max(target_sec // 60, 1)
    raw_limit = min((limit * mult) + mult, MAX_RAW_ROWS)
    if (not use_hourly_first) and raw_limit >= MAX_RAW_ROWS and limit * mult > MAX_RAW_ROWS:
        raise HTTPException(400, "Requested range/limit is too large for this interval")

    candles = []
    if not use_hourly_first:
        try:
            candles = await asyncio.to_thread(
                _query_trino_1m, symbol, startTime, endTime, raw_limit,
            )
        except Exception:
            pass

    # Fallback to legacy historical_hourly data if 1m table is unavailable,
    # sparse, or range is too large for efficient 1m retrieval.
    if use_hourly_first or not candles:
        candles = await asyncio.to_thread(
            _query_trino_historical, symbol, startTime, endTime, limit,
        )
        # historical_hourly only has 1h granularity; do not fabricate 1m/5m/15m.
        if interval in ("1m", "5m", "15m"):
            return []
        # historical_hourly is already hourly; only aggregate for 4h/1d/1w.
        if candles and interval in ("4h", "1d", "1w"):
            candles = _aggregate(candles, target_sec * 1000)
            candles = candles[-limit:]
        return candles

    if interval != "1m":
        candles = _aggregate(candles, target_sec * 1000)
    candles = [c for c in candles if startTime <= c["openTime"] < endTime]
    candles = candles[-limit:]

    return candles
