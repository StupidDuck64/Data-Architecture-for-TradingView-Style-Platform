"""
Historical Klines API — cold storage queries for date-range OHLCV data.

Thin route handler that delegates business logic to candle_service.
"""

import asyncio
import time

from fastapi import APIRouter, HTTPException, Query

from backend.core.constants import INTERVAL_SECONDS, INFLUX_1M_RETENTION_DAYS, MAX_RAW_ROWS
from backend.services.candle_service import (
    validate_symbol,
    validate_interval,
    aggregate,
    merge_unique,
    query_influx_1m_range,
    query_trino_1m,
    query_trino_hourly,
)

router = APIRouter(prefix="/api", tags=["historical"])


@router.get("/klines/historical")
async def get_historical_klines(
    symbol: str,
    interval: str = Query("1h", description="Target interval (1m/5m/15m/1h/4h/1d/1w)"),
    startTime: int = Query(..., description="Range start in epoch milliseconds"),
    endTime: int = Query(..., description="Range end in epoch milliseconds"),
    limit: int = Query(500, ge=1, le=5000),
):
    """
    Query cold storage for historical OHLCV candles within a specific date range.
    All higher intervals are derived from 1m base candles.
    """
    symbol = validate_symbol(symbol)
    interval, target_sec = validate_interval(interval)

    if endTime <= startTime:
        raise HTTPException(400, "endTime must be greater than startTime")

    max_range_ms = 365 * 24 * 3600 * 1000
    if endTime - startTime > max_range_ms:
        raise HTTPException(400, "Date range cannot exceed 1 year")

    mult = max(target_sec // 60, 1)
    raw_limit = min((limit * mult) + mult, MAX_RAW_ROWS)
    candles: list[dict] = []

    now_ms = int(time.time() * 1000)
    influx_cutoff_ms = now_ms - (INFLUX_1M_RETENTION_DAYS * 24 * 3600 * 1000)

    # Recent overlap → InfluxDB 1m
    influx_start = max(startTime, influx_cutoff_ms)
    if endTime > influx_start:
        try:
            influx_rows = await asyncio.to_thread(
                query_influx_1m_range, symbol, influx_start, endTime, raw_limit,
            )
            candles = merge_unique(candles, influx_rows)
        except Exception:
            pass

    # Older overlap → Iceberg 1m via Trino
    trino_end = min(endTime, influx_cutoff_ms)
    if trino_end > startTime:
        try:
            trino_rows = await asyncio.to_thread(
                query_trino_1m, symbol, trino_end, raw_limit, start_ms=startTime,
            )
            candles = merge_unique(candles, trino_rows)
        except Exception:
            pass

    # No 1m data → fallback to hourly cold table for 1h+
    if not candles:
        if interval in ("1m", "5m", "15m"):
            return []
        hourly_limit = min(max(limit * max(target_sec // 3600, 1), limit), 5000)
        candles = await asyncio.to_thread(
            query_trino_hourly, symbol, endTime, hourly_limit, start_ms=startTime,
        )
        if candles and interval in ("4h", "1d", "1w"):
            candles = aggregate(candles, target_sec * 1000)
        candles = [c for c in candles if startTime <= c["openTime"] < endTime]
        return candles[-limit:]

    if interval != "1m":
        candles = aggregate(candles, target_sec * 1000)
    candles = [c for c in candles if startTime <= c["openTime"] < endTime]
    return candles[-limit:]
