"""
Candle service — shared business logic for OHLCV data operations.

Extracts duplicated code from klines and historical API modules into
a single source of truth for aggregation, merging, querying, and validation.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException

from backend.core.config import INFLUX_BUCKET
from backend.core.constants import (
    INTERVAL_SECONDS,
    SYMBOL_RE,
    MAX_RAW_CANDLES,
    MAX_RAW_PER_QUERY,
    MAX_BACKFILL_PAGES,
    INFLUX_1M_RETENTION_DAYS,
)
from backend.core.database import get_influx, get_trino_connection


# ─── Validation ──────────────────────────────────────────────────────────────

def validate_symbol(symbol: str) -> str:
    """Normalize and validate a trading symbol."""
    s = symbol.strip().upper()
    if not SYMBOL_RE.match(s):
        raise HTTPException(400, "Invalid symbol")
    return s


def validate_interval(interval: str) -> tuple[str, int]:
    """Normalize interval and return (interval, seconds). Raises 400 if unsupported."""
    interval = interval.strip().lower()
    target_sec = INTERVAL_SECONDS.get(interval)
    if target_sec is None:
        raise HTTPException(400, f"Unsupported interval: {interval}")
    return interval, target_sec


# ─── Data transformations ────────────────────────────────────────────────────

def ms_to_rfc3339(ms: int) -> str:
    """Convert epoch milliseconds to RFC3339 string for InfluxDB Flux queries."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_candle_rows(rows: list[tuple]) -> list[dict]:
    """Convert raw Trino result tuples to candle dicts."""
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


def merge_unique(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Merge candle rows by openTime, preferring the latest version per timestamp."""
    if not incoming:
        return existing
    merged: dict[int, dict] = {int(c["openTime"]): c for c in existing}
    for c in incoming:
        merged[int(c["openTime"])] = c
    return sorted(merged.values(), key=lambda x: x["openTime"])


def aggregate(candles: list[dict], target_ms: int) -> list[dict]:
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


# ─── InfluxDB queries ────────────────────────────────────────────────────────

def query_influx_candles(
    symbol: str,
    interval: str,
    limit: int,
    range_h: int,
    end_ms: int | None = None,
) -> list[dict]:
    """Query candles from InfluxDB. If end_ms is provided, queries a fixed window."""
    if end_ms:
        interval_sec = INTERVAL_SECONDS.get(interval, 60)
        start_ms = end_ms - (limit * interval_sec * 1000) - (interval_sec * 1000)
        start_rfc = ms_to_rfc3339(start_ms)
        stop_rfc = ms_to_rfc3339(end_ms)
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


def query_influx_1m_range(
    symbol: str, start_ms: int, end_ms: int, limit: int,
) -> list[dict]:
    """Query 1m candles from InfluxDB within a specific time range."""
    start_rfc = ms_to_rfc3339(start_ms)
    stop_rfc = ms_to_rfc3339(end_ms)
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


# ─── Trino queries ───────────────────────────────────────────────────────────

def query_trino_1m(
    symbol: str, end_ms: int, limit: int,
    start_ms: int | None = None,
) -> list[dict]:
    """Query 1m closed candles from Iceberg via Trino.

    If start_ms is provided, queries a range [start_ms, end_ms).
    Otherwise queries everything before end_ms (scroll-left mode).
    """
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        if start_ms is not None:
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
        else:
            cur.execute(
                """
                SELECT
                    kline_start AS open_time,
                    open, high, low, close, volume
                FROM coin_klines
                WHERE symbol = ?
                  AND interval = '1m'
                  AND is_closed = true
                  AND kline_start < ?
                ORDER BY kline_start DESC
                LIMIT ?
                """,
                (symbol, end_ms, limit),
            )
        rows = cur.fetchall()
        rows.reverse()
        return to_candle_rows(rows)
    finally:
        conn.close()


def query_trino_hourly(
    symbol: str, end_ms: int, limit: int,
    start_ms: int | None = None,
) -> list[dict]:
    """Query historical_hourly table from Iceberg via Trino.

    If start_ms is provided, queries a range [start_ms, end_ms).
    Otherwise queries everything before end_ms (scroll-left mode).
    """
    conn = get_trino_connection()
    try:
        cur = conn.cursor()
        if start_ms is not None:
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
        else:
            cur.execute(
                """
                SELECT
                    open_time,
                    open, high, low, close, volume
                FROM historical_hourly
                WHERE symbol = ?
                  AND open_time < ?
                ORDER BY open_time DESC
                LIMIT ?
                """,
                (symbol, end_ms, limit),
            )
        rows = cur.fetchall()
        rows.reverse()
        return to_candle_rows(rows)
    finally:
        conn.close()


# ─── Composite queries ───────────────────────────────────────────────────────

def collect_base_1m_candles(
    symbol: str,
    target_sec: int,
    limit: int,
    end_ms: int,
    now_ms: int,
    influx_cutoff_ms: int,
    max_pages: int = MAX_BACKFILL_PAGES,
    allow_trino: bool = True,
) -> list[dict]:
    """Fetch 1m base candles with bounded, paged backfill from Influx then Trino.

    This avoids giant single queries for high intervals (4h/1d/1w), while still
    filling enough raw 1m candles to build `limit` aggregated bars when data exists.
    """
    mult = max(target_sec // 60, 1)
    raw_target = min((limit * mult) + mult, MAX_RAW_CANDLES)
    per_page = min(MAX_RAW_PER_QUERY, max(mult * 240, mult * 8))
    per_page = max(per_page, mult * 2)

    candles: list[dict] = []
    cursor = end_ms
    pages = 0
    while pages < max_pages and cursor > 0:
        pages += 1
        batch: list[dict] = []
        if cursor >= influx_cutoff_ms:
            range_h = min(max((per_page * 60) // 3600 + 2, 1), INFLUX_1M_RETENTION_DAYS * 24)
            batch = query_influx_candles(symbol, "1m", per_page, range_h, cursor)
        if not batch and allow_trino:
            batch = query_trino_1m(symbol, cursor, per_page)
        if not batch:
            break

        candles = merge_unique(candles, batch)
        oldest = min(c["openTime"] for c in batch)
        if oldest >= cursor:
            break
        cursor = oldest

        if target_sec == 60:
            if len(candles) >= limit:
                break
        else:
            if len(aggregate(candles, target_sec * 1000)) >= limit:
                break

        if end_ms >= now_ms and cursor < influx_cutoff_ms and len(candles) >= raw_target:
            break

    return candles
