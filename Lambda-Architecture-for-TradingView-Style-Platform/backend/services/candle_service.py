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


def normalize_interval(interval: str) -> str:
    """Normalize interval to lowercase standard form (1h, 4h, 1d, 1w)."""
    return interval.strip().lower()


def interval_to_seconds(interval: str) -> int:
    """Convert interval string to seconds. Returns 0 if invalid."""
    normalized = normalize_interval(interval)
    return INTERVAL_SECONDS.get(normalized, 0)


def interval_to_ms(interval: str) -> int:
    """Convert interval string to milliseconds. Returns 0 if invalid."""
    return interval_to_seconds(interval) * 1000


def validate_interval(interval: str) -> tuple[str, int]:
    """Normalize interval and return (interval, seconds). Raises 400 if unsupported."""
    interval = normalize_interval(interval)
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
    """Merge candle rows by openTime, preferring higher-quality candles.

    Quality priority:
    1. Closed/final candle (if 'is_closed' or 'x' field exists)
    2. Higher volume (more complete data)
    3. Later in merge order (incoming preferred if equal quality)
    """
    if not incoming:
        return existing

    merged: dict[int, dict] = {}

    # First pass: add all existing candles
    for c in existing:
        merged[int(c["openTime"])] = c

    # Second pass: merge incoming, choosing better candle on conflict
    for c in incoming:
        t = int(c["openTime"])
        if t not in merged:
            merged[t] = c
        else:
            existing_candle = merged[t]
            # Choose better candle based on quality
            if _is_better_candle(c, existing_candle):
                merged[t] = c

    return sorted(merged.values(), key=lambda x: x["openTime"])


def _is_better_candle(new: dict, old: dict) -> bool:
    """Return True if new candle is better quality than old candle.

    Priority:
    1. Closed/final candle wins over partial
    2. Higher volume wins (more complete data)
    3. If equal, prefer new (incoming)
    """
    # Check if either candle has closed/final flag
    new_closed = new.get("is_closed") or new.get("x")
    old_closed = old.get("is_closed") or old.get("x")

    # Priority 1: Closed candle wins
    if new_closed and not old_closed:
        return True
    if old_closed and not new_closed:
        return False

    # Priority 2: Higher volume wins
    new_vol = float(new.get("volume", 0))
    old_vol = float(old.get("volume", 0))

    if new_vol > old_vol:
        return True
    if old_vol > new_vol:
        return False

    # Priority 3: If equal quality, prefer new (incoming)
    return True


def aggregate(candles: list[dict], target_ms: int) -> list[dict]:
    """Re-sample OHLCV candles into larger-interval buckets.

    Correctly handles:
    - Out-of-order input candles
    - Duplicate openTime (deduped via merge_unique first)
    - open = open of earliest timestamp in bucket
    - close = close of latest timestamp in bucket
    - high = max(high), low = min(low), volume = sum(volume)
    """
    if not candles:
        return []

    # First dedup any duplicate timestamps using merge_unique
    candles = merge_unique([], candles)

    # Group candles into buckets
    buckets: dict[int, list[dict]] = {}
    for c in candles:
        key = (c["openTime"] // target_ms) * target_ms
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(c)

    # Aggregate each bucket with correct OHLCV logic
    result = []
    for bucket_time, bucket_candles in buckets.items():
        # Sort by openTime to get correct open/close
        bucket_candles.sort(key=lambda x: x["openTime"])

        agg = {
            "openTime": bucket_time,
            "open": bucket_candles[0]["open"],      # First candle's open
            "close": bucket_candles[-1]["close"],   # Last candle's close
            "high": max(c["high"] for c in bucket_candles),
            "low": min(c["low"] for c in bucket_candles),
            "volume": round(sum(c["volume"] for c in bucket_candles), 8),
        }
        result.append(agg)

    return sorted(result, key=lambda x: x["openTime"])


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
