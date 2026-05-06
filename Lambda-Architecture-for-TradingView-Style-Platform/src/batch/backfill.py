#!/usr/bin/env python3
"""
Unified backfill, populate & historical import script.

Modes:
    --mode influx       : Detect + fill InfluxDB gaps (machine downtime → missing data)
    --mode iceberg      : Pull historical 1m klines from Binance → Iceberg
    --mode populate     : Force populate N days of 1m candles for all symbols (first startup)
    --mode all          : influx + iceberg (default, does not include populate)

Iceberg sub-modes:
    --iceberg-mode backfill     : Pull from 2017 → now (run once, first time)
    --iceberg-mode incremental  : Pull from last saved candle → now (daily scheduled)

Examples:
    # Dagster scheduled:
    python backfill.py --mode all --iceberg-mode incremental

    # First startup — populate 90 days for 400 symbols:
    python backfill.py --mode populate --days 90

    # Fill InfluxDB gaps only:
    python backfill.py --mode influx
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import (
    FLUSH_THRESHOLD,
    ICEBERG_CATALOG,
    ICEBERG_DB,
    ICEBERG_TABLE_KLINES,
    INFLUX_BUCKET,
    INFLUX_ORG,
    INFLUX_TOKEN,
    INFLUX_URL,
    KLINE_BATCH_INFLUX,
    KLINES_PER_REQ,
    MAX_BACKFILL_DAYS,
    MAX_WORKERS,
    MIN_GAP_SEC,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    BACKFILL_SPARK_CORES_MAX,
    BACKFILL_SPARK_SHUFFLE_PARTITIONS,
    REQUEST_DELAY,
    MAX_RETRIES,
)
from exchanges.binance.client import BinanceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backfill")

# ── Exchange client instance ─────────────────────────────────────────────────
exchange = BinanceClient(max_retries=MAX_RETRIES, request_delay=REQUEST_DELAY)


# ═══════════════════════════════════════════════════════════════════════════════
# INFLUXDB GAP BACKFILL
# ═══════════════════════════════════════════════════════════════════════════════

def wait_for_influx(client, retries: int = 20, delay: float = 5.0):
    """Wait for InfluxDB to be ready."""
    for i in range(retries):
        try:
            client.ping()
            log.info("InfluxDB ready.")
            return
        except Exception:
            log.info("Waiting for InfluxDB... (%d/%d)", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("InfluxDB not reachable after retries.")


def find_all_gaps(client) -> dict[str, list[tuple[int, int]]]:
    """Find all gaps > MIN_GAP_SEC in each symbol's time series.

    Uses InfluxDB ``elapsed()`` to detect gaps anywhere in the series.
    Returns ``{symbol: [(gap_start_ms, gap_end_ms), ...]}``.
    """
    max_lookback_s = MAX_BACKFILL_DAYS * 24 * 3600
    min_gap_ms     = MIN_GAP_SEC * 1000

    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{max_lookback_s}s)
  |> filter(fn: (r) => r._measurement == "market_ticks"
      and r._field == "price"
      and (not exists r.source or r.source != "backfill"))
  |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
  |> elapsed(unit: 1ms, columnName: "elapsed_ms")
  |> filter(fn: (r) => r.elapsed_ms > {min_gap_ms})
  |> keep(columns: ["symbol", "_time", "elapsed_ms"])
"""
    query_api = client.query_api()
    tables    = query_api.query(flux, org=INFLUX_ORG)

    result: dict[str, list[tuple[int, int]]] = {}
    for table in tables:
        for record in table.records:
            symbol     = record.values.get("symbol")
            elapsed_ms = int(record.values.get("elapsed_ms", 0))
            if not symbol or elapsed_ms <= 0:
                continue
            gap_end_ms   = int(record.get_time().timestamp() * 1000)
            gap_start_ms = gap_end_ms - elapsed_ms + 60_000
            result.setdefault(symbol, []).append((gap_start_ms, gap_end_ms))

    total_gaps = sum(len(v) for v in result.values())
    log.info("Detected %d gap(s) across %d symbol(s).", total_gaps, len(result))
    return result


def klines_to_influx_points(symbol: str, klines: list[list]) -> list:
    """Convert Binance klines to InfluxDB Points (market_ticks schema)."""
    from influxdb_client import Point, WritePrecision

    points = []
    for k in klines:
        try:
            open_ms     = int(k[0])
            open_price  = float(k[1])
            close_price = float(k[4])
            volume      = float(k[5])
            quote_vol   = float(k[7])
            trades      = int(k[8])
            pct_change  = (close_price - open_price) / open_price * 100 if open_price else 0.0

            point = (
                Point("market_ticks")
                .tag("symbol",   symbol)
                .tag("exchange", "binance")
                .tag("source",   "backfill")
                .field("price",             close_price)
                .field("bid",               close_price)
                .field("ask",               close_price)
                .field("volume",            volume)
                .field("quote_volume",      quote_vol)
                .field("price_change_pct",  pct_change)
                .field("trade_count",       trades)
                .time(open_ms, WritePrecision.MS)
            )
            points.append(point)
        except Exception as e:
            log.warning("[%s] skip kline row: %s", symbol, e)
    return points


def backfill_symbol_influx(symbol: str, gap_start_ms: int, gap_end_ms: int, write_api) -> int:
    """Backfill one gap for one symbol in InfluxDB. Returns points written."""
    gap_sec = (gap_end_ms - gap_start_ms) / 1000
    log.info(
        "[%s] Backfilling gap %.1f min: %s → %s",
        symbol, gap_sec / 60,
        datetime.fromtimestamp(gap_start_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
        datetime.fromtimestamp(gap_end_ms   / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
    )

    klines = exchange.fetch_klines(symbol, gap_start_ms, gap_end_ms, interval="1m", batch_limit=KLINE_BATCH_INFLUX)
    if not klines:
        log.warning("[%s] No klines returned for this gap.", symbol)
        return 0

    points = klines_to_influx_points(symbol, klines)
    if not points:
        return 0

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        log.info("[%s] Written %d points.", symbol, len(points))
        return len(points)
    except Exception as e:
        log.error("[%s] write error: %s", symbol, e)
        return 0


def run_influx_backfill():
    """Entry point for InfluxDB gap backfill."""
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    log.info("=== InfluxDB Gap Backfill ===")
    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    wait_for_influx(client)

    all_gaps = find_all_gaps(client)
    if not all_gaps:
        log.info("No gaps detected — InfluxDB is complete.")
        client.close()
        return

    tasks = [
        (sym, start, end)
        for sym, gaps in all_gaps.items()
        for (start, end) in gaps
    ]
    log.info("Total tasks: %d gaps to fill.", len(tasks))

    total_points = 0
    total_filled = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(backfill_symbol_influx, sym, start, end, write_api): (sym, start, end)
            for (sym, start, end) in tasks
        }
        for future in as_completed(futures):
            sym, start, end = futures[future]
            try:
                n = future.result()
                if n > 0:
                    total_points += n
                    total_filled += 1
            except Exception as e:
                log.error("[%s] Unexpected error: %s", sym, e)

    log.info(
        "InfluxDB backfill complete — %d/%d gaps filled, %d total points written.",
        total_filled, len(tasks), total_points,
    )
    client.close()


# ═══════════════════════════════════════════════════════════════════════════════
# INFLUXDB INITIAL POPULATE
# ═══════════════════════════════════════════════════════════════════════════════

def klines_to_candles_points(symbol: str, klines: list[list]) -> list:
    """Convert Binance klines to InfluxDB Points (candles measurement)."""
    from influxdb_client import Point, WritePrecision

    points = []
    for k in klines:
        try:
            point = (
                Point("candles")
                .tag("symbol",   symbol)
                .tag("exchange", "binance")
                .tag("interval", "1m")
                .field("open",         float(k[1]))
                .field("high",         float(k[2]))
                .field("low",          float(k[3]))
                .field("close",        float(k[4]))
                .field("volume",       float(k[5]))
                .field("quote_volume", float(k[7]))
                .field("trade_count",  int(k[8]))
                .field("is_closed",    True)
                .time(int(k[0]), WritePrecision.MS)
            )
            points.append(point)
        except Exception as e:
            log.warning("[%s] skip kline row: %s", symbol, e)
    return points


def populate_symbol(symbol: str, start_ms: int, end_ms: int, write_api) -> int:
    """Force pull historical 1m candles for a symbol to InfluxDB."""
    days = (end_ms - start_ms) / 1000 / 86400
    log.info("[%s] Populate %.1f days", symbol, days)

    klines = exchange.fetch_klines(symbol, start_ms, end_ms, interval="1m", batch_limit=KLINE_BATCH_INFLUX)
    if not klines:
        return 0

    points = klines_to_candles_points(symbol, klines)
    if not points:
        return 0

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        log.info("[%s] Written %d points to InfluxDB", symbol, len(points))
        return len(points)
    except Exception as e:
        log.error("[%s] Write error: %s", symbol, e)
        return 0


def run_initial_populate(days: int = 90, symbols_list: list[str] | None = None):
    """Pull N days of 1m candles for all USDT symbols."""
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    log.info("=== InfluxDB Initial Populate (days=%d) ===", days)

    symbols = symbols_list or exchange.fetch_symbols()[:400]
    log.info("Symbols: %d", len(symbols))

    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - (days * 24 * 3600 * 1000)

    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    wait_for_influx(client)

    total_points  = 0
    total_success = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(populate_symbol, sym, start_ms, end_ms, write_api): sym
            for sym in symbols
        }
        for future in as_completed(futures):
            sym = futures[future]
            try:
                n = future.result()
                if n > 0:
                    total_points += n
                    total_success += 1
            except Exception as e:
                log.error("[%s] Unexpected error: %s", sym, e)

    log.info("Initial populate complete — %d/%d symbols, %d total points.",
             total_success, len(symbols), total_points)
    client.close()


# ═══════════════════════════════════════════════════════════════════════════════
# ICEBERG HISTORICAL IMPORT (Spark)
# ═══════════════════════════════════════════════════════════════════════════════

def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName("BackfillHistorical_to_Iceberg")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "jdbc")
        .config("spark.sql.catalog.iceberg_catalog.uri",
                f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg_catalog.jdbc.user",     os.environ.get("POSTGRES_USER", ""))
        .config("spark.sql.catalog.iceberg_catalog.jdbc.password", os.environ.get("POSTGRES_PASSWORD", ""))
        .config("spark.sql.catalog.iceberg_catalog.warehouse",     "s3://cryptoprice/iceberg")
        .config("spark.sql.catalog.iceberg_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg_catalog.s3.endpoint",          MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg_catalog.s3.access-key-id",     MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg_catalog.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg_catalog.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg_catalog.client.region",        "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.defaultCatalog", "iceberg_catalog")
        .config("spark.sql.shuffle.partitions", BACKFILL_SPARK_SHUFFLE_PARTITIONS)
        .config("spark.cores.max", BACKFILL_SPARK_CORES_MAX)
        .getOrCreate()
    )


def ensure_iceberg_table(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE_KLINES} (
            event_time      BIGINT, symbol STRING,
            kline_start     BIGINT, kline_close BIGINT,
            interval        STRING,
            open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
            volume DOUBLE, quote_volume DOUBLE, trade_count BIGINT,
            is_closed BOOLEAN,
            kline_timestamp TIMESTAMP, ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(kline_timestamp))
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec'  = 'zstd',
            'write.metadata.compression-codec' = 'gzip',
            'write.target-file-size-bytes'     = '134217728'
        )
    """)
    log.info("Iceberg table %s is ready.", ICEBERG_TABLE_KLINES)


def get_last_open_time(spark, symbol: str) -> int:
    from exchanges.binance.client import EPOCH_MS
    try:
        row = spark.sql(f"""
            SELECT MAX(kline_start) AS max_ts
            FROM {ICEBERG_TABLE_KLINES}
            WHERE symbol = '{symbol}' AND interval = '1m'
        """).first()
        if row and row["max_ts"]:
            return int(row["max_ts"]) + 60_000
    except Exception:
        pass
    return EPOCH_MS


def process_and_write_chunk(spark, rows: list, symbol: str, chunk_start_ms: int) -> int:
    from pyspark.sql.types import (
        BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType,
    )
    from pyspark.sql import functions as F

    if not rows:
        return 0

    kline_schema = StructType([
        StructField("event_time",  LongType(),    False),
        StructField("symbol",     StringType(),   False),
        StructField("kline_start", LongType(),    False),
        StructField("kline_close", LongType(),    False),
        StructField("interval",   StringType(),   False),
        StructField("open",       DoubleType(),   True),
        StructField("high",       DoubleType(),   True),
        StructField("low",        DoubleType(),   True),
        StructField("close",      DoubleType(),   True),
        StructField("volume",     DoubleType(),   True),
        StructField("quote_volume", DoubleType(), True),
        StructField("trade_count", LongType(),    True),
        StructField("is_closed",  BooleanType(),  False),
    ])

    df = spark.createDataFrame(rows, schema=kline_schema)
    df = (
        df
        .withColumn("kline_timestamp", (F.col("kline_start") / 1000).cast(TimestampType()))
        .withColumn("ingested_at", F.current_timestamp())
    )
    df = df.sortWithinPartitions("symbol", "kline_start")
    df.writeTo(ICEBERG_TABLE_KLINES).append()

    log.info("[%s] Wrote %d candles (chunk start=%s).", symbol, len(rows),
             datetime.fromtimestamp(chunk_start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"))
    return len(rows)


def write_symbol_iceberg(spark, symbol: str, start_ms: int, end_ms: int) -> int:
    """Pull 1m klines page-by-page and write to Iceberg incrementally."""
    total_written  = 0
    current_ms     = start_ms
    chunk_start_ms = start_ms
    chunk_buffer: list = []

    while current_ms < end_ms:
        klines = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    "https://api.binance.com/api/v3/klines",
                    params={
                        "symbol": symbol, "interval": "1m",
                        "startTime": current_ms, "endTime": end_ms,
                        "limit": KLINES_PER_REQ,
                    },
                    timeout=15,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning("[%s] Rate limited. Sleeping %ds.", symbol, retry_after)
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                klines = resp.json()
                break
            except Exception as e:
                log.warning("[%s] page fetch attempt %d failed: %s", symbol, attempt + 1, e)
                time.sleep(2 ** attempt)

        if klines is None:
            raise RuntimeError(f"[{symbol}] failed to fetch klines at start={current_ms}")
        if not klines:
            break

        for k in klines:
            open_ms = int(k[0])
            chunk_buffer.append([
                open_ms, symbol, open_ms, int(k[6]),
                "1m",
                float(k[1]), float(k[2]), float(k[3]), float(k[4]),
                float(k[5]), float(k[7]), int(k[8]),
                True,
            ])

        if len(chunk_buffer) >= FLUSH_THRESHOLD:
            total_written += process_and_write_chunk(spark, chunk_buffer, symbol, chunk_start_ms)
            chunk_buffer.clear()
            chunk_start_ms = int(klines[-1][0]) + 60_000

        next_ms = int(klines[-1][0]) + 60_000
        if next_ms <= current_ms:
            break
        current_ms = next_ms
        time.sleep(REQUEST_DELAY)

    if chunk_buffer:
        total_written += process_and_write_chunk(spark, chunk_buffer, symbol, chunk_start_ms)

    if total_written == 0:
        log.info("[%s] No new candles.", symbol)
    return total_written


def run_iceberg_historical(iceberg_mode: str = "incremental", symbols_list: list[str] | None = None):
    """Entry point for Iceberg historical import."""
    log.info("=== Iceberg Historical Import (mode=%s) ===", iceberg_mode)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ms = now_ms - (now_ms % 60_000)

    symbols = symbols_list or exchange.fetch_symbols()
    log.info("Symbols: %d | End: %s", len(symbols),
             datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_iceberg_table(spark)

    total_rows = 0
    for idx, symbol in enumerate(symbols, 1):
        log.info("[%d/%d] Processing %s ...", idx, len(symbols), symbol)
        if iceberg_mode == "backfill":
            start_ms = exchange.fetch_first_available_start(symbol)
        else:
            start_ms = get_last_open_time(spark, symbol)

        if start_ms >= end_ms:
            log.info("[%s] Already up-to-date.", symbol)
            continue

        try:
            n = write_symbol_iceberg(spark, symbol, start_ms, end_ms)
            total_rows += n
        except Exception as e:
            log.error("[%s] Failed: %s — skipping.", symbol, e)

    log.info("Done. Total candles written: %d", total_rows)
    spark.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Unified backfill + populate + historical import.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--mode", choices=["influx", "iceberg", "populate", "all"], default="all")
    parser.add_argument("--iceberg-mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--days", type=int, default=90)
    args = parser.parse_args()

    log.info("=== Backfill & Historical Import | mode=%s ===", args.mode)

    if args.mode == "populate":
        try:
            run_initial_populate(days=args.days, symbols_list=args.symbols)
        except Exception as e:
            log.error("Initial populate failed: %s", e)
        return

    if args.mode in ("influx", "all"):
        try:
            run_influx_backfill()
        except Exception as e:
            log.error("InfluxDB backfill failed: %s", e)

    if args.mode in ("iceberg", "all"):
        try:
            run_iceberg_historical(iceberg_mode=args.iceberg_mode, symbols_list=args.symbols)
        except Exception as e:
            log.error("Iceberg historical import failed: %s", e)

    log.info("=== All done ===")


if __name__ == "__main__":
    main()
