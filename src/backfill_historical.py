#!/usr/bin/env python3
"""
backfill_historical.py
──────────────────────
Unified backfill, populate & historical import script.
Gộp chức năng của backfill_influx.py + ingest_historical_iceberg.py + initial_populate_influx.py.

Mode:
  --mode influx       : Detect + fill InfluxDB gaps (tắt máy → mất data)
  --mode iceberg      : Pull historical 1h klines from Binance → Iceberg
  --mode populate     : Force populate N ngày 1m candles cho tất cả symbols (lần đầu khởi động)
  --mode all          : Chạy influx + iceberg (không bao gồm populate)

Iceberg sub-modes:
  --iceberg-mode backfill     : Kéo từ 2017 → nay (chạy 1 lần đầu)
  --iceberg-mode incremental  : Chỉ kéo từ nến cuối đã lưu → nay (chạy hàng ngày)

Ví dụ:
  # Dagster scheduled:
  python backfill_historical.py --mode all --iceberg-mode incremental

  # Lần đầu khởi động — populate 90 ngày cho 400 symbols:
  python backfill_historical.py --mode populate --days 90

  # Chỉ fill gap InfluxDB:
  python backfill_historical.py --mode influx

  # Chỉ kéo lịch sử Iceberg:
  python backfill_historical.py --mode iceberg --iceberg-mode backfill --symbols BTCUSDT ETHUSDT
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

# ─── Shared Config ───────────────────────────────────────────────────────────

BINANCE_KLINES_URL     = "https://api.binance.com/api/v3/klines"
BINANCE_EXCHANGE_INFO  = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_EPOCH_MS       = int(datetime(2017, 7, 14, tzinfo=timezone.utc).timestamp() * 1000)

MAX_RETRIES    = 5
REQUEST_DELAY  = 0.12

# InfluxDB config
INFLUX_URL     = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN   = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG     = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET  = os.environ.get("INFLUX_BUCKET", "crypto")

# InfluxDB backfill params
KLINE_BATCH_INFLUX = 1000
MIN_GAP_SEC        = 300
MAX_BACKFILL_DAYS  = 7
MAX_WORKERS        = 5

# Iceberg / Spark config
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
MINIO_BUCKET     = "cryptoprice"

ICEBERG_CATALOG  = "iceberg_catalog"
ICEBERG_DB       = "crypto_lakehouse"
ICEBERG_TABLE    = "historical_hourly"
FULL_TABLE_NAME  = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE}"

KLINES_PER_REQ   = 1000
FLUSH_THRESHOLD  = 10_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backfill_historical")


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_usdt_symbols() -> list[str]:
    """Lấy tất cả USDT spot pairs đang TRADING từ Binance."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(BINANCE_EXCHANGE_INFO, timeout=15)
            resp.raise_for_status()
            symbols = [
                s["symbol"]
                for s in resp.json().get("symbols", [])
                if s["quoteAsset"] == "USDT"
                and s["status"] == "TRADING"
                and s.get("isSpotTradingAllowed", False)
            ]
            log.info("Found %d active USDT spot pairs.", len(symbols))
            return sorted(symbols)
        except Exception as e:
            log.warning("fetch_usdt_symbols attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    raise RuntimeError("Cannot fetch symbol list from Binance after retries.")


def fetch_klines(
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
    batch_limit: int = 1000,
) -> list[list]:
    """
    Lấy klines từ Binance REST API với phân trang tự động.
    Trả về list of [open_time, open, high, low, close, volume,
                     close_time, quote_volume, trades, taker_buy_vol, taker_buy_quote, ignore]
    """
    all_klines: list[list] = []
    current_start = start_ms
    step_ms = 60_000 if interval == "1m" else 3_600_000

    while current_start < end_ms:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    BINANCE_KLINES_URL,
                    params={
                        "symbol":    symbol,
                        "interval":  interval,
                        "startTime": current_start,
                        "endTime":   end_ms,
                        "limit":     batch_limit,
                    },
                    timeout=15,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning("[%s] Rate limited. Sleeping %ds.", symbol, retry_after)
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                batch = resp.json()
                break
            except Exception as e:
                log.warning("[%s] klines attempt %d failed: %s", symbol, attempt + 1, e)
                time.sleep(2 ** attempt)
        else:
            log.error("[%s] Giving up on window starting %d.", symbol, current_start)
            break

        if not batch:
            break

        all_klines.extend(batch)
        last_open_time = int(batch[-1][0])
        if last_open_time <= current_start:
            break
        current_start = last_open_time + step_ms
        time.sleep(REQUEST_DELAY)

    return all_klines


# ═══════════════════════════════════════════════════════════════════════════════
# INFLUXDB GAP BACKFILL
# ═══════════════════════════════════════════════════════════════════════════════

def wait_for_influx(client, retries: int = 20, delay: float = 5.0):
    """Đợi InfluxDB sẵn sàng."""
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
    """
    Tìm tất cả khoảng trống > MIN_GAP_SEC trong time series mỗi symbol.
    Dùng elapsed() để detect gap ở BẤT KỲ đâu trong chuỗi.
    Trả về {symbol: [(gap_start_ms, gap_end_ms), ...]}
    """
    from influxdb_client import InfluxDBClient

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
    """Chuyển Binance klines → InfluxDB Points (market_ticks schema)."""
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
    """Backfill 1 gap của 1 symbol trong InfluxDB. Trả về số points đã ghi."""
    gap_sec = (gap_end_ms - gap_start_ms) / 1000
    log.info(
        "[%s] Backfilling gap %.1f min: %s → %s",
        symbol, gap_sec / 60,
        datetime.fromtimestamp(gap_start_ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
        datetime.fromtimestamp(gap_end_ms   / 1000, tz=timezone.utc).strftime("%m-%d %H:%M"),
    )

    klines = fetch_klines(symbol, gap_start_ms, gap_end_ms, interval="1m", batch_limit=KLINE_BATCH_INFLUX)
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
    """Entry point cho InfluxDB gap backfill."""
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
# INFLUXDB INITIAL POPULATE (Force pull N ngày cho tất cả symbols)
# ═══════════════════════════════════════════════════════════════════════════════

def klines_to_candles_points(symbol: str, klines: list[list]) -> list:
    """
    Convert Binance klines to InfluxDB Points (candles measurement - matches Flink output).
    Schema: candles measurement with interval=1m tag.
    """
    from influxdb_client import Point, WritePrecision

    points = []
    for k in klines:
        try:
            open_ms     = int(k[0])
            open_price  = float(k[1])
            high        = float(k[2])
            low         = float(k[3])
            close_price = float(k[4])
            volume      = float(k[5])
            quote_vol   = float(k[7])
            trades      = int(k[8])

            point = (
                Point("candles")
                .tag("symbol",   symbol)
                .tag("exchange", "binance")
                .tag("interval", "1m")
                .field("open",         open_price)
                .field("high",         high)
                .field("low",          low)
                .field("close",        close_price)
                .field("volume",       volume)
                .field("quote_volume", quote_vol)
                .field("trade_count",  trades)
                .field("is_closed",    True)
                .time(open_ms, WritePrecision.MS)
            )
            points.append(point)
        except Exception as e:
            log.warning("[%s] skip kline row: %s", symbol, e)
    return points


def populate_symbol(symbol: str, start_ms: int, end_ms: int, write_api) -> int:
    """
    Force pull historical 1m candles for 1 symbol and write to InfluxDB (candles measurement).
    Không check gap, pull thẳng toàn bộ khoảng thời gian.
    Returns number of points written.
    """
    days = (end_ms - start_ms) / 1000 / 86400
    log.info(
        "[%s] Populate %.1f days (%s → %s)",
        symbol, days,
        datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%m-%d"),
        datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%m-%d"),
    )

    klines = fetch_klines(symbol, start_ms, end_ms, interval="1m", batch_limit=KLINE_BATCH_INFLUX)
    if not klines:
        log.warning("[%s] No klines returned.", symbol)
        return 0

    log.info("[%s] Fetched %d candles, converting...", symbol, len(klines))
    points = klines_to_candles_points(symbol, klines)

    if not points:
        return 0

    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        log.info("[%s] ✓ Written %d points to InfluxDB", symbol, len(points))
        return len(points)
    except Exception as e:
        log.error("[%s] Write error: %s", symbol, e)
        return 0


def run_initial_populate(days: int = 90, symbols_list: list[str] | None = None):
    """
    Entry point cho initial populate mode.
    Kéo N ngày 1m candles cho tất cả USDT symbols (hoặc danh sách cụ thể).
    """
    from influxdb_client import InfluxDBClient
    from influxdb_client.client.write_api import SYNCHRONOUS

    log.info("=== InfluxDB Initial Populate ===")
    log.info("Days: %d", days)

    # Fetch all 400 USDT symbols from Binance (matching producer)
    symbols = symbols_list or fetch_usdt_symbols()[:400]  # Cap at 400 like producer
    log.info("Symbols: %d (all active USDT pairs)", len(symbols))

    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = end_ms - (days * 24 * 3600 * 1000)

    log.info(
        "Time range: %s → %s",
        datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
        datetime.fromtimestamp(end_ms   / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
    )
    log.info(
        "⚠️  This will take ~2-3 hours for 400 symbols × 90 days (~52M candles)."
    )

    # Connect InfluxDB
    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    wait_for_influx(client)

    # Populate in parallel
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

    log.info(
        "Initial populate complete — %d/%d symbols, %d total points written.",
        total_success, len(symbols), total_points,
    )
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
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )


def ensure_iceberg_table(spark):
    from pyspark.sql.types import (
        DoubleType, LongType, StringType, StructField, StructType, TimestampType,
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            open_time               BIGINT,
            symbol                  STRING,
            open                    DOUBLE,
            high                    DOUBLE,
            low                     DOUBLE,
            close                   DOUBLE,
            volume                  DOUBLE,
            quote_volume            DOUBLE,
            trade_count             BIGINT,
            taker_buy_volume        DOUBLE,
            taker_buy_quote_volume  DOUBLE,
            event_time              TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (symbol, years(event_time))
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec'  = 'zstd',
            'write.metadata.compression-codec' = 'gzip',
            'write.target-file-size-bytes'     = '134217728'
        )
    """)
    log.info("Iceberg table %s is ready.", FULL_TABLE_NAME)


def get_last_open_time(spark, symbol: str) -> int:
    try:
        row = spark.sql(f"""
            SELECT MAX(open_time) AS max_ts
            FROM {FULL_TABLE_NAME}
            WHERE symbol = '{symbol}'
        """).first()
        if row and row["max_ts"]:
            return int(row["max_ts"]) + 3_600_000
    except Exception:
        pass
    return BINANCE_EPOCH_MS


def process_and_write_chunk(spark, rows: list, symbol: str, chunk_start_ms: int) -> int:
    from pyspark.sql.types import (
        DoubleType, LongType, StringType, StructField, StructType, TimestampType,
    )
    from pyspark.sql import functions as F

    if not rows:
        return 0

    kline_schema = StructType([
        StructField("open_time",              LongType(),   False),
        StructField("symbol",                 StringType(), False),
        StructField("open",                   DoubleType(), True),
        StructField("high",                   DoubleType(), True),
        StructField("low",                    DoubleType(), True),
        StructField("close",                  DoubleType(), True),
        StructField("volume",                 DoubleType(), True),
        StructField("quote_volume",           DoubleType(), True),
        StructField("trade_count",            LongType(),   True),
        StructField("taker_buy_volume",       DoubleType(), True),
        StructField("taker_buy_quote_volume", DoubleType(), True),
    ])

    df = (
        spark.createDataFrame(rows, schema=kline_schema)
        .withColumn("event_time", (F.col("open_time") / 1000).cast(TimestampType()))
    )
    df = df.sortWithinPartitions("symbol", "event_time")
    df.writeTo(FULL_TABLE_NAME).append()

    log.info("[%s] Wrote %d candles (chunk start=%s).", symbol, len(rows),
             datetime.fromtimestamp(chunk_start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d"))
    return len(rows)


def write_symbol_iceberg(spark, symbol: str, start_ms: int, end_ms: int) -> int:
    """Pull klines from Binance and write to Iceberg table. Returns total candles written."""
    total_written  = 0
    current_ms     = start_ms
    chunk_start_ms = start_ms
    chunk_buffer: list = []

    while current_ms < end_ms:
        klines = fetch_klines(symbol, current_ms, end_ms, interval="1h", batch_limit=KLINES_PER_REQ)
        if not klines:
            break

        for k in klines:
            chunk_buffer.append([
                int(k[0]),
                symbol,
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5]),
                float(k[7]),
                int(k[8]),
                float(k[9]),
                float(k[10]),
            ])

        if len(chunk_buffer) >= FLUSH_THRESHOLD:
            total_written += process_and_write_chunk(spark, chunk_buffer, symbol, chunk_start_ms)
            chunk_buffer.clear()
            chunk_start_ms = int(klines[-1][0]) + 3_600_000

        current_ms = int(klines[-1][0]) + 3_600_000
        time.sleep(REQUEST_DELAY)

    if chunk_buffer:
        total_written += process_and_write_chunk(spark, chunk_buffer, symbol, chunk_start_ms)

    if total_written == 0:
        log.info("[%s] No new candles.", symbol)
    return total_written


def run_iceberg_historical(iceberg_mode: str = "incremental", symbols_list: list[str] | None = None):
    """Entry point cho Iceberg historical import."""
    log.info("=== Iceberg Historical Import (mode=%s) ===", iceberg_mode)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ms = now_ms - (now_ms % 3_600_000)

    symbols = symbols_list or fetch_usdt_symbols()
    log.info("Symbols: %d | End: %s", len(symbols),
             datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_iceberg_table(spark)

    total_rows = 0
    total_syms = len(symbols)

    for idx, symbol in enumerate(symbols, 1):
        log.info("[%d/%d] Processing %s ...", idx, total_syms, symbol)
        if iceberg_mode == "backfill":
            start_ms = BINANCE_EPOCH_MS
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

    # Print table stats
    try:
        size_df = spark.sql(f"""
            SELECT
                COUNT(*)                AS total_rows,
                COUNT(DISTINCT symbol)  AS total_symbols,
                MIN(event_time)         AS earliest,
                MAX(event_time)         AS latest
            FROM {FULL_TABLE_NAME}
        """)
        size_df.show(truncate=False)
        files_df = spark.sql(f"SELECT SUM(file_size_in_bytes) AS bytes FROM {FULL_TABLE_NAME}.files")
        files_df.show()
    except Exception as e:
        log.warning("Could not compute table stats: %s", e)

    spark.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Unified backfill (gap fill) + populate (initial) + historical import (Iceberg).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--mode", choices=["influx", "iceberg", "populate", "all"], default="all",
        help=(
            "influx   = fill InfluxDB gaps\n"
            "iceberg  = historical klines → Iceberg\n"
            "populate = force pull N days for all symbols (lần đầu khởi động)\n"
            "all      = influx + iceberg (default, không bao gồm populate)"
        ),
    )
    parser.add_argument(
        "--iceberg-mode", choices=["backfill", "incremental"], default="incremental",
        help="backfill    = kéo từ 2017 → nay (chạy 1 lần đầu)\nincremental = từ nến cuối → nay (default)",
    )
    parser.add_argument(
        "--symbols", nargs="*", default=None,
        help="Danh sách symbols cụ thể (VD: BTCUSDT ETHUSDT). Mặc định: tất cả USDT pairs.",
    )
    parser.add_argument(
        "--days", type=int, default=90,
        help="Số ngày lịch sử muốn pull cho mode populate (default: 90 = 3 tháng)",
    )
    args = parser.parse_args()

    log.info("=== Backfill, Populate & Historical Import | mode=%s ===", args.mode)

    if args.mode == "populate":
        # Initial populate mode — force pull N days for all symbols
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
            run_iceberg_historical(
                iceberg_mode=args.iceberg_mode,
                symbols_list=args.symbols,
            )
        except Exception as e:
            log.error("Iceberg historical import failed: %s", e)

    log.info("=== All done ===")


if __name__ == "__main__":
    main()
