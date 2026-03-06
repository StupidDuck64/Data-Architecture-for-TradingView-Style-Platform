#!/usr/bin/env python3
"""
aggregate_candles.py
────────────────────
Gộp nến 1m → 1h để giảm dữ liệu phình.
Chạy trên cả InfluxDB (real-time layer) và Iceberg (data lake).

Workflow:
  1. InfluxDB: đọc candles interval=1m cũ hơn RETENTION_1M_DAYS ngày,
     aggregate OHLCV theo giờ, ghi lại interval=1h, xoá 1m cũ.
  2. Iceberg : Spark SQL aggregate coin_klines → coin_klines_hourly,
     xoá record 1m cũ khỏi coin_klines.

Chạy qua Dagster schedule hoặc manual:
    python aggregate_candles.py [--mode influx|iceberg|all] [--retention-days 7]
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone

# ─── Config ──────────────────────────────────────────────────────────────────

INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

RETENTION_1M_DAYS = int(os.environ.get("RETENTION_1M_DAYS", "7"))

ICEBERG_KLINES         = "iceberg_catalog.crypto_lakehouse.coin_klines"
ICEBERG_KLINES_HOURLY  = "iceberg_catalog.crypto_lakehouse.coin_klines_hourly"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("aggregate_candles")


# ═══════════════════════════════════════════════════════════════════════════════
# INFLUXDB AGGREGATION
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate_influx():
    """
    1. Đọc candles 1m cũ hơn RETENTION_1M_DAYS
    2. Aggregate open(first), high(max), low(min), close(last), volume(sum) theo 1h
    3. Ghi lại candles interval=1h
    4. Xoá 1m candles cũ
    """
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    from influxdb_client.client.delete_api import DeleteApi

    client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    delete_api = client.delete_api()

    cutoff_sec = RETENTION_1M_DAYS * 24 * 3600
    log.info("InfluxDB aggregation: 1m→1h for candles older than %d days", RETENTION_1M_DAYS)

    # ── Step 1: Lấy danh sách symbols có data 1m cũ ─────────────────────────
    flux_symbols = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -90d, stop: -{cutoff_sec}s)
  |> filter(fn: (r) => r._measurement == "candles" and r.interval == "1m")
  |> keep(columns: ["symbol"])
  |> distinct(column: "symbol")
"""
    tables = query_api.query(flux_symbols, org=INFLUX_ORG)
    symbols = []
    for table in tables:
        for record in table.records:
            s = record.values.get("symbol") or record.get_value()
            if s and s not in symbols:
                symbols.append(s)

    if not symbols:
        log.info("No 1m candles older than %d days — nothing to aggregate.", RETENTION_1M_DAYS)
        client.close()
        return

    log.info("Found %d symbol(s) with old 1m data to aggregate.", len(symbols))

    total_written = 0
    total_deleted = 0

    for idx, symbol in enumerate(symbols, 1):
        log.info("[%d/%d] Aggregating %s ...", idx, len(symbols), symbol)

        # ── Step 2: Aggregate OHLCV theo giờ ────────────────────────────────
        # Flux: dùng 4 query riêng cho open(first), high(max), low(min), close(last)
        # merge bằng cách lấy từng field rồi combine theo _time
        flux_agg = f"""
import "experimental"

data = from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -90d, stop: -{cutoff_sec}s)
  |> filter(fn: (r) => r._measurement == "candles"
      and r.interval == "1m"
      and r.symbol == "{symbol}")

open_h = data
  |> filter(fn: (r) => r._field == "open")
  |> aggregateWindow(every: 1h, fn: first, createEmpty: false)
  |> set(key: "_field", value: "open")

high_h = data
  |> filter(fn: (r) => r._field == "high")
  |> aggregateWindow(every: 1h, fn: max, createEmpty: false)
  |> set(key: "_field", value: "high")

low_h = data
  |> filter(fn: (r) => r._field == "low")
  |> aggregateWindow(every: 1h, fn: min, createEmpty: false)
  |> set(key: "_field", value: "low")

close_h = data
  |> filter(fn: (r) => r._field == "close")
  |> aggregateWindow(every: 1h, fn: last, createEmpty: false)
  |> set(key: "_field", value: "close")

volume_h = data
  |> filter(fn: (r) => r._field == "volume")
  |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
  |> set(key: "_field", value: "volume")

quote_vol_h = data
  |> filter(fn: (r) => r._field == "quote_volume")
  |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
  |> set(key: "_field", value: "quote_volume")

trade_count_h = data
  |> filter(fn: (r) => r._field == "trade_count")
  |> aggregateWindow(every: 1h, fn: sum, createEmpty: false)
  |> set(key: "_field", value: "trade_count")

union(tables: [open_h, high_h, low_h, close_h, volume_h, quote_vol_h, trade_count_h])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "open", "high", "low", "close", "volume", "quote_volume", "trade_count"])
  |> sort(columns: ["_time"])
"""
        tables_result = query_api.query(flux_agg, org=INFLUX_ORG)
        points = []
        for table in tables_result:
            for record in table.records:
                try:
                    ts = record.get_time()
                    ts_ms = int(ts.timestamp() * 1000)
                    point = (
                        Point("candles")
                        .tag("symbol",   symbol)
                        .tag("exchange", "binance")
                        .tag("interval", "1h")
                        .field("open",         float(record.values.get("open", 0)))
                        .field("high",         float(record.values.get("high", 0)))
                        .field("low",          float(record.values.get("low", 0)))
                        .field("close",        float(record.values.get("close", 0)))
                        .field("volume",       float(record.values.get("volume", 0)))
                        .field("quote_volume", float(record.values.get("quote_volume", 0)))
                        .field("trade_count",  int(record.values.get("trade_count", 0)))
                        .field("is_closed",    True)
                        .time(ts_ms, WritePrecision.MS)
                    )
                    points.append(point)
                except Exception as e:
                    log.warning("[%s] skip agg row: %s", symbol, e)

        if points:
            # Ghi batch 500
            for i in range(0, len(points), 500):
                batch = points[i : i + 500]
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=batch)
            log.info("[%s] Wrote %d hourly candles.", symbol, len(points))
            total_written += len(points)

        # ── Step 3: Xoá 1m candles cũ ──────────────────────────────────────
        cutoff_dt = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        from datetime import timedelta
        cutoff_dt = cutoff_dt - timedelta(days=RETENTION_1M_DAYS)
        start_delete = "1970-01-01T00:00:00Z"
        stop_delete  = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=f'_measurement="candles" AND interval="1m" AND symbol="{symbol}"',
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
            )
            total_deleted += 1
            log.info("[%s] Deleted 1m candles before %s.", symbol, stop_delete)
        except Exception as e:
            log.error("[%s] Delete failed: %s", symbol, e)

    log.info(
        "InfluxDB done: %d hourly candles written, %d symbol(s) cleaned.",
        total_written, total_deleted,
    )
    client.close()


# ═══════════════════════════════════════════════════════════════════════════════
# ICEBERG AGGREGATION (Spark SQL)
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate_iceberg():
    """
    1. Tạo bảng coin_klines_hourly nếu chưa có
    2. INSERT hourly aggregation từ coin_klines (interval=1m)
       cho data cũ hơn RETENTION_1M_DAYS, chỉ lấy is_closed=true
    3. DELETE 1m data cũ khỏi coin_klines
    """
    from pyspark.sql import SparkSession

    log.info("Iceberg aggregation: 1m→1h for klines older than %d days", RETENTION_1M_DAYS)

    spark = (
        SparkSession.builder.appName("AggregateCandles_1m_to_1h")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "jdbc")
        .config("spark.sql.catalog.iceberg_catalog.uri",
                f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg_catalog.jdbc.user",     "iceberg")
        .config("spark.sql.catalog.iceberg_catalog.jdbc.password", "iceberg123")
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Tạo bảng hourly nếu chưa có ────────────────────────────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_KLINES_HOURLY} (
            symbol          STRING,
            kline_start     BIGINT      COMMENT 'hour start epoch ms',
            kline_close     BIGINT      COMMENT 'hour end epoch ms',
            interval        STRING      COMMENT 'always 1h',
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          DOUBLE,
            quote_volume    DOUBLE,
            trade_count     BIGINT,
            kline_timestamp TIMESTAMP,
            aggregated_at   TIMESTAMP
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
    log.info("Table %s ready.", ICEBERG_KLINES_HOURLY)

    # ── Step 2: Cutoff timestamp ────────────────────────────────────────────
    from datetime import timedelta
    cutoff_ts = datetime.now(timezone.utc) - timedelta(days=RETENTION_1M_DAYS)
    cutoff_str = cutoff_ts.strftime("%Y-%m-%d %H:%M:%S")
    cutoff_ms  = int(cutoff_ts.timestamp() * 1000)

    log.info("Cutoff: %s (klines before this → aggregate to 1h)", cutoff_str)

    # ── Step 3: Aggregate INSERT ────────────────────────────────────────────
    # floor kline_start tới giờ chẵn (kline_start / 3600000) * 3600000
    agg_sql = f"""
        INSERT INTO {ICEBERG_KLINES_HOURLY}
        SELECT
            symbol,
            (CAST(kline_start / 3600000 AS BIGINT) * 3600000)   AS kline_start,
            (CAST(kline_start / 3600000 AS BIGINT) * 3600000) + 3599999  AS kline_close,
            '1h'                                                 AS interval,
            FIRST_VALUE(open)                                    AS open,
            MAX(high)                                            AS high,
            MIN(low)                                             AS low,
            LAST_VALUE(close)                                    AS close,
            SUM(volume)                                          AS volume,
            SUM(quote_volume)                                    AS quote_volume,
            SUM(trade_count)                                     AS trade_count,
            CAST((CAST(kline_start / 3600000 AS BIGINT) * 3600000) / 1000 AS TIMESTAMP)
                                                                 AS kline_timestamp,
            current_timestamp()                                  AS aggregated_at
        FROM {ICEBERG_KLINES}
        WHERE interval = '1m'
          AND is_closed = true
          AND kline_start < {cutoff_ms}
          AND (symbol, CAST(kline_start / 3600000 AS BIGINT) * 3600000)
              NOT IN (
                  SELECT symbol, kline_start
                  FROM {ICEBERG_KLINES_HOURLY}
              )
        GROUP BY
            symbol,
            (CAST(kline_start / 3600000 AS BIGINT) * 3600000)
        ORDER BY symbol, kline_start
    """
    try:
        result = spark.sql(agg_sql)
        log.info("Hourly aggregation INSERT completed.")
    except Exception as e:
        log.error("Aggregation INSERT failed: %s", e)
        spark.stop()
        return

    # Đếm kết quả
    try:
        count_df = spark.sql(f"SELECT COUNT(*) AS cnt FROM {ICEBERG_KLINES_HOURLY}")
        cnt = count_df.first()["cnt"]
        log.info("Total hourly candles in table: %d", cnt)
    except Exception:
        pass

    # ── Step 4: Xoá 1m data cũ khỏi coin_klines ───────────────────────────
    delete_sql = f"""
        DELETE FROM {ICEBERG_KLINES}
        WHERE interval = '1m'
          AND is_closed = true
          AND kline_start < {cutoff_ms}
    """
    try:
        spark.sql(delete_sql)
        log.info("Deleted 1m klines older than %s from %s.", cutoff_str, ICEBERG_KLINES)
    except Exception as e:
        log.error("Delete 1m data failed: %s", e)

    spark.stop()
    log.info("Iceberg aggregation done.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Aggregate 1m candles → 1h candles")
    parser.add_argument(
        "--mode", choices=["influx", "iceberg", "all"], default="all",
        help="Which backend to aggregate (default: all)",
    )
    parser.add_argument(
        "--retention-days", type=int, default=None,
        help=f"Keep 1m data for this many days (default: {RETENTION_1M_DAYS} from env)",
    )
    args = parser.parse_args()

    global RETENTION_1M_DAYS
    if args.retention_days is not None:
        RETENTION_1M_DAYS = args.retention_days

    log.info("=== Candle Aggregation: 1m → 1h | retention=%d days | mode=%s ===",
             RETENTION_1M_DAYS, args.mode)

    if args.mode in ("influx", "all"):
        try:
            aggregate_influx()
        except Exception as e:
            log.error("InfluxDB aggregation failed: %s", e)

    if args.mode in ("iceberg", "all"):
        try:
            aggregate_iceberg()
        except Exception as e:
            log.error("Iceberg aggregation failed: %s", e)

    log.info("=== Aggregation complete ===")


if __name__ == "__main__":
    main()
