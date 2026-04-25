#!/usr/bin/env python3
"""
Retention maintenance for 1m candle stores.

- InfluxDB: deletes 1m candles older than RETENTION_1M_DAYS
- Iceberg: deletes 1m candles older than RETENTION_1M_DAYS

Higher intervals (5m, 15m, 1h, ...) are derived on query from 1m data,
so this job only enforces retention and does not pre-aggregate fixed frames.

Usage:
    python aggregate.py --mode influx
    python aggregate.py --mode iceberg
    python aggregate.py --mode all
    python aggregate.py --mode all --retention-days 90
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.config import (
    ICEBERG_TABLE_KLINES,
    INFLUX_BUCKET,
    INFLUX_ORG,
    INFLUX_TOKEN,
    INFLUX_URL,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RETENTION_1M_DAYS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("aggregate_candles")


def cleanup_influx_1m(retention_days: int):
    """Delete 1m candles older than retention_days from InfluxDB."""
    from influxdb_client import InfluxDBClient

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    delete_api = client.delete_api()

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    stop_delete = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info("Influx cleanup: deleting candles interval=1m before %s", stop_delete)
    delete_api.delete(
        start="1970-01-01T00:00:00Z",
        stop=stop_delete,
        predicate='_measurement="candles" AND interval="1m"',
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG,
    )
    client.close()
    log.info("Influx cleanup complete.")


def cleanup_iceberg_1m(retention_days: int):
    """Delete 1m candles older than retention_days from Iceberg."""
    from pyspark.sql import SparkSession

    cutoff_ts = datetime.now(timezone.utc) - timedelta(days=retention_days)
    cutoff_ms = int(cutoff_ts.timestamp() * 1000)

    log.info("Iceberg cleanup: deleting interval=1m before %s", cutoff_ts.strftime("%Y-%m-%d %H:%M:%S"))

    spark = (
        SparkSession.builder.appName("Cleanup1mRetention")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "jdbc")
        .config("spark.sql.catalog.iceberg_catalog.uri",
                f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg_catalog.jdbc.user", os.environ.get("POSTGRES_USER", ""))
        .config("spark.sql.catalog.iceberg_catalog.jdbc.password", os.environ.get("POSTGRES_PASSWORD", ""))
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3://cryptoprice/iceberg")
        .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg_catalog.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg_catalog.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg_catalog.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg_catalog.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg_catalog.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.defaultCatalog", "iceberg_catalog")
        .getOrCreate()
    )

    spark.sql(f"""
        DELETE FROM {ICEBERG_TABLE_KLINES}
        WHERE interval = '1m'
          AND is_closed = true
          AND kline_start < {cutoff_ms}
    """)

    spark.stop()
    log.info("Iceberg cleanup complete.")


def main():
    parser = argparse.ArgumentParser(description="1m candle retention maintenance")
    parser.add_argument("--mode", choices=["influx", "iceberg", "all"], default="all",
                        help="Which backend to maintain (default: all)")
    parser.add_argument("--retention-days", type=int, default=RETENTION_1M_DAYS,
                        help=f"Retention for 1m candles (default: {RETENTION_1M_DAYS})")
    args = parser.parse_args()

    retention_days = max(args.retention_days, 1)
    log.info("=== Retention maintenance | mode=%s | retention=%d days ===", args.mode, retention_days)

    if args.mode in ("influx", "all"):
        try:
            cleanup_influx_1m(retention_days)
        except Exception as e:
            log.error("Influx cleanup failed: %s", e)

    if args.mode in ("iceberg", "all"):
        try:
            cleanup_iceberg_1m(retention_days)
        except Exception as e:
            log.error("Iceberg cleanup failed: %s", e)

    log.info("=== Maintenance complete ===")


if __name__ == "__main__":
    main()
