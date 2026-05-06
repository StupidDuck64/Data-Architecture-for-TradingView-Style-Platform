"""
Centralized environment configuration for data processing services.

All environment variables are read once at import time.  Flink writer
modules intentionally keep their own ``os.environ.get()`` calls for
serialization safety — this module serves the producer, batch jobs,
and lakehouse pipeline.
"""

import os
from datetime import datetime, timezone

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092")

KAFKA_TOPIC_TICKER = "crypto_ticker"
KAFKA_TOPIC_TRADES = "crypto_trades"
KAFKA_TOPIC_KLINES = "crypto_klines"
KAFKA_TOPIC_DEPTH  = "crypto_depth"

# ── Schema Registry ──────────────────────────────────────────────────────────
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL", "http://schema-registry:8080/apis/ccompat/v7"
)

# ── Redis / KeyDB ────────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "keydb")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# ── InfluxDB ─────────────────────────────────────────────────────────────────
INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

# ── MinIO / S3 ───────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")

# ── Iceberg ──────────────────────────────────────────────────────────────────
ICEBERG_CATALOG      = "iceberg_catalog"
ICEBERG_DB           = "crypto_lakehouse"
ICEBERG_TABLE_TICKER = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_ticker"
ICEBERG_TABLE_TRADES = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_trades"
ICEBERG_TABLE_KLINES = f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_klines"

# ── Producer tuning ──────────────────────────────────────────────────────────
KLINE_INTERVAL_WS      = os.environ.get("KLINE_INTERVAL", "1m")
DEPTH_LEVEL            = os.environ.get("DEPTH_LEVEL", "20")
DEPTH_UPDATE_MS        = os.environ.get("DEPTH_UPDATE_MS", "100")
SYMBOLS_PER_CONNECTION = int(os.environ.get("SYMBOLS_PER_CONNECTION", "25"))
SYMBOLS_PER_DEPTH_CONN = int(os.environ.get("SYMBOLS_PER_DEPTH_CONN", "15"))
MAX_SYMBOLS            = int(os.environ.get("MAX_SYMBOLS", "200"))
TICKER_HEARTBEAT_SEC   = 5.0

# ── Backfill ─────────────────────────────────────────────────────────────────
MAX_RETRIES          = 5
REQUEST_DELAY        = 0.12
KLINE_BATCH_INFLUX   = 1000
KLINES_PER_REQ       = 1000
MIN_GAP_SEC          = 300
MAX_BACKFILL_DAYS    = 7
MAX_WORKERS          = 5
FLUSH_THRESHOLD      = int(os.environ.get("BACKFILL_FLUSH_THRESHOLD", "10000"))
RETENTION_1M_DAYS    = int(os.environ.get("RETENTION_1M_DAYS", "90"))

# ── Spark ────────────────────────────────────────────────────────────────────
BACKFILL_SPARK_CORES_MAX          = os.environ.get("BACKFILL_SPARK_CORES_MAX", "2")
BACKFILL_SPARK_SHUFFLE_PARTITIONS = os.environ.get("BACKFILL_SPARK_SHUFFLE_PARTITIONS", "8")

# ── Iceberg maintenance ─────────────────────────────────────────────────────
SNAPSHOT_RETENTION_HOURS     = 48
ORPHAN_FILE_RETENTION_HOURS  = 72
TARGET_FILE_SIZE_BYTES       = 128 * 1024 * 1024

ICEBERG_TABLES = [
    f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_ticker",
    f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_trades",
    f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_klines",
    f"{ICEBERG_CATALOG}.{ICEBERG_DB}.coin_klines_hourly",
]
