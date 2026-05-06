#!/usr/bin/env python3
"""
Spark Structured Streaming pipeline: Kafka → Iceberg tables.

Reads Avro-encoded messages from Kafka topics (ticker, trades, klines),
deserializes them, and writes to Iceberg tables partitioned by day.

Usage (Docker)::

    spark-submit /app/src/lakehouse/pipeline.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.avro.functions import from_avro

from common.config import (
    KAFKA_BOOTSTRAP,
    ICEBERG_TABLE_TICKER,
    ICEBERG_TABLE_TRADES,
    ICEBERG_TABLE_KLINES,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)

# ── Checkpoint locations ─────────────────────────────────────────────────────
CHECKPOINT_TICKER = "s3a://cryptoprice/checkpoints/crypto_ticker_v1"
CHECKPOINT_TRADES = "s3a://cryptoprice/checkpoints/crypto_trades_v1"
CHECKPOINT_KLINES = "s3a://cryptoprice/checkpoints/crypto_klines_v1"

# ── Load Avro schemas from canonical schema files ────────────────────────────
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "schemas")


def _load_avro_schema(filename: str) -> str:
    """Load an Avro schema from the schemas/ directory as a JSON string."""
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path) as f:
        return json.dumps(json.load(f))


TICKER_AVRO_SCHEMA = _load_avro_schema("ticker.avsc")
TRADES_AVRO_SCHEMA = _load_avro_schema("trade.avsc")
KLINES_AVRO_SCHEMA = _load_avro_schema("kline.avsc")


def build_spark() -> SparkSession:
    """Build a SparkSession configured for Iceberg + MinIO."""
    return (
        SparkSession.builder.appName("BinanceDualStreamToIceberg")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type",            "jdbc")
        .config("spark.sql.catalog.iceberg_catalog.uri",
                f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/iceberg_catalog")
        .config("spark.sql.catalog.iceberg_catalog.jdbc.user",       os.environ.get("POSTGRES_USER", ""))
        .config("spark.sql.catalog.iceberg_catalog.jdbc.password",   os.environ.get("POSTGRES_PASSWORD", ""))
        .config("spark.sql.catalog.iceberg_catalog.warehouse",
                "s3://cryptoprice/iceberg")
        .config("spark.sql.catalog.iceberg_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg_catalog.s3.endpoint",          MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg_catalog.s3.access-key-id",     MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg_catalog.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.iceberg_catalog.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg_catalog.client.region",        "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint",         MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",       MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",       MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.defaultCatalog", "iceberg_catalog")
        .config("spark.cores.max", "2")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, topic: str, avro_schema: str):
    """Read from Kafka and deserialize Confluent Avro format.

    Confluent wire format: [magic_byte:1][schema_id:4][avro_binary:N]
    We strip the first 5 bytes before passing to from_avro().
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 500_000)
        .load()
        .selectExpr("substring(value, 6, length(value)-5) as avro_value")
        .select(from_avro(col("avro_value"), avro_schema).alias("data"))
        .select("data.*")
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Ensure Iceberg database + tables exist ───────────────────────────────
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.crypto_lakehouse")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE_TICKER} (
            event_time          BIGINT,
            symbol              STRING,
            close               DOUBLE,
            bid                 DOUBLE,
            ask                 DOUBLE,
            h24_open            DOUBLE,
            h24_high            DOUBLE,
            h24_low             DOUBLE,
            h24_volume          DOUBLE,
            h24_quote_volume    DOUBLE,
            h24_price_change    DOUBLE,
            h24_price_change_pct DOUBLE,
            h24_trade_count     BIGINT,
            event_timestamp     TIMESTAMP,
            ingested_at         TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_timestamp))
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE_TRADES} (
            event_time      BIGINT,
            symbol          STRING,
            agg_trade_id    BIGINT,
            price           DOUBLE,
            quantity        DOUBLE,
            trade_time      BIGINT,
            is_buyer_maker  BOOLEAN,
            event_timestamp TIMESTAMP,
            trade_timestamp TIMESTAMP,
            ingested_at     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(trade_timestamp))
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE_KLINES} (
            event_time      BIGINT,
            symbol          STRING,
            kline_start     BIGINT,
            kline_close     BIGINT,
            interval        STRING,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          DOUBLE,
            quote_volume    DOUBLE,
            trade_count     BIGINT,
            is_closed       BOOLEAN,
            kline_timestamp TIMESTAMP,
            ingested_at     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(kline_timestamp))
    """)

    # ── Ticker stream ────────────────────────────────────────────────────────
    ticker_df = (
        read_kafka(spark, "crypto_ticker", TICKER_AVRO_SCHEMA)
        .filter(col("event_time").isNotNull())
        .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp"))
        .withColumn("ingested_at", current_timestamp())
        .withWatermark("event_timestamp", "1 minute")
        .dropDuplicates(["symbol", "event_timestamp"])
    )

    ticker_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", CHECKPOINT_TICKER) \
        .toTable(ICEBERG_TABLE_TICKER)

    # ── Trades stream ────────────────────────────────────────────────────────
    trades_df = (
        read_kafka(spark, "crypto_trades", TRADES_AVRO_SCHEMA)
        .filter(col("event_time").isNotNull())
        .withColumn("event_timestamp", (col("event_time") / 1000).cast("timestamp"))
        .withColumn("trade_timestamp",  (col("trade_time") / 1000).cast("timestamp"))
        .withColumn("ingested_at", current_timestamp())
    )

    trades_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", CHECKPOINT_TRADES) \
        .toTable(ICEBERG_TABLE_TRADES)

    # ── Klines stream ────────────────────────────────────────────────────────
    klines_df = (
        read_kafka(spark, "crypto_klines", KLINES_AVRO_SCHEMA)
        .filter(col("kline_start").isNotNull())
        .filter(col("interval") == "1m")
        .filter(col("is_closed") == True)
        .withColumn("kline_timestamp", (col("kline_start") / 1000).cast("timestamp"))
        .withColumn("ingested_at", current_timestamp())
        .withWatermark("kline_timestamp", "2 minutes")
        .dropDuplicates(["symbol", "kline_start"])
    )

    klines_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", CHECKPOINT_KLINES) \
        .toTable(ICEBERG_TABLE_KLINES)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
