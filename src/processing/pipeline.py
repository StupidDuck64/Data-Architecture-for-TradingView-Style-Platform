#!/usr/bin/env python3
"""
Flink stream processing pipeline.

Consumes Kafka topics (ticker, klines, depth) via Avro-Confluent format
and writes to KeyDB (hot cache) and InfluxDB (time-series analytics).

Usage (Docker)::

    flink run -d -py /app/src/processing/pipeline.py
"""

import json
import logging
import os
import sys

# ── Ensure src/ and processing/ are on Python path ───────────────────────────
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from pyflink.common import Configuration, Types
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# Import from writers package (uploaded via --pyFiles)
from writers.keydb_ticker import KeyDBWriter
from writers.keydb_kline import KeyDBKlineWriter
from writers.keydb_depth import DepthWriter
from writers.influxdb_ticker import InfluxDBWriter
from writers.influxdb_kline import InfluxDBKlineWriter
from writers.indicators import IndicatorWriter
from writers.kline_aggregator import KlineWindowAggregator

# ── Config (read at module level for Flink compatibility) ────────────────────
KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP",   "kafka-1:9092,kafka-2:9092,kafka-3:9092")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",    "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY",  "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY",  "")
FLINK_PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "12"))
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL",
    "http://schema-registry:8080/apis/ccompat/v7",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    from pyflink.datastream.state_backend import HashMapStateBackend
    env.set_state_backend(HashMapStateBackend())
    env.set_parallelism(FLINK_PARALLELISM)

    s3_config = Configuration()
    s3_config.set_string("s3.endpoint",          MINIO_ENDPOINT)
    s3_config.set_string("s3.access-key",        MINIO_ACCESS_KEY)
    s3_config.set_string("s3.secret-key",        MINIO_SECRET_KEY)
    s3_config.set_string("s3.path.style.access", "true")
    s3_config.set_string("fs.s3a.endpoint",           MINIO_ENDPOINT)
    s3_config.set_string("fs.s3a.access.key",         MINIO_ACCESS_KEY)
    s3_config.set_string("fs.s3a.secret.key",         MINIO_SECRET_KEY)
    s3_config.set_string("fs.s3a.path.style.access",  "true")
    s3_config.set_string("fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
    env.configure(s3_config)

    env.get_checkpoint_config().set_checkpoint_storage_dir(
        "file:///tmp/flink-checkpoints"
    )
    env.enable_checkpointing(120_000)
    chk = env.get_checkpoint_config()
    chk.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    chk.enable_unaligned_checkpoints()
    chk.set_min_pause_between_checkpoints(30_000)
    chk.set_checkpoint_timeout(120_000)
    t_env = StreamTableEnvironment.create(env)

    # ═════════════════════════════════════════════════════════════════════════
    # Ticker pipeline: crypto_ticker → KeyDB + InfluxDB
    # ═════════════════════════════════════════════════════════════════════════

    t_env.execute_sql(f"""
        CREATE TABLE kafka_ticker (
            event_time             BIGINT,
            symbol                 STRING,
            `close`                DOUBLE,
            bid                    DOUBLE,
            ask                    DOUBLE,
            h24_volume             DOUBLE,
            h24_quote_volume       DOUBLE,
            h24_price_change_pct   DOUBLE,
            h24_trade_count        BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'crypto_ticker',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink_crypto_ticker_v1',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY_URL}'
        )
    """)

    table = t_env.sql_query("""
        SELECT
            event_time, symbol, `close`, bid, ask,
            h24_volume, h24_quote_volume, h24_price_change_pct, h24_trade_count
        FROM kafka_ticker
    """)
    ds_row = t_env.to_data_stream(table)

    def row_to_dict(row):
        return json.dumps({
            "event_time":           row[0],
            "symbol":               row[1],
            "close":                row[2],
            "bid":                  row[3],
            "ask":                  row[4],
            "h24_volume":           row[5],
            "h24_quote_volume":     row[6],
            "h24_price_change_pct": row[7],
            "h24_trade_count":      row[8],
        })

    ds_dict = ds_row.map(row_to_dict, output_type=Types.STRING())
    ds_dict.flat_map(KeyDBWriter(), output_type=Types.STRING()).name("Write_To_KeyDB")
    ds_dict.flat_map(InfluxDBWriter(), output_type=Types.STRING()).name("Write_To_InfluxDB")

    # ═════════════════════════════════════════════════════════════════════════
    # Kline pipeline: crypto_klines → KeyDB + InfluxDB + 1s→1m aggregation
    # ═════════════════════════════════════════════════════════════════════════

    t_env.execute_sql(f"""
        CREATE TABLE kafka_klines (
            event_time   BIGINT,
            symbol       STRING,
            kline_start  BIGINT,
            kline_close  BIGINT,
            `interval`   STRING,
            `open`       DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            `close`      DOUBLE,
            volume       DOUBLE,
            quote_volume DOUBLE,
            trade_count  BIGINT,
            is_closed    BOOLEAN
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'crypto_klines',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink_crypto_klines_v1',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY_URL}'
        )
    """)

    kline_table = t_env.sql_query("""
        SELECT
            event_time, symbol, kline_start, kline_close, `interval`,
            `open`, high, low, `close`, volume, quote_volume, trade_count, is_closed
        FROM kafka_klines
    """)
    ds_kline_row = t_env.to_data_stream(kline_table)

    def kline_row_to_dict(row):
        return json.dumps({
            "event_time":   row[0],  "symbol":       row[1],
            "kline_start":  row[2],  "kline_close":  row[3],
            "interval":     row[4],  "open":         row[5],
            "high":         row[6],  "low":          row[7],
            "close":        row[8],  "volume":       row[9],
            "quote_volume": row[10], "trade_count":  row[11],
            "is_closed":    row[12],
        })

    ds_kline_dict = ds_kline_row.map(kline_row_to_dict, output_type=Types.STRING())

    # Branch 1: write raw 1s candles to KeyDB + InfluxDB
    ds_kline_dict.flat_map(
        KeyDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1s_Klines_To_KeyDB")
    ds_kline_dict.flat_map(
        InfluxDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1s_Klines_To_InfluxDB")

    # Branch 2: in-flight 1s→1m aggregation (dedup + gap-fill)
    ds_1m_candles = (
        ds_kline_dict
        .key_by(lambda v: json.loads(v)["symbol"])
        .process(KlineWindowAggregator(), output_type=Types.STRING())
    )
    ds_1m_candles.flat_map(
        KeyDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1m_Klines_To_KeyDB")
    ds_1m_candles.flat_map(
        InfluxDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1m_Klines_To_InfluxDB")

    # Indicators pipeline: closed 1m klines → SMA/EMA → KeyDB + InfluxDB
    ds_1m_candles.flat_map(
        IndicatorWriter(), output_type=Types.STRING()
    ).name("Write_Indicators")

    # ═════════════════════════════════════════════════════════════════════════
    # Depth pipeline: crypto_depth → KeyDB
    # ═════════════════════════════════════════════════════════════════════════

    t_env.execute_sql(f"""
        CREATE TABLE kafka_depth (
            event_time     BIGINT,
            symbol         STRING,
            last_update_id BIGINT,
            bids           STRING,
            asks           STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'crypto_depth',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink_crypto_depth_v1',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY_URL}'
        )
    """)

    depth_table = t_env.sql_query("""
        SELECT event_time, symbol, last_update_id, bids, asks
        FROM kafka_depth
    """)
    ds_depth_row = t_env.to_data_stream(depth_table)

    def depth_row_to_dict(row):
        return json.dumps({
            "event_time":     row[0],
            "symbol":         row[1],
            "last_update_id": row[2],
            "bids":           json.loads(row[3]) if isinstance(row[3], str) else row[3],
            "asks":           json.loads(row[4]) if isinstance(row[4], str) else row[4],
        })

    ds_depth_dict = ds_depth_row.map(depth_row_to_dict, output_type=Types.STRING())
    ds_depth_dict.flat_map(
        DepthWriter(), output_type=Types.STRING()
    ).name("Write_Depth_To_KeyDB")

    env.execute("Crypto_MultiStream_Kafka_to_KeyDB_InfluxDB")


if __name__ == "__main__":
    run()
