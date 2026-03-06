#!/usr/bin/env python3
import json
import logging
import os
import time

import redis
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyflink.common import Configuration, Types
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction
from pyflink.datastream.state_backend import HashMapStateBackend
from pyflink.table import StreamTableEnvironment

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP",   "kafka:9092")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",    "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY",  "minioadmin")
REDIS_HOST       = os.environ.get("REDIS_HOST",        "keydb")
REDIS_PORT       = int(os.environ.get("REDIS_PORT",    "6379"))

INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


class KeyDBWriter(FlatMapFunction):
    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )

    def close(self):
        try:
            self._r.close()
        except Exception as e:
            log.error("[KeyDB] close error: %s", e)

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)
            symbol = value.get("symbol")
            if not symbol:
                return []
            event_time = int(value.get("event_time", 0))
            price      = float(value.get("close", 0))
            volume     = float(value.get("h24_volume", 0))
            cutoff     = event_time - 300_000
            pipe = self._r.pipeline()
            pipe.hset(
                f"ticker:latest:{symbol}",
                mapping={
                    "price":      price,
                    "bid":        float(value.get("bid", 0)),
                    "ask":        float(value.get("ask", 0)),
                    "volume":     volume,
                    "event_time": event_time,
                },
            )
            pipe.zadd(f"ticker:history:{symbol}", {f"{price}:{volume}": event_time})
            pipe.zremrangebyscore(f"ticker:history:{symbol}", 0, cutoff)
            pipe.execute()
        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[KeyDB] flat_map error | symbol=%s error=%s", s, e)
        return []


class InfluxDBWriter(FlatMapFunction):
    def __init__(self, batch_size: int = 500, flush_interval_sec: float = 1.0):
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec

    def open(self, runtime_context):
        self._client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._buffer = []
        self._last_flush_time = time.time()

    def _flush(self):
        if not self._buffer:
            return
        try:
            self._write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=self._buffer)
        except Exception as e:
            log.error("[InfluxDB] flush error (dropped %d points): %s", len(self._buffer), e)
        finally:
            self._buffer.clear()
            self._last_flush_time = time.time()

    def close(self):
        try:
            self._flush()
            self._client.close()
        except Exception as e:
            log.error("[InfluxDB] close error: %s", e)

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)
            point = (
                Point("market_ticks")
                .tag("symbol",   value["symbol"])
                .tag("exchange", "binance")
                .field("price",             float(value.get("close", 0)))
                .field("bid",               float(value.get("bid", 0)))
                .field("ask",               float(value.get("ask", 0)))
                .field("volume",            float(value.get("h24_volume", 0)))
                .field("quote_volume",      float(value.get("h24_quote_volume", 0)))
                .field("price_change_pct",  float(value.get("h24_price_change_pct", 0)))
                .field("trade_count",       int(value.get("h24_trade_count", 0)))
                .time(int(value["event_time"]), WritePrecision.MS)
            )
            self._buffer.append(point)
            if (
                len(self._buffer) >= self.batch_size
                or (time.time() - self._last_flush_time) >= self.flush_interval_sec
            ):
                self._flush()
        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[InfluxDB] flat_map error | symbol=%s error=%s", s, e)
        return []


class KeyDBKlineWriter(FlatMapFunction):
    """Writes kline candles to KeyDB: candle:latest:{symbol} + candle:history:{symbol}."""

    HISTORY_TTL_SEC = 86_400          # keep 24h of 1m candles

    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )

    def close(self):
        try:
            self._r.close()
        except Exception as e:
            log.error("[KeyDB/candles] close error: %s", e)

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)
            symbol      = value.get("symbol")
            if not symbol:
                return []
            kline_start = int(value["kline_start"])
            candle_json = json.dumps({
                "t": kline_start,
                "o": float(value["open"]),
                "h": float(value["high"]),
                "l": float(value["low"]),
                "c": float(value["close"]),
                "v": float(value["volume"]),
                "qv": float(value["quote_volume"]),
                "n": int(value["trade_count"]),
                "x": bool(value["is_closed"]),
            })
            cutoff = kline_start - self.HISTORY_TTL_SEC * 1000
            pipe = self._r.pipeline()
            pipe.hset(f"candle:latest:{symbol}", mapping={
                "open":         float(value["open"]),
                "high":         float(value["high"]),
                "low":          float(value["low"]),
                "close":        float(value["close"]),
                "volume":       float(value["volume"]),
                "quote_volume": float(value["quote_volume"]),
                "trade_count":  int(value["trade_count"]),
                "is_closed":    int(value["is_closed"]),
                "kline_start":  kline_start,
                "interval":     value.get("interval", "1m"),
            })
            pipe.zadd(f"candle:history:{symbol}", {candle_json: kline_start})
            pipe.zremrangebyscore(f"candle:history:{symbol}", 0, cutoff)
            pipe.execute()
        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[KeyDB/candles] flat_map error | symbol=%s error=%s", s, e)
        return []


class InfluxDBKlineWriter(FlatMapFunction):
    """Writes kline candles from crypto_klines topic to InfluxDB `candles` measurement."""

    def __init__(self, batch_size: int = 200, flush_interval_sec: float = 2.0):
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec

    def open(self, runtime_context):
        self._client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._buffer = []
        self._last_flush_time = time.time()

    def _flush(self):
        if not self._buffer:
            return
        try:
            self._write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=self._buffer)
        except Exception as e:
            log.error("[InfluxDB/candles] flush error (dropped %d points): %s", len(self._buffer), e)
        finally:
            self._buffer.clear()
            self._last_flush_time = time.time()

    def close(self):
        try:
            self._flush()
            self._client.close()
        except Exception as e:
            log.error("[InfluxDB/candles] close error: %s", e)

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)
            point = (
                Point("candles")
                .tag("symbol",   value["symbol"])
                .tag("exchange", "binance")
                .tag("interval", value.get("interval", "1m"))
                .field("open",         float(value["open"]))
                .field("high",         float(value["high"]))
                .field("low",          float(value["low"]))
                .field("close",        float(value["close"]))
                .field("volume",       float(value["volume"]))
                .field("quote_volume", float(value["quote_volume"]))
                .field("trade_count",  int(value["trade_count"]))
                .field("is_closed",    bool(value["is_closed"]))
                .time(int(value["kline_start"]), WritePrecision.MS)
            )
            self._buffer.append(point)
            if (
                len(self._buffer) >= self.batch_size
                or (time.time() - self._last_flush_time) >= self.flush_interval_sec
            ):
                self._flush()
        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[InfluxDB/candles] flat_map error | symbol=%s error=%s", s, e)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# SMA / EMA Indicator Writers — computes from closed kline candles
# ═══════════════════════════════════════════════════════════════════════════════

from collections import deque


class IndicatorWriter(FlatMapFunction):
    """
    Receives closed 1m klines → maintains rolling close‑price buffer per symbol
    → computes SMA20, SMA50, EMA12, EMA26 → writes to KeyDB + InfluxDB.

    indicator:latest:{symbol}  (hash)   — SMA20, SMA50, EMA12, EMA26, ts
    InfluxDB measurement 'indicators'   — same fields as tags/fields
    """

    SMA_PERIODS   = (20, 50)
    EMA_PERIODS   = (12, 26)
    MAX_HISTORY   = 60       # keep last 60 closes (enough for SMA50 + buffer)

    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )
        self._influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)
        # Per-symbol rolling state: { symbol: deque([close1, close2, ...]) }
        self._closes: dict[str, deque] = {}
        # Per-symbol EMA state: { symbol: { period: last_ema } }
        self._ema_state: dict[str, dict[int, float]] = {}
        # InfluxDB batch buffer
        self._buffer = []
        self._last_flush = time.time()

    def _flush_influx(self):
        if not self._buffer:
            return
        try:
            self._write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=self._buffer)
        except Exception as e:
            log.error("[Indicators/InfluxDB] flush error: %s", e)
        finally:
            self._buffer.clear()
            self._last_flush = time.time()

    def close(self):
        try:
            self._flush_influx()
            self._r.close()
            self._influx_client.close()
        except Exception as e:
            log.error("[Indicators] close error: %s", e)

    @staticmethod
    def _sma(prices, period):
        if len(prices) < period:
            return None
        return sum(list(prices)[-period:]) / period

    def _ema(self, symbol, close_price, period):
        key = (symbol, period)
        # Use symbol-period key stored in _ema_state
        sym_state = self._ema_state.setdefault(symbol, {})
        if period not in sym_state:
            # Initialize EMA as first close
            sym_state[period] = close_price
            return close_price
        k = 2.0 / (period + 1)
        prev = sym_state[period]
        new_ema = close_price * k + prev * (1 - k)
        sym_state[period] = new_ema
        return new_ema

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)

            # Only process CLOSED candles
            if not value.get("is_closed"):
                return []

            symbol   = value.get("symbol")
            if not symbol:
                return []

            close_price = float(value["close"])
            kline_start = int(value["kline_start"])

            # Update rolling window
            if symbol not in self._closes:
                self._closes[symbol] = deque(maxlen=self.MAX_HISTORY)
            self._closes[symbol].append(close_price)

            prices = self._closes[symbol]

            # Calculate SMAs
            sma20 = self._sma(prices, 20)
            sma50 = self._sma(prices, 50)

            # Calculate EMAs
            ema12 = self._ema(symbol, close_price, 12)
            ema26 = self._ema(symbol, close_price, 26)

            # Write to KeyDB
            mapping = {"timestamp": kline_start}
            if sma20 is not None:
                mapping["sma20"] = round(sma20, 8)
            if sma50 is not None:
                mapping["sma50"] = round(sma50, 8)
            mapping["ema12"] = round(ema12, 8)
            mapping["ema26"] = round(ema26, 8)

            self._r.hset(f"indicator:latest:{symbol}", mapping=mapping)

            # Write to InfluxDB
            point = Point("indicators").tag("symbol", symbol).tag("exchange", "binance")
            if sma20 is not None:
                point = point.field("sma20", round(sma20, 8))
            if sma50 is not None:
                point = point.field("sma50", round(sma50, 8))
            point = (
                point
                .field("ema12", round(ema12, 8))
                .field("ema26", round(ema26, 8))
                .field("close", close_price)
                .time(kline_start, WritePrecision.MS)
            )
            self._buffer.append(point)
            if len(self._buffer) >= 200 or (time.time() - self._last_flush) >= 5.0:
                self._flush_influx()

        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[Indicators] flat_map error | symbol=%s error=%s", s, e)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# Order Book Depth Writer — writes to KeyDB
# ═══════════════════════════════════════════════════════════════════════════════

class DepthWriter(FlatMapFunction):
    """
    Receives partial order-book depth snapshots from crypto_depth topic.
    Writes to KeyDB:
        orderbook:{symbol}  (hash) — bids (JSON), asks (JSON), last_update_id, ts
    """

    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )

    def close(self):
        try:
            self._r.close()
        except Exception as e:
            log.error("[Depth] close error: %s", e)

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)

            symbol = value.get("symbol")
            if not symbol:
                return []

            bids = value.get("bids", [])
            asks = value.get("asks", [])

            # Store compact representation in KeyDB hash
            pipe = self._r.pipeline()
            pipe.hset(f"orderbook:{symbol}", mapping={
                "bids":           json.dumps(bids),
                "asks":           json.dumps(asks),
                "last_update_id": int(value.get("last_update_id", 0)),
                "event_time":     int(value.get("event_time", 0)),
                "bid_depth":      len(bids),
                "ask_depth":      len(asks),
                "best_bid":       float(bids[0][0]) if bids else 0,
                "best_ask":       float(asks[0][0]) if asks else 0,
                "spread":         round(float(asks[0][0]) - float(bids[0][0]), 8) if bids and asks else 0,
            })
            pipe.expire(f"orderbook:{symbol}", 60)  # TTL 60s — refresh every 100ms
            pipe.execute()

        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[Depth] flat_map error | symbol=%s error=%s", s, e)
        return []


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_state_backend(HashMapStateBackend())

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
    env.enable_checkpointing(30_000)
    chk = env.get_checkpoint_config()
    chk.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    chk.enable_unaligned_checkpoints()
    chk.set_min_pause_between_checkpoints(10_000)
    chk.set_checkpoint_timeout(60_000)
    t_env = StreamTableEnvironment.create(env)

    t_env.execute_sql(f"""
        CREATE TABLE kafka_ticker (
            event_time             BIGINT,
            symbol                 STRING,
            `close`                DOUBLE,
            bid                    DOUBLE,
            ask                    DOUBLE,
            `24h_volume`           DOUBLE,
            `24h_quote_volume`     DOUBLE,
            `24h_price_change_pct` DOUBLE,
            `24h_trade_count`      BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'crypto_ticker',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink_crypto_ticker_v1',
            'scan.startup.mode'            = 'latest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true',
            'json.fail-on-missing-field'   = 'false'
        )
    """)

    table = t_env.sql_query("""
        SELECT
            event_time,
            symbol,
            `close`,
            bid,
            ask,
            `24h_volume`           AS h24_volume,
            `24h_quote_volume`     AS h24_quote_volume,
            `24h_price_change_pct` AS h24_price_change_pct,
            `24h_trade_count`      AS h24_trade_count
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
    ds_dict.flat_map(KeyDBWriter(),    output_type=Types.STRING()).name("Write_To_KeyDB").print()
    ds_dict.flat_map(InfluxDBWriter(), output_type=Types.STRING()).name("Write_To_InfluxDB").print()

    # ── Kline (candle) pipeline: crypto_klines → InfluxDB candles ──────────────
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
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true',
            'json.fail-on-missing-field'   = 'false'
        )
    """)

    kline_table = t_env.sql_query("""
        SELECT
            event_time,
            symbol,
            kline_start,
            kline_close,
            `interval`,
            `open`,
            high,
            low,
            `close`,
            volume,
            quote_volume,
            trade_count,
            is_closed
        FROM kafka_klines
    """)
    ds_kline_row = t_env.to_data_stream(kline_table)

    def kline_row_to_dict(row):
        return json.dumps({
            "event_time":   row[0],
            "symbol":       row[1],
            "kline_start":  row[2],
            "kline_close":  row[3],
            "interval":     row[4],
            "open":         row[5],
            "high":         row[6],
            "low":          row[7],
            "close":        row[8],
            "volume":       row[9],
            "quote_volume": row[10],
            "trade_count":  row[11],
            "is_closed":    row[12],
        })

    ds_kline_dict = ds_kline_row.map(kline_row_to_dict, output_type=Types.STRING())
    ds_kline_dict.flat_map(
        KeyDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_Klines_To_KeyDB").print()
    ds_kline_dict.flat_map(
        InfluxDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_Klines_To_InfluxDB").print()

    # ── Indicators pipeline: closed klines → SMA/EMA → KeyDB + InfluxDB ───────
    ds_kline_dict.flat_map(
        IndicatorWriter(), output_type=Types.STRING()
    ).name("Write_Indicators").print()

    # ── Order-book depth pipeline: crypto_depth → KeyDB ────────────────────────
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
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true',
            'json.fail-on-missing-field'   = 'false'
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
    ).name("Write_Depth_To_KeyDB").print()

    env.execute("Crypto_MultiStream_Kafka_to_KeyDB_InfluxDB")


if __name__ == "__main__":
    run()
