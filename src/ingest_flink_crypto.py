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
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY",  "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY",  "")
REDIS_HOST       = os.environ.get("REDIS_HOST",        "keydb")
REDIS_PORT       = int(os.environ.get("REDIS_PORT",    "6379"))

INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

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


class KeyDBWriter(FlatMapFunction):
    """Batch-buffered ticker writer to KeyDB.
    Accumulates ticker updates and flushes via a single pipeline.
    """
    BATCH_SIZE = 100
    FLUSH_INTERVAL = 0.5
    CLEANUP_EVERY = 60

    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )
        self._buffer: list[dict] = []
        self._last_flush = time.time()
        self._write_count: dict[str, int] = {}

    def close(self):
        try:
            self._flush()
            self._r.close()
        except Exception as e:
            log.error("[KeyDB] close error: %s", e)

    def _flush(self):
        if not self._buffer:
            return
        try:
            pipe = self._r.pipeline()
            for value in self._buffer:
                symbol = value["symbol"]
                event_time = value["event_time"]
                price = value["price"]
                volume = value["volume"]
                pipe.hset(
                    f"ticker:latest:{symbol}",
                    mapping={
                        "price":      price,
                        "bid":        value["bid"],
                        "ask":        value["ask"],
                        "volume":     volume,
                        "change24h":  value["change24h"],
                        "event_time": event_time,
                    },
                )
                pipe.zadd(f"ticker:history:{symbol}", {f"{price}:{volume}": event_time})

                count = self._write_count.get(symbol, 0) + 1
                self._write_count[symbol] = count
                if count % self.CLEANUP_EVERY == 0:
                    cutoff = event_time - 300_000
                    pipe.zremrangebyscore(f"ticker:history:{symbol}", 0, cutoff)
            pipe.execute()
        except Exception as e:
            log.error("[KeyDB] flush error (dropped %d records): %s",
                      len(self._buffer), e)
        finally:
            self._buffer.clear()
            self._last_flush = time.time()

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)
            symbol = value.get("symbol")
            if not symbol:
                return []
            self._buffer.append({
                "symbol":     symbol,
                "event_time": int(value.get("event_time", 0)),
                "price":      float(value.get("close", 0)),
                "bid":        float(value.get("bid", 0)),
                "ask":        float(value.get("ask", 0)),
                "volume":     float(value.get("h24_volume", 0)),
                "change24h":  float(value.get("h24_price_change_pct", 0)),
            })
            if (
                len(self._buffer) >= self.BATCH_SIZE
                or (time.time() - self._last_flush) >= self.FLUSH_INTERVAL
            ):
                self._flush()
        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[KeyDB] flat_map error | symbol=%s error=%s", s, e)
        return []


class InfluxDBWriter(FlatMapFunction):
    def __init__(self, batch_size: int = 1000, flush_interval_sec: float = 2.0):
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
    """Writes kline candles to KeyDB with interval-specific TTL:
    - candle:1s:{symbol} → TTL 2 hours (for 1-second candles)
    - candle:1m:{symbol} → TTL 7 days (for 1-minute candles)
    - candle:latest:{symbol} → latest candle info (only for 1m+, not 1s)

    Batch-buffered: accumulates records and flushes via a single Redis
    pipeline every BATCH_SIZE records or FLUSH_INTERVAL seconds.
    """

    TTL_1S = 7_200        # 2 hours for 1-second candles
    TTL_1M = 604_800      # 7 days for 1-minute candles
    CLEANUP_EVERY = 60    # run ZREMRANGEBYSCORE once every 60 writes / symbol
    BATCH_SIZE = 50       # flush every N records
    FLUSH_INTERVAL = 0.5  # flush every N seconds

    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )
        self._write_count: dict[str, int] = {}  # per-symbol counter
        self._buffer: list[dict] = []
        self._last_flush = time.time()

    def close(self):
        try:
            self._flush()
            self._r.close()
        except Exception as e:
            log.error("[KeyDB/candles] close error: %s", e)

    def _flush(self):
        if not self._buffer:
            return
        try:
            pipe = self._r.pipeline()
            for item in self._buffer:
                symbol = item["symbol"]
                interval = item["interval"]
                kline_start = item["kline_start"]
                candle_json = item["candle_json"]
                history_key = item["history_key"]
                ttl_sec = item["ttl_sec"]

                # Add to interval-specific sorted set
                pipe.zadd(history_key, {candle_json: kline_start})

                # Only update candle:latest for 1m+ (not raw 1s — we use ticker:latest)
                if interval != "1s":
                    pipe.hset(f"candle:latest:{symbol}", mapping=item["latest_mapping"])

                # Batch cleanup: only ZREMRANGEBYSCORE every N writes per symbol
                count = self._write_count.get(symbol, 0) + 1
                self._write_count[symbol] = count
                if count % self.CLEANUP_EVERY == 0:
                    cutoff = kline_start - (ttl_sec * 1000)
                    pipe.zremrangebyscore(history_key, 0, cutoff)
                    pipe.expire(history_key, ttl_sec)

            pipe.execute()
        except Exception as e:
            log.error("[KeyDB/candles] flush error (dropped %d records): %s",
                      len(self._buffer), e)
        finally:
            self._buffer.clear()
            self._last_flush = time.time()

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)
            symbol      = value.get("symbol")
            if not symbol:
                return []
            interval    = value.get("interval", "1m")
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
            
            # Determine TTL and key based on interval
            if interval == "1s":
                history_key = f"candle:1s:{symbol}"
                ttl_sec = self.TTL_1S
            else:  # 1m or other intervals
                history_key = f"candle:1m:{symbol}"
                ttl_sec = self.TTL_1M

            self._buffer.append({
                "symbol": symbol,
                "interval": interval,
                "kline_start": kline_start,
                "candle_json": candle_json,
                "history_key": history_key,
                "ttl_sec": ttl_sec,
                "latest_mapping": {
                    "open":         float(value["open"]),
                    "high":         float(value["high"]),
                    "low":          float(value["low"]),
                    "close":        float(value["close"]),
                    "volume":       float(value["volume"]),
                    "quote_volume": float(value["quote_volume"]),
                    "trade_count":  int(value["trade_count"]),
                    "is_closed":    int(value["is_closed"]),
                    "kline_start":  kline_start,
                    "interval":     interval,
                },
            })

            if (
                len(self._buffer) >= self.BATCH_SIZE
                or (time.time() - self._last_flush) >= self.FLUSH_INTERVAL
            ):
                self._flush()
        except Exception as e:
            s = value.get("symbol") if isinstance(value, dict) else "unknown"
            log.error("[KeyDB/candles] flat_map error | symbol=%s error=%s", s, e)
        return []


class InfluxDBKlineWriter(FlatMapFunction):
    """Writes kline candles from crypto_klines topic to InfluxDB `candles` measurement."""

    def __init__(self, batch_size: int = 500, flush_interval_sec: float = 3.0):
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
# In-flight 1s → 1m Window Aggregation  (dedup + gap-fill)
# ═══════════════════════════════════════════════════════════════════════════════

from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor


class KlineWindowAggregator(KeyedProcessFunction):
    """Aggregate 1s klines → 1m klines inside Flink state.

    Strategy
    --------
    1. Each 1s candle is stored in ``MapState<kline_start_ms, json>``
       (same key overwrites → **dedup**).
    2. When a candle from a *new* minute arrives the previous window
       is aggregated and emitted immediately.
    3. A processing-time **safety timer** fires 65 s after the window
       opens so the last window is never stuck (handles silence).
    4. Missing seconds are **forward-filled** from the previous
       candle's close price before aggregation.
    """

    WINDOW_MS = 60_000  # 1 minute

    def open(self, runtime_context):
        self._candles = runtime_context.get_map_state(
            MapStateDescriptor("candles_1s", Types.LONG(), Types.STRING())
        )
        self._window_start = runtime_context.get_state(
            ValueStateDescriptor("window_start", Types.LONG())
        )
        self._last_close = runtime_context.get_state(
            ValueStateDescriptor("last_close", Types.DOUBLE())
        )
        self._symbol = runtime_context.get_state(
            ValueStateDescriptor("symbol", Types.STRING())
        )

    # ── process each 1s candle ────────────────────────────────────────────

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        candle = json.loads(value) if isinstance(value, str) else value
        interval = candle.get("interval", "1s")
        if interval != "1s":
            return

        kline_start = int(candle["kline_start"])
        minute_start = (kline_start // self.WINDOW_MS) * self.WINDOW_MS

        # remember symbol for on_timer
        if self._symbol.value() is None:
            self._symbol.update(candle["symbol"])

        # upsert into state (dedup)
        self._candles.put(kline_start, json.dumps({
            "t":  kline_start,
            "o":  float(candle["open"]),
            "h":  float(candle["high"]),
            "l":  float(candle["low"]),
            "c":  float(candle["close"]),
            "v":  float(candle["volume"]),
            "qv": float(candle["quote_volume"]),
            "n":  int(candle["trade_count"]),
        }))
        self._last_close.update(float(candle["close"]))

        current_window = self._window_start.value()

        if current_window is None:
            # first candle ever for this key
            self._window_start.update(minute_start)
            self._register_safety_timer(ctx)

        elif minute_start > current_window:
            # new minute → aggregate & emit previous window
            result = self._aggregate(current_window)
            if result:
                yield result

            # start new window & safety timer
            self._window_start.update(minute_start)
            self._register_safety_timer(ctx)

    # ── safety timer ──────────────────────────────────────────────────────

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        current_window = self._window_start.value()
        if current_window is not None:
            result = self._aggregate(current_window)
            if result:
                yield result
            self._window_start.clear()

    def _register_safety_timer(self, ctx):
        fire_at = ctx.timer_service().current_processing_time() + 65_000
        ctx.timer_service().register_processing_time_timer(fire_at)

    # ── aggregation with gap-fill ─────────────────────────────────────────

    def _aggregate(self, window_start: int) -> str | None:
        # collect candles belonging to this window
        window_candles: dict[int, dict] = {}

        for ts, cjson in self._candles.items():
            minute = (ts // self.WINDOW_MS) * self.WINDOW_MS
            if minute == window_start:
                window_candles[ts] = json.loads(cjson)

        if not window_candles:
            return None

        # gap-fill missing seconds (forward-fill from previous close)
        last_c = self._last_close.value() or next(iter(window_candles.values()))["c"]
        for sec_offset in range(60):
            ts = window_start + sec_offset * 1000
            if ts in window_candles:
                last_c = window_candles[ts]["c"]
            else:
                window_candles[ts] = {
                    "t": ts, "o": last_c, "h": last_c,
                    "l": last_c, "c": last_c,
                    "v": 0.0, "qv": 0.0, "n": 0,
                }

        sorted_candles = [window_candles[k] for k in sorted(window_candles)]

        symbol = self._symbol.value() or "unknown"
        agg = {
            "event_time":   int(time.time() * 1000),
            "symbol":       symbol,
            "kline_start":  window_start,
            "kline_close":  window_start + 59_999,
            "interval":     "1m",
            "open":         sorted_candles[0]["o"],
            "high":         max(c["h"] for c in sorted_candles),
            "low":          min(c["l"] for c in sorted_candles),
            "close":        sorted_candles[-1]["c"],
            "volume":       sum(c["v"] for c in sorted_candles),
            "quote_volume": sum(c["qv"] for c in sorted_candles),
            "trade_count":  sum(c["n"] for c in sorted_candles),
            "is_closed":    True,
        }

        # remove only aggregated keys (keep future-window candles intact)
        for ts in window_candles:
            self._candles.remove(ts)

        real_count = sum(1 for c in sorted_candles if c["v"] > 0)
        log.info("[Window] %s 1s→1m window %d  real=%d/60",
                 symbol, window_start, real_count)
        return json.dumps(agg)


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
    Batch-buffered to reduce KeyDB round trips.
    """
    BATCH_SIZE = 50
    FLUSH_INTERVAL = 0.3

    def open(self, runtime_context):
        self._r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True,
            socket_keepalive=True,
        )
        self._buffer: list[dict] = []
        self._last_flush = time.time()

    def close(self):
        try:
            self._flush()
            self._r.close()
        except Exception as e:
            log.error("[Depth] close error: %s", e)

    def _flush(self):
        if not self._buffer:
            return
        try:
            pipe = self._r.pipeline()
            for rec in self._buffer:
                symbol = rec["symbol"]
                bids = rec["bids"]
                asks = rec["asks"]
                pipe.hset(f"orderbook:{symbol}", mapping={
                    "bids":           json.dumps(bids),
                    "asks":           json.dumps(asks),
                    "last_update_id": rec["last_update_id"],
                    "event_time":     rec["event_time"],
                    "bid_depth":      len(bids),
                    "ask_depth":      len(asks),
                    "best_bid":       float(bids[0][0]) if bids else 0,
                    "best_ask":       float(asks[0][0]) if asks else 0,
                    "spread":         round(float(asks[0][0]) - float(bids[0][0]), 8) if bids and asks else 0,
                })
                pipe.expire(f"orderbook:{symbol}", 60)
            pipe.execute()
        except Exception as e:
            log.error("[Depth] flush error (dropped %d records): %s",
                      len(self._buffer), e)
        finally:
            self._buffer.clear()
            self._last_flush = time.time()

    def flat_map(self, value):
        try:
            if isinstance(value, (str, bytes)):
                value = json.loads(value)

            symbol = value.get("symbol")
            if not symbol:
                return []

            self._buffer.append({
                "symbol":         symbol,
                "bids":           value.get("bids", []),
                "asks":           value.get("asks", []),
                "last_update_id": int(value.get("last_update_id", 0)),
                "event_time":     int(value.get("event_time", 0)),
            })
            if (
                len(self._buffer) >= self.BATCH_SIZE
                or (time.time() - self._last_flush) >= self.FLUSH_INTERVAL
            ):
                self._flush()
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
    env.enable_checkpointing(120_000)
    chk = env.get_checkpoint_config()
    chk.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    chk.enable_unaligned_checkpoints()
    chk.set_min_pause_between_checkpoints(30_000)
    chk.set_checkpoint_timeout(120_000)
    t_env = StreamTableEnvironment.create(env)

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
            event_time,
            symbol,
            `close`,
            bid,
            ask,
            h24_volume,
            h24_quote_volume,
            h24_price_change_pct,
            h24_trade_count
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
            'format'                       = 'avro-confluent',
            'avro-confluent.url'           = '{SCHEMA_REGISTRY_URL}'
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

    # ── Branch 1: write raw 1s candles to KeyDB + InfluxDB ─────────────────
    ds_kline_dict.flat_map(
        KeyDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1s_Klines_To_KeyDB").print()
    ds_kline_dict.flat_map(
        InfluxDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1s_Klines_To_InfluxDB").print()

    # ── Branch 2: in-flight 1s→1m aggregation (dedup + gap-fill) ──────────
    ds_1m_candles = (
        ds_kline_dict
        .key_by(lambda v: json.loads(v)["symbol"])
        .process(KlineWindowAggregator(), output_type=Types.STRING())
    )
    ds_1m_candles.flat_map(
        KeyDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1m_Klines_To_KeyDB").print()
    ds_1m_candles.flat_map(
        InfluxDBKlineWriter(), output_type=Types.STRING()
    ).name("Write_1m_Klines_To_InfluxDB").print()

    # ── Indicators pipeline: closed 1m klines → SMA/EMA → KeyDB + InfluxDB
    ds_1m_candles.flat_map(
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
    ).name("Write_Depth_To_KeyDB").print()

    env.execute("Crypto_MultiStream_Kafka_to_KeyDB_InfluxDB")


if __name__ == "__main__":
    run()
