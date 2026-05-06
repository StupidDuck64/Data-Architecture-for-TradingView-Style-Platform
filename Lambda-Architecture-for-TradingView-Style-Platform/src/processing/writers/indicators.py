"""
Technical indicator writer for Flink stream processing.

Receives closed 1m klines, maintains rolling close-price buffers per symbol,
computes SMA20, SMA50, EMA12, EMA26, and writes results to Redis Sentinel + InfluxDB.
"""

import json
import logging
import os
import time
from collections import deque

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyflink.datastream.functions import FlatMapFunction
from common.flink_redis_sentinel import get_flink_redis

INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

log = logging.getLogger(__name__)


class IndicatorWriter(FlatMapFunction):
    """Computes SMA/EMA indicators from closed 1m klines.

    Outputs:
        - ``indicator:latest:{symbol}`` hash in Redis Sentinel
        - ``indicators`` measurement in InfluxDB
    """

    SMA_PERIODS = (20, 50)
    EMA_PERIODS = (12, 26)
    MAX_HISTORY = 60  # keep last 60 closes (enough for SMA50 + buffer)

    def open(self, runtime_context):
        # Get Redis master connection via Sentinel
        self._r = get_flink_redis()
        self._influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)
        self._closes: dict[str, deque] = {}
        self._ema_state: dict[str, dict[int, float]] = {}
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
        sym_state = self._ema_state.setdefault(symbol, {})
        if period not in sym_state:
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

            if not value.get("is_closed"):
                return []

            symbol = value.get("symbol")
            if not symbol:
                return []

            close_price = float(value["close"])
            kline_start = int(value["kline_start"])

            if symbol not in self._closes:
                self._closes[symbol] = deque(maxlen=self.MAX_HISTORY)
            self._closes[symbol].append(close_price)

            prices = self._closes[symbol]

            sma20 = self._sma(prices, 20)
            sma50 = self._sma(prices, 50)
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
