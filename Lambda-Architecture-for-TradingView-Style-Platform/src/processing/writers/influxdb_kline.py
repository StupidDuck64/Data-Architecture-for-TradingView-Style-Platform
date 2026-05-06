"""
InfluxDB kline candle writer for Flink stream processing.

Writes closed 1m candles to InfluxDB ``candles`` measurement for 90-day analytics.
"""

import json
import logging
import os
import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyflink.datastream.functions import FlatMapFunction

INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

log = logging.getLogger(__name__)


class InfluxDBKlineWriter(FlatMapFunction):
    """Writes closed 1m klines to InfluxDB ``candles`` measurement."""

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

            # InfluxDB stores only closed 1m candles for 90-day analytics/history.
            if value.get("interval") != "1m" or not bool(value.get("is_closed", False)):
                return []

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
