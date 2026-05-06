"""
InfluxDB ticker writer for Flink stream processing.

Writes market tick data points to InfluxDB ``market_ticks`` measurement.
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


class InfluxDBWriter(FlatMapFunction):
    """Batch-buffered ticker writer to InfluxDB ``market_ticks`` measurement."""

    def __init__(self, batch_size: int = 200, flush_interval_sec: float = 0.5):
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
