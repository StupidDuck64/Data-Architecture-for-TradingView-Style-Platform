"""
Redis Sentinel kline candle writer for Flink stream processing.

Writes 1s and 1m candles to Redis sorted sets with interval-specific TTLs.
Batch-buffered to reduce Redis round trips.
"""

import json
import logging
import os
import time

from pyflink.datastream.functions import FlatMapFunction
from common.flink_redis_sentinel import get_flink_redis

KEYDB_1S_RETENTION_DAYS = int(os.environ.get("KEYDB_1S_RETENTION_DAYS", "1"))
KEYDB_1M_RETENTION_DAYS = int(os.environ.get("KEYDB_1M_RETENTION_DAYS", "7"))

log = logging.getLogger(__name__)


class KeyDBKlineWriter(FlatMapFunction):
    """Writes kline candles to Redis Sentinel with interval-specific TTL.

    - ``candle:1s:{symbol}`` → TTL KEYDB_1S_RETENTION_DAYS
    - ``candle:1m:{symbol}`` → TTL KEYDB_1M_RETENTION_DAYS
    - ``candle:latest:{symbol}`` → latest candle info (1m+ only)
    """

    TTL_1S = max(KEYDB_1S_RETENTION_DAYS, 1) * 86_400
    TTL_1M = max(KEYDB_1M_RETENTION_DAYS, 1) * 86_400
    CLEANUP_EVERY = 60
    BATCH_SIZE = 50
    FLUSH_INTERVAL = 0.5

    def open(self, runtime_context):
        # Get Redis master connection via Sentinel
        self._r = get_flink_redis()
        self._write_count: dict[str, int] = {}
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

                pipe.zremrangebyscore(history_key, kline_start, kline_start)
                pipe.zadd(history_key, {candle_json: kline_start})
                pipe.expire(history_key, ttl_sec)

                if interval != "1s":
                    pipe.hset(f"candle:latest:{symbol}", mapping=item["latest_mapping"])

                count = self._write_count.get(symbol, 0) + 1
                self._write_count[symbol] = count
                if count % self.CLEANUP_EVERY == 0:
                    cutoff = kline_start - (ttl_sec * 1000)
                    pipe.zremrangebyscore(history_key, 0, cutoff)

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
            symbol = value.get("symbol")
            if not symbol:
                return []
            interval = value.get("interval", "1m")
            if interval not in ("1s", "1m"):
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

            if interval == "1s":
                history_key = f"candle:1s:{symbol}"
                ttl_sec = self.TTL_1S
            else:
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
