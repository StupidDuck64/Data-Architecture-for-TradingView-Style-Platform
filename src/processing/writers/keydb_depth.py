"""
Redis Sentinel order-book depth writer for Flink stream processing.

Receives partial order-book depth snapshots and writes to Redis hashes.
"""

import json
import logging
import os
import time

from pyflink.datastream.functions import FlatMapFunction
from common.flink_redis_sentinel import get_flink_redis

log = logging.getLogger(__name__)


class DepthWriter(FlatMapFunction):
    """Writes order-book snapshots to ``orderbook:{symbol}`` hashes in Redis Sentinel."""

    BATCH_SIZE = 50
    FLUSH_INTERVAL = 0.3

    def open(self, runtime_context):
        # Get Redis master connection via Sentinel
        self._r = get_flink_redis()
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
                pipe.expire(f"orderbook:{symbol}", 300)
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
