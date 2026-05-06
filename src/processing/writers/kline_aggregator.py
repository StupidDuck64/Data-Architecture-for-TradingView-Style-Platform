"""
In-flight 1s → 1m kline window aggregator for Flink stream processing.

Aggregates 1-second candles into 1-minute candles using Flink keyed state,
with deduplication and gap-filling (forward-fill from previous close price).
"""

import json
import logging
import time

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

log = logging.getLogger(__name__)


class KlineWindowAggregator(KeyedProcessFunction):
    """Aggregate 1s klines → 1m klines inside Flink state.

    Strategy:
    1. Each 1s candle is stored in MapState<kline_start_ms, json>
       (same key overwrites → dedup).
    2. When a candle from a *new* minute arrives the previous window
       is aggregated and emitted immediately.
    3. A processing-time safety timer fires 65s after the window
       opens so the last window is never stuck (handles silence).
    4. Missing seconds are forward-filled from the previous
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

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        candle = json.loads(value) if isinstance(value, str) else value
        interval = candle.get("interval", "1s")
        if interval != "1s":
            return

        kline_start = int(candle["kline_start"])
        minute_start = (kline_start // self.WINDOW_MS) * self.WINDOW_MS

        if self._symbol.value() is None:
            self._symbol.update(candle["symbol"])

        # Upsert into state (dedup)
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
            self._window_start.update(minute_start)
            self._register_safety_timer(ctx)

        elif minute_start > current_window:
            result = self._aggregate(current_window)
            if result:
                yield result

            self._window_start.update(minute_start)
            self._register_safety_timer(ctx)

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

    def _aggregate(self, window_start: int) -> str | None:
        window_candles: dict[int, dict] = {}

        for ts, cjson in self._candles.items():
            minute = (ts // self.WINDOW_MS) * self.WINDOW_MS
            if minute == window_start:
                window_candles[ts] = json.loads(cjson)

        if not window_candles:
            return None

        # Gap-fill missing seconds (forward-fill from previous close)
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

        # Remove only aggregated keys (keep future-window candles intact)
        for ts in window_candles:
            self._candles.remove(ts)

        real_count = sum(1 for c in sorted_candles if c["v"] > 0)
        log.info("[Window] %s 1s→1m window %d  real=%d/60",
                 symbol, window_start, real_count)
        return json.dumps(agg)
