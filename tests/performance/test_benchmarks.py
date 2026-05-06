"""
Performance benchmark tests for critical data processing functions.

Measures execution time of aggregation, merging, and validation
functions to detect performance regressions.
"""

import time
import pytest

from backend.services.candle_service import (
    validate_symbol,
    validate_interval,
    merge_unique,
    aggregate,
    ms_to_rfc3339,
    to_candle_rows,
)


def _generate_candles(count: int, start_ms: int = 1700000000000, step_ms: int = 60000) -> list[dict]:
    """Generate a list of synthetic candle dicts."""
    candles = []
    for i in range(count):
        t = start_ms + i * step_ms
        price = 50000.0 + (i % 100)
        candles.append({
            "openTime": t,
            "open": price,
            "high": price + 50.0,
            "low": price - 30.0,
            "close": price + 20.0,
            "volume": 10.0 + (i % 50),
        })
    return candles


@pytest.mark.performance
class TestAggregationPerformance:

    def test_aggregate_10k_candles(self):
        """Aggregating 10,000 1m candles into 5m buckets completes in <500ms."""
        candles = _generate_candles(10_000)
        target_ms = 300_000  # 5 minutes

        start = time.perf_counter()
        result = aggregate(candles, target_ms)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) > 0
        assert elapsed_ms < 500, f"Aggregation took {elapsed_ms:.1f}ms (limit: 500ms)"

    def test_aggregate_50k_candles(self):
        """Aggregating 50,000 1m candles into 1h buckets completes in <2s."""
        candles = _generate_candles(50_000)
        target_ms = 3_600_000  # 1 hour

        start = time.perf_counter()
        result = aggregate(candles, target_ms)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) > 0
        assert elapsed_ms < 2000, f"Aggregation took {elapsed_ms:.1f}ms (limit: 2000ms)"

    def test_aggregate_1d_from_1m(self):
        """Aggregating 1440 1m candles into a single 1d bucket."""
        candles = _generate_candles(1440)
        target_ms = 86_400_000  # 1 day

        start = time.perf_counter()
        result = aggregate(candles, target_ms)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) >= 1
        assert elapsed_ms < 100, f"Aggregation took {elapsed_ms:.1f}ms (limit: 100ms)"


@pytest.mark.performance
class TestMergePerformance:

    def test_merge_10k_candles(self):
        """Merging two 5,000-candle lists completes in <500ms."""
        existing = _generate_candles(5_000, start_ms=1700000000000)
        incoming = _generate_candles(5_000, start_ms=1700000000000 + 3000 * 60000)

        start = time.perf_counter()
        result = merge_unique(existing, incoming)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) > 5_000  # Some overlap, mostly new
        assert elapsed_ms < 500, f"Merge took {elapsed_ms:.1f}ms (limit: 500ms)"

    def test_merge_with_full_overlap(self):
        """Merging identical lists (100% overlap) completes quickly."""
        candles = _generate_candles(5_000)

        start = time.perf_counter()
        result = merge_unique(candles, candles)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) == 5_000
        assert elapsed_ms < 300, f"Full-overlap merge took {elapsed_ms:.1f}ms (limit: 300ms)"


@pytest.mark.performance
class TestValidationPerformance:

    def test_validate_symbol_batch(self):
        """Validating 10,000 symbols completes in <100ms."""
        symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "SOLUSDT"] * 2000

        start = time.perf_counter()
        for s in symbols:
            validate_symbol(s)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"Symbol validation took {elapsed_ms:.1f}ms (limit: 100ms)"

    def test_validate_interval_batch(self):
        """Validating 10,000 intervals completes in <100ms."""
        intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1s"] * 1250

        start = time.perf_counter()
        for i in intervals:
            validate_interval(i)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"Interval validation took {elapsed_ms:.1f}ms (limit: 100ms)"


@pytest.mark.performance
class TestConversionPerformance:

    def test_ms_to_rfc3339_batch(self):
        """Converting 10,000 timestamps completes in <200ms."""
        timestamps = [1700000000000 + i * 1000 for i in range(10_000)]

        start = time.perf_counter()
        for ts in timestamps:
            ms_to_rfc3339(ts)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 200, f"Timestamp conversion took {elapsed_ms:.1f}ms (limit: 200ms)"

    def test_to_candle_rows_batch(self):
        """Converting 10,000 Trino rows completes in <200ms."""
        rows = [(1000 + i, 100.0, 110.0, 95.0, 105.0, 50.0) for i in range(10_000)]

        start = time.perf_counter()
        result = to_candle_rows(rows)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(result) == 10_000
        assert elapsed_ms < 200, f"Row conversion took {elapsed_ms:.1f}ms (limit: 200ms)"
