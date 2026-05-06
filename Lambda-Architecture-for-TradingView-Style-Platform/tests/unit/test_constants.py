"""
Unit tests for backend core constants module.

Verifies that interval definitions, regex patterns, and data limits
are correctly configured.
"""

import re
import pytest

from backend.core.constants import (
    INTERVAL_SECONDS,
    SYMBOL_RE,
    MAX_RAW_CANDLES,
    MAX_RAW_PER_QUERY,
    MAX_RAW_ROWS,
    MAX_BACKFILL_PAGES,
    LIVE_MAX_BASE_ROWS,
    HOURLY_INTERVALS,
)


@pytest.mark.unit
class TestIntervalSeconds:

    def test_all_intervals_present(self):
        """All expected intervals are defined."""
        expected = {"1s", "1m", "5m", "15m", "1h", "4h", "1d", "1w"}
        assert set(INTERVAL_SECONDS.keys()) == expected

    def test_second_values_ascending(self):
        """Interval second values increase as intervals grow."""
        order = ["1s", "1m", "5m", "15m", "1h", "4h", "1d", "1w"]
        values = [INTERVAL_SECONDS[k] for k in order]
        assert values == sorted(values)

    def test_1s_is_one(self):
        assert INTERVAL_SECONDS["1s"] == 1

    def test_1m_is_60(self):
        assert INTERVAL_SECONDS["1m"] == 60

    def test_1h_is_3600(self):
        assert INTERVAL_SECONDS["1h"] == 3600

    def test_1d_is_86400(self):
        assert INTERVAL_SECONDS["1d"] == 86400

    def test_1w_is_604800(self):
        assert INTERVAL_SECONDS["1w"] == 604800


@pytest.mark.unit
class TestSymbolRegex:

    def test_valid_symbols(self):
        """Standard crypto symbols match the regex."""
        for sym in ("BTCUSDT", "ETHUSDT", "BNB", "A", "A" * 20):
            assert SYMBOL_RE.match(sym), f"{sym} should match"

    def test_rejects_lowercase(self):
        """Lowercase symbols don't match (validation normalizes first)."""
        assert not SYMBOL_RE.match("btcusdt")

    def test_rejects_special_chars(self):
        """Special characters are rejected."""
        for sym in ("BTC/USDT", "BTC-USDT", "BTC USDT", "BTC@USDT"):
            assert not SYMBOL_RE.match(sym), f"{sym} should not match"

    def test_rejects_empty(self):
        assert not SYMBOL_RE.match("")

    def test_rejects_too_long(self):
        assert not SYMBOL_RE.match("A" * 21)

    def test_allows_numbers(self):
        assert SYMBOL_RE.match("1INCHUSDT")


@pytest.mark.unit
class TestDataLimits:

    def test_limits_are_positive(self):
        """All data limits must be positive integers."""
        for name, value in [
            ("MAX_RAW_CANDLES", MAX_RAW_CANDLES),
            ("MAX_RAW_PER_QUERY", MAX_RAW_PER_QUERY),
            ("MAX_RAW_ROWS", MAX_RAW_ROWS),
            ("MAX_BACKFILL_PAGES", MAX_BACKFILL_PAGES),
            ("LIVE_MAX_BASE_ROWS", LIVE_MAX_BASE_ROWS),
        ]:
            assert isinstance(value, int), f"{name} should be int"
            assert value > 0, f"{name} should be positive"

    def test_raw_per_query_less_than_max(self):
        """Per-query limit should be less than total max."""
        assert MAX_RAW_PER_QUERY <= MAX_RAW_CANDLES

    def test_hourly_intervals_set(self):
        """Hourly intervals should be a subset of all intervals."""
        assert HOURLY_INTERVALS <= set(INTERVAL_SECONDS.keys())
        assert HOURLY_INTERVALS == {"1h", "4h", "1d", "1w"}
