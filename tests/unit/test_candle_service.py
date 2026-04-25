"""
Unit tests for candle_service business logic.
Tests aggregation, merging, validation, and conversion functions.
"""

import pytest
from fastapi import HTTPException

from backend.services.candle_service import (
    validate_symbol,
    validate_interval,
    ms_to_rfc3339,
    to_candle_rows,
    merge_unique,
    aggregate,
)


class TestValidateSymbol:
    """Test symbol validation and normalization."""

    @pytest.mark.unit
    def test_valid_uppercase(self):
        assert validate_symbol("BTCUSDT") == "BTCUSDT"

    @pytest.mark.unit
    def test_lowercase_normalized(self):
        assert validate_symbol("btcusdt") == "BTCUSDT"

    @pytest.mark.unit
    def test_with_whitespace(self):
        assert validate_symbol("  ETHUSDT  ") == "ETHUSDT"

    @pytest.mark.unit
    def test_invalid_symbol_special_chars(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_symbol("BTC/USDT")
        assert exc_info.value.status_code == 400

    @pytest.mark.unit
    def test_invalid_symbol_empty(self):
        with pytest.raises(HTTPException):
            validate_symbol("")

    @pytest.mark.unit
    def test_invalid_symbol_too_long(self):
        with pytest.raises(HTTPException):
            validate_symbol("A" * 21)


class TestValidateInterval:
    """Test interval validation."""

    @pytest.mark.unit
    def test_valid_intervals(self):
        assert validate_interval("1m") == ("1m", 60)
        assert validate_interval("5m") == ("5m", 300)
        assert validate_interval("1h") == ("1h", 3600)
        assert validate_interval("1d") == ("1d", 86400)

    @pytest.mark.unit
    def test_case_normalization(self):
        assert validate_interval("1M") == ("1m", 60)
        assert validate_interval("  1H  ") == ("1h", 3600)

    @pytest.mark.unit
    def test_invalid_interval(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_interval("2m")
        assert exc_info.value.status_code == 400


class TestMsToRfc3339:
    """Test millisecond to RFC3339 conversion."""

    @pytest.mark.unit
    def test_known_timestamp(self):
        # 2024-01-01T00:00:00Z = 1704067200000 ms
        result = ms_to_rfc3339(1704067200000)
        assert result == "2024-01-01T00:00:00Z"

    @pytest.mark.unit
    def test_round_trip(self):
        from datetime import datetime, timezone
        ms = 1700000000000
        result = ms_to_rfc3339(ms)
        dt = datetime.strptime(result, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        assert int(dt.timestamp() * 1000) == ms


class TestToCandelRows:
    """Test Trino result tuple to candle dict conversion."""

    @pytest.mark.unit
    def test_basic_conversion(self):
        rows = [(1000000, "100.5", "110.0", "95.0", "105.0", "50.5")]
        result = to_candle_rows(rows)
        assert len(result) == 1
        assert result[0] == {
            "openTime": 1000000,
            "open": 100.5,
            "high": 110.0,
            "low": 95.0,
            "close": 105.0,
            "volume": 50.5,
        }

    @pytest.mark.unit
    def test_empty_input(self):
        assert to_candle_rows([]) == []

    @pytest.mark.unit
    def test_multiple_rows(self):
        rows = [
            (1000, 1.0, 2.0, 0.5, 1.5, 100.0),
            (2000, 1.5, 3.0, 1.0, 2.5, 200.0),
        ]
        result = to_candle_rows(rows)
        assert len(result) == 2
        assert result[0]["openTime"] == 1000
        assert result[1]["openTime"] == 2000


class TestMergeUnique:
    """Test candle merging with deduplication."""

    @pytest.mark.unit
    def test_no_overlap(self):
        existing = [{"openTime": 1000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10}]
        incoming = [{"openTime": 2000, "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5, "volume": 20}]
        result = merge_unique(existing, incoming)
        assert len(result) == 2
        assert result[0]["openTime"] == 1000
        assert result[1]["openTime"] == 2000

    @pytest.mark.unit
    def test_overlap_prefers_incoming(self):
        existing = [{"openTime": 1000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10}]
        incoming = [{"openTime": 1000, "open": 1.1, "high": 2.1, "low": 0.6, "close": 1.6, "volume": 11}]
        result = merge_unique(existing, incoming)
        assert len(result) == 1
        assert result[0]["close"] == 1.6  # incoming value

    @pytest.mark.unit
    def test_sorted_output(self):
        existing = [{"openTime": 3000, "open": 3, "high": 3, "low": 3, "close": 3, "volume": 3}]
        incoming = [{"openTime": 1000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
        result = merge_unique(existing, incoming)
        assert result[0]["openTime"] == 1000
        assert result[1]["openTime"] == 3000

    @pytest.mark.unit
    def test_empty_incoming(self):
        existing = [{"openTime": 1000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
        result = merge_unique(existing, [])
        assert result == existing


class TestAggregate:
    """Test OHLCV candle aggregation into larger intervals."""

    @pytest.mark.unit
    def test_aggregate_5m_from_1m(self, sample_1m_candles):
        """Aggregate 10 x 1m candles into 5m buckets."""
        target_ms = 300000  # 5 minutes in ms
        result = aggregate(sample_1m_candles, target_ms)
        # 10 candles at 60s intervals starting from a fixed timestamp
        # produces 3 buckets due to timestamp alignment
        assert len(result) >= 2
        # Verify all buckets are properly aligned
        for bar in result:
            assert bar["openTime"] % target_ms == 0

    @pytest.mark.unit
    def test_aggregate_preserves_ohlcv(self):
        """Verify OHLCV rules: open=first, high=max, low=min, close=last, volume=sum."""
        candles = [
            {"openTime": 0, "open": 10.0, "high": 15.0, "low": 8.0, "close": 12.0, "volume": 100.0},
            {"openTime": 60000, "open": 12.0, "high": 20.0, "low": 9.0, "close": 18.0, "volume": 200.0},
            {"openTime": 120000, "open": 18.0, "high": 22.0, "low": 16.0, "close": 19.0, "volume": 150.0},
        ]
        result = aggregate(candles, 300000)  # 5min bucket
        assert len(result) == 1
        bar = result[0]
        assert bar["open"] == 10.0    # first candle's open
        assert bar["high"] == 22.0    # max of all highs
        assert bar["low"] == 8.0      # min of all lows
        assert bar["close"] == 19.0   # last candle's close
        assert bar["volume"] == 450.0  # sum of volumes

    @pytest.mark.unit
    def test_aggregate_empty(self):
        assert aggregate([], 300000) == []

    @pytest.mark.unit
    def test_aggregate_single_candle(self):
        candles = [{"openTime": 0, "open": 10, "high": 15, "low": 8, "close": 12, "volume": 100}]
        result = aggregate(candles, 300000)
        assert len(result) == 1
        assert result[0]["open"] == 10
