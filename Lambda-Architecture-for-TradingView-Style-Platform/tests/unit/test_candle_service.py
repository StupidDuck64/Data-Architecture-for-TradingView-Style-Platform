"""
Unit tests for candle_service business logic.
Tests aggregation, merging, validation, and conversion functions.
"""

import pytest
from fastapi import HTTPException

from backend.services.candle_service import (
    validate_symbol,
    validate_interval,
    normalize_interval,
    interval_to_seconds,
    interval_to_ms,
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
        assert validate_interval("4H") == ("4h", 14400)
        assert validate_interval("1D") == ("1d", 86400)
        assert validate_interval("1W") == ("1w", 604800)

    @pytest.mark.unit
    def test_invalid_interval(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_interval("2m")
        assert exc_info.value.status_code == 400


class TestNormalizeInterval:
    """Test interval normalization."""

    @pytest.mark.unit
    def test_lowercase_passthrough(self):
        assert normalize_interval("1m") == "1m"
        assert normalize_interval("1h") == "1h"

    @pytest.mark.unit
    def test_uppercase_normalized(self):
        assert normalize_interval("1M") == "1m"
        assert normalize_interval("1H") == "1h"
        assert normalize_interval("4H") == "4h"
        assert normalize_interval("1D") == "1d"
        assert normalize_interval("1W") == "1w"

    @pytest.mark.unit
    def test_whitespace_trimmed(self):
        assert normalize_interval("  1h  ") == "1h"
        assert normalize_interval("\t1m\n") == "1m"


class TestIntervalToSeconds:
    """Test interval to seconds conversion."""

    @pytest.mark.unit
    def test_valid_intervals(self):
        assert interval_to_seconds("1s") == 1
        assert interval_to_seconds("1m") == 60
        assert interval_to_seconds("5m") == 300
        assert interval_to_seconds("15m") == 900
        assert interval_to_seconds("1h") == 3600
        assert interval_to_seconds("4h") == 14400
        assert interval_to_seconds("1d") == 86400
        assert interval_to_seconds("1w") == 604800

    @pytest.mark.unit
    def test_uppercase_normalized(self):
        assert interval_to_seconds("1H") == 3600
        assert interval_to_seconds("4H") == 14400
        assert interval_to_seconds("1D") == 86400

    @pytest.mark.unit
    def test_invalid_returns_zero(self):
        assert interval_to_seconds("2m") == 0
        assert interval_to_seconds("invalid") == 0


class TestIntervalToMs:
    """Test interval to milliseconds conversion."""

    @pytest.mark.unit
    def test_valid_intervals(self):
        assert interval_to_ms("1s") == 1000
        assert interval_to_ms("1m") == 60000
        assert interval_to_ms("1h") == 3600000

    @pytest.mark.unit
    def test_invalid_returns_zero(self):
        assert interval_to_ms("invalid") == 0


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

    @pytest.mark.unit
    def test_overlap_prefers_closed_candle(self):
        """When timestamps match, prefer closed candle over partial."""
        partial = {"openTime": 1000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10, "is_closed": False}
        closed = {"openTime": 1000, "open": 1.0, "high": 2.1, "low": 0.5, "close": 1.6, "volume": 10, "is_closed": True}

        # Closed should win regardless of order
        result1 = merge_unique([partial], [closed])
        assert len(result1) == 1
        assert result1[0]["is_closed"] is True
        assert result1[0]["close"] == 1.6

        result2 = merge_unique([closed], [partial])
        assert len(result2) == 1
        assert result2[0]["is_closed"] is True
        assert result2[0]["close"] == 1.6

    @pytest.mark.unit
    def test_overlap_prefers_higher_volume(self):
        """When timestamps match and both partial, prefer higher volume."""
        low_vol = {"openTime": 1000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10}
        high_vol = {"openTime": 1000, "open": 1.0, "high": 2.1, "low": 0.5, "close": 1.6, "volume": 50}

        result = merge_unique([low_vol], [high_vol])
        assert len(result) == 1
        assert result[0]["volume"] == 50
        assert result[0]["close"] == 1.6

    @pytest.mark.unit
    def test_idempotent_replay(self):
        """Replaying the same batch twice should produce identical result."""
        batch1 = [
            {"openTime": 1000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10},
            {"openTime": 2000, "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5, "volume": 20},
        ]
        batch2 = batch1.copy()

        result1 = merge_unique([], batch1)
        result2 = merge_unique(result1, batch2)

        # Should be identical (idempotent)
        assert len(result2) == 2
        assert result1 == result2

    @pytest.mark.unit
    def test_out_of_order_merge(self):
        """Out-of-order candles should be sorted correctly."""
        candles = [
            {"openTime": 3000, "open": 3, "high": 3, "low": 3, "close": 3, "volume": 30},
            {"openTime": 1000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10},
            {"openTime": 2000, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 20},
        ]
        result = merge_unique([], candles)
        assert [c["openTime"] for c in result] == [1000, 2000, 3000]


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

    @pytest.mark.unit
    def test_aggregate_out_of_order_input(self):
        """Aggregate should handle out-of-order input correctly."""
        # Input: 3 candles out of order within same 5m bucket
        candles = [
            {"openTime": 120000, "open": 18.0, "high": 22.0, "low": 16.0, "close": 19.0, "volume": 150.0},
            {"openTime": 0, "open": 10.0, "high": 15.0, "low": 8.0, "close": 12.0, "volume": 100.0},
            {"openTime": 60000, "open": 12.0, "high": 20.0, "low": 9.0, "close": 18.0, "volume": 200.0},
        ]
        result = aggregate(candles, 300000)  # 5min bucket
        assert len(result) == 1
        bar = result[0]
        # open should be from earliest timestamp (0), not first in list (120000)
        assert bar["open"] == 10.0
        # close should be from latest timestamp (120000), not last in list (60000)
        assert bar["close"] == 19.0
        assert bar["high"] == 22.0
        assert bar["low"] == 8.0
        assert bar["volume"] == 450.0

    @pytest.mark.unit
    def test_aggregate_duplicate_timestamps(self):
        """Aggregate should dedup duplicate timestamps before aggregating."""
        candles = [
            {"openTime": 0, "open": 10.0, "high": 15.0, "low": 8.0, "close": 12.0, "volume": 100.0},
            {"openTime": 0, "open": 10.0, "high": 16.0, "low": 7.0, "close": 13.0, "volume": 150.0},  # duplicate, higher volume
            {"openTime": 60000, "open": 12.0, "high": 20.0, "low": 9.0, "close": 18.0, "volume": 200.0},
        ]
        result = aggregate(candles, 300000)
        assert len(result) == 1
        bar = result[0]
        # Should use higher volume candle for openTime=0
        assert bar["open"] == 10.0
        assert bar["high"] == 20.0  # max of deduped candles
        assert bar["low"] == 7.0    # min of deduped candles
        assert bar["close"] == 18.0
        # Volume should be sum of deduped candles (150 + 200, not 100 + 150 + 200)
        assert bar["volume"] == 350.0

    @pytest.mark.unit
    def test_aggregate_1m_to_15m(self):
        """Aggregate 1m candles to 15m."""
        # 15 x 1m candles spanning 15 minutes
        candles = []
        for i in range(15):
            candles.append({
                "openTime": i * 60000,
                "open": 100.0 + i,
                "high": 110.0 + i,
                "low": 90.0 + i,
                "close": 105.0 + i,
                "volume": 10.0 + i,
            })
        result = aggregate(candles, 900000)  # 15m = 900000ms
        assert len(result) == 1
        bar = result[0]
        assert bar["openTime"] == 0
        assert bar["open"] == 100.0  # first candle
        assert bar["close"] == 119.0  # last candle (105 + 14)
        assert bar["high"] == 124.0   # max (110 + 14)
        assert bar["low"] == 90.0     # min (90 + 0)
        # sum(10 + 11 + ... + 24) = sum of 15 terms starting at 10
        # = 15 * (10 + 24) / 2 = 15 * 34 / 2 = 255
        assert bar["volume"] == 255.0

    @pytest.mark.unit
    def test_aggregate_1m_to_1h(self):
        """Aggregate 1m candles to 1h."""
        # 60 x 1m candles spanning 1 hour
        candles = []
        for i in range(60):
            candles.append({
                "openTime": i * 60000,
                "open": 100.0,
                "high": 110.0,
                "low": 90.0,
                "close": 105.0,
                "volume": 1.0,
            })
        result = aggregate(candles, 3600000)  # 1h = 3600000ms
        assert len(result) == 1
        bar = result[0]
        assert bar["openTime"] == 0
        assert bar["open"] == 100.0
        assert bar["close"] == 105.0
        assert bar["high"] == 110.0
        assert bar["low"] == 90.0
        assert bar["volume"] == 60.0

    @pytest.mark.unit
    def test_aggregate_multiple_buckets(self):
        """Aggregate should create multiple buckets correctly."""
        # 6 x 1m candles spanning 2 x 5m buckets
        candles = []
        for i in range(6):
            candles.append({
                "openTime": i * 60000,
                "open": 100.0 + i * 10,
                "high": 110.0 + i * 10,
                "low": 90.0 + i * 10,
                "close": 105.0 + i * 10,
                "volume": 10.0,
            })
        result = aggregate(candles, 300000)  # 5m
        assert len(result) == 2
        # First bucket: candles 0-4
        assert result[0]["openTime"] == 0
        assert result[0]["open"] == 100.0
        assert result[0]["close"] == 145.0  # candle 4
        # Second bucket: candle 5
        assert result[1]["openTime"] == 300000
        assert result[1]["open"] == 150.0
        assert result[1]["close"] == 155.0

    @pytest.mark.unit
    def test_aggregate_preserves_closed_flag_priority(self):
        """Aggregate should respect merge_unique's closed candle priority."""
        candles = [
            {"openTime": 0, "open": 10.0, "high": 15.0, "low": 8.0, "close": 12.0, "volume": 100.0, "is_closed": False},
            {"openTime": 0, "open": 10.0, "high": 16.0, "low": 7.0, "close": 13.0, "volume": 100.0, "is_closed": True},
            {"openTime": 60000, "open": 12.0, "high": 20.0, "low": 9.0, "close": 18.0, "volume": 200.0},
        ]
        result = aggregate(candles, 300000)
        assert len(result) == 1
        # Should use closed candle for openTime=0
        assert result[0]["high"] == 20.0  # max including closed candle's 16.0
        assert result[0]["low"] == 7.0    # min from closed candle
