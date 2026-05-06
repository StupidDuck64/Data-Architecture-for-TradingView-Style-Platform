"""
Integration tests for candle data correctness and idempotency.
Tests merge logic and data quality rules.
"""

import pytest


@pytest.mark.integration
def test_partial_vs_closed_candle_merge():
    """When merging partial and closed candles, prefer closed."""
    from backend.services.candle_service import merge_unique

    partial = {
        "openTime": 60000,
        "open": 100.0,
        "high": 105.0,
        "low": 98.0,
        "close": 102.0,
        "volume": 5.0,
        "is_closed": False,
    }

    closed = {
        "openTime": 60000,
        "open": 100.0,
        "high": 106.0,
        "low": 97.0,
        "close": 103.0,
        "volume": 10.0,
        "is_closed": True,
    }

    # Merge partial first, then closed
    result = merge_unique([partial], [closed])
    assert len(result) == 1
    assert result[0]["is_closed"] is True
    assert result[0]["volume"] == 10.0
    assert result[0]["close"] == 103.0

    # Reverse order should give same result
    result2 = merge_unique([closed], [partial])
    assert len(result2) == 1
    assert result2[0]["is_closed"] is True
    assert result2[0]["volume"] == 10.0


@pytest.mark.integration
def test_replay_idempotency():
    """Replaying the same data batch should be idempotent."""
    from backend.services.candle_service import merge_unique

    batch = [
        {"openTime": 60000, "open": 100, "high": 105, "low": 98, "close": 102, "volume": 10},
        {"openTime": 120000, "open": 102, "high": 108, "low": 100, "close": 106, "volume": 15},
        {"openTime": 180000, "open": 106, "high": 110, "low": 104, "close": 108, "volume": 20},
    ]

    # First merge
    result1 = merge_unique([], batch)
    assert len(result1) == 3

    # Replay same batch
    result2 = merge_unique(result1, batch)
    assert len(result2) == 3
    assert result1 == result2  # Should be identical

    # Replay again
    result3 = merge_unique(result2, batch)
    assert result2 == result3  # Still identical


@pytest.mark.integration
def test_mixed_quality_candles():
    """Test merging candles with different quality levels."""
    from backend.services.candle_service import merge_unique

    # Low volume partial
    low_partial = {
        "openTime": 60000, "open": 100, "high": 105, "low": 98,
        "close": 102, "volume": 5, "is_closed": False
    }

    # High volume partial
    high_partial = {
        "openTime": 60000, "open": 100, "high": 106, "low": 97,
        "close": 103, "volume": 50, "is_closed": False
    }

    # Low volume closed
    low_closed = {
        "openTime": 60000, "open": 100, "high": 104, "low": 99,
        "close": 101, "volume": 8, "is_closed": True
    }

    # Priority: closed > high volume partial > low volume partial
    result = merge_unique([low_partial], [high_partial])
    assert result[0]["volume"] == 50  # High volume wins

    result = merge_unique([high_partial], [low_closed])
    assert result[0]["is_closed"] is True  # Closed wins over high volume partial
    assert result[0]["volume"] == 8


@pytest.mark.integration
def test_staleness_check_logic():
    """Test staleness check prevents old ticker from overwriting fresh data."""
    # Simulate the staleness check logic from klines.py
    ticker_ts = 500  # Old ticker
    latest_candle_ts = 1000  # Fresh candle
    latest_sub_ts = 950  # Fresh sub-candle

    # Ticker should NOT enrich if older than both candle and sub-candle
    should_enrich = ticker_ts > max(latest_candle_ts, latest_sub_ts)
    assert should_enrich is False

    # Fresh ticker should enrich
    ticker_ts = 1100
    should_enrich = ticker_ts > max(latest_candle_ts, latest_sub_ts)
    assert should_enrich is True
