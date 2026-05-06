"""
Extended unit tests for Pydantic response models.

Covers edge cases, type coercion, serialization, and
additional model variants not tested in test_models.py.
"""

import pytest
from pydantic import ValidationError

from backend.models.candle import CandleResponse
from backend.models.ticker import (
    TickerResponse,
    OrderBookResponse,
    OrderBookEntry,
    TradeResponse,
    SymbolResponse,
    IndicatorResponse,
)
from backend.models.health import HealthResponse


@pytest.mark.unit
class TestCandleResponseEdgeCases:

    def test_zero_values(self):
        """Candle with all zero values is valid."""
        c = CandleResponse(openTime=0, open=0.0, high=0.0, low=0.0, close=0.0, volume=0.0)
        assert c.open == 0.0

    def test_large_values(self):
        """Candle handles very large price/volume values."""
        c = CandleResponse(
            openTime=9999999999999,
            open=999999.99,
            high=999999.99,
            low=0.00000001,
            close=500000.0,
            volume=1e12,
        )
        assert c.volume == 1e12

    def test_integer_coercion(self):
        """Integer values are coerced to float for price fields."""
        c = CandleResponse(openTime=1000, open=100, high=110, low=95, close=105, volume=50)
        assert isinstance(c.open, float)

    def test_json_roundtrip(self):
        """Model survives JSON serialization roundtrip."""
        c = CandleResponse(openTime=1000, open=100.5, high=110.0, low=95.0, close=105.0, volume=50.5)
        json_str = c.model_dump_json()
        c2 = CandleResponse.model_validate_json(json_str)
        assert c == c2


@pytest.mark.unit
class TestTradeResponse:

    def test_valid_buy(self):
        t = TradeResponse(time=1700000000000, price=50000.0, volume=0.5, side="buy")
        assert t.side == "buy"

    def test_valid_sell(self):
        t = TradeResponse(time=1700000000000, price=50000.0, volume=0.5, side="sell")
        assert t.side == "sell"

    def test_serialization(self):
        t = TradeResponse(time=1700000000000, price=50000.0, volume=0.5, side="buy")
        d = t.model_dump()
        assert d["time"] == 1700000000000
        assert d["side"] == "buy"


@pytest.mark.unit
class TestOrderBookEntry:

    def test_valid_entry(self):
        e = OrderBookEntry(price=50000.0, quantity=1.5)
        assert e.price == 50000.0
        assert e.quantity == 1.5

    def test_missing_fields(self):
        with pytest.raises(ValidationError):
            OrderBookEntry(price=50000.0)


@pytest.mark.unit
class TestOrderBookResponseEdgeCases:

    def test_empty_book(self):
        ob = OrderBookResponse(symbol="BTCUSDT", bids=[], asks=[])
        assert ob.spread == 0.0

    def test_defaults(self):
        ob = OrderBookResponse(symbol="BTCUSDT", bids=[], asks=[])
        assert ob.best_bid == 0.0
        assert ob.best_ask == 0.0
        assert ob.event_time == 0


@pytest.mark.unit
class TestTickerResponseEdgeCases:

    def test_negative_change(self):
        """Negative 24h change is valid."""
        t = TickerResponse(symbol="BTCUSDT", price=48000.0, change24h=-5.2)
        assert t.change24h == -5.2

    def test_zero_price(self):
        """Zero price is technically valid."""
        t = TickerResponse(symbol="BTCUSDT", price=0.0)
        assert t.price == 0.0


@pytest.mark.unit
class TestIndicatorResponseEdgeCases:

    def test_all_none(self):
        """All indicator fields can be None."""
        i = IndicatorResponse(symbol="BTCUSDT")
        assert i.sma20 is None
        assert i.sma50 is None
        assert i.ema12 is None
        assert i.ema26 is None
        assert i.timestamp is None

    def test_all_present(self):
        i = IndicatorResponse(
            symbol="BTCUSDT",
            sma20=50000.0, sma50=48000.0,
            ema12=50500.0, ema26=49000.0,
            timestamp=1700000000000,
        )
        assert i.ema12 == 50500.0


@pytest.mark.unit
class TestHealthResponseEdgeCases:

    def test_degraded_status(self):
        h = HealthResponse(
            status="degraded",
            checks={"keydb": "ok", "influxdb": "Connection refused", "trino": "ok"},
            latency_ms={"keydb_ms": 1.0, "influxdb_ms": 0.0, "trino_ms": 50.0},
            total_latency_ms=51.0,
            checked_at="2024-01-01T00:00:00Z",
            uptime_sec=0,
        )
        assert h.status == "degraded"
        assert "Connection refused" in h.checks["influxdb"]

    def test_zero_uptime(self):
        h = HealthResponse(
            status="ok",
            checks={},
            latency_ms={},
            total_latency_ms=0.0,
            checked_at="2024-01-01T00:00:00Z",
            uptime_sec=0,
        )
        assert h.uptime_sec == 0
