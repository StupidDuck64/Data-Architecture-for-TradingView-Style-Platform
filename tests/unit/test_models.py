"""
Unit tests for Pydantic response models.
"""

import pytest
from pydantic import ValidationError

from backend.models.candle import CandleResponse
from backend.models.ticker import TickerResponse, OrderBookResponse, SymbolResponse, IndicatorResponse
from backend.models.health import HealthResponse


class TestCandleResponse:

    @pytest.mark.unit
    def test_valid_candle(self):
        c = CandleResponse(openTime=1000, open=100.0, high=110.0, low=95.0, close=105.0, volume=50.0)
        assert c.openTime == 1000
        assert c.close == 105.0

    @pytest.mark.unit
    def test_serialization(self):
        c = CandleResponse(openTime=1000, open=100.0, high=110.0, low=95.0, close=105.0, volume=50.0)
        d = c.model_dump()
        assert d["openTime"] == 1000
        assert isinstance(d["volume"], float)

    @pytest.mark.unit
    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            CandleResponse(openTime=1000, open=100.0, high=110.0)


class TestTickerResponse:

    @pytest.mark.unit
    def test_minimal_ticker(self):
        t = TickerResponse(symbol="BTCUSDT", price=50000.0)
        assert t.symbol == "BTCUSDT"
        assert t.change24h == 0.0  # default

    @pytest.mark.unit
    def test_full_ticker(self):
        t = TickerResponse(
            symbol="ETHUSDT", price=3000.0,
            change24h=2.5, bid=2999.0, ask=3001.0,
            volume=1000000.0, event_time=1700000000000,
        )
        assert t.bid == 2999.0


class TestOrderBookResponse:

    @pytest.mark.unit
    def test_valid_orderbook(self):
        ob = OrderBookResponse(
            symbol="BTCUSDT",
            bids=[[50000.0, 1.5], [49999.0, 2.0]],
            asks=[[50001.0, 1.0], [50002.0, 0.5]],
            spread=1.0,
        )
        assert len(ob.bids) == 2
        assert ob.spread == 1.0


class TestSymbolResponse:

    @pytest.mark.unit
    def test_symbol(self):
        s = SymbolResponse(symbol="BTCUSDT", name="BTC / USDT")
        assert s.type == "crypto"  # default


class TestIndicatorResponse:

    @pytest.mark.unit
    def test_partial_indicators(self):
        i = IndicatorResponse(symbol="BTCUSDT", sma20=50000.0)
        assert i.sma20 == 50000.0
        assert i.sma50 is None


class TestHealthResponse:

    @pytest.mark.unit
    def test_health(self):
        h = HealthResponse(
            status="ok",
            checks={"keydb": "ok", "influxdb": "ok", "trino": "ok"},
            latency_ms={"keydb_ms": 1.5, "influxdb_ms": 3.0, "trino_ms": 50.0},
            total_latency_ms=54.5,
            checked_at="2024-01-01T00:00:00Z",
            uptime_sec=3600,
        )
        assert h.status == "ok"
        assert h.checks["keydb"] == "ok"
