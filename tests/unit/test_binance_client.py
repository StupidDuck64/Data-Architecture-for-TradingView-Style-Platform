"""
Unit tests for the Binance exchange client.

Tests stream name builders, URL construction, and configuration
without making actual network requests.
"""

import sys
import os

# Add src to path for direct exchange imports as used in the client module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

import pytest

from exchanges.binance.client import BinanceClient, WS_TICKER_URL, WS_COMBINED_BASE, EPOCH_MS


@pytest.fixture
def client():
    return BinanceClient(max_retries=1, request_delay=0.01)


@pytest.mark.unit
class TestBinanceStreamNames:

    def test_trade_stream_name(self, client):
        """Trade stream name follows Binance format."""
        assert client.trade_stream_name("BTCUSDT") == "btcusdt@aggTrade"

    def test_trade_stream_lowercase(self, client):
        """Trade stream always uses lowercase symbol."""
        assert client.trade_stream_name("ETHUSDT") == "ethusdt@aggTrade"

    def test_kline_stream_name(self, client):
        """Kline stream name includes interval."""
        assert client.kline_stream_name("BTCUSDT", "1m") == "btcusdt@kline_1m"
        assert client.kline_stream_name("BTCUSDT", "1h") == "btcusdt@kline_1h"

    def test_depth_stream_name(self, client):
        """Depth stream includes level and update interval."""
        assert client.depth_stream_name("BTCUSDT", "20", "100") == "btcusdt@depth20@100ms"


@pytest.mark.unit
class TestBinanceURLBuilders:

    def test_ticker_stream_url(self, client):
        """Ticker stream URL points to miniTicker endpoint."""
        url = client.build_ticker_stream_url()
        assert url == WS_TICKER_URL
        assert "miniTicker" in url

    def test_combined_stream_url_single(self, client):
        """Combined URL with single stream."""
        url = client.build_combined_stream_url(["btcusdt@aggTrade"])
        assert url == f"{WS_COMBINED_BASE}?streams=btcusdt@aggTrade"

    def test_combined_stream_url_multiple(self, client):
        """Combined URL joins multiple streams with /."""
        streams = ["btcusdt@aggTrade", "ethusdt@aggTrade"]
        url = client.build_combined_stream_url(streams)
        assert "btcusdt@aggTrade/ethusdt@aggTrade" in url
        assert url.startswith(WS_COMBINED_BASE)


@pytest.mark.unit
class TestBinanceConfig:

    def test_epoch_ms_reasonable(self):
        """Binance epoch should be around 2017-07-14."""
        # 1_500_000_000_000 ms = ~2017-07-14
        assert EPOCH_MS == 1_500_000_000_000

    def test_client_default_retries(self):
        """Default client has 5 retries."""
        client = BinanceClient()
        assert client.max_retries == 5

    def test_client_custom_config(self):
        """Custom config values are stored."""
        client = BinanceClient(max_retries=3, request_delay=0.5)
        assert client.max_retries == 3
        assert client.request_delay == 0.5


@pytest.mark.unit
class TestBinanceMapperDelegation:
    """Verify mapper delegation methods exist and call the right functions."""

    def test_map_ticker_delegates(self, client):
        """map_ticker delegates to mappers.map_ticker."""
        raw = {"E": 1, "s": "TEST", "c": "100"}
        result = client.map_ticker(raw)
        assert result["symbol"] == "TEST"
        assert result["close"] == 100.0

    def test_map_trade_delegates(self, client):
        """map_trade delegates to mappers.map_agg_trade."""
        raw = {"E": 1, "s": "TEST", "a": 1, "p": "50", "q": "1", "T": 1, "m": False}
        result = client.map_trade(raw)
        assert result["price"] == 50.0

    def test_map_kline_delegates(self, client):
        """map_kline delegates to mappers.map_kline."""
        raw = {
            "E": 1, "s": "TEST",
            "k": {"t": 0, "T": 0, "i": "1m", "o": "0", "h": "0",
                   "l": "0", "c": "0", "v": "0", "q": "0", "n": 0, "x": True},
        }
        result = client.map_kline(raw)
        assert result["is_closed"] is True

    def test_map_depth_delegates(self, client):
        """map_depth delegates to mappers.map_depth."""
        raw = {"s": "TEST", "bids": [], "asks": []}
        result = client.map_depth(raw)
        assert result["symbol"] == "TEST"
