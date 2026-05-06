"""
Unit tests for Binance data mappers.

Verifies that raw Binance WebSocket/REST JSON messages are correctly
converted to canonical records matching the Avro schemas.
"""

import json
import pytest

from src.exchanges.binance.mappers import (
    map_ticker,
    map_agg_trade,
    map_kline,
    map_depth,
)


@pytest.mark.unit
class TestMapTicker:

    def test_basic_mapping(self):
        """Canonical ticker record has all expected fields."""
        raw = {
            "E": 1700000000000,
            "s": "BTCUSDT",
            "c": "50000.0",
            "b": "49999.0",
            "a": "50001.0",
            "o": "49000.0",
            "h": "51000.0",
            "l": "48000.0",
            "v": "1000.0",
            "q": "50000000.0",
            "p": "1000.0",
            "P": "2.04",
            "n": 50000,
        }
        result = map_ticker(raw)
        assert result["event_time"] == 1700000000000
        assert result["symbol"] == "BTCUSDT"
        assert result["close"] == 50000.0
        assert result["bid"] == 49999.0
        assert result["ask"] == 50001.0
        assert result["h24_open"] == 49000.0
        assert result["h24_high"] == 51000.0
        assert result["h24_low"] == 48000.0
        assert result["h24_volume"] == 1000.0
        assert result["h24_quote_volume"] == 50000000.0
        assert result["h24_price_change"] == 1000.0
        assert result["h24_price_change_pct"] == 2.04
        assert result["h24_trade_count"] == 50000

    def test_missing_optional_fields_default_to_zero(self):
        """Missing optional fields default to 0."""
        raw = {"E": 1700000000000, "s": "ETHUSDT"}
        result = map_ticker(raw)
        assert result["close"] == 0
        assert result["bid"] == 0
        assert result["h24_volume"] == 0

    def test_string_to_float_conversion(self):
        """String numbers from WebSocket are converted to float."""
        raw = {"E": 1, "s": "TEST", "c": "123.456", "v": "789.012"}
        result = map_ticker(raw)
        assert isinstance(result["close"], float)
        assert isinstance(result["h24_volume"], float)


@pytest.mark.unit
class TestMapAggTrade:

    def test_basic_mapping(self):
        """Canonical trade record has all expected fields."""
        raw = {
            "E": 1700000000000,
            "s": "BTCUSDT",
            "a": 123456789,
            "p": "50000.0",
            "q": "0.5",
            "T": 1700000000001,
            "m": True,
        }
        result = map_agg_trade(raw)
        assert result["event_time"] == 1700000000000
        assert result["symbol"] == "BTCUSDT"
        assert result["agg_trade_id"] == 123456789
        assert result["price"] == 50000.0
        assert result["quantity"] == 0.5
        assert result["trade_time"] == 1700000000001
        assert result["is_buyer_maker"] is True

    def test_buyer_maker_false(self):
        """is_buyer_maker correctly maps False."""
        raw = {"E": 1, "s": "X", "a": 1, "p": "1", "q": "1", "T": 1, "m": False}
        assert map_agg_trade(raw)["is_buyer_maker"] is False


@pytest.mark.unit
class TestMapKline:

    def test_basic_mapping(self):
        """Canonical kline record has all expected fields."""
        raw = {
            "E": 1700000000000,
            "s": "BTCUSDT",
            "k": {
                "t": 1700000000000,
                "T": 1700000059999,
                "i": "1m",
                "o": "50000.0",
                "h": "50100.0",
                "l": "49900.0",
                "c": "50050.0",
                "v": "10.5",
                "q": "525000.0",
                "n": 1000,
                "x": True,
            },
        }
        result = map_kline(raw)
        assert result["event_time"] == 1700000000000
        assert result["symbol"] == "BTCUSDT"
        assert result["kline_start"] == 1700000000000
        assert result["kline_close"] == 1700000059999
        assert result["interval"] == "1m"
        assert result["open"] == 50000.0
        assert result["high"] == 50100.0
        assert result["low"] == 49900.0
        assert result["close"] == 50050.0
        assert result["volume"] == 10.5
        assert result["quote_volume"] == 525000.0
        assert result["trade_count"] == 1000
        assert result["is_closed"] is True

    def test_unclosed_kline(self):
        """is_closed correctly maps False for open klines."""
        raw = {
            "E": 1, "s": "X",
            "k": {"t": 0, "T": 0, "i": "1m", "o": "0", "h": "0",
                   "l": "0", "c": "0", "v": "0", "q": "0", "n": 0, "x": False},
        }
        assert map_kline(raw)["is_closed"] is False


@pytest.mark.unit
class TestMapDepth:

    def test_basic_mapping(self):
        """Canonical depth record has JSON-encoded bids/asks."""
        raw = {
            "E": 1700000000000,
            "s": "btcusdt",
            "lastUpdateId": 12345,
            "bids": [["50000.0", "1.0"], ["49999.0", "2.0"]],
            "asks": [["50001.0", "0.5"], ["50002.0", "1.5"]],
        }
        result = map_depth(raw)
        assert result["event_time"] == 1700000000000
        assert result["symbol"] == "BTCUSDT"
        assert result["last_update_id"] == 12345
        bids = json.loads(result["bids"])
        asks = json.loads(result["asks"])
        assert bids == [[50000.0, 1.0], [49999.0, 2.0]]
        assert asks == [[50001.0, 0.5], [50002.0, 1.5]]

    def test_alternative_keys(self):
        """Handles 'b'/'a' keys used by combined stream format."""
        raw = {
            "s": "ETHUSDT",
            "u": 67890,
            "b": [["3000.0", "5.0"]],
            "a": [["3001.0", "3.0"]],
        }
        result = map_depth(raw)
        assert result["last_update_id"] == 67890
        bids = json.loads(result["bids"])
        assert bids == [[3000.0, 5.0]]

    def test_empty_bids_asks(self):
        """Handles missing or empty bids/asks."""
        raw = {"s": "TEST"}
        result = map_depth(raw)
        assert json.loads(result["bids"]) == []
        assert json.loads(result["asks"]) == []

    def test_event_time_fallback(self):
        """Falls back to current time when E is missing."""
        import time
        before = int(time.time() * 1000)
        raw = {"s": "TEST"}
        result = map_depth(raw)
        after = int(time.time() * 1000)
        assert before <= result["event_time"] <= after
