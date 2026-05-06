"""
Integration tests for the ticker API endpoints.

Uses mocked Redis to verify endpoint behavior without requiring
running services.
"""

import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


def _make_mock_redis(ticker_data=None, keys=None):
    """Build a mock Redis instance with configurable ticker data."""
    r = AsyncMock()

    async def mock_hgetall(key):
        if ticker_data and key in ticker_data:
            return ticker_data[key]
        return {}

    r.hgetall = AsyncMock(side_effect=mock_hgetall)

    async def mock_scan_iter(match=None, count=200):
        for key in (keys or []):
            yield key

    r.scan_iter = mock_scan_iter
    return r


@pytest.mark.integration
class TestTickerEndpoint:

    @pytest.mark.asyncio
    async def test_get_ticker_found(self):
        """Returns ticker data when symbol exists in KeyDB."""
        mock_r = _make_mock_redis(ticker_data={
            "ticker:latest:BTCUSDT": {
                "price": "50000.0", "change24h": "2.5",
                "bid": "49999.0", "ask": "50001.0",
                "volume": "1000000.0", "event_time": "1700000000000",
            }
        })
        with patch("backend.api.ticker.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ticker/BTCUSDT")
            assert resp.status_code == 200
            data = resp.json()
            assert data["symbol"] == "BTCUSDT"
            assert data["price"] == 50000.0
            assert data["change24h"] == 2.5
            assert data["bid"] == 49999.0
            assert data["ask"] == 50001.0

    @pytest.mark.asyncio
    async def test_get_ticker_not_found(self):
        """Returns 404 when symbol has no ticker data."""
        mock_r = _make_mock_redis()
        with patch("backend.api.ticker.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ticker/FAKEUSDT")
            assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_ticker_case_insensitive(self):
        """Symbol is normalized to uppercase."""
        mock_r = _make_mock_redis(ticker_data={
            "ticker:latest:ETHUSDT": {
                "price": "3000.0", "change24h": "1.0",
                "bid": "0", "ask": "0", "volume": "0",
                "event_time": "0",
            }
        })
        with patch("backend.api.ticker.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ticker/ethusdt")
            assert resp.status_code == 200
            assert resp.json()["symbol"] == "ETHUSDT"

    @pytest.mark.asyncio
    async def test_get_all_tickers(self):
        """Returns all tickers sorted alphabetically."""
        mock_r = _make_mock_redis(
            ticker_data={
                "ticker:latest:BTCUSDT": {
                    "price": "50000.0", "change24h": "0",
                    "bid": "0", "ask": "0", "volume": "0", "event_time": "0",
                },
                "ticker:latest:ETHUSDT": {
                    "price": "3000.0", "change24h": "0",
                    "bid": "0", "ask": "0", "volume": "0", "event_time": "0",
                },
            },
            keys=["ticker:latest:ETHUSDT", "ticker:latest:BTCUSDT"],
        )
        with patch("backend.api.ticker.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ticker")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 2
            assert data[0]["symbol"] == "BTCUSDT"
            assert data[1]["symbol"] == "ETHUSDT"

    @pytest.mark.asyncio
    async def test_get_all_tickers_empty(self):
        """Returns empty list when no tickers exist."""
        mock_r = _make_mock_redis()
        with patch("backend.api.ticker.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ticker")
            assert resp.status_code == 200
            assert resp.json() == []
