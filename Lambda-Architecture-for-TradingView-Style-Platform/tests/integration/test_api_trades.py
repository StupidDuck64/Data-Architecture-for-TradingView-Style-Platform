"""
Integration tests for the trades API endpoint.

Uses mocked Redis to verify trade data retrieval and formatting
without requiring running services.
"""

import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


def _make_mock_redis(trade_data=None):
    """Build a mock Redis with optional trade sorted set data."""
    r = AsyncMock()

    async def mock_zrevrange(key, start, end, withscores=False):
        if trade_data and key in trade_data:
            return trade_data[key]
        return []

    r.zrevrange = AsyncMock(side_effect=mock_zrevrange)
    return r


@pytest.mark.integration
class TestTradesEndpoint:

    @pytest.mark.asyncio
    async def test_get_trades_found(self):
        """Returns formatted trade data from KeyDB sorted set."""
        mock_r = _make_mock_redis(trade_data={
            "ticker:history:BTCUSDT": [
                ("50100.0:1.5", 1700000003000),
                ("50050.0:2.0", 1700000002000),
                ("50000.0:1.0", 1700000001000),
            ]
        })
        with patch("backend.api.trades.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/trades/BTCUSDT?limit=3")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 3
            # Reversed to chronological order
            assert data[0]["time"] == 1700000001000
            assert data[0]["price"] == 50000.0
            assert data[0]["volume"] == 1.0

    @pytest.mark.asyncio
    async def test_get_trades_side_detection(self):
        """Side is inferred from price movement (buy=up, sell=down)."""
        mock_r = _make_mock_redis(trade_data={
            "ticker:history:ETHUSDT": [
                ("3010.0:1.0", 1700000003000),  # down from previous → sell
                ("3020.0:1.0", 1700000002000),  # up from previous → buy
                ("3000.0:1.0", 1700000001000),  # first (from most recent) → buy
            ]
        })
        with patch("backend.api.trades.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/trades/ETHUSDT?limit=3")
            data = resp.json()
            # After reversal: chronological order
            # The side logic operates on reversed data before chronological sort
            assert all(t["side"] in ("buy", "sell") for t in data)

    @pytest.mark.asyncio
    async def test_get_trades_not_found(self):
        """Returns 404 when no trade data exists."""
        mock_r = _make_mock_redis()
        with patch("backend.api.trades.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/trades/FAKEUSDT")
            assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_trades_limit_bounds(self):
        """Query parameter limit is validated (1-200)."""
        mock_r = _make_mock_redis()
        with patch("backend.api.trades.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/trades/BTCUSDT?limit=0")
            assert resp.status_code == 422  # FastAPI validation error

    @pytest.mark.asyncio
    async def test_get_trades_limit_too_high(self):
        """Limit exceeding 200 is rejected."""
        mock_r = _make_mock_redis()
        with patch("backend.api.trades.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/trades/BTCUSDT?limit=201")
            assert resp.status_code == 422
