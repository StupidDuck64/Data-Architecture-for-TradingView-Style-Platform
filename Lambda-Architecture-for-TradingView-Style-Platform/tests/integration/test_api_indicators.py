"""
Integration tests for the indicators API endpoint.

Uses mocked Redis to verify indicator data retrieval
without requiring running services.
"""

import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


def _make_mock_redis(indicator_data=None):
    """Build a mock Redis with optional indicator hash data."""
    r = AsyncMock()

    async def mock_hgetall(key):
        if indicator_data and key in indicator_data:
            return indicator_data[key]
        return {}

    r.hgetall = AsyncMock(side_effect=mock_hgetall)
    return r


@pytest.mark.integration
class TestIndicatorsEndpoint:

    @pytest.mark.asyncio
    async def test_get_indicators_full(self):
        """Returns all indicator fields when available."""
        mock_r = _make_mock_redis(indicator_data={
            "indicator:latest:BTCUSDT": {
                "sma20": "50000.0",
                "sma50": "48000.0",
                "ema12": "50500.0",
                "ema26": "49000.0",
                "timestamp": "1700000000000",
            }
        })
        with patch("backend.api.indicators.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/indicators/BTCUSDT")
            assert resp.status_code == 200
            data = resp.json()
            assert data["symbol"] == "BTCUSDT"
            assert data["sma20"] == 50000.0
            assert data["sma50"] == 48000.0
            assert data["ema12"] == 50500.0
            assert data["ema26"] == 49000.0
            assert data["timestamp"] == 1700000000000

    @pytest.mark.asyncio
    async def test_get_indicators_partial(self):
        """Returns only available indicator fields."""
        mock_r = _make_mock_redis(indicator_data={
            "indicator:latest:ETHUSDT": {
                "sma20": "3000.0",
            }
        })
        with patch("backend.api.indicators.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/indicators/ETHUSDT")
            data = resp.json()
            assert data["sma20"] == 3000.0
            assert "sma50" not in data

    @pytest.mark.asyncio
    async def test_get_indicators_not_found(self):
        """Returns 404 when no indicator data exists."""
        mock_r = _make_mock_redis()
        with patch("backend.api.indicators.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/indicators/FAKEUSDT")
            assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_indicators_case_insensitive(self):
        """Symbol is normalized to uppercase."""
        mock_r = _make_mock_redis(indicator_data={
            "indicator:latest:BTCUSDT": {"sma20": "50000.0"}
        })
        with patch("backend.api.indicators.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/indicators/btcusdt")
            assert resp.status_code == 200
            assert resp.json()["symbol"] == "BTCUSDT"
