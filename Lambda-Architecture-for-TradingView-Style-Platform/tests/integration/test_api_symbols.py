"""
Integration tests for the symbols API endpoint.

Uses mocked Redis to verify the symbols endpoint correctly
extracts and formats symbol data from KeyDB scan results.
"""

import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


def _make_mock_redis(keys=None):
    """Build a mock Redis that yields specified keys on scan."""
    r = AsyncMock()

    async def mock_scan_iter(match=None, count=200):
        for key in (keys or []):
            yield key

    r.scan_iter = mock_scan_iter
    return r


@pytest.mark.integration
class TestSymbolsEndpoint:

    @pytest.mark.asyncio
    async def test_symbols_usdt_pairs(self):
        """USDT pairs are formatted as 'BASE / USDT'."""
        mock_r = _make_mock_redis(keys=[
            "ticker:latest:BTCUSDT",
            "ticker:latest:ETHUSDT",
        ])
        with patch("backend.api.symbols.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/symbols")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 2
            # Sorted alphabetically
            assert data[0]["symbol"] == "BTCUSDT"
            assert data[0]["name"] == "BTC / USDT"
            assert data[0]["type"] == "crypto"

    @pytest.mark.asyncio
    async def test_symbols_btc_pairs(self):
        """BTC pairs are formatted as 'BASE / BTC'."""
        mock_r = _make_mock_redis(keys=["ticker:latest:ETHBTC"])
        with patch("backend.api.symbols.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/symbols")
            data = resp.json()
            assert len(data) == 1
            assert data[0]["name"] == "ETH / BTC"

    @pytest.mark.asyncio
    async def test_symbols_unknown_quote(self):
        """Non-USDT/BTC pairs use full symbol as name."""
        mock_r = _make_mock_redis(keys=["ticker:latest:ETHEUR"])
        with patch("backend.api.symbols.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/symbols")
            data = resp.json()
            assert data[0]["name"] == "ETHEUR"

    @pytest.mark.asyncio
    async def test_symbols_sorted(self):
        """Symbols are returned in alphabetical order."""
        mock_r = _make_mock_redis(keys=[
            "ticker:latest:XRPUSDT",
            "ticker:latest:ADAUSDT",
            "ticker:latest:BTCUSDT",
        ])
        with patch("backend.api.symbols.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/symbols")
            data = resp.json()
            symbols = [s["symbol"] for s in data]
            assert symbols == sorted(symbols)

    @pytest.mark.asyncio
    async def test_symbols_empty(self):
        """Returns empty list when no tickers exist."""
        mock_r = _make_mock_redis()
        with patch("backend.api.symbols.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/symbols")
            assert resp.json() == []
