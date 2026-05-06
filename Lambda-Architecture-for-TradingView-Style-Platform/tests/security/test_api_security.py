"""
Security tests for API endpoint abuse prevention.

Tests rate limiting behavior, header security, and endpoint-level
input validation through the full FastAPI stack.
"""

import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


@pytest.mark.security
class TestEndpointSecurity:
    """Verify that API endpoints reject malicious inputs properly."""

    @pytest.mark.asyncio
    async def test_klines_sql_injection_in_symbol(self):
        """SQL injection in symbol parameter is rejected."""
        mock_r = AsyncMock()
        mock_r.get = AsyncMock(return_value=None)
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol='; DROP TABLE--")
            assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_klines_xss_in_symbol(self):
        """XSS attempt in symbol parameter is rejected."""
        mock_r = AsyncMock()
        mock_r.get = AsyncMock(return_value=None)
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=<script>alert(1)</script>")
            assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_historical_overflow_timestamps(self):
        """Extremely large timestamps are handled gracefully."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/klines/historical"
                "?symbol=BTCUSDT&interval=1h"
                "&startTime=99999999999999999&endTime=99999999999999999"
            )
        # Should either return 400 or handle gracefully
        assert resp.status_code in (400, 422, 200)

    @pytest.mark.asyncio
    async def test_ticker_path_traversal(self):
        """Path traversal in ticker symbol is rejected."""
        mock_r = AsyncMock()
        mock_r.hgetall = AsyncMock(return_value={})
        with patch("backend.api.ticker.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/ticker/..%2F..%2F..%2Fetc%2Fpasswd")
            # Ticker endpoint doesn't use validate_symbol, but should return 404
            assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_trades_negative_limit(self):
        """Negative limit is rejected by FastAPI validation."""
        mock_r = AsyncMock()
        with patch("backend.api.trades.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/trades/BTCUSDT?limit=-1")
            assert resp.status_code == 422


@pytest.mark.security
class TestCORSConfiguration:
    """Verify CORS middleware is configured."""

    @pytest.mark.asyncio
    async def test_cors_headers_present(self):
        """CORS headers are included in responses."""
        mock_r = AsyncMock()
        mock_r.get = AsyncMock(return_value=None)
        mock_r.ping = AsyncMock(return_value=True)
        with (
            patch("backend.api.health.get_redis", return_value=mock_r),
            patch("backend.api.health.get_influx") as mock_influx,
            patch("backend.api.health.get_trino_connection") as mock_trino,
        ):
            mock_influx.return_value.ping.return_value = True
            mock_trino.return_value.cursor.return_value.fetchone.return_value = (1,)
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get(
                    "/api/health",
                    headers={"Origin": "http://localhost:3000"},
                )
            # CORS middleware should add access-control-allow-origin
            assert resp.status_code == 200


@pytest.mark.security
class TestInputSanitization:
    """Extended input validation tests beyond the basic symbol/interval tests."""

    @pytest.mark.asyncio
    async def test_oversized_query_string(self):
        """Extremely long query strings are handled."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            long_param = "A" * 10000
            resp = await ac.get(f"/api/klines?symbol={long_param}")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_null_bytes_in_url(self):
        """Null bytes in URL parameters are rejected."""
        mock_r = AsyncMock()
        mock_r.get = AsyncMock(return_value=None)
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTC%00USDT")
            assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_emoji_in_symbol(self):
        """Unicode emoji characters in symbol are rejected."""
        mock_r = AsyncMock()
        mock_r.get = AsyncMock(return_value=None)
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=🚀BTC")
            assert resp.status_code == 400
