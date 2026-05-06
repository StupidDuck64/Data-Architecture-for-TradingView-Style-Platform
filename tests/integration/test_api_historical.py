"""
Integration tests for the historical klines API endpoint.

Validates date range validation logic and parameter boundaries
with mocked data sources.
"""

import pytest
import time
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


def _make_mock_redis():
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    return r


@pytest.mark.integration
class TestHistoricalEndpoint:

    @pytest.mark.asyncio
    async def test_historical_missing_params(self):
        """Returns 422 when required params are missing."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/klines/historical?symbol=BTCUSDT")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_historical_end_before_start(self):
        """Returns 400 when endTime <= startTime."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/klines/historical"
                "?symbol=BTCUSDT&interval=1h&startTime=2000000&endTime=1000000"
            )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_historical_end_equals_start(self):
        """Returns 400 when endTime == startTime."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/klines/historical"
                "?symbol=BTCUSDT&interval=1h&startTime=1000000&endTime=1000000"
            )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_historical_range_too_large(self):
        """Returns 400 when date range exceeds 1 year."""
        now_ms = int(time.time() * 1000)
        start = now_ms - (400 * 24 * 3600 * 1000)  # 400 days ago
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                f"/api/klines/historical"
                f"?symbol=BTCUSDT&interval=1h&startTime={start}&endTime={now_ms}"
            )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_historical_invalid_interval(self):
        """Returns 400 for unsupported interval."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/klines/historical"
                "?symbol=BTCUSDT&interval=2m&startTime=1000000&endTime=2000000"
            )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_historical_invalid_symbol(self):
        """Returns 400 for invalid symbol."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/klines/historical"
                "?symbol=DROP_TABLE&interval=1h&startTime=1000000&endTime=2000000"
            )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_historical_limit_bounds(self):
        """Returns 422 for limit outside 1-5000."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get(
                "/api/klines/historical"
                "?symbol=BTCUSDT&interval=1h&startTime=1000000&endTime=2000000&limit=5001"
            )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_historical_empty_result(self):
        """Returns empty list when no data exists in the range."""
        now_ms = int(time.time() * 1000)
        start = now_ms - (3600 * 1000)  # 1 hour ago
        with (
            patch("backend.api.historical.query_influx_1m_range", return_value=[]),
            patch("backend.api.historical.query_trino_1m", return_value=[]),
            patch("backend.api.historical.query_trino_hourly", return_value=[]),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get(
                    f"/api/klines/historical"
                    f"?symbol=BTCUSDT&interval=1h&startTime={start}&endTime={now_ms}"
                )
            assert resp.status_code == 200
            assert resp.json() == []
