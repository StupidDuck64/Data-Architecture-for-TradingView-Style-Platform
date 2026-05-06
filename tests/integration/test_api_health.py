"""
Integration tests for the health check API endpoint.

Uses mocked database connections to verify endpoint behavior
without requiring running services.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


@pytest.fixture
def mock_redis():
    """Mock Redis connection that responds to ping."""
    r = AsyncMock()
    r.ping = AsyncMock(return_value=True)
    return r


@pytest.fixture
def mock_influx():
    """Mock InfluxDB client that responds to ping."""
    client = MagicMock()
    client.ping.return_value = True
    return client


@pytest.fixture
def mock_trino():
    """Mock Trino connection that executes a simple query."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    conn.cursor.return_value = cursor
    return conn


@pytest.mark.integration
class TestHealthEndpoint:

    @pytest.mark.asyncio
    async def test_health_all_ok(self, mock_redis, mock_influx, mock_trino):
        """Health endpoint returns 'ok' when all services are reachable."""
        with (
            patch("backend.api.health.get_redis", return_value=mock_redis),
            patch("backend.api.health.get_influx", return_value=mock_influx),
            patch("backend.api.health.get_trino_connection", return_value=mock_trino),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"
            assert data["checks"]["keydb"] == "ok"
            assert data["checks"]["influxdb"] == "ok"
            assert data["checks"]["trino"] == "ok"
            assert "total_latency_ms" in data
            assert "uptime_sec" in data
            assert "checked_at" in data

    @pytest.mark.asyncio
    async def test_health_degraded_redis_down(self, mock_influx, mock_trino):
        """Health endpoint returns 'degraded' when Redis is unreachable."""
        bad_redis = AsyncMock()
        bad_redis.ping = AsyncMock(side_effect=ConnectionError("Redis down"))
        with (
            patch("backend.api.health.get_redis", return_value=bad_redis),
            patch("backend.api.health.get_influx", return_value=mock_influx),
            patch("backend.api.health.get_trino_connection", return_value=mock_trino),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/health")
            data = resp.json()
            assert data["status"] == "degraded"
            assert "Redis down" in data["checks"]["keydb"]

    @pytest.mark.asyncio
    async def test_health_degraded_influx_down(self, mock_redis, mock_trino):
        """Health endpoint returns 'degraded' when InfluxDB is unreachable."""
        bad_influx = MagicMock()
        bad_influx.ping.side_effect = ConnectionError("InfluxDB timeout")
        with (
            patch("backend.api.health.get_redis", return_value=mock_redis),
            patch("backend.api.health.get_influx", return_value=bad_influx),
            patch("backend.api.health.get_trino_connection", return_value=mock_trino),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/health")
            data = resp.json()
            assert data["status"] == "degraded"
            assert "InfluxDB timeout" in data["checks"]["influxdb"]

    @pytest.mark.asyncio
    async def test_health_degraded_trino_down(self, mock_redis, mock_influx):
        """Health endpoint returns 'degraded' when Trino is unreachable."""
        bad_trino = MagicMock()
        bad_trino.cursor.side_effect = ConnectionError("Trino unreachable")
        with (
            patch("backend.api.health.get_redis", return_value=mock_redis),
            patch("backend.api.health.get_influx", return_value=mock_influx),
            patch("backend.api.health.get_trino_connection", return_value=bad_trino),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/health")
            data = resp.json()
            assert data["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_health_latency_present(self, mock_redis, mock_influx, mock_trino):
        """Health response includes per-service latency metrics."""
        with (
            patch("backend.api.health.get_redis", return_value=mock_redis),
            patch("backend.api.health.get_influx", return_value=mock_influx),
            patch("backend.api.health.get_trino_connection", return_value=mock_trino),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/health")
            data = resp.json()
            assert "keydb_ms" in data["latency_ms"]
            assert "influxdb_ms" in data["latency_ms"]
            assert "trino_ms" in data["latency_ms"]
            assert all(v >= 0 for v in data["latency_ms"].values())
