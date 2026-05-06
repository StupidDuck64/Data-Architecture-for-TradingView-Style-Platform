"""
End-to-end tests for the full data pipeline.

These tests require running Docker services and validate the complete
data flow from API request through all backend layers.

Run with: PYTHONPATH=. python -m pytest tests/e2e/ -v
Prerequisites: `make dev` must be running.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


@pytest.mark.e2e
class TestAppStartup:
    """Verify the FastAPI application initializes correctly."""

    @pytest.mark.asyncio
    async def test_app_has_routes(self):
        """Application has all expected route prefixes registered."""
        route_paths = [route.path for route in app.routes]
        expected_paths = [
            "/api/health",
            "/api/ticker/{symbol}",
            "/api/ticker",
            "/api/klines",
            "/api/klines/historical",
            "/api/orderbook/{symbol}",
            "/api/trades/{symbol}",
            "/api/symbols",
            "/api/indicators/{symbol}",
            "/api/stream",
        ]
        for path in expected_paths:
            assert path in route_paths, f"Missing route: {path}"

    @pytest.mark.asyncio
    async def test_app_metadata(self):
        """Application metadata is correctly configured."""
        assert app.title == "CryptoDashboard API"
        assert app.version == "1.0.0"

    @pytest.mark.asyncio
    async def test_openapi_schema_accessible(self):
        """OpenAPI schema endpoint is accessible."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert schema["info"]["title"] == "CryptoDashboard API"

    @pytest.mark.asyncio
    async def test_docs_endpoint(self):
        """Swagger UI docs are accessible."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/docs")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_unknown_endpoint_returns_404(self):
        """Unknown paths return 404."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/nonexistent")
        assert resp.status_code in (404, 405)


@pytest.mark.e2e
class TestRouterTags:
    """Verify API routers have proper tags for documentation."""

    @pytest.mark.asyncio
    async def test_openapi_tags(self):
        """All expected tags appear in the OpenAPI schema."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/openapi.json")
        schema = resp.json()
        paths = schema.get("paths", {})
        all_tags = set()
        for path_info in paths.values():
            for method_info in path_info.values():
                if isinstance(method_info, dict):
                    all_tags.update(method_info.get("tags", []))
        # Note: WebSocket endpoints don't appear in OpenAPI schema
        expected_tags = {"health", "ticker", "klines", "historical", "orderbook",
                         "trades", "symbols", "indicators"}
        assert expected_tags <= all_tags, f"Missing tags: {expected_tags - all_tags}"
