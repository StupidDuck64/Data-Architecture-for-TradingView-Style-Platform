"""
Integration tests for the klines API endpoint.

Validates request parameter validation and caching behavior
with mocked data sources.
"""

import json
import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from backend.app import app


def _make_mock_redis(cached_data=None, candle_data=None, ticker_data=None):
    """Build a mock Redis with optional cache and candle data."""
    r = AsyncMock()
    _store = {}

    async def mock_get(key):
        if cached_data and key in cached_data:
            return cached_data[key]
        return _store.get(key)

    async def mock_set(key, value):
        _store[key] = value

    async def mock_pexpire(key, ms):
        pass

    async def mock_zrangebyscore(key, min_score, max_score):
        if candle_data and key in candle_data:
            return candle_data[key]
        return []

    async def mock_zrevrange(key, start, end, withscores=False):
        if candle_data and key in candle_data:
            return candle_data[key]
        return []

    async def mock_hgetall(key):
        if ticker_data and key in ticker_data:
            return ticker_data[key]
        return {}

    r.get = AsyncMock(side_effect=mock_get)
    r.set = AsyncMock(side_effect=mock_set)
    r.pexpire = AsyncMock(side_effect=mock_pexpire)
    r.zrangebyscore = AsyncMock(side_effect=mock_zrangebyscore)
    r.zrevrange = AsyncMock(side_effect=mock_zrevrange)
    r.hgetall = AsyncMock(side_effect=mock_hgetall)
    r.pipeline = lambda: _MockPipeline()
    return r


class _MockPipeline:
    """Minimal pipeline mock for cache set + pexpire."""
    def __init__(self):
        self._ops = []

    def set(self, key, value):
        self._ops.append(("set", key, value))
        return self

    def pexpire(self, key, ms):
        self._ops.append(("pexpire", key, ms))
        return self

    async def execute(self):
        return [True] * len(self._ops)


@pytest.mark.integration
class TestKlinesEndpoint:

    @pytest.mark.asyncio
    async def test_klines_invalid_symbol(self):
        """Returns 400 for invalid symbol containing special chars."""
        mock_r = _make_mock_redis()
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTC/USDT")
            assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_klines_invalid_interval(self):
        """Returns 400 for unsupported interval."""
        mock_r = _make_mock_redis()
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTCUSDT&interval=2m")
            assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_klines_limit_validation(self):
        """Returns 422 for limit outside bounds."""
        mock_r = _make_mock_redis()
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTCUSDT&limit=0")
            assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_klines_limit_too_high(self):
        """Returns 422 for limit exceeding 1500."""
        mock_r = _make_mock_redis()
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTCUSDT&limit=1501")
            assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_klines_cache_hit(self):
        """Returns cached data when available and no endTime specified."""
        cached = json.dumps([
            {"openTime": 1000, "open": 100, "high": 110, "low": 90, "close": 105, "volume": 50}
        ])
        mock_r = _make_mock_redis(cached_data={
            "klines_cache:BTCUSDT:1m:200": cached,
        })
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTCUSDT")
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 1
            assert data[0]["openTime"] == 1000

    @pytest.mark.asyncio
    async def test_klines_1s_from_keydb(self):
        """1s candles are served exclusively from KeyDB sorted set."""
        candle = json.dumps({"t": 1700000000000, "o": 50000, "h": 50100, "l": 49900, "c": 50050, "v": 1.5})
        mock_r = _make_mock_redis(
            candle_data={"candle:1s:BTCUSDT": [candle]},
        )
        with patch("backend.api.klines.get_redis", return_value=mock_r):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                resp = await ac.get("/api/klines?symbol=BTCUSDT&interval=1s&limit=1")
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_klines_missing_symbol_param(self):
        """Returns 422 when required symbol param is missing."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/api/klines")
        assert resp.status_code == 422
