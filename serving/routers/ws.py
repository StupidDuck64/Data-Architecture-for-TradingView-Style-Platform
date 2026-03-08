import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from serving.connections import get_redis

router = APIRouter(prefix="/api", tags=["websocket"])
log = logging.getLogger(__name__)

INTERVAL_SECONDS = {
    "1s": 1, "1m": 60, "5m": 300, "15m": 900,
    "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800,
}


@router.websocket("/stream")
async def stream(websocket: WebSocket, symbol: str = "", interval: str = "1m"):
    """
    Real-time candle streaming over WebSocket.

    The frontend connects with:
        ``ws://host/api/stream?symbol=BTCUSDT&interval=1m``

    Each frame: ``{"openTime": ms, "open": float, "high": float, ...}``
    """
    await websocket.accept()
    r = await get_redis()
    symbol = symbol.upper()
    target_sec = INTERVAL_SECONDS.get(interval, 60)
    target_ms = target_sec * 1000
    last_sent: dict | None = None

    try:
        while True:
            candle = await _build_candle(r, symbol, interval, target_ms)
            if candle and candle != last_sent:
                await websocket.send_json(candle)
                last_sent = candle
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("WebSocket error for %s: %s", symbol, e)


async def _build_candle(r, symbol: str, interval: str, target_ms: int) -> dict | None:
    """Read the current candle from KeyDB, aggregating if interval > 1m."""
    if interval == "1m":
        data = await r.hgetall(f"candle:latest:{symbol}")
        if not data:
            return None
        return {
            "openTime": int(data["kline_start"]),
            "open": float(data["open"]),
            "high": float(data["high"]),
            "low": float(data["low"]),
            "close": float(data["close"]),
            "volume": float(data["volume"]),
        }

    # For larger intervals, aggregate 1m candles from the history sorted set
    now_ms = int(time.time() * 1000)
    window_start = (now_ms // target_ms) * target_ms
    raw = await r.zrangebyscore(f"candle:history:{symbol}", window_start, "+inf")

    if raw:
        candles = [json.loads(c) for c in raw]
        return {
            "openTime": window_start,
            "open": candles[0]["o"],
            "high": max(c["h"] for c in candles),
            "low": min(c["l"] for c in candles),
            "close": candles[-1]["c"],
            "volume": round(sum(c["v"] for c in candles), 8),
        }

    # Fallback: use the latest 1m candle mapped to the target window
    data = await r.hgetall(f"candle:latest:{symbol}")
    if data:
        kline_start = int(data["kline_start"])
        return {
            "openTime": (kline_start // target_ms) * target_ms,
            "open": float(data["open"]),
            "high": float(data["high"]),
            "low": float(data["low"]),
            "close": float(data["close"]),
            "volume": float(data["volume"]),
        }
    return None
