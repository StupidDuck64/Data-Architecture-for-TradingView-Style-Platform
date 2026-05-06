"""
WebSocket streaming API for real-time candle updates.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from backend.core.constants import INTERVAL_SECONDS
from backend.core.database import get_redis

router = APIRouter(prefix="/api", tags=["websocket"])
log = logging.getLogger(__name__)


@router.websocket("/stream")
async def stream(websocket: WebSocket, symbol: str = "", interval: str = "1m"):
    """
    Real-time candle streaming over WebSocket.

    The frontend connects with:
        ``ws://host/api/stream?symbol=BTCUSDT&interval=1m``
    """
    await websocket.accept()
    r = await get_redis()
    symbol = symbol.upper()
    interval = interval.strip().lower()
    if interval not in INTERVAL_SECONDS:
        await websocket.close(code=1008)
        return
    target_sec = INTERVAL_SECONDS[interval]
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
    """Build the latest candle by merging Flink aggregate data with the
    real-time ticker price."""

    # Read the real-time ticker price (near-zero lag)
    ticker = await r.hgetall(f"ticker:latest:{symbol}")
    live_price = float(ticker["price"]) if ticker.get("price") else None
    live_ts = int(ticker["event_time"]) if ticker.get("event_time") else None

    # 1s interval: serve directly from KeyDB
    if interval == "1s":
        raw = await r.zrevrange(f"candle:1s:{symbol}", 0, 0)
        if raw:
            c = json.loads(raw[0])
            return {
                "openTime": int(c["t"]),
                "open": c["o"], "high": c["h"],
                "low": c["l"], "close": c["c"],
                "volume": c["v"],
            }
        return None

    # 1m+: aggregate from the appropriate source sorted set
    source_key = f"candle:1s:{symbol}" if interval == "1m" else f"candle:1m:{symbol}"
    latest = await r.zrevrange(source_key, 0, 0, withscores=True)

    flink_candle = None
    flink_window = 0
    latest_source_ts = 0
    if latest:
        latest_score = int(latest[0][1])
        flink_window = (latest_score // target_ms) * target_ms
        raw = await r.zrangebyscore(
            source_key, flink_window, flink_window + target_ms - 1,
        )
        if raw:
            candles = [json.loads(c) for c in raw]
            latest_source_ts = max(int(c["t"]) for c in candles)
            flink_candle = {
                "openTime": flink_window,
                "open": candles[0]["o"],
                "high": max(c["h"] for c in candles),
                "low": min(c["l"] for c in candles),
                "close": candles[-1]["c"],
                "volume": round(sum(c["v"] for c in candles), 8),
            }

    # Keep 1m candles exchange-consistent: no ticker-based override
    if interval == "1m":
        return flink_candle

    # Merge with real-time ticker for 5m+ only
    if live_price and live_ts:
        live_window = (live_ts // target_ms) * target_ms
        if flink_candle and live_window == flink_window:
            window_close_ms = flink_window + target_ms
            if live_ts > latest_source_ts and int(time.time() * 1000) < window_close_ms:
                flink_candle["close"] = live_price
                flink_candle["high"] = max(flink_candle["high"], live_price)
                flink_candle["low"] = min(flink_candle["low"], live_price)
            return flink_candle
        if live_window > flink_window:
            return {
                "openTime": live_window,
                "open": live_price, "high": live_price,
                "low": live_price, "close": live_price,
                "volume": 0,
            }

    if flink_candle:
        return flink_candle

    # Last resort fallback
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
