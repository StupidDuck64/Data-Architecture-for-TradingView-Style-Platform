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
    """Build the latest candle by merging Flink aggregate data with the
    real-time ticker price.

    The kline Flink pipeline processes ~400 symbols and can lag several
    minutes behind wall-clock time.  ``ticker:latest`` is near-real-time
    (seconds old) so we use it to keep the chart's live candle agitating.
    """
    # ── Read the real-time ticker price (near-zero lag) ──────────────
    ticker = await r.hgetall(f"ticker:latest:{symbol}")
    live_price = float(ticker["price"]) if ticker.get("price") else None
    live_ts = int(ticker["event_time"]) if ticker.get("event_time") else None

    # ── 1s interval ──────────────────────────────────────────────────
    if interval == "1s":
        raw = await r.zrevrange(f"candle:1s:{symbol}", 0, 0)
        if raw:
            c = json.loads(raw[0])
            candle = {
                "openTime": int(c["t"]),
                "open": c["o"], "high": c["h"],
                "low": c["l"], "close": c["c"],
                "volume": c["v"],
            }
        else:
            candle = None

        # If ticker is newer (different second), synthesise a 1s candle
        if live_price and live_ts:
            live_sec = (live_ts // 1000) * 1000  # align to second
            if candle is None or live_sec > candle["openTime"]:
                return {
                    "openTime": live_sec,
                    "open": live_price, "high": live_price,
                    "low": live_price, "close": live_price,
                    "volume": 0,
                }
            # Same second → enrich Flink candle with live price
            if live_sec == candle["openTime"]:
                candle["close"] = live_price
                candle["high"] = max(candle["high"], live_price)
                candle["low"] = min(candle["low"], live_price)
        return candle

    # ── 1m and larger ────────────────────────────────────────────────
    # Aggregate from the appropriate source sorted set.
    # Use data-driven window: find the latest candle's timestamp and aggregate
    # the window it belongs to.
    source_key = f"candle:1s:{symbol}" if interval == "1m" else f"candle:1m:{symbol}"
    latest = await r.zrevrange(source_key, 0, 0, withscores=True)

    flink_candle = None
    flink_window = 0
    if latest:
        latest_score = int(latest[0][1])
        flink_window = (latest_score // target_ms) * target_ms
        raw = await r.zrangebyscore(
            source_key, flink_window, flink_window + target_ms - 1,
        )
        if raw:
            candles = [json.loads(c) for c in raw]
            flink_candle = {
                "openTime": flink_window,
                "open": candles[0]["o"],
                "high": max(c["h"] for c in candles),
                "low": min(c["l"] for c in candles),
                "close": candles[-1]["c"],
                "volume": round(sum(c["v"] for c in candles), 8),
            }

    # Merge with real-time ticker
    if live_price and live_ts:
        live_window = (live_ts // target_ms) * target_ms
        if flink_candle and live_window == flink_window:
            # Same window → update close / extend high-low from ticker
            flink_candle["close"] = live_price
            flink_candle["high"] = max(flink_candle["high"], live_price)
            flink_candle["low"] = min(flink_candle["low"], live_price)
            return flink_candle
        if live_window > flink_window:
            # Ticker is in a newer window → synthesise a candle from ticker price
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
