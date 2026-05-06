"""
Order book API — real-time bid/ask depth data.
"""

from __future__ import annotations

import json
import time

import httpx
from fastapi import APIRouter, HTTPException

from backend.core.database import get_redis
<<<<<<< HEAD
=======
from backend.core.redis_sentinel import get_redis_master
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)

router = APIRouter(prefix="/api", tags=["orderbook"])

_BINANCE_CLIENT = httpx.AsyncClient(timeout=3.0)


async def _fetch_binance_orderbook(symbol: str, limit: int = 50) -> dict | None:
    """Fetch order book from Binance REST API as async fallback."""
    url = "https://api.binance.com/api/v3/depth"
    try:
        resp = await _BINANCE_CLIENT.get(url, params={"symbol": symbol, "limit": limit})
        resp.raise_for_status()
        payload = resp.json()
        bids = [[float(p), float(q)] for p, q in payload.get("bids", [])]
        asks = [[float(p), float(q)] for p, q in payload.get("asks", [])]
        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        return {
            "bids": bids,
            "asks": asks,
            "spread": round(best_ask - best_bid, 8) if (best_bid and best_ask) else 0.0,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "event_time": int(time.time() * 1000),
        }
    except (httpx.HTTPError, ValueError, KeyError):
        return None


@router.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str):
    symbol_u = symbol.upper()
    r = await get_redis()
    data = await r.hgetall(f"orderbook:{symbol_u}")
    if not data:
        ticker = await r.hgetall(f"ticker:latest:{symbol_u}")
        if ticker:
            bid = float(ticker.get("bid", 0) or 0)
            ask = float(ticker.get("ask", 0) or 0)
            event_time = int(float(ticker.get("event_time", 0) or 0))
            if bid > 0 and ask > 0:
                return {
                    "symbol": symbol_u,
                    "bids": [[bid, 0.0]],
                    "asks": [[ask, 0.0]],
                    "spread": round(ask - bid, 8),
                    "best_bid": bid,
                    "best_ask": ask,
                    "event_time": event_time,
                }

        fallback = await _fetch_binance_orderbook(symbol_u)
        if not fallback:
            raise HTTPException(404, f"No order book for {symbol}")

<<<<<<< HEAD
        # Warm cache for clients that poll frequently
        await r.hset(
=======
        # Warm cache for clients that poll frequently - use master for writes
        r_master = await get_redis_master()
        await r_master.hset(
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)
            f"orderbook:{symbol_u}",
            mapping={
                "bids": json.dumps(fallback["bids"]),
                "asks": json.dumps(fallback["asks"]),
                "spread": fallback["spread"],
                "best_bid": fallback["best_bid"],
                "best_ask": fallback["best_ask"],
                "event_time": fallback["event_time"],
            },
        )
        await r.expire(f"orderbook:{symbol_u}", 30)
        return {"symbol": symbol_u, **fallback}

    return {
        "symbol": symbol_u,
        "bids": json.loads(data.get("bids", "[]")),
        "asks": json.loads(data.get("asks", "[]")),
        "spread": float(data.get("spread", 0)),
        "best_bid": float(data.get("best_bid", 0)),
        "best_ask": float(data.get("best_ask", 0)),
        "event_time": int(float(data.get("event_time", 0))),
    }
