import json

from fastapi import APIRouter, HTTPException

from serving.connections import get_redis

router = APIRouter(prefix="/api", tags=["orderbook"])


@router.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str):
    r = await get_redis()
    data = await r.hgetall(f"orderbook:{symbol.upper()}")
    if not data:
        raise HTTPException(404, f"No order book for {symbol}")
    return {
        "symbol": symbol.upper(),
        "bids": json.loads(data.get("bids", "[]")),
        "asks": json.loads(data.get("asks", "[]")),
        "spread": float(data.get("spread", 0)),
        "best_bid": float(data.get("best_bid", 0)),
        "best_ask": float(data.get("best_ask", 0)),
        "event_time": int(float(data.get("event_time", 0))),
    }
