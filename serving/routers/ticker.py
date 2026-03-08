from fastapi import APIRouter, HTTPException

from serving.connections import get_redis

router = APIRouter(prefix="/api", tags=["ticker"])


@router.get("/ticker/{symbol}")
async def get_ticker(symbol: str):
    r = await get_redis()
    data = await r.hgetall(f"ticker:latest:{symbol.upper()}")
    if not data:
        raise HTTPException(404, f"No ticker data for {symbol}")
    return {
        "symbol": symbol.upper(),
        "price": float(data.get("price", 0)),
        "bid": float(data.get("bid", 0)),
        "ask": float(data.get("ask", 0)),
        "volume": float(data.get("volume", 0)),
        "event_time": int(float(data.get("event_time", 0))),
    }


@router.get("/ticker")
async def get_all_tickers():
    r = await get_redis()
    result = []
    async for key in r.scan_iter(match="ticker:latest:*", count=200):
        symbol = key.split(":", 2)[-1]
        data = await r.hgetall(key)
        if data:
            result.append({
                "symbol": symbol,
                "price": float(data.get("price", 0)),
                "bid": float(data.get("bid", 0)),
                "ask": float(data.get("ask", 0)),
                "volume": float(data.get("volume", 0)),
                "event_time": int(float(data.get("event_time", 0))),
            })
    result.sort(key=lambda t: t["symbol"])
    return result
