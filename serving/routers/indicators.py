from fastapi import APIRouter, HTTPException

from serving.connections import get_redis

router = APIRouter(prefix="/api", tags=["indicators"])


@router.get("/indicators/{symbol}")
async def get_indicators(symbol: str):
    r = await get_redis()
    data = await r.hgetall(f"indicator:latest:{symbol.upper()}")
    if not data:
        raise HTTPException(404, f"No indicator data for {symbol}")
    result: dict = {"symbol": symbol.upper()}
    for field in ("sma20", "sma50", "ema12", "ema26"):
        if field in data:
            result[field] = float(data[field])
    if "timestamp" in data:
        result["timestamp"] = int(float(data["timestamp"]))
    return result
