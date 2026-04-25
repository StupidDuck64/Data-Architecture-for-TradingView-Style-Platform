from fastapi import APIRouter

from backend.core.database import get_redis

router = APIRouter(prefix="/api", tags=["symbols"])


@router.get("/symbols")
async def get_symbols():
    """
    List all active symbols by scanning ``ticker:latest:*`` keys in KeyDB.

    Returns the shape expected by the React frontend:
    ``[{"symbol": "BTCUSDT", "name": "BTC / USDT", "type": "crypto"}]``
    """
    r = await get_redis()
    symbols = []
    async for key in r.scan_iter(match="ticker:latest:*", count=200):
        sym = key.split(":", 2)[-1]
        if sym.endswith("USDT"):
            base = sym[:-4]
            name = f"{base} / USDT"
        elif sym.endswith("BTC"):
            base = sym[:-3]
            name = f"{base} / BTC"
        else:
            name = sym
        symbols.append({"symbol": sym, "name": name, "type": "crypto"})
    symbols.sort(key=lambda s: s["symbol"])
    return symbols
