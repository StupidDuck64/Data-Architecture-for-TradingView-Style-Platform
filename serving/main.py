from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from serving.config import CORS_ORIGINS
from serving.connections import close_all, get_redis, get_influx
from serving.routers import ticker, klines, orderbook, trades, symbols, indicators, ws


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_all()


app = FastAPI(title="CryptoDashboard API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for router in (
    ticker.router, klines.router, orderbook.router,
    trades.router, symbols.router, indicators.router, ws.router,
):
    app.include_router(router)


@app.get("/api/health")
async def health():
    checks = {}
    try:
        r = await get_redis()
        await r.ping()
        checks["keydb"] = "ok"
    except Exception as e:
        checks["keydb"] = str(e)
    try:
        get_influx().ping()
        checks["influxdb"] = "ok"
    except Exception as e:
        checks["influxdb"] = str(e)
    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    return {"status": status, "checks": checks}
