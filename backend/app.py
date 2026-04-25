"""
FastAPI application entry point.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.core.config import CORS_ORIGINS
from backend.core.database import close_all
from backend.api import (
    health,
    ticker,
    klines,
    historical,
    orderbook,
    trades,
    symbols,
    indicators,
    websocket,
)


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

for router_module in (
    health,
    ticker,
    klines,
    historical,
    orderbook,
    trades,
    symbols,
    indicators,
    websocket,
):
    app.include_router(router_module.router)
