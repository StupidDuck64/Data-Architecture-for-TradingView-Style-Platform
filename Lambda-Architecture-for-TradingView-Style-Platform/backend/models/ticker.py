"""Pydantic models for ticker, orderbook, and trade data."""

from typing import Optional

from pydantic import BaseModel


class TickerResponse(BaseModel):
    symbol: str
    price: float
    change24h: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    volume: float = 0.0
    event_time: int = 0


class OrderBookEntry(BaseModel):
    price: float
    quantity: float


class OrderBookResponse(BaseModel):
    symbol: str
    bids: list
    asks: list
    spread: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    event_time: int = 0


class TradeResponse(BaseModel):
    time: int
    price: float
    volume: float
    side: str


class SymbolResponse(BaseModel):
    symbol: str
    name: str
    type: str = "crypto"


class IndicatorResponse(BaseModel):
    symbol: str
    sma20: Optional[float] = None
    sma50: Optional[float] = None
    ema12: Optional[float] = None
    ema26: Optional[float] = None
    timestamp: Optional[int] = None
