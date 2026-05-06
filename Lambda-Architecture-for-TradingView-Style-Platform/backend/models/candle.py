"""Pydantic models for OHLCV candle data."""

from pydantic import BaseModel


class CandleResponse(BaseModel):
    """Single OHLCV candle as returned by /api/klines and /api/klines/historical."""
    openTime: int
    open: float
    high: float
    low: float
    close: float
    volume: float
