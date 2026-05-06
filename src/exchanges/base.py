"""
Abstract base class for exchange data source integrations.

To add a new exchange (e.g., OKX), create a subclass implementing
all abstract methods under ``src/exchanges/okx/client.py``.
"""

from abc import ABC, abstractmethod


class ExchangeClient(ABC):
    """Blueprint for any exchange data source.

    Each exchange must provide:
    - REST methods:  fetch active trading symbols, fetch historical klines
    - WS URL builders: construct WebSocket URLs for each stream type
    - Mappers: convert raw exchange-specific JSON → canonical records
    """

    @abstractmethod
    def fetch_symbols(self, quote_asset: str = "USDT") -> list[str]:
        """Return a sorted list of active spot trading pair symbols."""

    @abstractmethod
    def fetch_klines(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        interval: str = "1m",
        batch_limit: int = 1000,
    ) -> list[list]:
        """Fetch OHLCV klines from exchange REST API with auto-pagination.

        Returns raw kline rows as lists (exchange-native format).
        """

    @abstractmethod
    def fetch_first_available_start(self, symbol: str) -> int:
        """Return the earliest available candle open time (ms) for a symbol."""

    # ── WebSocket URL builders ───────────────────────────────────────────────

    @abstractmethod
    def build_ticker_stream_url(self) -> str:
        """Return the WebSocket URL for the all-ticker stream."""

    @abstractmethod
    def build_combined_stream_url(self, streams: list[str]) -> str:
        """Return a combined WebSocket URL for the given stream names."""

    # ── Data mappers (raw exchange JSON → canonical record) ──────────────────

    @abstractmethod
    def map_ticker(self, raw: dict) -> dict:
        """Convert a raw ticker message to a canonical ticker record."""

    @abstractmethod
    def map_trade(self, raw: dict) -> dict:
        """Convert a raw aggregate trade to a canonical trade record."""

    @abstractmethod
    def map_kline(self, raw: dict) -> dict:
        """Convert a raw kline event to a canonical kline record."""

    @abstractmethod
    def map_depth(self, raw: dict) -> dict:
        """Convert a raw depth snapshot to a canonical depth record."""

    # ── Stream name builders ─────────────────────────────────────────────────

    @abstractmethod
    def trade_stream_name(self, symbol: str) -> str:
        """Return the stream name for aggregate trades (e.g., 'btcusdt@aggTrade')."""

    @abstractmethod
    def kline_stream_name(self, symbol: str, interval: str) -> str:
        """Return the stream name for kline updates."""

    @abstractmethod
    def depth_stream_name(self, symbol: str, level: str, update_ms: str) -> str:
        """Return the stream name for partial order book depth."""
