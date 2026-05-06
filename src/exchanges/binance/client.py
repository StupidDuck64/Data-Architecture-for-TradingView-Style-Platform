"""
Binance exchange client implementation.

Implements the ExchangeClient interface for Binance's REST and WebSocket APIs.
All Binance-specific URLs, endpoints, and protocols are encapsulated here.
"""

import logging
import time

import requests

from exchanges.base import ExchangeClient
from exchanges.binance import mappers

log = logging.getLogger(__name__)

# ── Binance constants ────────────────────────────────────────────────────────
REST_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
REST_KLINES_URL    = "https://api.binance.com/api/v3/klines"
WS_TICKER_URL      = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
WS_COMBINED_BASE   = "wss://stream.binance.com:9443/stream"
EPOCH_MS           = 1_500_000_000_000  # ~2017-07-14 Binance launch


class BinanceClient(ExchangeClient):
    """Binance exchange implementation.

    Usage::

        client = BinanceClient(max_retries=5, request_delay=0.12)
        symbols = client.fetch_symbols("USDT")
        klines  = client.fetch_klines("BTCUSDT", start_ms, end_ms)
    """

    def __init__(self, max_retries: int = 5, request_delay: float = 0.12):
        self.max_retries = max_retries
        self.request_delay = request_delay

    # ── REST: Symbols ────────────────────────────────────────────────────────

    def fetch_symbols(self, quote_asset: str = "USDT") -> list[str]:
        """Fetch all active spot trading pairs from Binance."""
        for attempt in range(self.max_retries):
            try:
                log.info("Fetching %s trading pairs from Binance REST API...", quote_asset)
                resp = requests.get(REST_EXCHANGE_INFO, timeout=15)
                resp.raise_for_status()
                symbols = [
                    s["symbol"]
                    for s in resp.json().get("symbols", [])
                    if s["quoteAsset"] == quote_asset
                    and s["status"] == "TRADING"
                    and s.get("isSpotTradingAllowed", False)
                ]
                log.info("Found %d active %s spot pairs.", len(symbols), quote_asset)
                return sorted(symbols)
            except Exception as e:
                log.warning("fetch_symbols attempt %d failed: %s", attempt + 1, e)
                time.sleep(2 ** attempt)
        raise RuntimeError("Cannot fetch symbol list from Binance after retries.")

    # ── REST: Klines ─────────────────────────────────────────────────────────

    def fetch_klines(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        interval: str = "1m",
        batch_limit: int = 1000,
    ) -> list[list]:
        """Fetch OHLCV klines with auto-pagination and rate-limit handling."""
        all_klines: list[list] = []
        current_start = start_ms
        step_ms = 60_000 if interval == "1m" else 3_600_000

        while current_start < end_ms:
            for attempt in range(self.max_retries):
                try:
                    resp = requests.get(
                        REST_KLINES_URL,
                        params={
                            "symbol":    symbol,
                            "interval":  interval,
                            "startTime": current_start,
                            "endTime":   end_ms,
                            "limit":     batch_limit,
                        },
                        timeout=15,
                    )
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        log.warning("[%s] Rate limited. Sleeping %ds.", symbol, retry_after)
                        time.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    batch = resp.json()
                    break
                except Exception as e:
                    log.warning("[%s] klines attempt %d failed: %s", symbol, attempt + 1, e)
                    time.sleep(2 ** attempt)
            else:
                log.error("[%s] Giving up on window starting %d.", symbol, current_start)
                break

            if not batch:
                break

            all_klines.extend(batch)
            last_open_time = int(batch[-1][0])
            if last_open_time <= current_start:
                break
            current_start = last_open_time + step_ms
            time.sleep(self.request_delay)

        return all_klines

    def fetch_first_available_start(self, symbol: str) -> int:
        """Return the earliest available 1m candle open time for a symbol.

        Falls back to global Binance epoch if detection fails.
        """
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(
                    REST_KLINES_URL,
                    params={
                        "symbol": symbol,
                        "interval": "1m",
                        "startTime": EPOCH_MS,
                        "limit": 1,
                    },
                    timeout=15,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning("[%s] Rate limited while probing first candle. Sleeping %ds.", symbol, retry_after)
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                rows = resp.json()
                if rows:
                    return int(rows[0][0])
                return EPOCH_MS
            except Exception as e:
                log.warning("[%s] first-candle probe attempt %d failed: %s", symbol, attempt + 1, e)
                time.sleep(2 ** attempt)
        return EPOCH_MS

    # ── WebSocket URL builders ───────────────────────────────────────────────

    def build_ticker_stream_url(self) -> str:
        return WS_TICKER_URL

    def build_combined_stream_url(self, streams: list[str]) -> str:
        return f"{WS_COMBINED_BASE}?streams={'/'.join(streams)}"

    # ── Stream name builders ─────────────────────────────────────────────────

    def trade_stream_name(self, symbol: str) -> str:
        return f"{symbol.lower()}@aggTrade"

    def kline_stream_name(self, symbol: str, interval: str) -> str:
        return f"{symbol.lower()}@kline_{interval}"

    def depth_stream_name(self, symbol: str, level: str, update_ms: str) -> str:
        return f"{symbol.lower()}@depth{level}@{update_ms}ms"

    # ── Data mappers (delegate to mappers module) ────────────────────────────

    def map_ticker(self, raw: dict) -> dict:
        return mappers.map_ticker(raw)

    def map_trade(self, raw: dict) -> dict:
        return mappers.map_agg_trade(raw)

    def map_kline(self, raw: dict) -> dict:
        return mappers.map_kline(raw)

    def map_depth(self, raw: dict) -> dict:
        return mappers.map_depth(raw)
