#!/usr/bin/env python3
"""
Exchange-agnostic Kafka producer service.

Spawns WebSocket stream threads for ticker, aggTrade, kline, and depth
data from any exchange implementing ``ExchangeClient``.  Currently wired
to Binance; adding a second source is a matter of instantiating another
client and calling ``run_streams()`` again.

Usage (Docker)::

    CMD ["python", "src/producer/main.py"]
"""

import json
import logging
import os
import random
import signal
import sys
import threading
import time
from typing import Any

# ── Ensure project root is on the path so shared modules are importable ──────
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import websocket

from common.config import (
    DEPTH_LEVEL,
    DEPTH_UPDATE_MS,
    KAFKA_TOPIC_DEPTH,
    KAFKA_TOPIC_KLINES,
    KAFKA_TOPIC_TICKER,
    KAFKA_TOPIC_TRADES,
    KLINE_INTERVAL_WS,
    MAX_SYMBOLS,
    SCHEMA_REGISTRY_URL,
    SYMBOLS_PER_CONNECTION,
    SYMBOLS_PER_DEPTH_CONN,
    TICKER_HEARTBEAT_SEC,
)
from common.avro_serializer import AvroSerializer
from common.kafka_client import flush_and_close, init_producer, send_to_kafka
from common.logging import setup_logging
from exchanges.base import ExchangeClient
from exchanges.binance.client import BinanceClient

log = logging.getLogger(__name__)

# ── Per-symbol dedup state for ticker throttling ─────────────────────────────
_last_close: dict[str, float] = {}
_last_sent_ts: dict[str, float] = {}

avro_serializer: AvroSerializer | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# Generic stream runners (exchange-agnostic)
# ═══════════════════════════════════════════════════════════════════════════════

def handle_ticker_message(message: str, client: ExchangeClient) -> None:
    """Process a batch ticker update, applying change+heartbeat throttle."""
    try:
        batch: list[dict[str, Any]] = json.loads(message)
    except json.JSONDecodeError as e:
        log.error("Ticker JSON decode error: %s", e)
        return

    if not isinstance(batch, list):
        return

    now = time.monotonic()
    sent_change = sent_heartbeat = skipped = 0

    for raw in batch:
        symbol = raw.get("s", "")
        if not symbol.endswith("USDT"):
            continue

        cur = float(raw.get("c", 0))
        price_changed = _last_close.get(symbol) != cur
        heartbeat_due = (now - _last_sent_ts.get(symbol, 0)) >= TICKER_HEARTBEAT_SEC

        if not price_changed and not heartbeat_due:
            skipped += 1
            continue

        _last_close[symbol] = cur
        _last_sent_ts[symbol] = now
        send_to_kafka(KAFKA_TOPIC_TICKER, client.map_ticker(raw), avro_serializer)

        if price_changed:
            sent_change += 1
        else:
            sent_heartbeat += 1

    total_sent = sent_change + sent_heartbeat
    if total_sent > 0:
        log.info(
            "[TICKER] sent=%d (change=%d, heartbeat=%d), skipped=%d",
            total_sent, sent_change, sent_heartbeat, skipped,
        )


def run_ticker_stream(client: ExchangeClient) -> None:
    """Connect to the all-ticker WebSocket and stream indefinitely."""
    url = client.build_ticker_stream_url()
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=lambda ws: log.info("[TICKER] WebSocket opened."),
                on_message=lambda ws, msg: handle_ticker_message(msg, client),
                on_error=lambda ws, err: log.error("[TICKER] Error: %s", err),
                on_close=lambda ws, code, msg: log.warning(
                    "[TICKER] Closed. code=%s msg=%s", code, msg
                ),
            )
            log.info("[TICKER] Connecting to %s", url)
            ws.run_forever(ping_interval=40, ping_timeout=30, reconnect=0)
            delay = 5 + random.random() * 2
            log.warning("[TICKER] Dropped. Reconnecting in %.1fs...", delay)
            time.sleep(delay)
        except Exception as e:
            delay = 5 + random.random() * 2
            log.exception("[TICKER] Unexpected error: %s. Retry in %.1fs...", e, delay)
            time.sleep(delay)


def _handle_combined_message(
    message: str,
    event_type: str,
    mapper,
    topic: str,
    tag: str,
) -> None:
    """Generic handler for combined-stream WebSocket messages."""
    try:
        envelope: Any = json.loads(message)
    except json.JSONDecodeError as e:
        log.error("[%s] JSON decode error: %s", tag, e)
        return

    if not isinstance(envelope, dict):
        return

    payload = envelope.get("data", envelope)

    if event_type == "depth":
        # Partial book depth uses different event type / structure
        if not isinstance(payload, dict) or "lastUpdateId" not in payload:
            if isinstance(payload, dict) and payload.get("e") != "depthUpdate":
                return
        # For combined streams, symbol comes from the stream name
        if "s" not in payload:
            stream_name = envelope.get("stream", "")
            symbol = stream_name.split("@")[0].upper() if stream_name else ""
            payload["s"] = symbol
        symbol = str(payload.get("s", "")).upper()
        payload["s"] = symbol
    else:
        if not isinstance(payload, dict) or payload.get("e") != event_type:
            return
        symbol = payload.get("s", "")

    if not symbol.endswith("USDT"):
        return

    send_to_kafka(topic, mapper(payload), avro_serializer)
    log.debug("[%s] %s processed", tag, symbol)


def run_combined_batch(
    stream_url: str,
    batch_idx: int,
    event_type: str,
    mapper,
    topic: str,
    tag: str,
) -> None:
    """Run a combined-stream WebSocket with auto-reconnect."""
    url_preview = stream_url[:120] + "..." if len(stream_url) > 120 else stream_url
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_open=lambda ws: log.info("[%s] Batch #%d WebSocket opened.", tag, batch_idx),
                on_message=lambda ws, msg: _handle_combined_message(
                    msg, event_type, mapper, topic, tag
                ),
                on_error=lambda ws, err: log.error("[%s] Batch #%d error: %s", tag, batch_idx, err),
                on_close=lambda ws, code, msg: log.warning(
                    "[%s] Batch #%d closed. code=%s", tag, batch_idx, code
                ),
            )
            log.info("[%s] Batch #%d connecting: %s", tag, batch_idx, url_preview)
            ws.run_forever(ping_interval=40, ping_timeout=30, reconnect=0)
            delay = 5 + random.random() * batch_idx
            log.warning("[%s] Batch #%d dropped. Reconnecting in %.1fs...", tag, batch_idx, delay)
            time.sleep(delay)
        except Exception as e:
            delay = 5 + random.random() * batch_idx
            log.exception("[%s] Batch #%d unexpected error: %s. Retry in %.1fs...", tag, batch_idx, e, delay)
            time.sleep(delay)


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestrator
# ═══════════════════════════════════════════════════════════════════════════════

def run_streams(client: ExchangeClient) -> None:
    """Spawn all WebSocket stream threads for a given exchange client."""
    symbols = [s.lower() for s in client.fetch_symbols()[:MAX_SYMBOLS]]

    # ── Stream A: All-ticker ─────────────────────────────────────────────────
    ticker_thread = threading.Thread(
        target=run_ticker_stream,
        args=(client,),
        daemon=True,
        name="ws-ticker",
    )
    ticker_thread.start()

    # ── Stream B: Aggregate trades ───────────────────────────────────────────
    batches = [
        symbols[i : i + SYMBOLS_PER_CONNECTION]
        for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)
    ]
    log.info("Spawning %d aggTrade thread(s) for %d symbols (%d/connection).",
             len(batches), len(symbols), SYMBOLS_PER_CONNECTION)

    for idx, batch in enumerate(batches):
        streams = [client.trade_stream_name(s) for s in batch]
        url = client.build_combined_stream_url(streams)
        t = threading.Thread(
            target=run_combined_batch,
            args=(url, idx + 1, "aggTrade", client.map_trade, KAFKA_TOPIC_TRADES, "TRADES"),
            daemon=True,
            name=f"ws-trades-{idx + 1}",
        )
        t.start()
        time.sleep(1.0)

    # ── Stream C: Kline candles ──────────────────────────────────────────────
    log.info("Spawning %d kline thread(s) (interval=%s).", len(batches), KLINE_INTERVAL_WS)
    for idx, batch in enumerate(batches):
        streams = [client.kline_stream_name(s, KLINE_INTERVAL_WS) for s in batch]
        url = client.build_combined_stream_url(streams)
        t = threading.Thread(
            target=run_combined_batch,
            args=(url, idx + 1, "kline", client.map_kline, KAFKA_TOPIC_KLINES, "KLINES"),
            daemon=True,
            name=f"ws-klines-{idx + 1}",
        )
        t.start()
        time.sleep(1.0)

    # ── Stream D: Order-book depth ───────────────────────────────────────────
    depth_batches = [
        symbols[i : i + SYMBOLS_PER_DEPTH_CONN]
        for i in range(0, len(symbols), SYMBOLS_PER_DEPTH_CONN)
    ]
    log.info("Spawning %d depth thread(s) (@depth%s@%sms).",
             len(depth_batches), DEPTH_LEVEL, DEPTH_UPDATE_MS)
    for idx, batch in enumerate(depth_batches):
        streams = [client.depth_stream_name(s, DEPTH_LEVEL, DEPTH_UPDATE_MS) for s in batch]
        url = client.build_combined_stream_url(streams)
        t = threading.Thread(
            target=run_combined_batch,
            args=(url, idx + 1, "depth", client.map_depth, KAFKA_TOPIC_DEPTH, "DEPTH"),
            daemon=True,
            name=f"ws-depth-{idx + 1}",
        )
        t.start()
        time.sleep(1.0)


def run() -> None:
    """Main entry point."""
    setup_logging("producer")

    log.info("=" * 60)
    log.info("Quad-Stream Producer starting...")
    log.info("  → Stream A: !ticker@arr                    → topic: %s", KAFKA_TOPIC_TICKER)
    log.info("  → Stream B: @aggTrade                      → topic: %s", KAFKA_TOPIC_TRADES)
    log.info("  → Stream C: @kline_%s                    → topic: %s", KLINE_INTERVAL_WS, KAFKA_TOPIC_KLINES)
    log.info("  → Stream D: @depth%s@%sms               → topic: %s", DEPTH_LEVEL, DEPTH_UPDATE_MS, KAFKA_TOPIC_DEPTH)
    log.info("=" * 60)

    # ── Initialize Kafka producer ────────────────────────────────────────────
    init_producer()

    # ── Register Avro schemas ────────────────────────────────────────────────
    global avro_serializer
    schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "..", "..", "schemas")
    avro_serializer = AvroSerializer(SCHEMA_REGISTRY_URL)
    avro_serializer.register(KAFKA_TOPIC_TICKER, os.path.join(schema_dir, "ticker.avsc"))
    avro_serializer.register(KAFKA_TOPIC_KLINES, os.path.join(schema_dir, "kline.avsc"))
    avro_serializer.register(KAFKA_TOPIC_TRADES, os.path.join(schema_dir, "trade.avsc"))
    avro_serializer.register(KAFKA_TOPIC_DEPTH,  os.path.join(schema_dir, "depth.avsc"))
    log.info("All Avro schemas registered.")

    # ── Start streams (currently Binance only) ───────────────────────────────
    binance = BinanceClient()
    run_streams(binance)

    # ── Block main thread until shutdown ─────────────────────────────────────
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Received interrupt signal. Breaking main loop...")
    finally:
        log.info("Shutting down: flushing Kafka producer buffer...")
        flush_and_close()
        log.info("Shutdown complete.")


def _handle_sigterm(signum: int, frame: Any) -> None:
    log.info("SIGTERM received, initiating graceful shutdown...")
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    run()
