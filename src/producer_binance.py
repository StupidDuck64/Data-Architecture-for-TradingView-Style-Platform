#!/usr/bin/env python3
import json
import logging
import random
import signal
import threading
import time
from typing import Any, Dict, List

import os

import requests
import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BINANCE_REST_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_TICKER_WS_URL = "wss://stream.binance.com:9443/ws/!ticker@arr"
BINANCE_COMBINED_WS_BASE = "wss://stream.binance.com:9443/stream"

SYMBOLS_PER_CONNECTION = 200
TICKER_HEARTBEAT_INTERVAL = 5.0

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_TICKER = "crypto_ticker"
KAFKA_TOPIC_TRADES = "crypto_trades"
KAFKA_TOPIC_KLINES = "crypto_klines"
KAFKA_TOPIC_DEPTH  = "crypto_depth"
KLINE_INTERVAL_WS  = os.environ.get("KLINE_INTERVAL", "1m")
DEPTH_LEVEL        = os.environ.get("DEPTH_LEVEL", "20")   # top 20 bids/asks
DEPTH_UPDATE_MS    = os.environ.get("DEPTH_UPDATE_MS", "100")  # 100ms updates
SYMBOLS_PER_DEPTH_CONN = 200  # same batch size as trades/klines
MAX_SYMBOLS        = 400      # cap to avoid exceeding Binance WS connection limits

producer: KafkaProducer | None = None
producer_lock = threading.Lock()

_last_close: Dict[str, float] = {}
_last_sent_ts: Dict[str, float] = {}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    )


def fetch_usdt_symbols() -> List[str]:
    while True:
        try:
            logging.info("Fetching USDT trading pairs from Binance REST API...")
            resp = requests.get(BINANCE_REST_EXCHANGE_INFO, timeout=15)
            resp.raise_for_status()
            symbols = [
                s["symbol"].lower()
                for s in resp.json().get("symbols", [])
                if s["quoteAsset"] == "USDT"
                and s["status"] == "TRADING"
                and s.get("isSpotTradingAllowed", False)
            ]
            logging.info("Found %d active USDT spot pairs.", len(symbols))
            return symbols
        except Exception as e:
            logging.error("Failed to fetch symbols: %s. Retrying in 10s...", e)
            time.sleep(10)


def create_kafka_producer() -> KafkaProducer:
    while True:
        try:
            logging.info("Connecting to Kafka at %s ...", KAFKA_BOOTSTRAP_SERVERS)
            p = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                compression_type="lz4",
                linger_ms=5,
                batch_size=64 * 1024,
                buffer_memory=64 * 1024 * 1024,
                retries=5,
                max_in_flight_requests_per_connection=5,
            )
            p.bootstrap_connected()
            logging.info("Successfully connected to Kafka.")
            return p
        except NoBrokersAvailable as e:
            logging.error("Kafka unavailable (%s). Retrying in 5s...", e)
            time.sleep(5)


def _on_send_error(topic: str, symbol: str, exc: Exception) -> None:
    logging.error("[KAFKA] Async send failed | topic=%s symbol=%s error=%s", topic, symbol, exc)


def send_to_kafka(topic: str, record: Dict[str, Any]) -> None:
    global producer

    with producer_lock:
        if producer is None:
            producer = create_kafka_producer()

    key: str = record.get("symbol", "")

    try:
        future = producer.send(topic, key=key, value=record)
        future.add_errback(_on_send_error, topic, key)
    except KafkaError as e:
        logging.error("[KAFKA] Sync send error (dropped message) | topic=%s symbol=%s error=%s", topic, key, e)
    except Exception as e:
        logging.error("[KAFKA] Unexpected send error | topic=%s symbol=%s error=%s", topic, key, e)


def map_ticker(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_time":           raw["E"],
        "symbol":               raw["s"],
        "close":                float(raw.get("c", 0)),
        "bid":                  float(raw.get("b", 0)),
        "ask":                  float(raw.get("a", 0)),
        "24h_open":             float(raw.get("o", 0)),
        "24h_high":             float(raw.get("h", 0)),
        "24h_low":              float(raw.get("l", 0)),
        "24h_volume":           float(raw.get("v", 0)),
        "24h_quote_volume":     float(raw.get("q", 0)),
        "24h_price_change":     float(raw.get("p", 0)),
        "24h_price_change_pct": float(raw.get("P", 0)),
        "24h_trade_count":      int(raw.get("n", 0)),
    }


def handle_ticker_message(message: str) -> None:
    global _last_close, _last_sent_ts
    try:
        batch: List[Dict[str, Any]] = json.loads(message)
    except json.JSONDecodeError as e:
        logging.error("Ticker JSON decode error: %s", e)
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
        heartbeat_due = (now - _last_sent_ts.get(symbol, 0)) >= TICKER_HEARTBEAT_INTERVAL

        if not price_changed and not heartbeat_due:
            skipped += 1
            continue

        _last_close[symbol] = cur
        _last_sent_ts[symbol] = now
        send_to_kafka(KAFKA_TOPIC_TICKER, map_ticker(raw))

        if price_changed:
            sent_change += 1
        else:
            sent_heartbeat += 1

    total_sent = sent_change + sent_heartbeat
    if total_sent > 0:
        logging.info(
            "[TICKER] sent=%d (change=%d, heartbeat=%d), skipped=%d",
            total_sent, sent_change, sent_heartbeat, skipped,
        )


def run_ticker_stream() -> None:
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_TICKER_WS_URL,
                on_open=lambda ws: logging.info("[TICKER] WebSocket opened."),
                on_message=lambda ws, msg: handle_ticker_message(msg),
                on_error=lambda ws, err: logging.error("[TICKER] Error: %s", err),
                on_close=lambda ws, code, msg: logging.warning(
                    "[TICKER] Closed. code=%s msg=%s", code, msg
                ),
            )
            logging.info("[TICKER] Connecting to %s", BINANCE_TICKER_WS_URL)
            ws.run_forever(ping_interval=20, ping_timeout=10, reconnect=0)
            delay = 5 + random.random() * 2
            logging.warning("[TICKER] Dropped. Reconnecting in %.1fs...", delay)
            time.sleep(delay)
        except Exception as e:
            delay = 5 + random.random() * 2
            logging.exception("[TICKER] Unexpected error: %s. Retry in %.1fs...", e, delay)
            time.sleep(delay)


def map_agg_trade(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_time":     raw["E"],
        "symbol":         raw["s"],
        "agg_trade_id":   raw["a"],
        "price":          float(raw["p"]),
        "quantity":       float(raw["q"]),
        "trade_time":     raw["T"],
        "is_buyer_maker": bool(raw["m"]),
    }


def handle_agg_trade_message(message: str) -> None:
    try:
        envelope: Any = json.loads(message)
    except json.JSONDecodeError as e:
        logging.error("[TRADES] JSON decode error: %s", e)
        return

    if not isinstance(envelope, dict):
        return

    payload = envelope.get("data", envelope)

    if not isinstance(payload, dict) or payload.get("e") != "aggTrade":
        return

    symbol = payload.get("s", "")
    if not symbol.endswith("USDT"):
        return

    send_to_kafka(KAFKA_TOPIC_TRADES, map_agg_trade(payload))
    logging.debug(
        "[TRADES] %s price=%.6f qty=%.6f buyer_maker=%s",
        symbol, float(payload.get("p", 0)),
        float(payload.get("q", 0)), payload.get("m"),
    )


def run_agg_trade_batch(stream_url: str, batch_idx: int) -> None:
    url_preview = stream_url[:120] + "..." if len(stream_url) > 120 else stream_url
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_open=lambda ws: logging.info(
                    "[TRADES] Batch #%d WebSocket opened.", batch_idx
                ),
                on_message=lambda ws, msg: handle_agg_trade_message(msg),
                on_error=lambda ws, err: logging.error(
                    "[TRADES] Batch #%d error: %s", batch_idx, err
                ),
                on_close=lambda ws, code, msg: logging.warning(
                    "[TRADES] Batch #%d closed. code=%s", batch_idx, code
                ),
            )
            logging.info("[TRADES] Batch #%d connecting: %s", batch_idx, url_preview)
            ws.run_forever(ping_interval=20, ping_timeout=10, reconnect=0)
            delay = 5 + random.random() * batch_idx
            logging.warning(
                "[TRADES] Batch #%d dropped. Reconnecting in %.1fs...", batch_idx, delay
            )
            time.sleep(delay)
        except Exception as e:
            delay = 5 + random.random() * batch_idx
            logging.exception(
                "[TRADES] Batch #%d unexpected error: %s. Retry in %.1fs...",
                batch_idx, e, delay,
            )
            time.sleep(delay)


def map_kline(raw: Dict[str, Any]) -> Dict[str, Any]:
    k = raw["k"]
    return {
        "event_time":   raw["E"],
        "symbol":       raw["s"],
        "kline_start":  k["t"],
        "kline_close":  k["T"],
        "interval":     k["i"],
        "open":         float(k["o"]),
        "high":         float(k["h"]),
        "low":          float(k["l"]),
        "close":        float(k["c"]),
        "volume":       float(k["v"]),
        "quote_volume": float(k["q"]),
        "trade_count":  int(k["n"]),
        "is_closed":    bool(k["x"]),
    }


def handle_kline_message(message: str) -> None:
    try:
        envelope: Any = json.loads(message)
    except json.JSONDecodeError as e:
        logging.error("[KLINES] JSON decode error: %s", e)
        return

    if not isinstance(envelope, dict):
        return

    payload = envelope.get("data", envelope)
    if not isinstance(payload, dict) or payload.get("e") != "kline":
        return

    symbol = payload.get("s", "")
    if not symbol.endswith("USDT"):
        return

    send_to_kafka(KAFKA_TOPIC_KLINES, map_kline(payload))
    k = payload.get("k", {})
    logging.debug(
        "[KLINES] %s o=%.4f h=%.4f l=%.4f c=%.4f closed=%s",
        symbol,
        float(k.get("o", 0)), float(k.get("h", 0)),
        float(k.get("l", 0)), float(k.get("c", 0)),
        k.get("x", False),
    )


def run_kline_batch(stream_url: str, batch_idx: int) -> None:
    url_preview = stream_url[:120] + "..." if len(stream_url) > 120 else stream_url
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_open=lambda ws: logging.info(
                    "[KLINES] Batch #%d WebSocket opened.", batch_idx
                ),
                on_message=lambda ws, msg: handle_kline_message(msg),
                on_error=lambda ws, err: logging.error(
                    "[KLINES] Batch #%d error: %s", batch_idx, err
                ),
                on_close=lambda ws, code, msg: logging.warning(
                    "[KLINES] Batch #%d closed. code=%s", batch_idx, code
                ),
            )
            logging.info("[KLINES] Batch #%d connecting: %s", batch_idx, url_preview)
            ws.run_forever(ping_interval=20, ping_timeout=10, reconnect=0)
            delay = 5 + random.random() * batch_idx
            logging.warning(
                "[KLINES] Batch #%d dropped. Reconnecting in %.1fs...", batch_idx, delay
            )
            time.sleep(delay)
        except Exception as e:
            delay = 5 + random.random() * batch_idx
            logging.exception(
                "[KLINES] Batch #%d unexpected error: %s. Retry in %.1fs...",
                batch_idx, e, delay,
            )
            time.sleep(delay)


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM D: Order-book depth (@depth{LEVEL}@{UPDATE_MS}ms)
# ═══════════════════════════════════════════════════════════════════════════════

def map_depth(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map Binance partial depth event to Kafka record."""
    return {
        "event_time":    raw.get("E", int(time.time() * 1000)),
        "symbol":        raw.get("s", ""),
        "last_update_id": raw.get("lastUpdateId", 0),
        "bids":          [[float(p), float(q)] for p, q in raw.get("bids", [])],
        "asks":          [[float(p), float(q)] for p, q in raw.get("asks", [])],
    }


def handle_depth_message(message: str) -> None:
    try:
        envelope: Any = json.loads(message)
    except json.JSONDecodeError as e:
        logging.error("[DEPTH] JSON decode error: %s", e)
        return

    if not isinstance(envelope, dict):
        return

    payload = envelope.get("data", envelope)
    if not isinstance(payload, dict) or payload.get("e") != "depthUpdate":
        # partial book depth uses different event type
        # for @depth<levels>@<speed>, payload has 'lastUpdateId', 'bids', 'asks'
        if "lastUpdateId" not in payload:
            return

    # For combined streams, symbol comes from the stream name
    if "s" not in payload:
        stream_name = envelope.get("stream", "")
        # e.g. "btcusdt@depth20@100ms" → "BTCUSDT"
        symbol = stream_name.split("@")[0].upper() if stream_name else ""
        payload["s"] = symbol

    symbol = payload.get("s", "")
    if not symbol.endswith("USDT"):
        return

    send_to_kafka(KAFKA_TOPIC_DEPTH, map_depth(payload))


def run_depth_batch(stream_url: str, batch_idx: int) -> None:
    url_preview = stream_url[:120] + "..." if len(stream_url) > 120 else stream_url
    while True:
        try:
            ws = websocket.WebSocketApp(
                stream_url,
                on_open=lambda ws: logging.info(
                    "[DEPTH] Batch #%d WebSocket opened.", batch_idx
                ),
                on_message=lambda ws, msg: handle_depth_message(msg),
                on_error=lambda ws, err: logging.error(
                    "[DEPTH] Batch #%d error: %s", batch_idx, err
                ),
                on_close=lambda ws, code, msg: logging.warning(
                    "[DEPTH] Batch #%d closed. code=%s", batch_idx, code
                ),
            )
            logging.info("[DEPTH] Batch #%d connecting: %s", batch_idx, url_preview)
            ws.run_forever(ping_interval=20, ping_timeout=10, reconnect=0)
            delay = 5 + random.random() * batch_idx
            logging.warning(
                "[DEPTH] Batch #%d dropped. Reconnecting in %.1fs...", batch_idx, delay
            )
            time.sleep(delay)
        except Exception as e:
            delay = 5 + random.random() * batch_idx
            logging.exception(
                "[DEPTH] Batch #%d unexpected error: %s. Retry in %.1fs...",
                batch_idx, e, delay,
            )
            time.sleep(delay)


def run() -> None:
    setup_logging()
    logging.info("=" * 60)
    logging.info("Binance Quad-Stream Producer starting...")
    logging.info("  \u2192 Stream A: !ticker@arr                    \u2192 topic: %s", KAFKA_TOPIC_TICKER)
    logging.info("  \u2192 Stream B: @aggTrade                      \u2192 topic: %s", KAFKA_TOPIC_TRADES)
    logging.info("  \u2192 Stream C: @kline_%s                    \u2192 topic: %s", KLINE_INTERVAL_WS, KAFKA_TOPIC_KLINES)
    logging.info("  \u2192 Stream D: @depth%s@%sms               \u2192 topic: %s", DEPTH_LEVEL, DEPTH_UPDATE_MS, KAFKA_TOPIC_DEPTH)
    logging.info("=" * 60)

    global producer
    producer = create_kafka_producer()

    ticker_thread = threading.Thread(
        target=run_ticker_stream,
        daemon=True,
        name="ws-ticker",
    )
    ticker_thread.start()

    symbols = fetch_usdt_symbols()[:MAX_SYMBOLS]
    batches = [
        symbols[i : i + SYMBOLS_PER_CONNECTION]
        for i in range(0, len(symbols), SYMBOLS_PER_CONNECTION)
    ]
    logging.info(
        "Spawning %d aggTrade thread(s) for %d symbols (%d/connection).",
        len(batches), len(symbols), SYMBOLS_PER_CONNECTION,
    )

    trade_threads: List[threading.Thread] = []
    for idx, batch in enumerate(batches):
        streams = "/".join(f"{s}@aggTrade" for s in batch)
        url = f"{BINANCE_COMBINED_WS_BASE}?streams={streams}"
        t = threading.Thread(
            target=run_agg_trade_batch,
            args=(url, idx + 1),
            daemon=True,
            name=f"ws-trades-{idx + 1}",
        )
        t.start()
        trade_threads.append(t)
        time.sleep(1.0)

    logging.info(
        "Spawning %d kline thread(s) for %d symbols (interval=%s, %d/connection).",
        len(batches), len(symbols), KLINE_INTERVAL_WS, SYMBOLS_PER_CONNECTION,
    )
    kline_threads: List[threading.Thread] = []
    for idx, batch in enumerate(batches):
        streams = "/".join(f"{s}@kline_{KLINE_INTERVAL_WS}" for s in batch)
        url = f"{BINANCE_COMBINED_WS_BASE}?streams={streams}"
        t = threading.Thread(
            target=run_kline_batch,
            args=(url, idx + 1),
            daemon=True,
            name=f"ws-klines-{idx + 1}",
        )
        t.start()
        kline_threads.append(t)
        time.sleep(1.0)

    # ── Stream D: Order book depth ──────────────────────────────────────────
    depth_batches = [
        symbols[i : i + SYMBOLS_PER_DEPTH_CONN]
        for i in range(0, len(symbols), SYMBOLS_PER_DEPTH_CONN)
    ]
    logging.info(
        "Spawning %d depth thread(s) for %d symbols (@depth%s@%sms, %d/connection).",
        len(depth_batches), len(symbols), DEPTH_LEVEL, DEPTH_UPDATE_MS, SYMBOLS_PER_DEPTH_CONN,
    )
    depth_threads: List[threading.Thread] = []
    for idx, batch in enumerate(depth_batches):
        streams = "/".join(f"{s}@depth{DEPTH_LEVEL}@{DEPTH_UPDATE_MS}ms" for s in batch)
        url = f"{BINANCE_COMBINED_WS_BASE}?streams={streams}"
        t = threading.Thread(
            target=run_depth_batch,
            args=(url, idx + 1),
            daemon=True,
            name=f"ws-depth-{idx + 1}",
        )
        t.start()
        depth_threads.append(t)
        time.sleep(1.0)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Received interrupt signal. Breaking main loop...")
    finally:
        logging.info("Shutting down: flushing Kafka producer buffer...")
        if producer is not None:
            producer.flush(timeout=10)
            producer.close()
        logging.info("Shutdown complete.")


def _handle_sigterm(signum: int, frame: Any) -> None:
    logging.info("SIGTERM received, initiating graceful shutdown...")
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_sigterm)
    run()
