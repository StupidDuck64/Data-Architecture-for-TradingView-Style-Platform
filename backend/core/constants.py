"""
Shared constants used across the backend.
Centralized here to avoid duplication between klines, historical, and websocket modules.
"""

import os
import re

# ─── Interval definitions ────────────────────────────────────────────────────
INTERVAL_SECONDS: dict[str, int] = {
    "1s": 1,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}

# ─── Symbol validation ───────────────────────────────────────────────────────
SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}$")

# ─── Data limits ─────────────────────────────────────────────────────────────
MAX_RAW_CANDLES = 50_000
MAX_RAW_PER_QUERY = 12_000
MAX_RAW_ROWS = 200_000
MAX_BACKFILL_PAGES = 8
LIVE_MAX_BASE_ROWS = 2_500

# ─── Retention ───────────────────────────────────────────────────────────────
INFLUX_1M_RETENTION_DAYS = int(os.environ.get("INFLUX_1M_RETENTION_DAYS", "90"))

# ─── Interval groups ─────────────────────────────────────────────────────────
HOURLY_INTERVALS = {"1h", "4h", "1d", "1w"}
