import os
import sys
import logging

log = logging.getLogger("backend.core.config")

# ─── Redis Sentinel (High Availability) ─────────────────────────────────────
# Redis Sentinel configuration is handled in redis_sentinel.py
# using REDIS_SENTINELS and REDIS_MASTER_NAME environment variables

# ─── InfluxDB ───────────────────────────────────────────────────────────────
INFLUX_URL = os.environ.get("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

# ─── Trino (Iceberg query engine) ───────────────────────────────────────────
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

# ─── CORS ───────────────────────────────────────────────────────────────────
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "*").split(",")

# ─── Startup validation ────────────────────────────────────────────────────
_missing = []
if not INFLUX_TOKEN:
    _missing.append("INFLUX_TOKEN")
if _missing:
    log.error("Missing required environment variables: %s", ", ".join(_missing))
    sys.exit(1)
