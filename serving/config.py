import os

# ─── KeyDB (Redis-compatible) ───────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "keydb")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# ─── InfluxDB ───────────────────────────────────────────────────────────────
INFLUX_URL = os.environ.get("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "vi")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "crypto")

# ─── CORS ───────────────────────────────────────────────────────────────────
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "*").split(",")
