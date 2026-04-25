"""
Health check API endpoint.
"""

import time
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.core.database import get_redis, get_influx, get_trino_connection

router = APIRouter(prefix="/api", tags=["health"])

APP_START_TS = time.time()


@router.get("/health")
async def health():
    """Check connectivity to all backend dependencies and report overall status."""
    overall_start = time.perf_counter()
    checks = {}
    latencies = {}

    keydb_start = time.perf_counter()
    try:
        r = await get_redis()
        await r.ping()
        checks["keydb"] = "ok"
    except Exception as e:
        checks["keydb"] = str(e)
    finally:
        latencies["keydb_ms"] = round((time.perf_counter() - keydb_start) * 1000, 2)

    influx_start = time.perf_counter()
    try:
        get_influx().ping()
        checks["influxdb"] = "ok"
    except Exception as e:
        checks["influxdb"] = str(e)
    finally:
        latencies["influxdb_ms"] = round((time.perf_counter() - influx_start) * 1000, 2)

    trino_start = time.perf_counter()
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        checks["trino"] = "ok"
    except Exception as e:
        checks["trino"] = str(e)
    finally:
        latencies["trino_ms"] = round((time.perf_counter() - trino_start) * 1000, 2)

    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
    total_latency_ms = round((time.perf_counter() - overall_start) * 1000, 2)
    return {
        "status": status,
        "checks": checks,
        "latency_ms": latencies,
        "total_latency_ms": total_latency_ms,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "uptime_sec": int(time.time() - APP_START_TS),
    }
