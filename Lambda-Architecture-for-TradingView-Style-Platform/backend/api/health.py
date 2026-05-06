"""
Health check API endpoint.
"""

import time
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.core.database import get_redis, get_influx, get_trino_connection
from backend.core.redis_sentinel import get_redis_health

router = APIRouter(prefix="/api", tags=["health"])

APP_START_TS = time.time()


@router.get("/health")
async def health():
    """Check connectivity to all backend dependencies and report overall status."""
    overall_start = time.perf_counter()
    checks = {}
    latencies = {}

    # Redis Sentinel cluster health
    redis_start = time.perf_counter()
    try:
        redis_health_info = await get_redis_health()
        checks["redis"] = redis_health_info

        # Also test connection
        r = await get_redis()
        await r.ping()
    except Exception as e:
        checks["redis"] = {"status": "error", "error": str(e)}
    finally:
        latencies["redis_ms"] = round((time.perf_counter() - redis_start) * 1000, 2)

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

    # Determine overall status
    redis_ok = isinstance(checks.get("redis"), dict) and checks["redis"].get("status") in ["ok", "healthy"]
    influx_ok = checks.get("influxdb") == "ok"
    trino_ok = checks.get("trino") == "ok"

    status = "ok" if (redis_ok and influx_ok and trino_ok) else "degraded"

    total_latency_ms = round((time.perf_counter() - overall_start) * 1000, 2)
    return {
        "status": status,
        "checks": checks,
        "latency_ms": latencies,
        "total_latency_ms": total_latency_ms,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "uptime_sec": int(time.time() - APP_START_TS),
    }
