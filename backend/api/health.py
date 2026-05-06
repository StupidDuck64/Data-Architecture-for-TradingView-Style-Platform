"""
Health check API endpoint.
"""

import time
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.core.database import get_redis, get_influx, get_trino_connection
<<<<<<< HEAD
=======
from backend.core.redis_sentinel import get_redis_health
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)

router = APIRouter(prefix="/api", tags=["health"])

APP_START_TS = time.time()


@router.get("/health")
async def health():
    """Check connectivity to all backend dependencies and report overall status."""
    overall_start = time.perf_counter()
    checks = {}
    latencies = {}

<<<<<<< HEAD
    keydb_start = time.perf_counter()
    try:
        r = await get_redis()
        await r.ping()
        checks["keydb"] = "ok"
    except Exception as e:
        checks["keydb"] = str(e)
    finally:
        latencies["keydb_ms"] = round((time.perf_counter() - keydb_start) * 1000, 2)
=======
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
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)

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

<<<<<<< HEAD
    status = "ok" if all(v == "ok" for v in checks.values()) else "degraded"
=======
    # Determine overall status
    redis_ok = isinstance(checks.get("redis"), dict) and checks["redis"].get("status") in ["ok", "healthy"]
    influx_ok = checks.get("influxdb") == "ok"
    trino_ok = checks.get("trino") == "ok"

    status = "ok" if (redis_ok and influx_ok and trino_ok) else "degraded"

>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)
    total_latency_ms = round((time.perf_counter() - overall_start) * 1000, 2)
    return {
        "status": status,
        "checks": checks,
        "latency_ms": latencies,
        "total_latency_ms": total_latency_ms,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "uptime_sec": int(time.time() - APP_START_TS),
    }
