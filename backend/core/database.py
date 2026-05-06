from __future__ import annotations

from influxdb_client import InfluxDBClient
import trino

from backend.core.config import (
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG,
    TRINO_HOST, TRINO_PORT,
)
from backend.core.redis_sentinel import get_redis_master, get_redis_replica, get_redis_sentinel

_influx: InfluxDBClient | None = None


async def get_redis():
    """
    Get Redis client for general use (reads from replica, writes to master)

    For explicit read/write splitting, use:
    - get_redis_master() for writes
    - get_redis_replica() for reads
    """
    return await get_redis_replica()


def get_influx() -> InfluxDBClient:
    global _influx
    if _influx is None:
        _influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    return _influx


def get_trino_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="fastapi",
        catalog="iceberg",
        schema="crypto_lakehouse",
    )


async def close_all():
    global _influx

    # Close Redis Sentinel connections
    sentinel = get_redis_sentinel()
    await sentinel.close()

    if _influx:
        _influx.close()
        _influx = None
