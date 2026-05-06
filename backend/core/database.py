from __future__ import annotations

<<<<<<< HEAD
import redis.asyncio as aioredis
=======
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)
from influxdb_client import InfluxDBClient
import trino

from backend.core.config import (
<<<<<<< HEAD
    REDIS_HOST, REDIS_PORT,
    INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG,
    TRINO_HOST, TRINO_PORT,
)

_redis: aioredis.Redis | None = None
_influx: InfluxDBClient | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True, socket_keepalive=True,
        )
    return _redis
=======
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
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)


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
<<<<<<< HEAD
    global _redis, _influx
    if _redis:
        await _redis.close()
        _redis = None
=======
    global _influx

    # Close Redis Sentinel connections
    sentinel = get_redis_sentinel()
    await sentinel.close()

>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)
    if _influx:
        _influx.close()
        _influx = None
