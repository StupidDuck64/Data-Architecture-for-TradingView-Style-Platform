from __future__ import annotations

import redis.asyncio as aioredis
from influxdb_client import InfluxDBClient
import trino

from backend.core.config import (
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
    global _redis, _influx
    if _redis:
        await _redis.close()
        _redis = None
    if _influx:
        _influx.close()
        _influx = None
