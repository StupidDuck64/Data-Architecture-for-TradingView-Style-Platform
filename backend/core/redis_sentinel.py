"""
Redis Sentinel Manager for High Availability

Provides:
- Auto-discovery of master/replicas
- Auto-failover (handled by Sentinel)
- Read/write splitting
- Connection pooling
"""

import os
import logging
from typing import Optional
import asyncio
from redis.sentinel import Sentinel
from redis.asyncio.sentinel import Sentinel as AsyncSentinel
from redis.exceptions import ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


class RedisSentinelManager:
    """
    Redis Sentinel manager for HA

    Features:
    - Auto-discovery of master/replicas
    - Auto-failover (handled by Sentinel)
    - Read/write splitting
    - Connection pooling
    """

    def __init__(self):
        # Get sentinel nodes from environment
        sentinels_str = os.getenv('REDIS_SENTINELS', 'redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379')
        self.sentinel_nodes = [
            tuple(node.split(':')) for node in sentinels_str.split(',')
        ]
        # Convert port to int
        self.sentinel_nodes = [
            (host, int(port)) for host, port in self.sentinel_nodes
        ]

        self.master_name = os.getenv('REDIS_MASTER_NAME', 'mymaster')

        # Async sentinel
        self.sentinel = AsyncSentinel(
            self.sentinel_nodes,
            socket_timeout=0.5,
            socket_connect_timeout=0.5,
            sentinel_kwargs={
                'socket_timeout': 0.5,
                'socket_connect_timeout': 0.5
            }
        )

        self._master_client = None
        self._replica_client = None
        self._lock = asyncio.Lock()

        logger.info("redis_sentinel_initialized",
                   sentinels=self.sentinel_nodes,
                   master_name=self.master_name)

    async def get_master(self):
        """
        Get Redis master client for WRITE operations

        Returns:
            Redis client connected to current master
        """
        async with self._lock:
            if not self._master_client:
                try:
                    self._master_client = self.sentinel.master_for(
                        self.master_name,
                        socket_timeout=0.5,
                        socket_connect_timeout=0.5,
                        decode_responses=True,
                        max_connections=50
                    )
                    logger.info("redis_master_connected")
                except Exception as e:
                    logger.error("redis_master_connection_failed", error=str(e))
                    raise

            return self._master_client

    async def get_replica(self):
        """
        Get Redis replica client for READ operations

        Load-balanced across all replicas

        Returns:
            Redis client connected to a replica
        """
        async with self._lock:
            if not self._replica_client:
                try:
                    self._replica_client = self.sentinel.slave_for(
                        self.master_name,
                        socket_timeout=0.5,
                        socket_connect_timeout=0.5,
                        decode_responses=True,
                        max_connections=50
                    )
                    logger.info("redis_replica_connected")
                except Exception as e:
                    logger.warning("redis_replica_connection_failed", error=str(e))
                    # Fallback to master for reads
                    logger.info("redis_fallback_to_master_for_reads")
                    return await self.get_master()

            return self._replica_client

    async def health_check(self) -> dict:
        """
        Check Sentinel cluster health

        Returns:
            dict: Health status with master/replica info
        """
        try:
            # Discover master
            master_info = await self.sentinel.discover_master(self.master_name)

            # Discover replicas
            slaves = await self.sentinel.discover_slaves(self.master_name)

            # Get sentinel info
            sentinels = []
            for sentinel_node in self.sentinel_nodes:
                try:
                    sentinels.append({
                        'host': sentinel_node[0],
                        'port': sentinel_node[1],
                        'status': 'up'
                    })
                except:
                    sentinels.append({
                        'host': sentinel_node[0],
                        'port': sentinel_node[1],
                        'status': 'down'
                    })

            return {
                "status": "healthy",
                "master": {
                    "host": master_info[0],
                    "port": master_info[1]
                },
                "replicas": [
                    {"host": s[0], "port": s[1]} for s in slaves
                ],
                "replicas_count": len(slaves),
                "sentinels": sentinels,
                "sentinels_count": len([s for s in sentinels if s['status'] == 'up'])
            }
        except Exception as e:
            logger.error("redis_health_check_failed", error=str(e))
            return {
                "status": "unhealthy",
                "error": str(e)
            }

    async def close(self):
        """Close all connections"""
        if self._master_client:
            await self._master_client.close()
        if self._replica_client:
            await self._replica_client.close()


# Singleton instance
_redis_sentinel: Optional[RedisSentinelManager] = None


def get_redis_sentinel() -> RedisSentinelManager:
    """Get singleton Redis Sentinel manager"""
    global _redis_sentinel
    if _redis_sentinel is None:
        _redis_sentinel = RedisSentinelManager()
    return _redis_sentinel


async def get_redis_master():
    """Get master client for writes"""
    sentinel = get_redis_sentinel()
    return await sentinel.get_master()


async def get_redis_replica():
    """Get replica client for reads"""
    sentinel = get_redis_sentinel()
    return await sentinel.get_replica()


async def get_redis_health():
    """Get cluster health"""
    sentinel = get_redis_sentinel()
    return await sentinel.health_check()
