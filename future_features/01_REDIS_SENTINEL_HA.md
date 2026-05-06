# Feature 1: Redis Sentinel High Availability

## 📋 Tổng quan

**Mục tiêu:** Thay thế KeyDB single-node bằng Redis Sentinel cluster để đạt 99.9% uptime với auto-failover < 10s.

**Lý do cần thiết:**
- KeyDB hiện tại là single point of failure
- Khi KeyDB sập → FastAPI fallback InfluxDB → latency tăng 100x
- Không có replication → mất data khi container restart

**Kiến trúc:**
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Redis Master│────▶│Redis Replica│────▶│Redis Replica│
│   (Write)   │     │   (Read)    │     │   (Read)    │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       └───────────────────┴───────────────────┘
                           │
              ┌────────────┴────────────┐
              │   Sentinel Cluster      │
              │  (3 nodes, quorum=2)    │
              └─────────────────────────┘
```

---

## 🔧 Implementation

### Step 1: Docker Compose Configuration

**File:** `docker-compose.yml`

```yaml
services:
  # ══════════════════════════════════════════════════════════════════════════════
  # REDIS SENTINEL CLUSTER (Replace KeyDB)
  # ══════════════════════════════════════════════════════════════════════════════
  
  redis-master:
    image: redis:7.2-alpine
    container_name: redis-master
    hostname: redis-master
    restart: unless-stopped
    ports:
      - "6379:6379"
    command: >
      redis-server
      --appendonly yes
      --appendfsync everysec
      --maxmemory 2gb
      --maxmemory-policy allkeys-lru
      --save 60 1000
      --hz 50
      --tcp-backlog 511
    volumes:
      - redis-master-data:/data
    networks:
      - crypto-net
    deploy:
      resources:
        limits:
          memory: 2.5G
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

  redis-replica-1:
    image: redis:7.2-alpine
    container_name: redis-replica-1
    hostname: redis-replica-1
    restart: unless-stopped
    command: >
      redis-server
      --replicaof redis-master 6379
      --replica-read-only yes
      --appendonly yes
      --appendfsync everysec
    depends_on:
      redis-master:
        condition: service_healthy
    volumes:
      - redis-replica-1-data:/data
    networks:
      - crypto-net
    deploy:
      resources:
        limits:
          memory: 2.5G
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

  redis-replica-2:
    image: redis:7.2-alpine
    container_name: redis-replica-2
    hostname: redis-replica-2
    restart: unless-stopped
    command: >
      redis-server
      --replicaof redis-master 6379
      --replica-read-only yes
      --appendonly yes
      --appendfsync everysec
    depends_on:
      redis-master:
        condition: service_healthy
    volumes:
      - redis-replica-2-data:/data
    networks:
      - crypto-net
    deploy:
      resources:
        limits:
          memory: 2.5G
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

  redis-sentinel-1:
    image: redis:7.2-alpine
    container_name: redis-sentinel-1
    hostname: redis-sentinel-1
    restart: unless-stopped
    ports:
      - "26379:26379"
    command: redis-sentinel /etc/redis/sentinel.conf
    volumes:
      - ./docker/redis/sentinel.conf:/etc/redis/sentinel.conf
      - redis-sentinel-1-data:/data
    depends_on:
      redis-master:
        condition: service_healthy
    networks:
      - crypto-net
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "26379", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

  redis-sentinel-2:
    image: redis:7.2-alpine
    container_name: redis-sentinel-2
    hostname: redis-sentinel-2
    restart: unless-stopped
    ports:
      - "26380:26379"
    command: redis-sentinel /etc/redis/sentinel.conf
    volumes:
      - ./docker/redis/sentinel.conf:/etc/redis/sentinel.conf
      - redis-sentinel-2-data:/data
    depends_on:
      redis-master:
        condition: service_healthy
    networks:
      - crypto-net
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "26379", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

  redis-sentinel-3:
    image: redis:7.2-alpine
    container_name: redis-sentinel-3
    hostname: redis-sentinel-3
    restart: unless-stopped
    ports:
      - "26381:26379"
    command: redis-sentinel /etc/redis/sentinel.conf
    volumes:
      - ./docker/redis/sentinel.conf:/etc/redis/sentinel.conf
      - redis-sentinel-3-data:/data
    depends_on:
      redis-master:
        condition: service_healthy
    networks:
      - crypto-net
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "26379", "ping"]
      interval: 5s
      timeout: 3s
      retries: 3

volumes:
  redis-master-data:
  redis-replica-1-data:
  redis-replica-2-data:
  redis-sentinel-1-data:
  redis-sentinel-2-data:
  redis-sentinel-3-data:
```

---

### Step 2: Sentinel Configuration

**File:** `docker/redis/sentinel.conf`

```conf
# Sentinel port
port 26379

# Monitor master
sentinel monitor mymaster redis-master 6379 2

# Master down after 5 seconds
sentinel down-after-milliseconds mymaster 5000

# Only 1 replica can sync at a time during failover
sentinel parallel-syncs mymaster 1

# Failover timeout
sentinel failover-timeout mymaster 10000

# Disable script reconfiguration
sentinel deny-scripts-reconfig yes

# Notification script (optional)
# sentinel notification-script mymaster /path/to/notify.sh

# Client reconfig script (optional)
# sentinel client-reconfig-script mymaster /path/to/reconfig.sh
```

---

### Step 3: Backend - Sentinel Client

**File:** `backend/core/redis_sentinel.py`

```python
from redis.sentinel import Sentinel
from redis.exceptions import ConnectionError, TimeoutError
import logging
from typing import Optional
import asyncio

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
        self.sentinel_nodes = [
            ('redis-sentinel-1', 26379),
            ('redis-sentinel-2', 26379),
            ('redis-sentinel-3', 26379)
        ]
        
        self.sentinel = Sentinel(
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
                        'mymaster',
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
                        'mymaster',
                        socket_timeout=0.5,
                        socket_connect_timeout=0.5,
                        decode_responses=True,
                        max_connections=50
                    )
                    logger.info("redis_replica_connected")
                except Exception as e:
                    logger.warning("redis_replica_connection_failed", error=str(e))
                    # Fallback to master for reads
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
            master_info = self.sentinel.discover_master('mymaster')
            
            # Discover replicas
            slaves = self.sentinel.discover_slaves('mymaster')
            
            # Get sentinel info
            sentinels = []
            for sentinel_node in self.sentinel_nodes:
                try:
                    s = self.sentinel.sentinel_for_service('mymaster')
                    info = s.sentinel('sentinels', 'mymaster')
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
redis_sentinel = RedisSentinelManager()

async def get_redis_master():
    """Get master client for writes"""
    return await redis_sentinel.get_master()

async def get_redis_replica():
    """Get replica client for reads"""
    return await redis_sentinel.get_replica()

async def get_redis_health():
    """Get cluster health"""
    return await redis_sentinel.health_check()
```

---

### Step 4: Update Backend Services

**File:** `backend/services/candle_service.py`

```python
from backend.core.redis_sentinel import get_redis_master, get_redis_replica
import json
import logging

logger = logging.getLogger(__name__)

async def get_candles(symbol: str, interval: str, limit: int):
    """
    Get candles with read/write splitting
    
    Reads from replica (load balanced)
    Falls back to InfluxDB if Redis unavailable
    """
    try:
        # Read from replica
        redis = await get_redis_replica()
        
        key = f"candle:{interval}:{symbol}"
        
        # Check key exists
        exists = await redis.exists(key)
        if not exists:
            logger.warning("redis_key_not_found", key=key)
            raise KeyError(f"Key {key} not found")
        
        # Get candles
        raw = await redis.zrangebyscore(key, '-inf', '+inf', withscores=True)
        
        if len(raw) < limit * 0.5:
            logger.warning("redis_insufficient_data", 
                key=key, 
                expected=limit, 
                actual=len(raw))
            raise ValueError("Insufficient data")
        
        candles = _parse_candles(raw)
        logger.info("redis_read_success", key=key, count=len(candles))
        
        return candles
        
    except Exception as e:
        logger.error("redis_read_failed", error=str(e))
        
        # Fallback to InfluxDB
        candles = await _query_influx(symbol, interval, limit)
        
        # Warm cache in background
        asyncio.create_task(_warm_redis_cache(symbol, interval, candles))
        
        return candles

async def write_candle(symbol: str, interval: str, candle: dict):
    """
    Write candle to master
    
    Writes to master only
    Replicas sync automatically
    """
    try:
        # Write to master
        redis = await get_redis_master()
        
        key = f"candle:{interval}:{symbol}"
        
        # Dedup: remove old entry with same timestamp
        await redis.zremrangebyscore(key, candle['openTime'], candle['openTime'])
        
        # Add new entry
        await redis.zadd(key, {json.dumps(candle): candle['openTime']})
        
        # Set TTL
        ttl = _get_ttl_for_interval(interval)
        await redis.expire(key, ttl)
        
        logger.debug("redis_write_success", key=key, timestamp=candle['openTime'])
        
    except Exception as e:
        logger.error("redis_write_failed", key=key, error=str(e))
        raise

async def _warm_redis_cache(symbol: str, interval: str, candles: list):
    """
    Warm Redis cache after fallback
    
    Runs in background
    """
    try:
        redis = await get_redis_master()
        key = f"candle:{interval}:{symbol}"
        
        # Batch write
        pipe = redis.pipeline()
        for c in candles:
            pipe.zadd(key, {json.dumps(c): c['openTime']})
        
        # Set TTL
        ttl = _get_ttl_for_interval(interval)
        pipe.expire(key, ttl)
        
        await pipe.execute()
        
        logger.info("redis_cache_warmed", key=key, count=len(candles))
    except Exception as e:
        logger.error("redis_warm_failed", key=key, error=str(e))
```

---

### Step 5: Health Check Endpoint

**File:** `backend/api/health.py`

```python
from fastapi import APIRouter
from backend.core.redis_sentinel import get_redis_health
from backend.core.database import get_influx, get_trino_connection
import time

router = APIRouter()

@router.get("/api/health")
async def health_check():
    """
    Health check endpoint
    
    Checks:
    - Redis Sentinel cluster
    - InfluxDB
    - Trino
    """
    start = time.time()
    
    checks = {}
    
    # Redis Sentinel
    try:
        redis_health = await get_redis_health()
        checks["redis"] = redis_health
    except Exception as e:
        checks["redis"] = {"status": "error", "error": str(e)}
    
    # InfluxDB
    try:
        influx = await get_influx()
        await influx.ping()
        checks["influxdb"] = {"status": "ok"}
    except Exception as e:
        checks["influxdb"] = {"status": "error", "error": str(e)}
    
    # Trino
    try:
        trino = await get_trino_connection()
        cursor = trino.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        checks["trino"] = {"status": "ok"}
    except Exception as e:
        checks["trino"] = {"status": "error", "error": str(e)}
    
    total_latency = (time.time() - start) * 1000
    
    # Overall status
    overall_status = "ok" if all(
        c.get("status") == "ok" or c.get("status") == "healthy" 
        for c in checks.values()
    ) else "degraded"
    
    return {
        "status": overall_status,
        "checks": checks,
        "total_latency_ms": round(total_latency, 2)
    }
```

---

## 🧪 Testing

### Test 1: Normal Operation

```bash
# Start cluster
docker compose up -d redis-master redis-replica-1 redis-replica-2 \
  redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# Wait for healthy
sleep 10

# Check master
docker exec redis-sentinel-1 redis-cli -p 26379 \
  SENTINEL get-master-addr-by-name mymaster

# Expected output:
# 1) "redis-master"
# 2) "6379"

# Check replicas
docker exec redis-sentinel-1 redis-cli -p 26379 \
  SENTINEL replicas mymaster

# Write to master
docker exec redis-master redis-cli SET test_key "hello"

# Read from replica
docker exec redis-replica-1 redis-cli GET test_key
# Output: "hello"

# Check health endpoint
curl http://localhost:8080/api/health | jq '.checks.redis'
```

### Test 2: Master Failover

```bash
# Stop master
docker stop redis-master

# Watch sentinel logs
docker logs -f redis-sentinel-1

# Expected logs:
# +sdown master mymaster 172.18.0.2 6379
# +odown master mymaster 172.18.0.2 6379 #quorum 2/2
# +failover-triggered master mymaster 172.18.0.2 6379
# +failover-state-select-slave master mymaster 172.18.0.2 6379
# +selected-slave slave 172.18.0.3:6379 172.18.0.3 6379
# +failover-state-send-slaveof-noone slave 172.18.0.3:6379
# +failover-state-wait-promotion slave 172.18.0.3:6379
# +promoted-slave slave 172.18.0.3:6379
# +failover-end master mymaster 172.18.0.2 6379

# Check new master (should be replica-1 or replica-2)
docker exec redis-sentinel-1 redis-cli -p 26379 \
  SENTINEL get-master-addr-by-name mymaster

# Application should continue working
curl http://localhost:8080/api/klines?symbol=BTCUSDT&interval=1m&limit=10

# Restart old master (becomes replica)
docker start redis-master

# Check replication
docker exec redis-master redis-cli INFO replication
# Should show: role:slave
```

### Test 3: Split Brain Prevention

```bash
# Isolate master from sentinels (network partition)
docker network disconnect crypto-net redis-master

# Sentinels should promote a replica
sleep 10

# Check new master
docker exec redis-sentinel-1 redis-cli -p 26379 \
  SENTINEL get-master-addr-by-name mymaster

# Reconnect old master
docker network connect crypto-net redis-master

# Old master should become replica automatically
docker exec redis-master redis-cli INFO replication
```

---

## 📊 Monitoring

### Prometheus Metrics

**File:** `backend/core/metrics.py`

```python
from prometheus_client import Gauge, Counter, Histogram

# Redis Sentinel metrics
redis_master_up = Gauge('redis_master_up', 'Redis master is up')
redis_replicas_count = Gauge('redis_replicas_count', 'Number of replicas')
redis_sentinels_count = Gauge('redis_sentinels_count', 'Number of sentinels')

redis_read_latency = Histogram('redis_read_latency_seconds', 'Redis read latency')
redis_write_latency = Histogram('redis_write_latency_seconds', 'Redis write latency')

redis_failovers_total = Counter('redis_failovers_total', 'Total failovers')
redis_read_errors_total = Counter('redis_read_errors_total', 'Total read errors')
redis_write_errors_total = Counter('redis_write_errors_total', 'Total write errors')
```

---

## 🚀 Deployment

### Migration từ KeyDB sang Redis Sentinel

```bash
# Step 1: Backup KeyDB data
docker exec keydb redis-cli --rdb /data/dump.rdb
docker cp keydb:/data/dump.rdb ./backup/keydb_dump_$(date +%Y%m%d).rdb

# Step 2: Start Redis Sentinel cluster
docker compose up -d redis-master redis-replica-1 redis-replica-2 \
  redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# Step 3: Restore data to Redis master
docker cp ./backup/keydb_dump_*.rdb redis-master:/data/dump.rdb
docker restart redis-master

# Step 4: Wait for replication
sleep 30

# Step 5: Update backend to use Sentinel
# (Code changes already done above)

# Step 6: Restart backend
docker compose restart fastapi

# Step 7: Verify
curl http://localhost:8080/api/health

# Step 8: Stop KeyDB
docker compose stop keydb

# Step 9: Monitor for 24h before removing KeyDB
```

---

## ✅ Success Criteria

- [ ] 3 Redis nodes running (1 master + 2 replicas)
- [ ] 3 Sentinel nodes running (quorum=2)
- [ ] Health check shows all nodes healthy
- [ ] Failover completes in < 10s
- [ ] Application continues during failover (< 10s downtime)
- [ ] Read traffic load-balanced across replicas
- [ ] Write traffic goes to master only
- [ ] Data persisted (AOF + RDB)
- [ ] Monitoring metrics exposed

---

## 📈 Expected Improvements

| Metric | Before (KeyDB) | After (Sentinel) | Improvement |
|--------|----------------|------------------|-------------|
| Uptime | 95% | 99.9% | +4.9% |
| Failover time | Manual (hours) | Auto (< 10s) | 360x faster |
| Read throughput | 10k ops/s | 30k ops/s | 3x |
| Write throughput | 10k ops/s | 10k ops/s | Same |
| Recovery time | Manual restore | Auto sync | Instant |
