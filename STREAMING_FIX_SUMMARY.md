# Streaming Fix Summary - 2026-05-04

## Vấn đề ban đầu
- Flink job liên tục restart với lỗi `ModuleNotFoundError: No module named 'writers'`
- Backend API gặp lỗi `ReadOnlyError: You can't write against a read only replica`
- UI không hiển thị giá real-time

## Các sửa đổi đã thực hiện

### 1. Sửa Flink Python Module Import
**File:** `scripts/auto_submit_jobs.sh`

**Vấn đề:** Script tạo zip file không bao gồm `__init__.py` cho package `writers`

**Giải pháp:**
```bash
# Thêm __init__.py vào zip file
writers_init = 'processing/writers/__init__.py'
if os.path.exists(writers_init):
    zf.write(writers_init, 'writers/__init__.py')
else:
    zf.writestr('writers/__init__.py', '')
```

### 2. Cập nhật Kafka Bootstrap Servers (3-node cluster)
**Files:** 
- `src/common/config.py`
- `src/processing/pipeline.py`

**Thay đổi:**
```python
# Before
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

# After  
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
```

**Lý do:** Hệ thống giờ có 3 Kafka brokers cho high availability

### 3. Tăng Flink Parallelism
**File:** `src/processing/pipeline.py`

**Thay đổi:**
```python
# Before
FLINK_PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "1"))

# After
FLINK_PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "12"))
```

**Lý do:** Kafka có 12 partitions, cần 12 parallel tasks để xử lý hiệu quả

### 4. Sửa Redis Sentinel Read/Write Splitting
**Files:**
- `backend/api/klines.py`
- `backend/api/orderbook.py`

**Vấn đề:** Backend đang dùng `get_redis()` (trả về replica) cho cả read và write operations

**Giải pháp:**
```python
# Import thêm
from backend.core.redis_sentinel import get_redis_master

# Sử dụng master cho write operations
r_master = await get_redis_master()
pipe = r_master.pipeline()
pipe.set(cache_key, json.dumps(result))
pipe.pexpire(cache_key, ttl_ms)
await pipe.execute()
```

**Lý do:** Redis replicas là read-only, chỉ master mới có thể write

## Kết quả sau khi sửa

### ✅ Flink Job Status
```
Job ID: c39ba093351b8f8ee61a09a3ecd060f8
Status: RUNNING
Parallelism: 12
All 48 tasks (4 operators × 12 parallelism) đang RUNNING
```

### ✅ Kafka Cluster
```
- kafka-1: HEALTHY (port 19092)
- kafka-2: HEALTHY (port 9093)
- kafka-3: HEALTHY (port 9094)
- Replication Factor: 3
- Min ISR: 2
- Partitions per topic: 12
```

### ✅ Redis Sentinel Cluster
```
Master: redis-master (172.18.0.2:6379)
Replicas: 2 (redis-replica-1, redis-replica-2)
Sentinels: 3 (all UP)
  - redis-sentinel-1:26379
  - redis-sentinel-2:26379
  - redis-sentinel-3:26379
```

### ✅ Data Flow Verification
```bash
# Producer đang gửi data
ws-ticker [TICKER] sent=37 (change=24, heartbeat=13)

# Redis có data mới
$ docker exec redis-master redis-cli HGETALL "ticker:latest:BTCUSDT"
price: 80291.99
event_time: 1777913175015  # Timestamp mới nhất

# Candles đang được aggregate
$ docker exec redis-master redis-cli ZCARD "candle:1m:BTCUSDT"
4  # Số lượng 1m candles

# Backend API hoạt động
$ curl http://localhost:8080/api/ticker/BTCUSDT
{"symbol":"BTCUSDT","price":80291.99,"change24h":0.0,...}

$ curl http://localhost:8080/api/orderbook/BTCUSDT
{"bids":[[...]],"asks":[[...]],"spread":0.01}
```

### ✅ System Health
```json
{
  "status": "ok",
  "checks": {
    "redis": {
      "status": "healthy",
      "replicas_count": 2,
      "sentinels_count": 3
    },
    "influxdb": "ok",
    "trino": "ok"
  },
  "latency_ms": {
    "redis_ms": 1.27,
    "influxdb_ms": 4.7,
    "trino_ms": 36.22
  }
}
```

## Kiến trúc streaming hiện tại

```
┌─────────────┐
│   Binance   │
│  WebSocket  │
└──────┬──────┘
       │
       v
┌─────────────┐     ┌──────────────────────────────┐
│  Producer   │────>│  Kafka 3-Node Cluster        │
│  (Python)   │     │  - kafka-1, kafka-2, kafka-3 │
└─────────────┘     │  - 12 partitions/topic       │
                    │  - Replication Factor: 3     │
                    └──────────┬───────────────────┘
                               │
                               v
                    ┌──────────────────────┐
                    │  Flink Cluster       │
                    │  - Parallelism: 12   │
                    │  - 48 tasks running  │
                    └──────┬───────────────┘
                           │
                ┏━━━━━━━━━━┻━━━━━━━━━━┓
                v                      v
    ┌───────────────────┐   ┌──────────────────┐
    │ Redis Sentinel    │   │   InfluxDB       │
    │ - 1 Master        │   │   (Time-series)  │
    │ - 2 Replicas      │   └──────────────────┘
    │ - 3 Sentinels     │
    └─────────┬─────────┘
              │
              v
    ┌──────────────────┐
    │  FastAPI Backend │
    │  - Read: Replica │
    │  - Write: Master │
    └─────────┬────────┘
              │
              v
    ┌──────────────────┐
    │   React UI       │
    │   (WebSocket)    │
    └──────────────────┘
```

## Các điểm cần lưu ý

1. **Kafka Consumer Group "no active members"**: Đây là behavior bình thường của Flink Table API. Flink quản lý offsets internally và không hiển thị như traditional consumer groups. Data vẫn đang được consume (verify qua Redis timestamps).

2. **Redis Sentinel Read/Write Splitting**: 
   - Reads: Dùng replicas (load balancing)
   - Writes: Dùng master (consistency)
   - Backend phải import `get_redis_master()` cho write operations

3. **Flink Parallelism = Kafka Partitions**: Để tối ưu throughput, parallelism nên bằng số partitions (12).

4. **High Availability**:
   - Kafka: Có thể mất 1 broker mà vẫn hoạt động (min.insync.replicas=2)
   - Redis: Sentinel tự động failover nếu master down
   - Flink: TaskManager có thể restart tasks tự động

## Testing Commands

```bash
# Check Flink job status
curl http://localhost:8081/jobs | jq

# Check Redis data
docker exec redis-master redis-cli HGETALL "ticker:latest:BTCUSDT"

# Check Kafka consumer lag
docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --group flink_crypto_ticker_v1 --describe"

# Test backend APIs
curl http://localhost:8080/api/health
curl http://localhost:8080/api/ticker/BTCUSDT
curl http://localhost:8080/api/klines?symbol=BTCUSDT&interval=1m&limit=5

# Check producer logs
docker logs producer --tail 20
```

## Next Steps

1. ✅ Flink job running stable
2. ✅ Kafka 3-node cluster operational
3. ✅ Redis Sentinel HA working
4. ✅ Backend API serving data
5. 🔄 Test WebSocket streaming to UI
6. 🔄 Verify UI displays real-time prices correctly
