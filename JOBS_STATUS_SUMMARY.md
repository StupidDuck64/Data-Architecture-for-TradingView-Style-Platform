# Jobs Status Summary - 2026-05-05

## ✅ Hoàn thành

### 1. Flink Streaming Job
**Status:** ✅ RUNNING (Stable)

**Job Details:**
- Job ID: `c39ba093351b8f8ee61a09a3ecd060f8`
- Job Name: `Crypto_MultiStream_Kafka_to_KeyDB_InfluxDB`
- Parallelism: 12
- Total Tasks: 48 (4 operators × 12 parallelism)
- All tasks: RUNNING

**Data Flow:**
```
Binance WebSocket → Producer → Kafka (3 brokers, 12 partitions)
                                  ↓
                            Flink (12 parallel tasks)
                                  ↓
                    ┌─────────────┴─────────────┐
                    ↓                           ↓
            Redis Sentinel              InfluxDB
         (1 master + 2 replicas)    (Time-series)
```

**Operators:**
1. `Source: kafka_ticker` → KeyDB + InfluxDB (12 tasks RUNNING)
2. `Source: kafka_klines` → KeyDB + InfluxDB + Aggregation (12 tasks RUNNING)
3. `Source: kafka_depth` → KeyDB (12 tasks RUNNING)
4. `KEYED PROCESS` → 1m candles + Indicators (12 tasks RUNNING)

**Verification:**
```bash
# Check job status
curl http://localhost:8081/jobs/c39ba093351b8f8ee61a09a3ecd060f8

# Verify data in Redis
docker exec redis-master redis-cli HGETALL "ticker:latest:BTCUSDT"
# Output: price=80291.99, event_time=1777913175015 (real-time)

# Check candles
docker exec redis-master redis-cli ZCARD "candle:1m:BTCUSDT"
# Output: 4 candles
```

**Kafka Consumer Groups:**
```bash
# Flink is consuming from Kafka (LAG exists but being processed)
docker exec kafka-1 bash -c "/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --group flink_crypto_ticker_v1 --describe"

# LAG: 200-300 messages per partition (normal for real-time streaming)
```

---

### 2. Backend API
**Status:** ✅ RUNNING

**Health Check:**
```json
{
  "status": "ok",
  "checks": {
    "redis": {
      "status": "healthy",
      "master": {"host": "172.18.0.2", "port": 6379},
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

**API Endpoints Working:**
- ✅ `/api/health` - System health
- ✅ `/api/ticker/BTCUSDT` - Real-time ticker
- ✅ `/api/orderbook/BTCUSDT` - Order book
- ✅ `/api/klines?symbol=BTCUSDT&interval=1m&limit=5` - Candles
- ✅ `/api/symbols` - 403 symbols available

**Fixes Applied:**
- Redis Sentinel read/write splitting (use master for writes)
- Kafka 3-node cluster configuration
- Flink parallelism = 12 (match Kafka partitions)

---

### 3. Infrastructure
**Status:** ✅ ALL HEALTHY

**Kafka Cluster (3 nodes):**
```
- kafka-1: HEALTHY (port 19092)
- kafka-2: HEALTHY (port 9093)
- kafka-3: HEALTHY (port 9094)
- Replication Factor: 3
- Min ISR: 2
- Partitions: 12 per topic
```

**Redis Sentinel Cluster:**
```
- Master: redis-master (172.18.0.2:6379)
- Replicas: 2 (redis-replica-1, redis-replica-2)
- Sentinels: 3 (all UP)
  - redis-sentinel-1:26379
  - redis-sentinel-2:26379
  - redis-sentinel-3:26379
```

**Producer:**
```
Status: RUNNING
Sending: ~30-40 messages/sec
Topics: crypto_ticker, crypto_klines, crypto_trades, crypto_depth
```

---

## 🔄 Đang chạy

### 4. InfluxDB Backfill
**Status:** 🔄 IN PROGRESS

**Command:**
```bash
docker compose run --rm influx-backfill \
  python /app/src/batch/backfill.py --mode populate --days 7
```

**Purpose:** Nạp dữ liệu lịch sử 7 ngày vào InfluxDB cho tất cả symbols

**Expected Duration:** ~10-15 minutes (for 7 days × 403 symbols)

**Note:** Đang test với 7 ngày trước. Sau khi thành công, có thể chạy lại với `--days 90` để nạp đầy đủ 90 ngày.

---

## ⏸️ Tạm hoãn / Cần debug

### 5. Spark Streaming Job (Kafka → Iceberg)
**Status:** ⏸️ FAILED - Cần debug

**Error:**
```
IllegalArgumentException: Cannot initialize FileIO implementation 
org.apache.iceberg.aws.s3.S3FileIO: Cannot find constructor for 
interface org.apache.iceberg.io.FileIO
Missing org.apache.iceberg.aws.s3.S3FileIO 
[java.lang.NoClassDefFoundError: software/amazon/awssdk/core/exception/SdkException]
```

**Root Cause:** Thiếu AWS SDK v2 dependencies cho Iceberg S3FileIO

**Attempted Fix:**
```bash
# Đã thử thêm packages:
--packages 'software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18'
```

**Status:** Job vẫn exit sau khi start. Cần investigate thêm.

**Priority:** MEDIUM - Spark streaming job ghi vào Iceberg (lakehouse) cho long-term storage và analytics. Không ảnh hưởng đến real-time UI vì UI đọc từ Redis/InfluxDB.

**Next Steps:**
1. Kiểm tra Iceberg catalog configuration (PostgreSQL)
2. Verify MinIO bucket permissions
3. Test với simple Spark job trước
4. Consider alternative: Sử dụng Flink để ghi vào Iceberg thay vì Spark

---

### 6. Iceberg Incremental Backfill
**Status:** ⏸️ PENDING (Chờ Spark streaming job hoạt động)

**Command:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,..." \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  /app/src/batch/backfill.py --mode all --iceberg-mode incremental
```

**Purpose:** Nạp dữ liệu lịch sử vào Iceberg tables

**Dependency:** Cần Spark streaming job hoạt động trước

---

## 📊 System Performance

### Current Metrics

**Flink:**
- Throughput: Processing real-time (no backpressure)
- Checkpointing: Every 2 minutes
- State: Healthy

**Kafka:**
- Producer rate: ~30-40 msg/sec
- Consumer lag: 200-300 messages (normal)
- Replication: All in-sync

**Redis:**
- Latency: ~1.27ms (excellent)
- Keys: ~403 ticker keys + candles
- Memory: Within limits

**InfluxDB:**
- Latency: ~4.7ms (good)
- Writing: Real-time ticks + candles
- Retention: 90 days for 1m data

---

## 🎯 Priorities

### High Priority (Completed ✅)
1. ✅ Flink streaming job stable
2. ✅ Kafka 3-node cluster operational
3. ✅ Redis Sentinel HA working
4. ✅ Backend API serving real-time data
5. ✅ Producer sending data to Kafka

### Medium Priority (In Progress 🔄)
6. 🔄 InfluxDB backfill (7 days test running)
7. ⏸️ Spark streaming to Iceberg (needs debug)

### Low Priority (Can defer)
8. ⏸️ Iceberg backfill (depends on #7)
9. ⏸️ Full 90-day InfluxDB backfill (after 7-day test succeeds)

---

## 🚀 Next Actions

### Immediate (Today)
1. ✅ Monitor InfluxDB backfill progress
2. ✅ Verify UI can display real-time prices from Redis
3. ✅ Test WebSocket streaming to frontend

### Short-term (This Week)
4. Debug Spark streaming job Iceberg integration
5. Run full 90-day InfluxDB backfill
6. Set up Dagster scheduled jobs

### Long-term (Next Week)
7. Implement Iceberg backfill
8. Add monitoring dashboards
9. Performance tuning

---

## 📝 Notes

### Why Spark Streaming is Lower Priority
- **Real-time UI** reads from Redis (hot cache) and InfluxDB (recent data)
- **Iceberg** is for long-term storage and batch analytics
- **Current setup** already provides full real-time functionality
- **Spark job** can be fixed later without impacting user experience

### Data Flow Summary
```
Real-time Path (Working ✅):
Binance → Producer → Kafka → Flink → Redis/InfluxDB → Backend API → UI

Batch Path (Pending ⏸️):
Binance → Producer → Kafka → Spark → Iceberg → Trino → Analytics
```

### Testing Commands
```bash
# Check Flink job
curl http://localhost:8081/jobs

# Check real-time data
curl http://localhost:8080/api/ticker/BTCUSDT

# Check Redis
docker exec redis-master redis-cli HGETALL "ticker:latest:BTCUSDT"

# Check InfluxDB backfill progress
docker logs influx-backfill --tail 50

# Check Spark Master UI
curl http://localhost:8082
```

---

## ✅ Summary

**Working Systems:**
- ✅ Flink streaming (real-time processing)
- ✅ Kafka 3-node cluster (message queue)
- ✅ Redis Sentinel (hot cache)
- ✅ InfluxDB (time-series storage)
- ✅ Backend API (serving data)
- ✅ Producer (ingesting from Binance)

**In Progress:**
- 🔄 InfluxDB backfill (7 days test)

**Needs Work:**
- ⏸️ Spark streaming to Iceberg (dependency issues)
- ⏸️ Iceberg backfill (depends on Spark)

**Overall Status:** 🟢 **PRODUCTION READY** for real-time trading view functionality. Batch analytics layer (Iceberg) can be added later.
