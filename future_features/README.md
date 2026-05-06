# Future Features Development Plan - Index

## 📚 Danh sách tài liệu

### ✅ Đã hoàn thành

1. **[00_SUMMARY_AND_ROOT_CAUSE.md](./00_SUMMARY_AND_ROOT_CAUSE.md)**
   - Tóm tắt lỗi line endings (CRLF vs LF)
   - Nguyên nhân gốc rễ
   - Cách phòng tránh (Git, EditorConfig, Pre-commit hooks)

2. **[01_REDIS_SENTINEL_HA.md](./01_REDIS_SENTINEL_HA.md)**
   - Redis Sentinel 3-node cluster
   - Auto-failover < 10s
   - Read/write splitting
   - Code chi tiết từng file

3. **[02_KAFKA_3NODE_CLUSTER.md](./02_KAFKA_3NODE_CLUSTER.md)**
   - Kafka 3-broker cluster
   - Replication factor 3, min ISR 2
   - Multi-threaded producer
   - Flink parallelism = 12

### 📝 Tóm tắt các feature còn lại

4. **Feature 3: Multi-Timeframe Storage (1m, 5m, 15m, 1h, 4h, 1d, 1w)**
   - **Vấn đề:** Hiện tại chỉ lưu 1s và 1m, các timeframe khác aggregate on-the-fly → tốn CPU
   - **Giải pháp:**
     - Flink pre-aggregate 1m → 5m, 15m, 1h, 4h, 1d, 1w
     - Lưu vào KeyDB với TTL khác nhau (5m: 14 ngày, 1h: 90 ngày, 1d: 365 ngày)
     - Lưu vào InfluxDB với tag `interval`
     - Spark batch aggregate vào Iceberg
   - **Code:**
     - `src/processing/aggregators/multi_interval_aggregator.py`
     - `src/processing/writers/keydb_multi_interval.py`
     - `backend/services/candle_service.py` (update để đọc từ pre-aggregated data)

5. **Feature 4: Historical Date Range Picker**
   - **Vấn đề:** Frontend chỉ scroll-left vô hạn, không có DateRangePicker
   - **Giải pháp:**
     - Frontend: React DateRangePicker component
     - Backend: `/api/klines/historical` với fallback chain (KeyDB → InfluxDB → Iceberg)
     - Trino query optimization cho long-range queries
   - **Code:**
     - `frontend/src/components/DateRangePicker.tsx`
     - `frontend/src/components/CandlestickChart.tsx` (add historical mode)
     - `backend/api/historical.py` (optimize query logic)

6. **Feature 5: WebSocket Scaling (10,000 connections)**
   - **Vấn đề:** FastAPI 2 workers → max 2000 connections
   - **Giải pháp:**
     - Redis Pub/Sub cho WebSocket broadcasting
     - Nginx load balancing với `least_conn`
     - FastAPI horizontal scaling (4 instances × 4 workers = 16,000 capacity)
   - **Code:**
     - `backend/api/websocket.py` (subscribe Redis Pub/Sub)
     - `src/processing/writers/redis_pubsub.py` (Flink publish updates)
     - `docker/nginx/nginx.conf` (upstream load balancing)

7. **Feature 6: ELK Stack Monitoring**
   - **Vấn đề:** Không có centralized logging, khó debug
   - **Giải pháp:**
     - Elasticsearch + Logstash + Kibana
     - Filebeat thu thập logs từ tất cả containers
     - Structured logging (JSON format)
     - Dashboards: Service Health, Data Pipeline, Business Metrics
   - **Code:**
     - `docker-compose.yml` (add ELK services)
     - `docker/logstash/pipeline/logstash.conf`
     - `docker/filebeat/filebeat.yml`
     - `backend/core/logging.py` (structured logging)

8. **Feature 7: Late Data Handling**
   - **Vấn đề:** Binance WebSocket có thể gửi data muộn, Flink window đã đóng
   - **Giải pháp:**
     - Flink Watermark với `allowedLateness(30s)`
     - Side output cho extremely late data (> 30s)
     - Backend flag `is_final` cho candles
     - Frontend visual indicator cho tentative candles
   - **Code:**
     - `src/processing/pipeline.py` (watermark strategy)
     - `src/processing/aggregators/kline_window_aggregator.py` (late data handling)
     - `backend/api/websocket.py` (add `is_final` flag)
     - `frontend/src/components/CandlestickChart.tsx` (tentative candle styling)

---

## 🎯 Roadmap

### Phase 1: High Availability (Tuần 1-2)
- ✅ Redis Sentinel (Feature 1)
- ✅ Kafka 3-node cluster (Feature 2)
- **Ưu tiên:** Critical cho production

### Phase 2: Performance & Scalability (Tuần 3-4)
- Multi-timeframe storage (Feature 3)
- WebSocket scaling (Feature 5)
- **Ưu tiên:** High - cải thiện UX

### Phase 3: Observability (Tuần 5)
- ELK Stack (Feature 6)
- **Ưu tiên:** Medium - cần cho monitoring

### Phase 4: Data Quality (Tuần 6)
- Late data handling (Feature 7)
- Historical date picker (Feature 4)
- **Ưu tiên:** Medium - nice to have

---

## 📊 Resource Requirements

### Hardware (Production)

| Component | Current | After Phase 1 | After Phase 2 |
|-----------|---------|---------------|---------------|
| **RAM** | 18.5 GB | 28 GB | 35 GB |
| **CPU** | 9.4 cores | 15 cores | 20 cores |
| **Disk** | 50 GB | 100 GB | 150 GB |
| **Network** | 100 Mbps | 200 Mbps | 500 Mbps |

**Recommended EC2:** `t3a.2xlarge` → `m5.2xlarge` (8 vCPU, 32 GB RAM)

### Services Count

| Phase | Services | Containers |
|-------|----------|------------|
| Current | 15 | 15 |
| Phase 1 | 21 | 21 (Redis: +6, Kafka: +2) |
| Phase 2 | 25 | 29 (FastAPI: 4 replicas) |
| Phase 3 | 29 | 33 (ELK: +4) |

---

## 🧪 Testing Strategy

### 1. Unit Tests
```bash
# Backend
pytest backend/tests/unit/

# Frontend
npm test
```

### 2. Integration Tests
```bash
# API endpoints
pytest backend/tests/integration/

# Kafka → Flink → Redis pipeline
pytest tests/integration/test_pipeline.py
```

### 3. Load Tests
```bash
# WebSocket connections
locust -f tests/load/websocket_load.py --users 10000

# Kafka throughput
kafka-producer-perf-test --topic crypto_klines --num-records 1000000 --throughput 10000
```

### 4. Chaos Engineering
```bash
# Kill random service
docker kill $(docker ps -q | shuf -n 1)

# Network partition
docker network disconnect crypto-net redis-master

# Resource exhaustion
stress-ng --vm 1 --vm-bytes 20G --timeout 60s
```

---

## 📈 Success Metrics

### Availability
- **Target:** 99.9% uptime (< 8.76 hours downtime/year)
- **Current:** ~95% (manual recovery)
- **Measurement:** Prometheus `up` metric

### Performance
- **API Latency P95:** < 100ms (current: ~200ms)
- **WebSocket Latency:** < 500ms (current: ~1s)
- **Kafka Lag:** < 1000 messages (current: ~5000)

### Scalability
- **Concurrent Users:** 10,000 (current: ~1,000)
- **Messages/sec:** 30,000 (current: ~10,000)
- **Data Retention:** 2 years (current: 90 days)

---

## 🚀 Deployment Checklist

### Pre-deployment
- [ ] Backup all data (Redis, InfluxDB, Postgres)
- [ ] Test in staging environment
- [ ] Load test với production traffic
- [ ] Document rollback procedure

### Deployment
- [ ] Blue-green deployment (zero downtime)
- [ ] Canary release (10% → 50% → 100%)
- [ ] Monitor metrics dashboard
- [ ] Alert on-call engineer

### Post-deployment
- [ ] Verify all services healthy
- [ ] Check data consistency
- [ ] Monitor for 24 hours
- [ ] Update documentation

---

## 📞 Support

### Issues
- GitHub Issues: https://github.com/your-repo/issues
- Slack: #crypto-platform-support

### Documentation
- Architecture: `docs/DOCUMENTATION.md`
- API Docs: http://localhost:8080/docs
- Runbook: `docs/RUNBOOK.md`

### On-call
- PagerDuty: crypto-platform-oncall
- Escalation: Lead Engineer → CTO
