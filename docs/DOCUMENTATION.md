# Tài liệu Kỹ thuật — Lambda Architecture Crypto Platform

> **Mục tiêu tài liệu:** Giải thích sâu toàn bộ hệ thống từ luồng dữ liệu, kiến trúc, từng service, đến logic xử lý trong code — để bạn hiểu được "cái gì làm gì và tại sao".

> **Bổ sung cập nhật hiện trạng (2026-04-25):**
> - Docker Compose hiện chạy theo cấu hình **21 services**.
> - **Backend đã refactor**: `serving/` → `backend/` (MVC: `api/`, `services/`, `models/`, `core/`).
> - **Dev/Prod mode**: `docker-compose.override.yml` (dev), `docker-compose.prod.yml` (prod).
> - **Makefile**: `make dev`, `make prod`, `make test`, `make test-cov`.
> - **Testing**: pytest framework with 40+ tests (unit, security).
> - Endpoint historical chính thức: `GET /api/klines/historical`.
> - WebSocket stream: `WS /api/stream?symbol=&interval=`.
> - Dagster chỉ giữ 2 schedule: `daily_candle_aggregation` (04:00 hằng ngày) và `weekly_iceberg_maintenance` (03:00 Chủ Nhật).
> - HTTPS automation: Let's Encrypt via certbot + DuckDNS dynamic DNS.
> - Nginx rate limiting: 30r/s API, 5r/s WebSocket per IP.
> - Xem `docs/TRACKING.md` (gitignored) cho changelog chi tiết.

---

## Mục lục

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc Lambda — Tổng thể](#2-kiến-trúc-lambda--tổng-thể)
3. [Luồng dữ liệu đầy đủ](#3-luồng-dữ-liệu-đầy-đủ)
4. [Lớp Data (Data Layer)](#4-lớp-data-data-layer)
5. [Lớp Compute (Processing Layer)](#5-lớp-compute-processing-layer)
6. [Lớp Query & Orchestration](#6-lớp-query--orchestration)
7. [Lớp API (Backend)](#7-lớp-api-backend)
8. [Lớp Frontend (React)](#8-lớp-frontend-react)
9. [Chi tiết từng file source code](#9-chi-tiết-từng-file-source-code)
10. [Kafka topics và Avro schemas](#10-kafka-topics-và-avro-schemas)
11. [KeyDB — cấu trúc key và TTL](#11-keydb--cấu-trúc-key-và-ttl)
12. [InfluxDB — measurement và retention](#12-influxdb--measurement-và-retention)
13. [Iceberg / MinIO — cold storage](#13-iceberg--minio--cold-storage)
14. [Dagster — Orchestration & Scheduling](#14-dagster--orchestration--scheduling)
15. [Startup & Vận hành](#15-startup--vận-hành)
16. [Các vấn đề đã gặp và cách sửa](#16-các-vấn-đề-đã-gặp-và-cách-sửa)

---

## 1. Tổng quan dự án

**Lambda Architecture for TradingView-Style Platform** là một hệ thống real-time xử lý và hiển thị giá tiền điện tử, hoạt động hoàn toàn trên Docker (21 services theo `docker-compose.yml` hiện tại). Hệ thống theo dõi ~400 cặp USDT Spot từ Binance, xử lý dữ liệu theo kiến trúc Lambda gồm 3 lớp: **Speed layer** (Flink), **Batch layer** (Spark), **API layer** (FastAPI + KeyDB), và hiển thị qua dashboard TradingView-style (React + lightweight-charts).

**Tech stack:**

| Thành phần | Công nghệ | Phiên bản |
|---|---|---|
| Message broker | Apache Kafka (KRaft mode) | 3.9.0 |
| Schema registry | Apicurio (Confluent-compatible) | 2.6.2 |
| Stream processing | Apache Flink | 1.18.1 |
| Batch processing | Apache Spark | 3.5 |
| Hot cache | KeyDB (Redis-compatible) | latest |
| Time-series DB | InfluxDB | 2.7 |
| Cold storage | Apache Iceberg + MinIO | 1.5.2 + latest |
| Federated query | Trino | 442 |
| Orchestration | Dagster | latest |
| API server | FastAPI + Uvicorn | 0.115+ |
| Frontend | React 18 + lightweight-charts | v5.1.0 |
| Reverse proxy | Nginx | 1.27 |
| Metadata DB | PostgreSQL | 16 |

---

## 2. Kiến trúc Lambda — Tổng thể

Kiến trúc Lambda chia dữ liệu xử lý thành 3 đường song song:

```
                         ┌─────────────────────────────────────────────────────────┐
                         │                 BINANCE WebSocket API                   │
                         │   !ticker@arr  |  @kline_1s  |  @aggTrade  |  @depth   │
                         └──────────────────────────┬──────────────────────────────┘
                                                    │
                                            [producer_binance.py]
                                         Avro serialize → Kafka
                                                    │
               ┌────────────────────────────────────┼──────────────────────────────────┐
               │                                    │                                  │
        crypto_ticker                         crypto_klines                   crypto_depth
        crypto_trades                                │                         crypto_trades
               │                                    │                                  │
               │                          ┌─────────▼──────────┐                      │
               │                          │   Apache Flink      │                      │
               │                          │  (Speed Layer)      │◄─────────────────────┘
               │                          │                      │
               │                          │ KlineWindowAggregator│
               │                          │  1s → 1m aggregation │
               │                          │                      │
               │                          │ KeyDBKlineWriter     │
               │                          │  candle:1s, candle:1m│
               │                          │                      │
               │                          │ InfluxDBKlineWriter  │
               │                          │  candles measurement │
               │                          │                      │
               │                          │ IndicatorWriter      │
               │                          │  SMA20/50, EMA12/26  │
               │                          └──────────────────────┘
               │
               │  [Batch Layer — chạy qua Dagster schedule]
               │
               ▼
        [backfill_historical.py]           [aggregate_candles.py]
         Binance REST → Iceberg             1m → 1h aggregation
         historical_hourly table            coin_klines_hourly table
               │
               ▼
         ┌────────────┐    ┌──────────┐    ┌──────────┐
         │  KeyDB     │    │ InfluxDB │    │  Iceberg │
         │ (hot cache)│    │ (warm)   │    │ (cold)   │
         └─────┬──────┘    └────┬─────┘    └────┬─────┘
               │               │                │
               └───────────────┴────────────────┘
                                       │
                               [FastAPI API Layer]
                               /api/klines, /api/stream (WS)
                               /api/ticker, /api/orderbook
                               /api/klines/historical (Trino)
                                       │
                               [Nginx reverse proxy]
                                       │
                               [React Frontend]
                               CandlestickChart (lightweight-charts)
                               OrderBook, RecentTrades, Watchlist
```

**Tại sao Lambda Architecture?**

- **Speed layer** (Flink): Xử lý real-time với độ trễ < 2s, lưu vào KeyDB + InfluxDB
- **Batch layer** (Spark): Xử lý lại chính xác dữ liệu lịch sử, lưu vào Iceberg (MinIO)
- **API layer** (FastAPI): Merge 2 nguồn theo độ ưu tiên: KeyDB → InfluxDB → Trino/Iceberg

---

## 3. Luồng dữ liệu đầy đủ

### 3.1 Real-time path (< 2s latency)

```
Binance WS (!ticker@arr, batch mỗi 14-30s)
  → producer_binance.py → Avro serialize
  → Kafka topic: crypto_ticker
  → Flink: KeyDBWriter / InfluxDBWriter
  → KeyDB: ticker:latest:{symbol} (hash)
  → InfluxDB: market_ticks measurement

Binance WS (@kline_1s mỗi symbol, 1 message/giây)
  → producer_binance.py → Avro serialize
  → Kafka topic: crypto_klines (interval=1s)
  → Flink: KeyDBKlineWriter → candle:1s:{symbol} (sorted set, TTL 8h)
  → Flink: InfluxDBKlineWriter → InfluxDB candles (interval=1s)
  → Flink KlineWindowAggregator:
       - Mỗi giây nhận 1 candle, lưu vào MapState (dedup tự động)
       - Khi bước qua phút mới → aggregate 60 giây → emit 1m candle
       - Safety timer 65s: emit nếu stream im lặng > 65s
       - Gap-fill: forward-fill close price cho giây bị thiếu
  → emit 1m candle
  → Flink: KeyDBKlineWriter → candle:1m:{symbol} (sorted set, TTL 7 ngày)
  → Flink: InfluxDBKlineWriter → InfluxDB candles (interval=1m)
  → Flink: IndicatorWriter (chỉ is_closed=True)
       → SMA20, SMA50, EMA12, EMA26
       → KeyDB indicator:latest:{symbol}
       → InfluxDB indicators measurement

Binance WS (@aggTrade mỗi symbol)
  → Kafka: crypto_trades
  → Flink: TradeWriter → KeyDB trades:{symbol} (list, max 100)

Binance WS (@depth20@100ms mỗi symbol)
  → Kafka: crypto_depth
  → Flink: DepthWriter → KeyDB orderbook:{symbol} (hash)
```

### 3.2 Batch path (lịch sử)

```
Manual run (không schedule):
  spark-submit backfill_historical.py --mode all --iceberg-mode incremental
    - Lấy timestamp mới nhất trong Iceberg historical_hourly
    - Binance REST API: kéo 1h klines từ đó đến nay → Iceberg
    - Detect gap InfluxDB 7 ngày → fill từ Binance REST → InfluxDB

Dagster schedule (daily 04:00 UTC):
  spark-submit aggregate_candles.py
    - Đọc InfluxDB 1m → aggregate → 1h → ghi lại InfluxDB
    - Đọc Iceberg historical_hourly → aggregate → coin_klines_hourly
    - Xóa InfluxDB 1m data cũ hơn RETENTION_1M_DAYS ngày (mặc định 90)

Dagster schedule (weekly Sunday 03:00 UTC):
  spark-submit iceberg_maintenance.py
```

### 3.3 API serving path (backend/)

```
GET /api/klines?symbol=BTCUSDT&interval=5m&limit=200
  → klines.py:
    1. Kiểm tra Redis cache klines_cache:{symbol}:{interval}:{limit} (100ms TTL)
    2. Nếu miss: đọc KeyDB candle:1m (7 ngày gần nhất) qua ZRANGEBYSCORE
    3. Nếu không đủ limit: fallback InfluxDB range(start: -90d) → thêm data, dedup
    4. Client-side aggregate: 1m → 5m/15m/1h/4h/1d/1w (bucketing theo interval)
    5. Merge in-progress candle:
       - Build từ candle:1s sub-candles trong phút hiện tại
       - Enrich close/high/low từ ticker nếu ticker_ts > last_1s_ts
    6. Cache 100ms → trả về JSON array

GET /api/klines?symbol=BTCUSDT&interval=5m&limit=200&endTime=1772965200000 (scroll-left)
  → klines.py:
    - Bỏ qua cache và KeyDB (chỉ 7 ngày, không đủ cho scroll xa)
    - start_ms = endTime - limit * interval_seconds * 1000
    - Query InfluxDB: range(start: RFC3339_start, stop: RFC3339_end) (absolute range)
    - Aggregate 1m → 5m nếu cần
    - Trả về candles có openTime < endTime, lấy limit cuối

WS /api/stream?symbol=BTCUSDT&interval=1m
  → ws.py:
    - Loop 0.5s
    - Đọc candle:1s đủ tất cả seconds của phút hiện tại
    - Aggregate → 1m candle với đúng OHLCV
    - Enrich close từ ticker nếu ticker mới hơn last 1s candle
    - Push nếu có thay đổi (diff check)
```

---

## 4. Lớp Data (Data Layer)

### 4.1 Apache Kafka 3.9.0 (KRaft mode)

- **Không cần Zookeeper** — KRaft built-in Raft consensus tự quản lý metadata
- **Config quan trọng:** `KAFKA_NUM_PARTITIONS=3`, retention 48h, LZ4 compression
- **4 topics:**
  - `crypto_ticker` — 24h stats cho tất cả ~400 USDT pairs (batch throttled)
  - `crypto_klines` — 1s kline candles, ~400 symbols × 1 msg/giây ≈ 400 msg/s
  - `crypto_trades` — aggregate trades (1 msg per aggTrade event)
  - `crypto_depth` — order book depth snapshots top-20 (100ms interval per symbol)
- **Port:** 9092 (internal `kafka:9092`)

### 4.2 Apicurio Schema Registry 2.6.2

- Confluent-compatible API tại `/apis/ccompat/v7`
- Producer đăng ký 4 Avro schemas lúc cold start: ticker, kline, trade, depth
- Wire format: `0x00` magic byte + 4-byte big-endian schema_id + Avro binary
- Flink consumer: `AvroDeserializer` dùng schema_id để lookup schema, deserialize message
- **Port:** 8085 → internal 8080

### 4.3 KeyDB (Redis-compatible hot cache)

High-performance fork của Redis hỗ trợ multi-threading và Server Assisted Client Caching.

- **Cấu hình:** `maxmemory 2560mb`, `maxmemory-policy allkeys-lru`, `hz 50`
- **Port:** 6379

Xem chi tiết key patterns tại [Section 11](#11-keydb--cấu-trúc-key-và-ttl).

### 4.4 InfluxDB 2.7 (Warm time-series storage)

Time-series database tối ưu cho dữ liệu có timestamp liên tục. Lưu ~90 ngày dữ liệu 1m candles.

- **Org:** `vi`, **Bucket:** `crypto`
- **Query language:** Flux (functional pipeline)
- **Port:** 8086

Xem chi tiết measurements tại [Section 12](#12-influxdb--measurement-và-retention).

### 4.5 PostgreSQL 16

Lưu metadata cho 2 mục đích:

1. **Dagster metadata:** scheduler state, run history, asset materialization events
2. **Iceberg catalog:** table definitions, partition specs, snapshot history, file locations

- Databases: `dagster` và `iceberg_catalog`
- **Port:** 5432 (internal)

### 4.6 MinIO (S3-compatible object storage)

- **Buckets:**
  - `cryptoprice` — Iceberg data files (Parquet), table metadata JSON
  - `flink-checkpoints` — Flink job checkpoints (RocksDB snapshots)
- **Port:** 9000 (API), 9001 (console UI: http://localhost:9001)

---

## 5. Lớp Compute (Processing Layer)

### 5.1 Apache Flink 1.18.1 — Speed Layer

**2 containers:** `flink-jobmanager` (2.5GB RAM cap) và `flink-taskmanager` (7GB RAM cap, 6GB reserved)

**Job file:** `src/ingest_flink_crypto.py` — PyFlink, submit qua REST API

**Checkpointing:**
- Backend: HashMapStateBackend
- Interval: 120s, mode: EXACTLY_ONCE, unaligned checkpoints enabled
- Min pause: 30s, timeout: 120s
- Storage: `file:///tmp/flink-checkpoints` (mounted volume)
- Failover: Tự động restart từ checkpoint gần nhất khi job crash

**Web UI:** http://localhost:8081

Job tạo ra **5 processing pipelines song song:**

#### Pipeline 1 — Ticker → KeyDB + InfluxDB

```
Source: crypto_ticker (Kafka, Avro)
  ↓
KeyDBWriter (MapFunction):
  - Buffer 100 messages, flush mỗi 0.5s
  - Ghi ticker:latest:{symbol} (HSET)
  - Ghi ticker:history:{symbol} (ZADD, cleanup khi > 1000 entries)
  ↓
InfluxDBWriter (MapFunction):
  - Buffer 200 messages, flush mỗi 0.5s
  - Ghi market_ticks measurement
```

#### Pipeline 2 — Raw 1s candles → KeyDB + InfluxDB

```
Source: crypto_klines (Kafka, Avro, filter: interval="1s")
  ↓
KeyDBKlineWriter:
  - ZREMRANGEBYSCORE(key, ts, ts)  # dedup
  - ZADD candle:1s:{symbol} ts json
  - EXPIRE candle:1s:{symbol} 28800  # 8h
  ↓
InfluxDBKlineWriter:
  - Ghi candles measurement (interval tag = "1s")
```

#### Pipeline 3 — 1s → 1m Window Aggregation

```
Source: crypto_klines (Kafka, Avro, filter: interval="1s")
  ↓
KlineWindowAggregator (KeyedProcessFunction, key=symbol):
  [State: MapState<kline_start_ms, candle_json>]
  [State: ValueState<window_start_ms>]
  [State: ValueState<timer_ts>]
  [State: ValueState<symbol_name>]

  processElement(candle):
    - map_state[kline_start_ms] = candle  # dedup giống HashMap
    - nếu kline_start_ms bước qua phút mới (> window_start + 60000):
        → _flush_window(): aggregate all 1s candles → emit 1m candle
        → update window_start = new minute
        → cancel timer cũ, đăng ký timer mới window_start + 65000

  on_timer(timestamp):
    - Kiểm tra timestamp == saved_timer_ts (tránh stale timer)
    - _flush_window() với gap-fill: forward-fill close cho giây thiếu
    - Emit 1m candle ngay cả khi ít hơn 60 sub-candles

  _flush_window():
    - Sort entries theo timestamp
    - Gap-fill (ít nhất 1 giây → fill với close từ giây trước)
    - open = first.open, high = max(high), low = min(low)
    - close = last.close, volume = sum(volume)
    - Emit JSON message
  ↓
(emit 1m candle)
  ↓  ↓  ↓
KeyDBKlineWriter  InfluxDBKlineWriter  IndicatorWriter
 candle:1m          candles(1m)          SMA/EMA
```

#### Pipeline 4 — Depth → KeyDB

```
Source: crypto_depth (Kafka, Avro)
  ↓
DepthWriter (MapFunction):
  - Buffer 50, flush 0.3s
  - HSET orderbook:{symbol} bids (json) asks (json) ts last_update_id
```

#### Pipeline 5 — Trades → KeyDB

```
Source: crypto_trades (Kafka, Avro)
  ↓
TradeWriter (MapFunction):
  - LPUSH trades:{symbol} json
  - LTRIM trades:{symbol} 0 99  # giữ 100 gần nhất
```

### 5.2 Apache Spark 3.5 — Batch Layer

**2 containers:** `spark-master` và `spark-worker` (4GB RAM, 4 vCores)

Spark chỉ chạy khi được gọi bởi Dagster (không idle). Các jobs:

#### `src/backfill_historical.py`

```
--mode populate:
  Khởi tạo lần đầu — kéo 90 ngày 1m candles từ Binance REST
  Multi-threaded (5 workers), batch 1000 candles/request
  Delay 120ms giữa requests (Binance rate limit: 1200 req/min)
  Ghi InfluxDB theo batch 500 write points

--mode influx:
  Query InfluxDB tìm gap (khoảng thiếu dữ liệu)
  Fill gap từ Binance REST 1m API
  Use case: máy tắt, Kafka consumer lag, Flink job crash

--mode iceberg:
  Gọi Spark job để ghi Iceberg (dùng Spark DataFrame API)
  Đọc Binance REST 1h API → DataFrame → write Iceberg incremental

--mode all --iceberg-mode incremental:
  Chạy cả influx + iceberg incremental (dùng bởi Dagster daily)
```

#### `src/aggregate_candles.py`

```
Input: InfluxDB 1m candles (query Flux → DataFrame)
Processing: Spark resample 1m → aggregate → 1h
  - openTime = floor(openTime / 3600000) * 3600000
  - open = first, high = max, low = min, close = last, volume = sum
Output: InfluxDB write (1h interval tag)

Input: Iceberg historical_hourly
Processing: Spark aggregate → coin_klines_hourly
Output: Iceberg upsert

Cleanup: Delete InfluxDB 1m points older than RETENTION_1M_DAYS
```

#### `src/iceberg_maintenance.py`

```
expireSnapshots(older_than=30_days)
rewriteDataFiles(target_file_size=128MB)  # compact small Parquet files
```

**Spark History Server:** http://localhost:18080

---

## 6. Lớp Query & Orchestration

### 6.1 Trino 442 — Federated Query Engine

- Query xuyên source mà không cần copy data
- FastAPI dùng Trino JDBC/Python client để query Iceberg tables qua `/api/klines/historical`
- **Catalog:** `iceberg` — kết nối MinIO (Parquet) + PostgreSQL (catalog metadata)
- **Use case:** DateRangePicker query dài hạn (ví dụ: cả năm 2024), chỉ query Iceberg, không phải InfluxDB
- **Port:** 8083 → internal 8080

### 6.2 Dagster — Workflow Orchestration

**2 containers:** `dagster-webserver` + `dagster-daemon`

State lưu trong PostgreSQL `dagster` database.

**`orchestration/assets.py`:**

| Asset | Mô tả | Schedule |
|---|---|---|
| `backfill_historical` | `spark-submit backfill_historical.py --mode all --iceberg-mode incremental` | Thủ công (không có schedule) |
| `aggregate_candles` | `spark-submit aggregate_candles.py --mode all` — 1m→1h + retention cleanup | Daily 04:00 UTC |
| `iceberg_table_maintenance` | `spark-submit iceberg_maintenance.py` — compact + expire | Weekly Sunday 03:00 UTC |

Dagster Daemon poll schedule mỗi 30s, tạo Run khi đến schedule, submit Spark job qua subprocess, stream log vào Dagster UI.

**Web UI:** http://localhost:3000

---

## 7. Lớp API (Backend)

`backend/` — FastAPI app theo kiến trúc MVC:

### 7.1 `main.py`

```python
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"])
app.include_router(klines.router)
app.include_router(ws.router)
app.include_router(ticker.router)
# ... các routers khác

@app.get("/api/health")
async def health():
    # ping KeyDB + InfluxDB + Trino
    # trả về {"status": "healthy", "services": {...}}
```

### 7.2 `connections.py`

Singleton pattern — tạo connection 1 lần, dùng lại:

```python
_redis_client = None
async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = await aioredis.from_url("redis://keydb:6379")
    return _redis_client

# get_influx() và get_trino_connection() tương tự
```

### 7.3 `routers/klines.py` — Router quan trọng nhất

**Endpoint:** `GET /api/klines?symbol=&interval=&limit=&endTime=`

**`_base_interval(interval)`** — quyết định resolution nào query từ KeyDB/InfluxDB:

| Request interval | Query interval | Source |
|---|---|---|
| `1s` | `1s` | `candle:1s` (KeyDB) |
| `1m` | `1m` | `candle:1m` (KeyDB) |
| `5m`, `15m` | `1m` | `candle:1m` (KeyDB) → aggregate |
| `1h`, `4h` | `1m` | `candle:1m` → aggregate (hoặc `1h` nếu span dài) |
| `1d`, `1w` | `1h` | InfluxDB 1h → aggregate |

**`_aggregate(candles, target_ms)`** — OHLCV re-sampling:

```python
buckets = {}
for c in candles:
    key = (c["openTime"] // target_ms) * target_ms  # floor to bucket
    if key not in buckets:
        buckets[key] = [c]
    else:
        buckets[key].append(c)

result = []
for key, group in sorted(buckets.items()):
    result.append({
        "openTime": key,
        "open": group[0]["open"],
        "high": max(c["high"] for c in group),
        "low": min(c["low"] for c in group),
        "close": group[-1]["close"],
        "volume": sum(c["volume"] for c in group),
    })
```

**`_query_influx_sync(symbol, interval, limit, range_h, end_ms)`:**

```python
if end_ms:  # scroll-left — absolute range
    stop_dt = datetime.utcfromtimestamp(end_ms / 1000).isoformat() + "Z"
    start_ms = end_ms - limit * interval_seconds * 1000 * BUFFER_FACTOR
    start_dt = datetime.utcfromtimestamp(start_ms / 1000).isoformat() + "Z"
    range_str = f'range(start: {start_dt}, stop: {stop_dt})'
else:  # real-time — relative range
    range_str = f'range(start: -{range_h}h)'
    
query = f"""
from(bucket: "crypto")
  |> {range_str}
  |> filter(fn: (r) => r._measurement == "candles")
  |> filter(fn: (r) => r.symbol == "{symbol}")
  |> filter(fn: (r) => r.interval == "{interval}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
  |> tail(n: {limit})
"""
```

**Flow chi tiết cho real-time request:**

```
1. Cache hit? → return immediately (100ms TTL)

2. KeyDB: ZRANGEBYSCORE candle:1m:{symbol} -inf +inf
   → parse JSON, sort by openTime, dedup by volume

3. Đủ limit? Nếu không: InfluxDB query -90d
   → merge, dedup (giữ entry có volume cao hơn cùng timestamp)

4. Aggregate 1m → target interval

5. In-progress candle:
   a. ZRANGEBYSCORE candle:1s:{symbol} current_minute_start +inf
   b. Aggregate 1s candles → {open, high, low, close, volume}
   c. HGET ticker:latest:{symbol}
   d. Nếu ticker.event_time > last_1s_candle.kline_start → override close/high/low

6. Cache 100ms, return
```

### 7.4 `routers/ws.py` — WebSocket real-time

**Endpoint:** `WS /api/stream?symbol=&interval=`

Lưu ý hành vi hiện tại của `ws.py`:
- `1s`: chỉ phát candle 1s mới nhất từ `candle:1s:{symbol}`, **không** override/synthesize bằng ticker.
- `1m`: aggregate từ `candle:1s` theo phút, không override bằng ticker.
- `5m+`: có thể merge với ticker khi ticker mới hơn và vẫn nằm trong window đang mở.

**`_build_candle(symbol, interval)`:**

```python
if interval == "1s":
    # Lấy candle 1s mới nhất từ candle:1s sorted set
    raw = await redis.zrange(f"candle:1s:{symbol}", -1, -1, withscores=True)
    candle = json.loads(raw[0])
    # Nếu ticker.event_time > candle.kline_start → enrich close/high/low
    
elif interval in ("1m", "5m", ...):
    source_key = "candle:1s" if interval == "1m" else "candle:1m"
    # Tính window start cho phút/period hiện tại
    window_start = (now_ms // interval_ms) * interval_ms
    # ZRANGEBYSCORE: lấy tất cả candles trong window hiện tại
    subs = await redis.zrangebyscore(f"{source_key}:{symbol}", window_start, "+inf")
    # Aggregate → {open, high, low, close, volume}
    candle = _aggregate_subs(subs)
    # Enrich từ ticker chỉ khi ticker mới hơn sub-candle mới nhất
    if ticker["event_time"] > latest_sub_ts:
        candle["close"] = ticker["price"]
        candle["high"] = max(candle["high"], ticker["price"])
        candle["low"] = min(candle["low"], ticker["price"])
```

**Push logic:** So sánh với candle trước → push nếu diff > threshold → tránh spam WebSocket.

### 7.5 Tất cả API Endpoints

| Method | Endpoint | Source | Mô tả |
|---|---|---|---|
| GET | `/api/klines` | KeyDB → InfluxDB | OHLCV candles, hỗ trợ scroll-left qua `endTime` |
| WS | `/api/stream` | KeyDB (real-time) | Live candle stream, 0.5s interval |
| GET | `/api/ticker/{symbol}` | KeyDB `ticker:latest` | Price, bid, ask, 24h change |
| GET | `/api/ticker` | KeyDB scan | Tất cả tickers |
| GET | `/api/orderbook/{symbol}` | KeyDB `orderbook` | Top-20 bid/ask depth |
| GET | `/api/trades/{symbol}` | KeyDB `trades` | 100 giao dịch gần nhất |
| GET | `/api/symbols` | KeyDB scan | Danh sách tất cả symbols |
| GET | `/api/indicators/{symbol}` | KeyDB `indicator:latest` | SMA20/50, EMA12/26 |
| GET | `/api/klines/historical` | Trino → Iceberg | Long-range date queries |
| GET | `/api/health` | All backends | Health check |

---

## 8. Lớp Frontend (React)

Nginx serve React SPA + proxy API:
- `location /api/` → `proxy_pass fastapi:8000`
- `location /api/stream` → WebSocket upgrade: `proxy_pass fastapi:8000`, `Upgrade: websocket`

**TimeFrame constants** (`frontend/src/constants/chartConstants.js`):

```js
export const TIMEFRAMES = ["1s", "1m", "5m", "15m", "1H", "4H", "1D", "1W"]
```
  a. fetchCandles(symbol, tf, limit) → preloadInitialCandles() khi thiếu nến ban đầu
    - timeframe <= 1h: viewport mặc định 50 nến mới nhất
    - timeframe > 1h: viewport mặc định 20 nến mới nhất
    - áp dụng cho cả live mode và historical mode
    → applyDataToChart()
Lưu ý uppercase H/D/W — tất cả API calls dùng `.toLowerCase()` trước khi gửi backend.

### 8.1 `marketDataService.js`

Service layer duy nhất giữa components và backend API:

```js
// endTime tính bằng SECONDS (lightweight-charts convention)
// Khi gọi API, convert sang ms: endTime * 1000
fetchCandles(symbol, timeframe, limit = 200, endTime = null)
  → GET /api/klines?symbol=&interval=&limit=[&endTime=ms]

// Trả về hàm unsubscribe
subscribeCandle(symbol, timeframe, onCandle)
  → WS /api/stream?symbol=&interval=
  → const unsub = subscribeCandle(...); // cleanup khi unmount

fetchHistoricalCandles(symbol, startMs, endMs, limit)
  → GET /api/klines/historical?...

fetchOrderBook(symbol) → GET /api/orderbook/{symbol}
fetchTrades(symbol) → GET /api/trades/{symbol}
fetchSymbols() → GET /api/symbols
```

### 8.2 `CandlestickChart.js` — Component chính (~600 dòng)

**State & Refs quan trọng:**

| Ref/State | Mô tả |
|---|---|
| `candlesRef` | Mảng candles hiện tại (không re-render, dùng cho WS callback) |
| `earliestTimestampRef` | Timestamp cũ nhất — dùng làm endTime khi scroll-left |
| `isLoadingMoreRef` | Lock boolean — tránh concurrent scroll-left requests |
| `chartRef` | lightweight-charts `IChartApi` instance |
| `candleRef` | `ICandlestickSeriesApi` — series chính |
| `volumeRef` | `IHistogramSeriesApi` — volume bars |
| `smaSeries`, `emaSeries` | Indicator series |

**3 luồng cập nhật chart độc lập:**

#### Luồng 1: Initial load
```
useEffect([symbol, timeframe]):
  fetchCandles(symbol, tf, 200) → applyDataToChart(data)
  
applyDataToChart(data):
  candleRef.setData(data)          // FULL replace (không append)
  volumeRef.setData(volumeData)
  recalculate SMA, EMA → set data
  chartRef.timeScale().fitContent()
  update earliestTimestampRef = data[0].time
```

#### Luồng 2: WebSocket real-time (1 candle/message)
```
subscribeCandle(symbol, tf, onCandle):
  onCandle(candle):
    if tf == "1s":
      if candle.time <= candlesRef.last.time → skip (stale)
      candleRef.update(candle)    // append or update last bar
    else (1m+):
      candleRef.update(candle)    // lightweight-charts handles append vs update
      update candlesRef
```

#### Luồng 3: Poll incremental (bù cho WS gaps)
```
setInterval(1000ms) → fetchCandles(symbol, tf, 3):
  pollIncremental(newCandles):
    for each newCandle in newCandles:
      idx = candlesRef.findIndex(c => c.time == newCandle.time)
      
      if idx == candlesRef.length - 1 && tf >= 1m:
        SKIP  // WS là authoritative cho candle đang chạy
              // Tránh flicker giữa REST và WS
      
      if idx found && changed:
        candleRef.update(newCandle)  // cập nhật bar đã đóng
      
      if not found && newCandle.time > candlesRef.last.time:
        candleRef.update(newCandle)  // append candle mới
        candlesRef.push(newCandle)
```

**Tại sao dùng cả WS và poll?**
- WS: cực nhanh (0.5s lag), nhưng có thể miss message nếu network jitter
- Poll: đảm bảo không miss candle đã đóng (closed candles quan trọng hơn)
- Poll skip live bar → WS không bị REST giá trị cũ override (không flicker)

#### Scroll-left loading

```
onVisibleLogicalRangeChanged(range):
  if range.from < 20 && !isLoadingMoreRef:
    isLoadingMoreRef = true
    earliestTime = candlesRef[0].time      // seconds (lightweight-charts)
    
    fetchCandles(symbol, tf, 500, earliestTime):
      // API nhận endTime=earliestTime*1000 ms
      // Trả về 500 candles trước earliestTime
    
    newCandles = result.filter(c => c.time < earliestTime)
    merged = [...newCandles, ...candlesRef.current]
    
    // Chart re-render với merged data
    candleRef.setData(merged)
    
    // Khôi phục viewport position (shift offset)
    const shift = newCandles.length
    chartRef.timeScale().scrollToPosition(-shift, false)
    
    update earliestTimestampRef = merged[0].time
    isLoadingMoreRef = false
```

---

## 9. Chi tiết từng file source code

| File | Dòng code | Chức năng chính |
|---|---|---|
| `src/ingest_flink_crypto.py` | 995 | Flink job: 5 pipelines, 7+ writer/processor classes |
| `src/backfill_historical.py` | 841 | Multi-mode backfill script (Spark/direct) |
| `src/producer_binance.py` | 631 | Binance WS → Kafka Avro producer |
| `src/candle_query_helper.py` | 357 | Shared Flux query helpers |
| `src/ingest_crypto.py` | 324 | Spark Structured Streaming → Iceberg |
| `src/iceberg_maintenance.py` | 173 | Iceberg compaction + snapshot expiry |
| `src/aggregate_candles.py` | 163 | Spark 1m→1h aggregation + retention |
| `frontend/src/components/CandlestickChart.js` | 997 | Main chart component |
| `frontend/src/services/marketDataService.js` | 388 | API service layer |
| `frontend/src/App.js` | 290 | Main React app layout |
| `backend/api/klines.py` | ~170 | OHLCV REST endpoint (thin handler) |
| `backend/api/historical.py` | ~90 | Historical range queries |
| `backend/api/websocket.py` | ~135 | WebSocket real-time candle stream |
| `backend/api/orderbook.py` | ~85 | Order book depth endpoint |
| `backend/api/health.py` | ~65 | Health check endpoint |
| `backend/app.py` | ~50 | FastAPI app root, CORS, routers |
| `backend/core/database.py` | ~50 | Singleton connection management |
| `backend/core/config.py` | ~30 | Environment variable config |
| `backend/core/constants.py` | ~35 | Shared constants |
| `backend/services/candle_service.py` | ~280 | Core OHLCV business logic |
| `backend/models/candle.py` | ~15 | Pydantic candle response model |
| `backend/models/ticker.py` | ~55 | Pydantic ticker/orderbook/trade models |
| `backend/api/ticker.py` | ~43 | Ticker price/stats endpoint |
| `backend/api/trades.py` | ~37 | Recent trades endpoint |
| `backend/api/symbols.py` | ~30 | Symbols list endpoint |
| `backend/api/indicators.py` | ~20 | SMA/EMA indicators endpoint |
| `orchestration/assets.py` | 151 | Dagster asset + schedule definitions |

---

## 10. Kafka topics và Avro schemas

### `crypto_ticker` (`schemas/ticker.avsc`)

```json
{
  "symbol": "BTCUSDT",
  "event_time": 1772965200000,
  "close": 83500.00,
  "bid": 83499.00,
  "ask": 83501.00,
  "h24_open": 82000.00,
  "h24_high": 84000.00,
  "h24_low": 81500.00,
  "h24_volume": 12345.67,
  "h24_quote_volume": 1029876543.21,
  "h24_price_change": 1500.00,
  "h24_price_change_pct": 1.83,
  "h24_trade_count": 987654
}
```

### `crypto_klines` (`schemas/kline.avsc`)

```json
{
  "symbol": "BTCUSDT",
  "interval": "1s",
  "kline_start": 1772965200000,
  "kline_close": 1772965200999,
  "open": 83500.00,
  "high": 83510.00,
  "low": 83495.00,
  "close": 83505.00,
  "volume": 1.234,
  "quote_volume": 103142.37,
  "trade_count": 45,
  "is_closed": false,
  "event_time": 1772965200500
}
```

### `crypto_trades` (`schemas/trade.avsc`)

```json
{
  "symbol": "BTCUSDT",
  "event_time": 1772965200123,
  "agg_trade_id": 123456789,
  "price": 83505.00,
  "quantity": 0.01234,
  "trade_time": 1772965200100,
  "is_buyer_maker": false
}
```

### `crypto_depth` (`schemas/depth.avsc`)

```json
{
  "symbol": "BTCUSDT",
  "event_time": 1772965200050,
  "last_update_id": 987654321,
  "bids": [["83499.00", "1.234"], ["83498.00", "2.100"]],
  "asks": [["83501.00", "0.567"], ["83502.00", "1.890"]]
}
```

---

## 11. KeyDB — cấu trúc key và TTL

| Key pattern | Type | TTL | Nội dung | Viết bởi |
|---|---|---|---|---|
| `ticker:latest:{symbol}` | Hash | None (overwritten) | price, bid, ask, volume, change24h, event_time | Flink KeyDBWriter |
| `ticker:history:{symbol}` | Sorted Set | Cleanup auto | score=event_time, member=`price:volume` | Flink KeyDBWriter |
| `candle:1s:{symbol}` | Sorted Set | 8h (28800s) | score=kline_start_ms, member=candle JSON | Flink KeyDBKlineWriter |
| `candle:1m:{symbol}` | Sorted Set | 7d (604800s) | score=kline_start_ms, member=candle JSON | Flink KeyDBKlineWriter |
| `candle:latest:{symbol}` | Hash | None | open/high/low/close/volume/kline_start/interval | Flink KeyDBKlineWriter |
| `indicator:latest:{symbol}` | Hash | None | sma20, sma50, ema12, ema26, timestamp | Flink IndicatorWriter |
| `orderbook:{symbol}` | Hash | None | bids (JSON), asks (JSON), ts, last_update_id | Flink DepthWriter |
| `trades:{symbol}` | List | None | List of trade JSON, max 100 entries | Flink TradeWriter |
| `klines_cache:{symbol}:{interval}:{limit}` | String | 100ms | Cached klines response JSON | FastAPI klines.py |

**Candle JSON format** trong sorted set:

```json
{
  "t": 1772965200000,
  "o": 83500.0,
  "h": 83510.0,
  "l": 83495.0,
  "c": 83505.0,
  "v": 1.234,
  "qv": 103142.37,
  "n": 45,
  "x": false
}
```

- `t`: kline_start ms, `o/h/l/c`: OHLC, `v`: base volume, `qv`: quote volume, `n`: trade count, `x`: is_closed

---

## 12. InfluxDB — measurement và retention

### Measurement: `candles`

- **Tags:** `symbol` (BTCUSDT), `exchange` ("binance"), `interval` ("1s" | "1m")
- **Fields:** open, high, low, close, volume, quote_volume (float64); trade_count (int64); is_closed (bool)
- **Time precision:** ms (epoch milliseconds)
- **Data:** ~129,000 candles/symbol cho 90 ngày 1m data
- **Retention:** Spark job xóa 1m data cũ hơn 90 ngày (aggregate_candles.py)

### Measurement: `market_ticks`

- **Tags:** `symbol`, `exchange`
- **Fields:** price, bid, ask, volume, quote_volume, price_change_pct, trade_count
- **Time:** event_time ms
- **Source:** Flink InfluxDBWriter (từ crypto_ticker topic)

### Measurement: `indicators`

- **Tags:** `symbol`, `exchange`
- **Fields:** sma20, sma50, ema12, ema26, close (float64)
- **Time:** kline_start ms
- **Source:** Flink IndicatorWriter (chỉ closed 1m candles)

### Flux query mẫu (scroll-left absolute range):

```flux
from(bucket: "crypto")
  |> range(start: 2026-01-01T00:00:00Z, stop: 2026-02-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "candles")
  |> filter(fn: (r) => r.symbol == "BTCUSDT")
  |> filter(fn: (r) => r.interval == "1m")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
  |> tail(n: 200)
```

---

## 13. Iceberg / MinIO — cold storage

### Table: `historical_hourly`

Dữ liệu raw 1h candles từ Binance REST API, backfilled từ đầu lịch sử coin.

| Column | Type | Mô tả |
|---|---|---|
| symbol | STRING | BTCUSDT |
| open_time | BIGINT | Epoch ms |
| open, high, low, close | DOUBLE | OHLC |
| volume, quote_volume | DOUBLE | Volume |
| trade_count | BIGINT | Số giao dịch |

- **Partitioning:** `PARTITION BY (symbol, year(to_timestamp(open_time/1000)), month(to_timestamp(open_time/1000)))`
- **Location:** `s3a://cryptoprice/historical_hourly/`

### Table: `coin_klines_hourly`

Dữ liệu 1h đã được aggregate từ Spark job, dùng cho Trino query.

- **Schema:** Giống `historical_hourly`
- **Location:** `s3a://cryptoprice/coin_klines_hourly/`

### Iceberg metadata flow:

```
Spark write:
  DataFrameWriter.format("iceberg")
               .mode("append")
               .save("iceberg.default.historical_hourly")
  
  → Parquet files: MinIO s3a://cryptoprice/historical_hourly/data/
  → Metadata JSON: MinIO s3a://cryptoprice/historical_hourly/metadata/
  → Snapshot catalog: PostgreSQL iceberg_catalog.iceberg_tables

Trino read:
  SELECT * FROM iceberg.default.coin_klines_hourly
  WHERE symbol = 'BTCUSDT'
    AND open_time BETWEEN 1704067200000 AND 1735689600000
  ORDER BY open_time
  
  → Trino reads PostgreSQL catalog → discovers Parquet file locations in MinIO
  → Reads Parquet files directly với column projection + predicate pushdown
```

---

## 14. Dagster — Orchestration & Scheduling

**`orchestration/assets.py`:**

```python
from dagster import asset, AssetExecutionContext, ScheduleDefinition

@asset(group_name="ingestion", description="Daily backfill: Binance → Iceberg + InfluxDB gap fill")
def backfill_historical(context: AssetExecutionContext):
    subprocess.run(["spark-submit", 
                    "/app/src/backfill_historical.py",
                    "--mode", "all",
                    "--iceberg-mode", "incremental"], check=True)

@asset(group_name="maintenance", description="Daily aggregate 1m→1h + retention cleanup")
def aggregate_candles(context: AssetExecutionContext):
    subprocess.run(["spark-submit",
                    "/app/src/aggregate_candles.py"], check=True)

backfill_schedule = ScheduleDefinition(
    job=define_asset_job("backfill_job", [backfill_historical]),
    cron_schedule="0 2 * * *",  # daily 02:00 UTC
)
aggregate_schedule = ScheduleDefinition(
    job=define_asset_job("aggregate_job", [aggregate_candles]),
    cron_schedule="0 3 * * *",  # daily 03:00 UTC
)
```

**`orchestration/workspace.yaml`:**

```yaml
load_from:
  - python_file:
      relative_path: assets.py
      working_directory: /app/orchestration
```

---

## 15. Startup & Vận hành

### Khởi động lần đầu (First-time setup)

```bash
# 1. Clone và cấu hình env
git clone https://github.com/StupidDuck64/Lambda-Architecture-for-TradingView-Style-Platform.git
cd Lambda-Architecture-for-TradingView-Style-Platform
cp *.env .env
# Edit .env: INFLUX_TOKEN, MINIO_ROOT_PASSWORD, POSTGRES_PASSWORD, ...

# 2. Khởi động toàn bộ stack
docker compose up -d

# 3. Chờ services healthy (~3-5 phút)
docker compose ps  # tất cả "healthy"

# 4. Khởi tạo dữ liệu lịch sử InfluxDB (90 ngày)
docker compose run --rm influx-backfill python /app/backfill_historical.py --mode populate --days 90
# Chạy khoảng 30-60 phút cho 400 symbols × 90 ngày

# 5. Submit Flink job
docker exec flink-jobmanager flink run \
  --python /app/src/ingest_flink_crypto.py \
  --pyFiles /app/src -d

# 6. Verify frontend
# Truy cập http://localhost → chart phải hiển thị dữ liệu real-time
```

### Rebuild sau khi sửa code

```bash
# FastAPI (backend/ thay đổi)
docker compose up -d --build fastapi

# Frontend (frontend/ thay đổi)
docker compose up -d --build nginx

# Flink job (src/ thay đổi) — không cần rebuild image vì volume mount
JOB_ID=$(docker exec flink-jobmanager flink list | grep RUNNING | awk '{print $4}')
docker exec flink-jobmanager flink cancel $JOB_ID
docker exec flink-jobmanager flink run --python /app/src/ingest_flink_crypto.py --pyFiles /app/src -d

# Producer (producer/ thay đổi)
docker compose up -d --build producer
```

### Port reference

| Service | External Port | URL |
|---|---|---|
| Frontend (Nginx) | 80 | http://localhost |
| FastAPI docs | 8080 | http://localhost:8080/docs |
| Flink Web UI | 8081 | http://localhost:8081 |
| Spark Master UI | 8082 | http://localhost:8082 |
| Trino UI | 8083 | http://localhost:8083 |
| Spark History Server | 18080 | http://localhost:18080 |
| Dagster Web UI | 3000 | http://localhost:3000 |
| InfluxDB UI | 8086 | http://localhost:8086 |
| MinIO Console | 9001 | http://localhost:9001 |
| Schema Registry | 8085 | http://localhost:8085 |
| KeyDB | 6379 | keydb:6379 (internal) |
| Kafka | 9092 | kafka:9092 (internal) |
| PostgreSQL | 5432 | postgres:5432 (internal) |

### Kiểm tra health

```bash
# Tất cả containers
docker compose ps

# Flink job đang chạy
docker exec flink-jobmanager flink list

# KeyDB
docker exec keydb keydb-cli ping  # PONG

# InfluxDB (số candles BTCUSDT)
docker exec influxdb influx query \
  'from(bucket:"crypto") |> range(start:-1d) |> filter(fn:(r)=>r.symbol=="BTCUSDT" and r.interval=="1m") |> count()' \
  -o vi --token $INFLUX_TOKEN

# FastAPI health
curl http://localhost:8080/api/health
```

### Xem logs

```bash
# FastAPI logs
docker compose logs -f fastapi

# Flink TaskManager real-time logs
docker compose logs -f flink-taskmanager | grep -v "heartbeat"

# Producer logs (confirm Kafka write)
docker compose logs -f producer

# Dagster run logs
# → Dagster UI: http://localhost:3000 → Runs
```

---

## 16. Các vấn đề đã gặp và cách sửa

### Bug 1: KeyDB ZADD tạo duplicate entries (dedup ratio 5.8x)

**Vấn đề:** Cùng 1 `kline_start` ghi nhiều lần (vì Flink xử lý nhiều updates của cùng giây), `ZADD` với member khác nhau tạo nhiều entries trong sorted set.

**Nguyên nhân:** Sorted set dedup theo `(score, member)` pair — nếu member string khác nhau (JSON khác) thì không dedup dù cùng timestamp.

**Sửa (`KeyDBKlineWriter`):**
```python
# Trước mỗi ZADD: xóa entry cũ cùng score (timestamp)
pipeline.zremrangebyscore(key, kline_start_ms, kline_start_ms)
pipeline.zadd(key, {json_str: kline_start_ms})
```

**Kết quả:** Dedup ratio từ 5.8x → 1.00x (không còn duplicate).

---

### Bug 2: KlineWindowAggregator safety timer cascade

**Vấn đề:** Safety timer cũ không bị cancel khi window mới bắt đầu → timer cũ fire giữa window đang chạy → emit partial candle với volume rất thấp bất thường (chỉ gom được 10-15 giây thay vì 60).

**Nguyên nhân:** `flink.api.common.state.ValueState` để lưu timer timestamp bị bỏ sót, không cancel timer trước khi đăng ký timer mới.

**Sửa (`KlineWindowAggregator`):**
```python
# Trong state: ValueState<Long> timer_ts
# Khi đăng ký timer mới:
if self.timer_ts.value():
    ctx.timer_service().delete_event_time_timer(self.timer_ts.value())
new_timer = window_start + 65000
ctx.timer_service().register_event_time_timer(new_timer)
self.timer_ts.update(new_timer)

# Trong on_timer:
def on_timer(self, timestamp, ctx, out):
    if self.timer_ts.value() != timestamp:
        return  # stale timer — ignore
    self._flush_window(out)
```

---

### Bug 3: klines.py dedup giữ partial candle

**Vấn đề:** Khi merge KeyDB (newer) và InfluxDB (older), cùng timestamp xuất hiện ở cả 2 → dedup giữ random → đôi khi giữ partial candle từ Flink (volume thấp) thay vì closed candle từ InfluxDB (volume đầy đủ).

**Sửa (`klines.py`):**
```python
# Dedup: giữ entry có volume cao nhất cho mỗi timestamp
best_by_time = {}
for c in all_candles:
    t = c["openTime"]
    if t not in best_by_time or c["volume"] > best_by_time[t]["volume"]:
        best_by_time[t] = c
result = sorted(best_by_time.values(), key=lambda x: x["openTime"])
```

---

### Bug 4: ticker stale overwrite candle → 1m+ chart latency 18s

**Vấn đề:** `ticker:latest` cập nhật qua `!ticker@arr` stream của Binance (batch 14-30s). Code cũ dùng ticker để override `close` price của candle hiện tại → chart hiển thị giá cách 18-30s trước, không phải giá live.

**Sửa (`ws.py` và `klines.py`):**
```python
# Chỉ dùng ticker nếu event_time MỚI HƠN sub-candle mới nhất
if ticker_event_time > latest_sub_candle_ts:
    candle["close"] = ticker_price
    candle["high"] = max(candle["high"], ticker_price)
    candle["low"] = min(candle["low"], ticker_price)
# Nếu ticker cũ hơn → giữ close từ 1s sub-candle (lag ~1-1.5s thay vì 18s)
```

---

### Bug 5: scroll-left không load historical data

**Vấn đề:** `_query_influx_sync()` luôn dùng `range(start: -{range_h}h)` tương đối (ví dụ `-9h`). Khi scroll đến dữ liệu 30 ngày trước, range `-9h` hoàn toàn không bao phủ → trả về 0 candles.

**Sửa (`klines.py`):**
```python
def _query_influx_sync(symbol, interval, limit, range_h, end_ms=None):
    if end_ms:
        # Tính absolute range: [end_ms - buffer, end_ms]
        stop_dt = _ms_to_rfc3339(end_ms + 1000)
        start_ms = end_ms - limit * _interval_to_seconds(interval) * 1000 * 1.5
        start_dt = _ms_to_rfc3339(int(start_ms))
        range_clause = f'range(start: {start_dt}, stop: {stop_dt})'
    else:
        range_clause = f'range(start: -{range_h}h)'
```

---

### Bug 6: 1m chart flickering (nhấp nháy candle cuối)

**Vấn đề:** WS (`ws.py`) build candle từ 1s sub-candles → OHLCV chính xác (ví dụ volume=1.23). REST poll (`klines.py`) build in-progress candle từ ticker only → `{O=H=L=C=ticker, volume=0}`. Frontend nhận WS candle rồi 0.5s sau overwrite bởi REST candle khác giá trị → nhấp nháy 2 lần/giây.

**Sửa 2 nơi:**

1. `klines.py` — in-progress candle cũng build từ 1s sub-candles giống ws.py:
```python
subs = await redis.zrangebyscore(f"candle:1s:{symbol}", minute_start, "+inf")
if subs:
    # aggregate như ws.py
    inprogress_candle = _aggregate_1s(subs)
    inprogress_candle["close"] = ticker_price if ticker_newer else inprogress_close
```

2. `CandlestickChart.js` — poll skip live bar:
```python
if idx == candlesRef.current.length - 1 && intervalMs >= 60000:
    continue;  // WS là authoritative cho candle đang chạy trên 1m+ chart
```

---

**Ghi chú phiên bản tài liệu:**
- Phiên bản: 2.1
- Cập nhật lần cuối: 2026-04-25
- Covers commit: `0802fe0` (HEAD, main)
- Cập nhật bởi: AI assistant (xem `docs/TRACKING.md` cho changelog chi tiết)
