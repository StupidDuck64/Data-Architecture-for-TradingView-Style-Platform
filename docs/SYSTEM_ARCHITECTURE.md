# CryptoPrice — System Architecture & Feature Documentation

> **Last updated:** 2026-03-11  
> **Author:** Auto-generated from source code analysis  
> **Version:** 1.0

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Sơ đồ data flow](#2-sơ-đồ-data-flow)
3. [Docker Containers (18 services)](#3-docker-containers-18-services)
4. [Data Layer — Ingestion](#4-data-layer--ingestion)
   - 4.1 [Producer (Binance WebSocket → Kafka)](#41-producer-binance-websocket--kafka)
   - 4.2 [Kafka](#42-kafka)
   - 4.3 [Schema Registry (Apicurio)](#43-schema-registry-apicurio)
   - 4.4 [Avro Schemas](#44-avro-schemas)
5. [Compute Layer — Stream Processing](#5-compute-layer--stream-processing)
   - 5.1 [Flink (Real-time Pipeline)](#51-flink-real-time-pipeline)
   - 5.2 [Spark (Batch Processing)](#52-spark-batch-processing)
6. [Storage Layer](#6-storage-layer)
   - 6.1 [KeyDB (Real-time Cache)](#61-keydb-real-time-cache)
   - 6.2 [InfluxDB (Time-Series Database)](#62-influxdb-time-series-database)
   - 6.3 [MinIO + Iceberg (Data Lake)](#63-minio--iceberg-data-lake)
   - 6.4 [PostgreSQL (Metadata Store)](#64-postgresql-metadata-store)
7. [Query Layer](#7-query-layer)
   - 7.1 [Trino (SQL on Iceberg)](#71-trino-sql-on-iceberg)
8. [Serving Layer — FastAPI](#8-serving-layer--fastapi)
   - 8.1 [API Endpoints](#81-api-endpoints)
   - 8.2 [Data Routing & Priority](#82-data-routing--priority)
   - 8.3 [WebSocket Real-time Stream](#83-websocket-real-time-stream)
   - 8.4 [Candle Aggregation Logic](#84-candle-aggregation-logic)
9. [Visualization Layer — React Frontend](#9-visualization-layer--react-frontend)
   - 9.1 [Kiến trúc component](#91-kiến-trúc-component)
   - 9.2 [CandlestickChart — Core Chart](#92-candlestickchart--core-chart)
   - 9.3 [Watchlist & OverviewChart](#93-watchlist--overviewchart)
   - 9.4 [OrderBook & RecentTrades](#94-orderbook--recenttrades)
   - 9.5 [Drawing Tools & Indicators](#95-drawing-tools--indicators)
   - 9.6 [Internationalization (i18n)](#96-internationalization-i18n)
   - 9.7 [Data Service Layer](#97-data-service-layer)
10. [Orchestration — Dagster](#10-orchestration--dagster)
11. [Backfill & Historical Data](#11-backfill--historical-data)
12. [Retention & Aggregation Policies](#12-retention--aggregation-policies)
13. [Nginx (Reverse Proxy)](#13-nginx-reverse-proxy)
14. [Cấu hình & Environment Variables](#14-cấu-hình--environment-variables)
15. [Ports & URLs](#15-ports--urls)
16. [Data Flow Chi Tiết Theo Timeframe](#16-data-flow-chi-tiết-theo-timeframe)

---

## 1. Tổng quan kiến trúc

CryptoPrice là một nền tảng **Lambda Architecture** cho phép theo dõi giá tiền điện tử real-time, tương tự TradingView. Hệ thống kết hợp:

- **Speed Layer (Real-time):** Binance WebSocket → Kafka → Flink → KeyDB/InfluxDB → FastAPI → React
- **Batch Layer (Historical):** Binance REST API → InfluxDB/Iceberg (via Spark) → Trino → FastAPI
- **Serving Layer:** FastAPI merge cả 2 nguồn dữ liệu, ưu tiên real-time → trả về React frontend

**Đặc điểm chính:**
- ~400 USDT trading pairs được theo dõi đồng thời
- Latency: ~500ms từ Binance → chart hiển thị
- Timeframes hỗ trợ: 1s, 1m, 5m, 15m, 1h, 4h, 1d, 1w
- Scroll-left load historical data vô hạn (giống Binance/TradingView)
- Order Book depth 20 levels, cập nhật mỗi 100ms
- Technical indicators: SMA20, SMA50, EMA12, EMA26, RSI, MFI

---

## 2. Sơ đồ data flow

```
                         ┌──────────────────────────────────────┐
                         │        REAL-TIME PIPELINE            │
                         │                                      │
Binance WS ─► Producer ─► Kafka ─► Flink ─┬─► KeyDB (cache)   │
  (ticker)    (Python)    (3 topics)       │     ↓             │
  (klines)                                 ├─► InfluxDB (TSDB) │
  (depth)                                  │     ↓             │
                                           └─► Indicators      │
                         └──────────────────────────────────────┘
                                    ↓
                              FastAPI ◄── Trino ◄── Iceberg/MinIO
                                ↓
                              Nginx
                                ↓
                            React SPA
                         (lightweight-charts)
```

```
┌─────────────────────────────────────────────────────────┐
│                 BATCH PIPELINE                          │
│                                                         │
│ Dagster ─► Spark ─┬─► backfill_historical.py            │
│   (scheduler)     │     (Binance REST → InfluxDB 1m)    │
│                   │     (Binance REST → Iceberg 1h)     │
│                   ├─► aggregate_candles.py               │
│                   │     (InfluxDB 1m → 1h aggregation)   │
│                   │     (Iceberg 1m → 1h aggregation)    │
│                   └─► iceberg_maintenance.py             │
│                       (compact, rewrite, expire, orphan) │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Docker Containers (18 services)

| # | Container | Image | Port | Vai trò |
|---|-----------|-------|------|---------|
| 1 | `kafka` | apache/kafka:3.9.0 | 9092 | Message broker |
| 2 | `schema-registry` | apicurio/apicurio-registry-mem:2.6.2 | 8085 | Avro schema registry |
| 3 | `minio` | minio/minio:latest | 9000, 9001 | Object storage (S3-compatible) |
| 4 | `minio-init` | minio/mc:latest | — | One-shot: tạo buckets `cryptoprice`, `flink-checkpoints` |
| 5 | `influxdb` | influxdb:2.7 | 8086 | Time-series database |
| 6 | `postgres` | postgres:16-alpine | 5432 | Metadata store (Iceberg catalog, Dagster) |
| 7 | `keydb` | eqalpha/keydb:latest | 6379 | In-memory cache (Redis-compatible) |
| 8 | `flink-jobmanager` | cryptoprice/flink:1.18.1 | 8081 | Flink cluster manager |
| 9 | `flink-taskmanager` | cryptoprice/flink:1.18.1 | — | Flink worker (4.5GB RAM) |
| 10 | `spark-master` | custom build | 7077, 8082, 18080 | Spark cluster manager |
| 11 | `spark-worker` | custom build | 8084 | Spark executor (4GB RAM, 4 cores) |
| 12 | `trino` | cryptoprice/trino:442 | 8083 | SQL query engine on Iceberg |
| 13 | `dagster-webserver` | cryptoprice/dagster | 3000 | Orchestration UI |
| 14 | `dagster-daemon` | cryptoprice/dagster | — | Schedule executor |
| 15 | `fastapi` | custom build | 8080 | REST + WebSocket API |
| 16 | `nginx` | custom build | 80 | Reverse proxy + SPA hosting |
| 17 | `producer` | custom build | — | Binance WS → Kafka |
| 18 | `influx-backfill` | custom build | — | One-shot: fill InfluxDB gaps |

**Network:** Tất cả containers cùng network `crypto-net` (bridge driver).

**Volumes:**
- `kafka-data` — Kafka log segments
- `minio-data` — MinIO object storage (Iceberg data files)
- `influxdb-data`, `influxdb-config` — InfluxDB TSM data + config
- `postgres-data` — PostgreSQL data (Iceberg catalog + Dagster metadata)
- `keydb-data` — KeyDB RDB snapshots
- `flink-checkpoints` — Flink checkpoint data
- `spark-events` — Spark event logs
- `trino-data` — Trino temp data
- `dagster-home` — Dagster run storage

---

## 4. Data Layer — Ingestion

### 4.1 Producer (Binance WebSocket → Kafka)

**File:** `src/producer_binance.py`  
**Container:** `producer`  
**Chức năng:** Kết nối Binance WebSocket Streams, serialize thành Avro, gửi vào Kafka.

**Chi tiết:**

| Feature | Mô tả |
|---------|-------|
| **Symbol discovery** | Gọi Binance REST `GET /api/v3/exchangeInfo`, lọc tất cả USDT spot pairs đang TRADING. Cap tối đa 400 symbols. |
| **Ticker stream** | Kết nối `wss://stream.binance.com:9443/ws/!ticker@arr` — nhận all-market ticker mỗi ~1s. Heartbeat: mỗi 5s gửi ping. |
| **Kline (candle) stream** | Chia 400 symbols thành groups 50, mỗi group 1 WebSocket combined stream `wss://stream.binance.com:9443/stream`. Subscribe `{symbol}@kline_1s` cho tất cả. Interval mặc định: `1s`. |
| **Depth (order book) stream** | Tương tự kline, subscribe `{symbol}@depth{DEPTH_LEVEL}@{DEPTH_UPDATE_MS}ms`. Mặc định: top 20 bids/asks, cập nhật 100ms. |
| **Avro serialization** | Sử dụng Confluent wire format: `0x00 + 4-byte schema_id + Avro binary`. Schema tự register vào Apicurio Registry. |
| **Kafka producer** | `acks=1`, `compression=lz4`, `linger_ms=5`, `batch_size=64KB`, `buffer_memory=64MB`. Key = symbol name. |
| **Reconnection** | Auto-reconnect khi WebSocket disconnect. Kafka producer auto-retry 5 lần. |
| **Price tracking** | Lưu `_last_close` per symbol để tính price change; `_last_sent_ts` để rate-limit. |

**Kafka Topics được ghi:**
- `crypto_ticker` — 400 symbols x ~1 msg/s = ~400 msg/s
- `crypto_klines` — 400 symbols x 1 msg/s (1s candles) = ~400 msg/s
- `crypto_depth` — 400 symbols x 10 msg/s (100ms updates) = ~4000 msg/s

### 4.2 Kafka

**Image:** `apache/kafka:3.9.0` (KRaft mode, không cần Zookeeper)

| Setting | Value | Mô tả |
|---------|-------|-------|
| `KAFKA_NUM_PARTITIONS` | 3 | Mỗi topic mặc định 3 partitions |
| `KAFKA_LOG_RETENTION_HOURS` | 48 | Giữ messages 48h |
| `KAFKA_COMPRESSION_TYPE` | lz4 | Nén messages |
| `KAFKA_LOG_SEGMENT_BYTES` | 128MB | Segment size |
| `KAFKA_LOG_CLEANUP_POLICY` | delete | Xóa segments cũ |

**Topics:**
| Topic | Schema | Tần suất | Mô tả |
|-------|--------|----------|-------|
| `crypto_ticker` | Ticker.avsc | ~400 msg/s | Ticker 24h data (price, bid, ask, volume, change) |
| `crypto_klines` | Kline.avsc | ~400 msg/s | 1-second OHLCV candles |
| `crypto_depth` | Depth.avsc | ~4000 msg/s | Partial order book depth snapshots |
| `crypto_trades` | Trade.avsc | Variable | Individual trades (reserved, hiện chưa dùng) |

### 4.3 Schema Registry (Apicurio)

**Image:** `apicurio/apicurio-registry-mem:2.6.2.Final`  
**URL:** `http://schema-registry:8080/apis/ccompat/v7`  
**Chức năng:** Lưu Avro schemas cho Kafka topics. API tương thích Confluent Schema Registry.

- Schemas được register tự động bởi producer khi khởi động
- Flink consumer đọc schema từ registry để deserialize
- In-memory storage (mất khi restart, auto re-register)

### 4.4 Avro Schemas

**Thư mục:** `schemas/`

**`ticker.avsc`** — Ticker 24h data:
```
Fields: event_time(long), symbol(string), close(double), bid, ask,
        h24_open, h24_high, h24_low, h24_volume, h24_quote_volume,
        h24_price_change, h24_price_change_pct, h24_trade_count
```

**`kline.avsc`** — Kline/Candle data:
```
Fields: event_time(long), symbol(string), kline_start(long), kline_close(long),
        interval(string), open(double), high, low, close, volume,
        quote_volume, trade_count(long), is_closed(boolean)
```

**`depth.avsc`** — Order book depth:
```
Fields: event_time(long), symbol(string), last_update_id(long),
        bids(string/JSON), asks(string/JSON)
```

**`trade.avsc`** — Individual trade (reserved):
```
Fields: event_time(long), symbol(string), trade_id(long),
        price(double), quantity(double), is_buyer_maker(boolean)
```

---

## 5. Compute Layer — Stream Processing

### 5.1 Flink (Real-time Pipeline)

**File:** `src/ingest_flink_crypto.py`  
**Job name:** `Crypto_MultiStream_Kafka_to_KeyDB_InfluxDB`  
**Containers:** `flink-jobmanager` (1.75GB), `flink-taskmanager` (4.5GB)

Flink là trung tâm xử lý real-time. Nó đọc 3 Kafka topics và fan-out thành **7 output sinks**.

#### Pipeline 1: Ticker → KeyDB + InfluxDB

```
kafka_ticker → Avro deserialize → JSON dict
  ├─► KeyDBWriter (batch 100, flush 0.5s)
  │     ticker:latest:{symbol}  — hash: price, bid, ask, volume, change24h, event_time
  │     ticker:history:{symbol} — sorted set (score=event_time, member=price:volume)
  │     Cleanup: mỗi 60 writes, xóa entries > 5 phút tuổi
  └─► InfluxDBWriter (batch 200, flush 0.5s)
        Measurement: market_ticks
        Tags: symbol, exchange=binance
        Fields: price, bid, ask, volume, quote_volume, price_change_pct, trade_count
        Time: event_time (ms precision)
```

#### Pipeline 2: Kline → Raw 1s + Aggregated 1m

```
kafka_klines → Avro deserialize → JSON dict
  │
  ├─ Branch 1: Raw 1s candles
  │  ├─► KeyDBKlineWriter
  │  │     candle:1s:{symbol} — sorted set (score=kline_start_ms, member=JSON{t,o,h,l,c,v,qv,n,x})
  │  │     TTL: 8 hours
  │  └─► InfluxDBKlineWriter
  │        Measurement: candles, Tags: symbol, exchange, interval=1s
  │        Fields: open, high, low, close, volume, quote_volume, trade_count, is_closed
  │
  └─ Branch 2: In-flight 1s→1m aggregation (KlineWindowAggregator)
     │  KeyBy: symbol
     │  Window: 60 seconds (processing time)
     │  Features:
     │    - MapState<kline_start_ms, JSON> per symbol (dedup)
     │    - Safety timer: 65s after window open (handle silence)
     │    - Gap-fill: forward-fill missing seconds from last close price
     │    - Output: aggregated 1m candle with OHLCV from all 60 seconds
     │
     ├─► KeyDBKlineWriter
     │     candle:1m:{symbol} — sorted set, TTL: 7 days
     │     candle:latest:{symbol} — hash (latest closed candl info)
     ├─► InfluxDBKlineWriter
     │     Measurement: candles, interval=1m
     └─► IndicatorWriter (only closed candles)
           → SMA20, SMA50, EMA12, EMA26
           → KeyDB: indicator:latest:{symbol}
           → InfluxDB: measurement=indicators
```

#### Pipeline 3: Depth → KeyDB

```
kafka_depth → Avro deserialize → JSON dict
  └─► DepthWriter (batch 50, flush 0.3s)
        orderbook:{symbol} — hash:
          bids(JSON), asks(JSON), last_update_id, event_time,
          bid_depth, ask_depth, best_bid, best_ask, spread
        TTL: 60 seconds per key
```

#### KlineWindowAggregator — Chi tiết

Đây là class quan trọng nhất, thực hiện aggregation 1s→1m in-flight:

| Feature | Mô tả |
|---------|-------|
| **Dedup** | Cùng `kline_start` ms → overwrite trong MapState |
| **Window detect** | Khi candle thuộc minute mới, emit aggregation của minute trước |
| **Safety timer** | Processing-time timer 65s sau window open → đảm bảo window cuối không bị stuck |
| **Gap-fill** | Nếu thiếu second nào trong window 60s, forward-fill = close price của second trước |
| **Logging** | Log `real=X/60` — cho biết bao nhiêu second thực sự có dữ liệu |

#### IndicatorWriter — Technical Indicators

| Indicator | Period | Buffer | Mô tả |
|-----------|--------|--------|-------|
| SMA20 | 20 | Per-symbol deque(60) | Simple Moving Average 20 periods |
| SMA50 | 50 | Per-symbol deque(60) | Simple Moving Average 50 periods |
| EMA12 | 12 | Per-symbol state | Exponential Moving Average 12 |
| EMA26 | 26 | Per-symbol state | Exponential Moving Average 26 |

- Chỉ xử lý **closed candles** (`is_closed=True`)
- Ghi vào KeyDB hash `indicator:latest:{symbol}` và InfluxDB measurement `indicators`

#### Flink Config

| Setting | Value |
|---------|-------|
| State Backend | HashMapStateBackend |
| Checkpoint interval | 120s |
| Checkpoint mode | EXACTLY_ONCE |
| Unaligned checkpoints | Enabled |
| Min pause between checkpoints | 30s |
| Checkpoint timeout | 120s |
| Checkpoint storage | `file:///tmp/flink-checkpoints` |

### 5.2 Spark (Batch Processing)

**Containers:** `spark-master` (cluster manager), `spark-worker` (4GB RAM, 4 cores)

Spark chạy 3 loại job, đều được submit thông qua Dagster:

#### Job 1: `ingest_crypto.py` — Kafka → Iceberg (Structured Streaming)

**Chức năng:** Đọc 3 Kafka topics bằng Spark Structured Streaming, ghi vào 3 Iceberg tables.

| Kafka Topic | Iceberg Table | Mô tả |
|-------------|---------------|-------|
| `crypto_ticker` | `iceberg_catalog.crypto_lakehouse.coin_ticker` | Ticker data dài hạn |
| `crypto_trades` | `iceberg_catalog.crypto_lakehouse.coin_trades` | Trade data |
| `crypto_klines` | `iceberg_catalog.crypto_lakehouse.coin_klines` | Kline candle data |

- Deserialize Avro (Confluent wire format) bằng UDF strip 5-byte header
- Checkpoint: `s3a://cryptoprice/checkpoints/crypto_{topic}_v1`
- Trigger: `processingTime("30 seconds")`

#### Job 2: `aggregate_candles.py` — 1m → 1h Aggregation

**Modes:** `--mode influx`, `--mode iceberg`, `--mode all`

**InfluxDB aggregation:**
1. Tìm symbols có 1m candles cũ hơn `RETENTION_1M_DAYS` (90 ngày)
2. Query Flux: `range(-90d, -7d)`, filter `interval="1m"`
3. Group by hour, aggregate: first(open), max(high), min(low), last(close), sum(volume)
4. Ghi lại candles `interval="1h"` vào cùng bucket
5. Xóa 1m candles cũ

**Iceberg aggregation:**
1. Spark SQL đọc `coin_klines` table
2. Window function group by `symbol`, `hour`
3. Ghi kết quả vào `coin_klines_hourly` table

#### Job 3: `iceberg_maintenance.py` — Table Optimization

| Task | Mô tả |
|------|-------|
| **Compact files** | Rewrite small data files into ~128MB files |
| **Rewrite manifests** | Reduce metadata overhead |
| **Expire snapshots** | Xóa snapshots > 48h |
| **Remove orphan files** | Xóa files không còn referenced (> 72h) |

Chạy trên 4 tables:
- `coin_ticker`, `coin_trades`, `coin_klines`, `coin_klines_hourly`

---

## 6. Storage Layer

### 6.1 KeyDB (Real-time Cache)

**Image:** `eqalpha/keydb:latest`  
**Config:** `maxmemory 2560mb`, `maxmemory-policy allkeys-lru`, `hz 50`

KeyDB là Redis-compatible in-memory store, đóng vai trò **speed layer** — dữ liệu mới nhất được serve từ đây.

#### Data Keys

| Key Pattern | Type | TTL | Nội dung |
|-------------|------|-----|----------|
| `ticker:latest:{SYMBOL}` | Hash | — | price, bid, ask, volume, change24h, event_time |
| `ticker:history:{SYMBOL}` | Sorted Set | Auto-cleanup >5min | member=`price:volume`, score=event_time(ms) |
| `candle:1s:{SYMBOL}` | Sorted Set | 8h | member=JSON{t,o,h,l,c,v,qv,n,x}, score=kline_start(ms) |
| `candle:1m:{SYMBOL}` | Sorted Set | 7d | member=JSON{t,o,h,l,c,v,qv,n,x}, score=kline_start(ms) |
| `candle:latest:{SYMBOL}` | Hash | — | open, high, low, close, volume, quote_volume, trade_count, is_closed, kline_start, interval |
| `orderbook:{SYMBOL}` | Hash | 60s | bids(JSON), asks(JSON), spread, best_bid, best_ask, event_time |
| `indicator:latest:{SYMBOL}` | Hash | — | sma20, sma50, ema12, ema26, timestamp |
| `klines_cache:{SYMBOL}:{INTERVAL}:{LIMIT}` | String | 100ms | Cached API response (JSON) |

#### Dữ liệu có sẵn

| Dataset | Độ sâu | Nguồn |
|---------|--------|-------|
| Ticker latest | Real-time | Flink ← Kafka ← Binance WS |
| Ticker history | 5 phút | Flink (auto cleanup) |
| 1s candles | 8 giờ | Flink (1s raw from Binance) |
| 1m candles | 7 ngày | Flink (aggregated 1s→1m) |
| Order book | 60s | Flink ← Kafka ← Binance WS |
| Indicators | Latest only | Flink (computed from closed 1m) |

### 6.2 InfluxDB (Time-Series Database)

**Image:** `influxdb:2.7`  
**Org:** `vi`, **Bucket:** `crypto`

InfluxDB lưu time-series data dài hạn hơn KeyDB.

#### Measurements

| Measurement | Tags | Fields | Time | Nguồn |
|-------------|------|--------|------|-------|
| `market_ticks` | symbol, exchange | price, bid, ask, volume, quote_volume, price_change_pct, trade_count | event_time (ms) | Flink ← crypto_ticker |
| `candles` | symbol, exchange, interval | open, high, low, close, volume, quote_volume, trade_count, is_closed | kline_start (ms) | Flink ← crypto_klines (1s + 1m) |
| `indicators` | symbol, exchange | sma20, sma50, ema12, ema26, close | kline_start (ms) | Flink ← closed 1m |

#### Retention Policies (InfluxDB Tasks)

| Data | Retention | Cleanup |
|------|-----------|---------|
| `candles` interval=1s | 7 ngày | InfluxDB task `delete_old_1s_candles` mỗi 6h |
| `candles` interval=1m | 90 ngày | InfluxDB task `delete_old_1m_candles` mỗi 6h → sau 90 ngày, `aggregate_candles.py` gộp thành 1h |
| `candles` interval=1h | Vĩnh viễn | Aggregated từ 1m |
| `market_ticks` | InfluxDB default bucket retention | — |

### 6.3 MinIO + Iceberg (Data Lake)

**MinIO:** S3-compatible object storage  
**Buckets:**
- `cryptoprice` — Iceberg data files + warehouse
- `flink-checkpoints` — Flink checkpoint data

**Iceberg Catalog:** JDBC catalog lưu trong PostgreSQL  
**Warehouse:** `s3://cryptoprice/iceberg`

#### Iceberg Tables

| Table | Nguồn | Mô tả |
|-------|-------|-------|
| `crypto_lakehouse.coin_ticker` | Spark Structured Streaming ← crypto_ticker | Ticker 24h data dài hạn |
| `crypto_lakehouse.coin_trades` | Spark Structured Streaming ← crypto_trades | Individual trades |
| `crypto_lakehouse.coin_klines` | Spark Structured Streaming ← crypto_klines | 1s + 1m candle data |
| `crypto_lakehouse.coin_klines_hourly` | `aggregate_candles.py` | Aggregated 1h candles |
| `crypto_lakehouse.historical_hourly` | `backfill_historical.py` | Backfilled 1h data from Binance REST API (2017→nay) |

### 6.4 PostgreSQL (Metadata Store)

**Image:** `postgres:16-alpine`

| Database | Mục đích |
|----------|----------|
| `iceberg_catalog` | JDBC catalog cho Apache Iceberg (table metadata, snapshots) |
| `dagster` | Dagster run storage, event log, schedule state |
| Default DB (`${POSTGRES_DB}`) | Application metadata nếu cần |

**Init script:** `docker/postgres/init.sql` — tạo databases khi khởi động lần đầu.

---

## 7. Query Layer

### 7.1 Trino (SQL on Iceberg)

**Image:** `cryptoprice/trino:442`  
**Catalog:** `iceberg` → JDBC catalog on PostgreSQL → data files on MinIO

**Chức năng:**
- Cho phép FastAPI query dữ liệu historical từ Iceberg tables bằng SQL
- Chỉ được dùng cho `/api/klines/historical` endpoint
- Connect config: user=`fastapi`, catalog=`iceberg`, schema=`crypto_lakehouse`

**Query patterns:**
```sql
-- Hourly historical candles for a date range
SELECT kline_start AS open_time, open, high, low, close, volume
FROM crypto_lakehouse.coin_klines_hourly
WHERE symbol = ? AND kline_start >= ? AND kline_start < ?
ORDER BY kline_start LIMIT ?

-- Fallback to backfilled data
SELECT open_time, open, high, low, close, volume
FROM crypto_lakehouse.historical_hourly
WHERE symbol = ? AND open_time >= ? AND open_time < ?
ORDER BY open_time LIMIT ?
```

---

## 8. Serving Layer — FastAPI

**File:** `serving/main.py`  
**Container:** `fastapi` (port 8000 internal, 8080 external)

### 8.1 API Endpoints

| Method | Path | Router | Mô tả |
|--------|------|--------|-------|
| GET | `/api/health` | main.py | Health check (KeyDB + InfluxDB + Trino) |
| GET | `/api/symbols` | symbols.py | Danh sách tất cả symbols (scan `ticker:latest:*` từ KeyDB) |
| GET | `/api/ticker/{symbol}` | ticker.py | Ticker data cho 1 symbol |
| GET | `/api/ticker` | ticker.py | Tất cả tickers |
| GET | `/api/klines?symbol=&interval=&limit=&endTime=` | klines.py | OHLCV candles (chính) |
| GET | `/api/klines/historical?symbol=&startTime=&endTime=&limit=` | historical.py | Historical candles từ Iceberg (qua Trino) |
| GET | `/api/orderbook/{symbol}` | orderbook.py | Order book depth |
| GET | `/api/trades/{symbol}?limit=` | trades.py | Recent price ticks |
| GET | `/api/indicators/{symbol}` | indicators.py | SMA/EMA indicator values |
| WS | `/api/stream?symbol=&interval=` | ws.py | Real-time candle WebSocket |

### 8.2 Data Routing & Priority

Endpoint `/api/klines` có logic routing phức tạp:

```
1. Check Redis cache (100ms TTL) → return nếu có (skip cho endTime queries)
2. PRIORITY 1: KeyDB sorted sets
   - interval=1s → candle:1s:{symbol}
   - interval=1m+ → candle:1m:{symbol}
   - Deduplicate by timestamp
3. PRIORITY 2: InfluxDB (nếu KeyDB không đủ data)
   - Query base interval: 1s→1s, 1m/5m/15m→1m, 1h+→1h
   - Fallback: nếu requesting >=1h mà không có aggregated 1h → query 1m và aggregate server-side
4. Merge KeyDB + InfluxDB (avoid duplicates)
5. Re-aggregate nếu cần (1m→5m, 1m→15m, 1h→4h, etc.)
6. Merge live ticker price vào candle cuối cùng (không cho endTime queries)
7. Cache result 100ms
```

**endTime queries (scroll loading):**
- Skip cache
- Filter `openTime < endTime`
- Return last `limit` candles
- Không merge live ticker

### 8.3 WebSocket Real-time Stream

**Endpoint:** `ws://host/api/stream?symbol=BTCUSDT&interval=1m`

**Logic mỗi 500ms:**
1. Đọc `ticker:latest:{symbol}` từ KeyDB (near-zero lag)
2. Đọc candle data từ KeyDB sorted sets
3. Merge ticker price vào candle (update close, extend high/low)
4. Nếu ticker thuộc window mới → synthesize candle mới
5. Send JSON nếu data thay đổi so với lần gửi trước

**Đặc biệt cho 1s interval:**
- Đọc `candle:1s:{symbol}` (latest entry)
- Nếu ticker mới hơn (second khác) → synthesize 1s candle từ ticker price

### 8.4 Candle Aggregation Logic

FastAPI thực hiện on-the-fly aggregation cho các timeframe lớn:

| Requested | Base Data | Aggregation |
|-----------|-----------|-------------|
| 1s | KeyDB 1s + InfluxDB 1s | None |
| 1m | KeyDB 1m + InfluxDB 1m | None |
| 5m | KeyDB 1m + InfluxDB 1m | Re-sample 1m→5m |
| 15m | KeyDB 1m + InfluxDB 1m | Re-sample 1m→15m |
| 1h | InfluxDB 1h (fallback: 1m→1h) | Re-sample nếu base=1m |
| 4h | InfluxDB 1h | Re-sample 1h→4h |
| 1d | InfluxDB 1h | Re-sample 1h→1d |
| 1w | InfluxDB 1h | Re-sample 1h→1w |

**Aggregation function:**
- `open` = first candle's open
- `high` = max of all highs
- `low` = min of all lows
- `close` = last candle's close
- `volume` = sum of all volumes

---

## 9. Visualization Layer — React Frontend

**Framework:** React 18 + TailwindCSS  
**Chart Library:** lightweight-charts v5.1.0  
**Build:** Nginx serves static build at port 80

### 9.1 Kiến trúc component

```
App.js (TradingDashboard)
├── Header.js
│   ├── Navigation drawer
│   └── LanguageSwitcher.js
├── DrawingToolbar.js
│   └── ToolSettingsPopup.js
├── CandlestickChart.js (CORE)
│   ├── MarketSelector.js
│   ├── DateRangePicker.js
│   ├── chart/IndicatorPanel.js
│   ├── chart/OHLCVBar.js
│   ├── chart/chartConstants.js
│   ├── chart/indicatorUtils.js
│   ├── ChartOverlay.js (drawings)
│   ├── OrderBook.js
│   └── RecentTrades.js
├── Watchlist.js
├── OverviewChart.js
├── AuthModal.js
└── ErrorBoundary.js
```

**Layout:** `h-screen overflow-hidden` — viewport-fit, không scroll

```
┌────────────────────────────────────────────────────────────┐
│ Header (flex-shrink-0)                                     │
├────────┬───────────────────────────────────┬───┬───────────┤
│Drawing │ CandlestickChart                  │ ⋮ │ Sidebar   │
│Toolbar │  ┌─ Top bar (symbol, TF, etc.)    │ D │ ┌────────┐│
│        │  ├─ OHLCV bar                     │ r │ │Watchlist││
│        │  └─ Chart canvas (flex-1)         │ a │ │ 65%    ││
│        │                                    │ g │ ├────────┤│
│        │                                    │   │ │Overview││
│        │                                    │   │ │ 35%    ││
│        │                                    │   │ └────────┘│
├────────┴───────────────────────────────────┴───┴───────────┤
│                    (no footer — full height)                │
└────────────────────────────────────────────────────────────┘
```

- Sidebar width: resizable 280–520px, default 340px
- Drag handle giữa chart và sidebar

### 9.2 CandlestickChart — Core Chart

**File:** `frontend/src/components/CandlestickChart.js`

**Đây là component phức tạp nhất**, ~800+ dòng, quản lý:

#### Features

| Feature | Mô tả |
|---------|-------|
| **Multi-timeframe** | 1s, 1m, 5m, 15m, 1H, 4H, 1D, 1W — chuyển đổi realtime |
| **Real-time updates** | WebSocket subscription + incremental polling |
| **Scroll-left loading** | Kéo chart sang trái → tự động load thêm historical data (giống TradingView) |
| **OHLCV tooltip** | Hover hiển thị Open, High, Low, Close, Volume |
| **Technical indicators** | SMA20, SMA50, EMA, RSI, MFI — toggle on/off |
| **Volume histogram** | Hiển thị dưới chart, màu xanh/đỏ theo candle direction |
| **Chart tabs** | Chart, Order Book, Recent Trades — cùng 1 panel |
| **Historical date range** | DateRangePicker chọn ngày → query Iceberg (1h candles) |
| **Export PNG** | Export chart thành .png file |
| **Market selector** | Dropdown chọn symbol, star favorites |
| **Theme** | Dark theme (gray-900 background) |

#### Data Flow (trong component)

```
1. Mount → createChart() → khởi tạo lightweight-charts instance
2. Symbol/TF change → useEffect:
   a. fetchCandles(symbol, tf, 200) → applyDataToChart()
   b. subscribeCandle(symbol, tf) → WebSocket listener
   c. setInterval(pollIncremental) → fetch last 3-5 candles, merge
3. Scroll left (logicalRange.from < 20):
   a. loadMoreHistoricalData()
   b. fetchCandles(symbol, tf, 500, earliestTime)
   c. Merge older data + current data
   d. setData(merged) + restore visible range
4. WebSocket message:
   a. 1s mode: append new candle if time > lastTime
   b. 1m+ mode: agitate last bar (update close/high/low) or append new period
5. Poll (1s/2s interval):
   a. Fetch last 3-5 candles
   b. Compare with existing data
   c. .update() only changed candles (no full redraw → no flicker)
```

#### Scroll-left Loading (Infinite History)

```
subscribeVisibleLogicalRangeChange → logicalRange.from < 20
  → loadMoreHistoricalData()
    → Guard: isLoadingMoreRef, noMoreDataRef, cooldown 500ms
    → fetchCandles(symbol, tf, 500, earliestTime)
    → Filter duplicates: olderData.filter(c => c.time < earliestTime)
    → Merge: [...newCandles, ...current]
    → Save visible range before setData
    → candleRef.setData(merged)
    → Restore visible range with shift offset
    → Update all indicator series
    → maxBars = 10000 (cho phép ~7 ngày 1m data in memory)
```

#### Incremental Polling (No Flicker)

```
Thay vì full-reload mỗi 1-2s (gây flicker):
1. fetchCandles(symbol, tf, 3-5)
2. So sánh từng candle với existing data:
   - Nếu cùng time & giá thay đổi → candleRef.update(c) (chỉ update 1 bar)
   - Nếu time mới → push + candleRef.update(c)
3. Không gọi setData() → không redraw toàn bộ chart
```

### 9.3 Watchlist & OverviewChart

**Watchlist.js:**
- Hiển thị tất cả ~400 symbols với price, 24h change
- Search/filter theo tên
- Star favorites (lưu localStorage)
- Click → đổi symbol trên chart
- Cập nhật qua periodic fetch `/api/ticker`

**OverviewChart.js:**
- Mini chart cho symbol đang chọn
- Sử dụng dữ liệu candles từ CandlestickChart (passed via props)
- Area chart style

### 9.4 OrderBook & RecentTrades

**OrderBook.js:**
- Tab trong CandlestickChart
- Fetch `/api/orderbook/{symbol}`
- Hiển thị bids (xanh) + asks (đỏ) + spread
- Depth visualization bars

**RecentTrades.js:**
- Tab trong CandlestickChart
- Fetch `/api/trades/{symbol}`
- Hiển thị time, price, volume, side (buy/sell)
- Color-coded: green=buy, red=sell

### 9.5 Drawing Tools & Indicators

**DrawingToolbar.js:**
- Tools: Cursor, Crosshair, Horizontal Line, Trend Line, Fibonacci, Rectangle, Text
- Tool settings popup (color, line width)
- Clear all drawings

**ChartOverlay.js:**
- Canvas overlay trên chart
- Render drawings (lines, rectangles, text)
- Mouse interaction: drag to draw

**IndicatorPanel.js:**
- Toggle on/off: SMA20, SMA50, EMA, Volume, RSI, MFI
- Custom settings: period, color, line width
- Dropdown panel bên phải top bar

**indicatorUtils.js:**
- Client-side calculation: `calcSMA()`, `calcEMA()`, `calcRSI()`, `calcMFI()`
- Chạy trên toàn bộ candles array mỗi khi data thay đổi

### 9.6 Internationalization (i18n)

**Files:** `frontend/src/i18n/`

- `translations.js` — Translation strings
- `index.js` — `useI18n()` hook
- `LanguageSwitcher.js` — UI component

Hỗ trợ: English, Tiếng Việt (có thể thêm ngôn ngữ khác).

### 9.7 Data Service Layer

**File:** `frontend/src/services/marketDataService.js`

| Function | Mô tả |
|----------|-------|
| `fetchCandles(symbol, tf, limit, endTime?)` | GET `/api/klines` → map openTime(ms)→time(s) |
| `fetchHistoricalCandles(symbol, startMs, endMs, limit)` | GET `/api/klines/historical` |
| `subscribeCandle(symbol, tf, onCandle)` | WebSocket `/api/stream` — auto-reconnect 3s |
| `fetchSymbols()` | GET `/api/symbols` |
| `fetchTicker(symbol)` | GET `/api/ticker/{symbol}` |
| `fetchAllTickers()` | GET `/api/ticker` |
| `fetchOrderbook(symbol)` | GET `/api/orderbook/{symbol}` |
| `fetchTrades(symbol, limit)` | GET `/api/trades/{symbol}` |
| `fetchIndicators(symbol)` | GET `/api/indicators/{symbol}` |

**Modes:**
- `api` (default) — call FastAPI backend qua Nginx
- `mock` — generate random data (for local dev without backend)

**WebSocket reconnection:**
- `onclose` → retry after 3 seconds
- `onerror` → log error
- Returns `unsubscribe()` function

**Auth Context:** `AuthContext.js` — placeholder cho authentication (chưa implement đầy đủ).

**Storage Helpers:** `storageHelpers.js` — Wrapper cho localStorage (starred symbols, preferences).

---

## 10. Orchestration — Dagster

**Files:** `orchestration/assets.py`, `orchestration/workspace.yaml`  
**Containers:** `dagster-webserver` (port 3000), `dagster-daemon`

### Assets

| Asset | Group | Mô tả |
|-------|-------|-------|
| `backfill_historical` | ingestion | Pull 1h klines Binance → Iceberg + fill InfluxDB gaps |
| `aggregate_candles` | maintenance | 1m→1h aggregation trên InfluxDB + Iceberg |
| `iceberg_table_maintenance` | maintenance | Compact, rewrite manifests, expire snapshots, remove orphans |

### Schedules

| Schedule | Cron | Asset | Mô tả |
|----------|------|-------|-------|
| `weekly_iceberg_maintenance` | `0 3 * * 0` (Chủ nhật 3AM) | iceberg_table_maintenance | Compact & cleanup |
| `daily_candle_aggregation` | `0 4 * * *` (Hàng ngày 4AM) | aggregate_candles | 1m→1h aggregation |

### Execution

Dagster submit Spark jobs bằng `spark-submit`:
```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:...,org.apache.hadoop:hadoop-aws:3.3.4,... \
  --conf spark.eventLog.enabled=true \
  /app/src/{script_name}.py [args]
```

---

## 11. Backfill & Historical Data

**File:** `src/backfill_historical.py`  
**Container:** `influx-backfill` (one-shot) hoặc chạy manual

### Modes

| Mode | Mục đích | Cách chạy |
|------|----------|-----------|
| `--mode influx` | Detect + fill InfluxDB gaps (sau khi tắt máy) | `python backfill_historical.py --mode influx` |
| `--mode iceberg` | Pull historical 1h klines → Iceberg | `--mode iceberg --iceberg-mode backfill` (2017→nay) hoặc `--iceberg-mode incremental` (từ nến cuối) |
| `--mode populate` | Force populate N ngày 1m candles cho tất cả symbols | `--mode populate --days 90` |
| `--mode all` | influx + iceberg (không bao gồm populate) | `--mode all --iceberg-mode incremental` |

### Populate Mode (chạy thủ công)

```bash
docker run --rm --network cryptoprice_crypto-net \
  -e INFLUX_URL=http://influxdb:8086 \
  -e INFLUX_TOKEN=... \
  -e INFLUX_ORG=vi \
  -e INFLUX_BUCKET=crypto \
  cryptoprice-influx-backfill \
  python /app/backfill_historical.py --mode populate --days 90
```

**Logic:**
1. Fetch ~400 USDT symbols từ Binance
2. Với mỗi symbol, fetch 1m klines từ Binance REST API (phân trang, 1000 candles/request)
3. Ghi vào InfluxDB measurement `candles`, interval=`1m`
4. ThreadPoolExecutor: 5 workers đồng thời
5. Rate limiting: 120ms giữa mỗi request

### InfluxDB Gap Detection

```
1. Query InfluxDB: elapsed() giữa các points trong 7 ngày gần nhất
2. Nếu gap > 300 seconds → đánh dấu là gap
3. Fetch missing data từ Binance REST API
4. Ghi lại vào InfluxDB
```

---

## 12. Retention & Aggregation Policies

### Tổng quan data lifecycle

```
Binance WS (1s candles)
  ↓ 0.5s
KeyDB candle:1s → 8 giờ → bị LRU evict
  ↓ realtime
InfluxDB candles(1s) → 7 ngày → bị xóa bởi retention task
  ↓ in-flight (Flink)
KeyDB candle:1m → 7 ngày → bị LRU evict
  ↓ realtime
InfluxDB candles(1m) → 90 ngày → aggregate_candles.py gộp thành 1h → xóa 1m
  ↓ batch (Dagster daily)
InfluxDB candles(1h) → VĨNH VIỄN
  ↓ optional
Iceberg coin_klines_hourly → VĨNH VIỄN (trên MinIO)
```

### InfluxDB Tasks

| Task | Frequency | Action |
|------|-----------|--------|
| `delete_old_1s_candles` | Mỗi 6h | Xóa `candles` interval=1s cũ hơn 7 ngày |
| `delete_old_1m_candles` | Mỗi 6h | Xóa `candles` interval=1m cũ hơn 90 ngày |

### Dagster Schedules

| Schedule | Frequency | Action |
|----------|-----------|--------|
| `daily_candle_aggregation` | Hàng ngày 4AM | 1m→1h aggregation + xóa 1m cũ |
| `weekly_iceberg_maintenance` | Chủ nhật 3AM | Compact + cleanup Iceberg tables |

---

## 13. Nginx (Reverse Proxy)

**File:** `docker/nginx/nginx.conf`  
**Port:** 80 (external)

| Path | Proxy Target | Mô tả |
|------|-------------|-------|
| `/` | Static files `/usr/share/nginx/html` | React SPA |
| `/api/stream` | `fastapi:8000` | WebSocket (upgrade headers) |
| `/api/*` | `fastapi:8000` | REST API |

**Features:**
- Gzip compression (level 6) cho text, CSS, JSON, JS, XML, SVG
- WebSocket proxy: `Upgrade`, `Connection` headers
- WS timeout: 86400s (24h)
- `tcp_nodelay on` cho low latency
- SPA fallback: `try_files $uri $uri/ /index.html` (React routing)

---

## 14. Cấu hình & Environment Variables

**File:** `.env` (hoặc `d41d8cd9 (1).env`)

| Variable | Mô tả | Default |
|----------|-------|---------|
| `INFLUX_TOKEN` | InfluxDB admin API token | (required) |
| `INFLUX_ADMIN_USER` | InfluxDB admin username | admin |
| `INFLUX_ADMIN_PASSWORD` | InfluxDB admin password | adminpass123 |
| `INFLUX_ORG` | InfluxDB organization | vi |
| `INFLUX_BUCKET` | InfluxDB bucket | crypto |
| `MINIO_ROOT_USER` | MinIO access key | (required) |
| `MINIO_ROOT_PASSWORD` | MinIO secret key | (required) |
| `POSTGRES_USER` | PostgreSQL username | (required) |
| `POSTGRES_PASSWORD` | PostgreSQL password | (required) |
| `POSTGRES_DB` | PostgreSQL default database | (required) |

**Biến runtime (trong container):**

| Variable | Container | Mô tả |
|----------|-----------|-------|
| `KAFKA_BOOTSTRAP` | producer, flink, spark, dagster | Kafka broker address |
| `REDIS_HOST` / `REDIS_PORT` | flink, fastapi | KeyDB connection |
| `INFLUX_URL` / `INFLUX_TOKEN` | flink, fastapi, spark, backfill | InfluxDB connection |
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | flink, spark, trino | MinIO connection |
| `SCHEMA_REGISTRY_URL` | producer, flink | Apicurio Schema Registry |
| `TRINO_HOST` / `TRINO_PORT` | fastapi | Trino connection |
| `CORS_ORIGINS` | fastapi | Allowed CORS origins |
| `RETENTION_1M_DAYS` | spark, dagster | 1m candle retention (90) |
| `KLINE_INTERVAL` | producer | WS kline interval (1s) |
| `DEPTH_LEVEL` | producer | Order book depth (20) |
| `DEPTH_UPDATE_MS` | producer | Depth update frequency (100ms) |

---

## 15. Ports & URLs

| Port | Service | URL |
|------|---------|-----|
| 80 | Nginx (Frontend) | http://localhost |
| 8080 | FastAPI (API) | http://localhost:8080/api/health |
| 8081 | Flink Web UI | http://localhost:8081 |
| 8082 | Spark Master UI | http://localhost:8082 |
| 8083 | Trino UI | http://localhost:8083 |
| 8084 | Spark Worker UI | http://localhost:8084 |
| 8085 | Schema Registry | http://localhost:8085 |
| 8086 | InfluxDB UI | http://localhost:8086 |
| 9000 | MinIO API | http://localhost:9000 |
| 9001 | MinIO Console | http://localhost:9001 |
| 9092 | Kafka | kafka:9092 (internal) |
| 5432 | PostgreSQL | postgres:5432 (internal) |
| 6379 | KeyDB | keydb:6379 (internal) |
| 3000 | Dagster UI | http://localhost:3000 |
| 7077 | Spark Master RPC | spark://spark-master:7077 (internal) |
| 18080 | Spark History Server | http://localhost:18080 |

---

## 16. Data Flow Chi Tiết Theo Timeframe

### User xem chart 1s

```
React → fetchCandles("BTCUSDT", "1s", 120)
  → Nginx → FastAPI GET /api/klines?interval=1s&limit=120
    → Check Redis cache (100ms TTL)
    → KeyDB: ZRANGEBYSCORE candle:1s:BTCUSDT -inf +inf → parse JSON
    → Nếu < 120: InfluxDB query candles interval=1s range -1h
    → Return 120 candles
  → WebSocket: ws://host/api/stream?symbol=BTCUSDT&interval=1s
    → Mỗi 500ms: đọc candle:1s + ticker:latest → merge → send JSON
  → Poll mỗi 1s: fetchCandles("BTCUSDT", "1s", 5) → incremental update
```

### User xem chart 1m

```
React → fetchCandles("BTCUSDT", "1m", 200)
  → FastAPI GET /api/klines?interval=1m&limit=200
    → KeyDB: ZRANGEBYSCORE candle:1m:BTCUSDT -inf +inf
    → Nếu < 200: InfluxDB query candles interval=1m range -4h
    → Merge ticker:latest vào candle cuối
    → Return 200 candles
  → WebSocket: subscribe 1m → mỗi 500ms merge candle:1s window + ticker
  → Poll mỗi 2s: incremental update
```

### User xem chart 5m/15m

```
React → fetchCandles("BTCUSDT", "5m", 200)
  → FastAPI GET /api/klines?interval=5m&limit=200
    → Base interval = 1m (cần 200*5+5 = 1005 candles 1m)
    → KeyDB candle:1m + InfluxDB candles interval=1m
    → _aggregate(): group by 5-minute bucket → OHLCV
    → Return 200 candles 5m
```

### User xem chart 1h/4h/1d/1w

```
React → fetchCandles("BTCUSDT", "1h", 200)
  → FastAPI GET /api/klines?interval=1h&limit=200
    → Base interval = 1h
    → InfluxDB: candles interval=1h range -8.5d
    → Fallback: nếu không có 1h → query 1m → aggregate server-side
    → Return 200 candles 1h
```

### User kéo chart sang trái (scroll loading)

```
React: logicalRange.from < 20
  → loadMoreHistoricalData()
  → fetchCandles("BTCUSDT", "1m", 500, earliestTime)
    → FastAPI GET /api/klines?interval=1m&limit=500&endTime={earliestTime*1000}
      → KeyDB: ZRANGEBYSCORE candle:1m ... (filter < endTime)
      → InfluxDB: candles interval=1m, range dynamic
      → Filter openTime < endTime, take last 500
  → Merge: [...olderCandles, ...currentCandles]
  → setData(merged) + restore visible range
  → Repeat khi user tiếp tục kéo (cooldown 500ms)
```

### User chọn Historical Date Range

```
React → setHistoricalRange({startMs, endMs})
  → fetchHistoricalCandles("BTCUSDT", startMs, endMs, 2000)
    → FastAPI GET /api/klines/historical?symbol=BTCUSDT&startTime=...&endTime=...
      → Trino SQL: SELECT FROM coin_klines_hourly WHERE ...
      → Fallback: SELECT FROM historical_hourly WHERE ...
  → applyDataToChart(data) — hiển thị 1h candles
  → UI: "Viewing historical data" banner, disable timeframe buttons
```

---

## Tóm tắt kiến trúc

```
┌─── INGESTION ────────────────────────────────────────────────────────┐
│ Binance WS (400 symbols) → Producer (Python) → Kafka (3 topics)    │
│   • crypto_ticker (~400 msg/s)                                      │
│   • crypto_klines (~400 msg/s, 1s candles)                          │
│   • crypto_depth  (~4000 msg/s, 100ms updates)                      │
└──────────────────────────────────────────────────────────────────────┘
                              ↓
┌─── STREAM PROCESSING (Flink) ────────────────────────────────────────┐
│ 7 output sinks:                                                      │
│   • Ticker → KeyDB (hash + sorted set) + InfluxDB (market_ticks)    │
│   • 1s Klines → KeyDB (sorted set, 8h TTL) + InfluxDB (candles)    │
│   • 1m Klines (aggregated 1s→1m) → KeyDB (7d) + InfluxDB + Indicators │
│   • Depth → KeyDB (hash, 60s TTL)                                   │
│   • Indicators (SMA/EMA) → KeyDB + InfluxDB                         │
└──────────────────────────────────────────────────────────────────────┘
                              ↓
┌─── BATCH PROCESSING (Spark + Dagster) ───────────────────────────────┐
│   • ingest_crypto.py: Kafka → Iceberg (Structured Streaming)        │
│   • backfill_historical.py: Binance REST → InfluxDB/Iceberg         │
│   • aggregate_candles.py: 1m→1h (daily cron)                        │
│   • iceberg_maintenance.py: compact + cleanup (weekly cron)          │
└──────────────────────────────────────────────────────────────────────┘
                              ↓
┌─── SERVING (FastAPI) ────────────────────────────────────────────────┐
│ Priority routing: KeyDB → InfluxDB → Trino/Iceberg                  │
│ REST: /api/klines, /api/ticker, /api/orderbook, /api/trades, etc.   │
│ WebSocket: /api/stream (real-time candle updates, 500ms)             │
│ Cache: Redis 100ms TTL cho API responses                             │
└──────────────────────────────────────────────────────────────────────┘
                              ↓
┌─── VISUALIZATION (React + Nginx) ────────────────────────────────────┐
│ lightweight-charts v5.1.0                                            │
│   • Candlestick + Volume + SMA/EMA/RSI/MFI                          │
│   • 8 timeframes: 1s, 1m, 5m, 15m, 1H, 4H, 1D, 1W                 │
│   • Scroll-left infinite history loading                             │
│   • Real-time WebSocket + incremental polling (no flicker)           │
│   • Order Book, Recent Trades, Watchlist, Overview                   │
│   • Drawing tools, Export PNG, i18n                                  │
└──────────────────────────────────────────────────────────────────────┘
```
