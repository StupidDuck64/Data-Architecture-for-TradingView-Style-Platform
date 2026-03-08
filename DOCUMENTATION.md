# Lambda Architecture for TradingView-Style Crypto Platform

## Complete Technical Documentation

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Project Structure](#4-project-structure)
5. [Data Types & Schemas](#5-data-types--schemas)
6. [Data Flow — End to End](#6-data-flow--end-to-end)
7. [Component Deep Dives](#7-component-deep-dives)
8. [Serving Layer Implementation Guide](#8-serving-layer-implementation-guide)
9. [Docker Compose Integration](#9-docker-compose-integration)
10. [AWS Cloud Deployment Considerations](#10-aws-cloud-deployment-considerations)
11. [Appendix](#11-appendix)

---

## 1. Project Overview

This project implements a **Lambda Architecture** for a real-time cryptocurrency market data platform, modeled after TradingView. It ingests live market data from Binance, processes it through both real-time (speed) and batch layers, stores it across purpose-built databases, and serves it to a React-based dashboard frontend.

### Core Principles

| Principle         | Implementation                                                |
| ----------------- | ------------------------------------------------------------- |
| **Speed Layer**   | Flink streaming → KeyDB (hot cache) + InfluxDB (time-series)  |
| **Batch Layer**   | Spark jobs → Iceberg tables on MinIO (data lake)              |
| **Serving Layer** | Trino (SQL analytics) + FastAPI (REST/WebSocket) + React (UI) |
| **Orchestration** | Dagster (scheduled maintenance & backfill)                    |

### What the System Does

- Subscribes to **400+ cryptocurrency trading pairs** via Binance WebSocket streams
- Captures **4 data types**: ticker stats, aggregate trades, 1-minute candles, and order book snapshots
- Processes data in real-time with sub-second latency to a hot cache (KeyDB)
- Persists time-series data to InfluxDB for operational queries
- Archives all data to an Iceberg data lake on MinIO for long-term analytics
- Aggregates 1m candles into 1h candles automatically
- Backfills gaps caused by downtime from Binance historical API
- Provides SQL query access via Trino for ad-hoc analytics

---

## 2. System Architecture

### Architecture Diagram

```
                        ┌─────────────────────────────────────────────┐
                        │           BINANCE EXCHANGE                  │
                        │                                             │
                        │  WebSocket Streams:                         │
                        │  ├─ !ticker@arr       (24h stats)           │
                        │  ├─ <symbol>@aggTrade  (trades)             │
                        │  ├─ <symbol>@kline_1m  (candles)            │
                        │  └─ <symbol>@depth20@100ms (order book)     │
                        └────────────────┬────────────────────────────┘
                                         │
                                         │ WebSocket (wss://)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  DOCKER COMPOSE (cryptoprice network)                                                   │
│                                                                                         │
│  ┌──────────────────────┐           ┌──────────────────────────────────────────────┐     │
│  │  producer_binance.py │           │              KAFKA (KRaft)                   │     │
│  │  ─────────────────── │    JSON   │  ┌──────────────┬──────────────────────────┐ │     │
│  │  4 WebSocket threads │──────────▶│  │crypto_ticker │ crypto_trades            │ │     │
│  │  LZ4-compressed msgs │           │  │crypto_klines │ crypto_depth             │ │     │
│  │  Auto-reconnect      │           │  │ (3 partitions each, 7-day retention)    │ │     │
│  └──────────────────────┘           │  └──────────────┴──────────────────────────┘ │     │
│                                     └───────────┬──────────────────────────────────┘     │
│                                                 │                                       │
│                              ┌──────────────────┼──────────────────────┐                 │
│                              │   FLINK CLUSTER (Speed Layer)          │                 │
│                              │   ingest_flink_crypto.py               │                 │
│                              │                                        │                 │
│                              │   ┌────────┐ ┌────────┐ ┌──────────┐  │                 │
│                              │   │Ticker  │ │Trades  │ │ Klines   │  │                 │
│                              │   │Stream  │ │Stream  │ │ Stream   │  │                 │
│                              │   └───┬────┘ └───┬────┘ └────┬─────┘  │                 │
│                              │       │          │           │        │                 │
│                              └───────┼──────────┼───────────┼────────┘                 │
│                                      │          │           │                           │
│              ┌───────────────────────┼──────────┼───────────┼──────────────┐            │
│              ▼                       ▼          ▼           ▼              ▼            │
│   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────────────┐        │
│   │     KeyDB        │   │    InfluxDB      │   │    ICEBERG on MinIO          │        │
│   │   (Hot Cache)    │   │  (Time-Series)   │   │    (Data Lake / Cold)        │        │
│   │                  │   │                  │   │                              │        │
│   │ ticker:latest:*  │   │ market_ticks     │   │ coin_ticker (partitioned/d)  │        │
│   │ ticker:history:* │   │ candles          │   │ coin_trades (partitioned/d)  │        │
│   │ candle:latest:*  │   │ indicators       │   │ coin_klines (partitioned/d)  │        │
│   │ candle:history:* │   │                  │   │ coin_klines_hourly           │        │
│   │ indicator:*      │   │                  │   │ historical_hourly            │        │
│   │ orderbook:*      │   │                  │   │                              │        │
│   └──────────────────┘   └──────────────────┘   └──────────────┬───────────────┘        │
│          │                       │                              │                       │
│          │                       │                     ┌────────┴────────┐               │
│          │                       │                     │     TRINO       │               │
│          │                       │                     │  (SQL Engine)   │               │
│          │                       │                     │  Iceberg JDBC   │               │
│          │                       │                     └────────┬────────┘               │
│          │                       │                              │                       │
│          │    ┌──────────────────┴──────────────────────────────┘                       │
│          │    │         │                                                                │
│          │    │    ┌────┴──────────────────────────────────────────────┐                 │
│          │    │    │            BATCH LAYER (Spark)                   │                 │
│          │    │    │                                                  │                 │
│          │    │    │  ┌─────────────────────┐  ┌──────────────────┐   │                 │
│          │    │    │  │backfill_historical  │  │aggregate_candles │   │                 │
│          │    │    │  │ (fill InfluxDB gaps │  │ (1m → 1h agg,   │   │                 │
│          │    │    │  │  + Iceberg incr.)   │  │  retention mgmt) │   │                 │
│          │    │    │  └─────────────────────┘  └──────────────────┘   │                 │
│          │    │    │  ┌─────────────────────┐                        │                 │
│          │    │    │  │iceberg_maintenance  │  Dagster Orchestration │                 │
│          │    │    │  │ (compact, expire,   │  (Daily/Weekly cron)   │                 │
│          │    │    │  │  orphan cleanup)    │                        │                 │
│          │    │    │  └─────────────────────┘                        │                 │
│          │    │    └─────────────────────────────────────────────────┘                 │
│          │    │                                                                         │
│   ┌──────┴────┴───────────────────────────────────┐                                    │
│   │              SERVING LAYER (To Build)          │                                    │
│   │                                                │                                    │
│   │  ┌─────────────┐   ┌─────────┐  ┌──────────┐  │                                    │
│   │  │   FastAPI    │   │  Nginx  │  │  React   │  │                                    │
│   │  │  REST + WS   │──▶│ Reverse │──▶│Dashboard │  │                                    │
│   │  │  Backend     │   │ Proxy   │  │ (SPA)    │  │                                    │
│   │  └─────────────┘   └─────────┘  └──────────┘  │                                    │
│   └────────────────────────────────────────────────┘                                    │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer             | Components                                                                         | Purpose                                             |
| ----------------- | ---------------------------------------------------------------------------------- | --------------------------------------------------- |
| **Ingestion**     | `producer_binance.py`                                                              | Capture live WebSocket data → Kafka                 |
| **Speed**         | Flink + `ingest_flink_crypto.py`                                                   | Real-time processing → KeyDB, InfluxDB, Iceberg     |
| **Batch**         | Spark + `backfill_historical.py`, `aggregate_candles.py`, `iceberg_maintenance.py` | Historical backfill, aggregation, table maintenance |
| **Storage**       | KeyDB, InfluxDB, Iceberg/MinIO, PostgreSQL                                         | Hot cache, time-series, data lake, catalog metadata |
| **Query**         | Trino                                                                              | Ad-hoc SQL queries over Iceberg tables              |
| **Orchestration** | Dagster                                                                            | Schedule and monitor batch jobs                     |
| **Serving**       | FastAPI + Nginx + React _(to build)_                                               | REST/WebSocket API + web UI                         |

---

## 3. Tech Stack

### Data & Messaging

| Technology       | Version       | Role                                             | Port(s)              |
| ---------------- | ------------- | ------------------------------------------------ | -------------------- |
| **Apache Kafka** | 3.9.0 (KRaft) | Event streaming, message broker                  | 9092                 |
| **MinIO**        | Latest        | S3-compatible object storage (Iceberg warehouse) | 9000, 9001 (console) |
| **InfluxDB**     | 2.7           | Time-series database (operational queries)       | 8086                 |
| **PostgreSQL**   | 16-alpine     | Metadata storage (Iceberg catalog + Dagster)     | 5432                 |
| **KeyDB**        | Latest        | Redis-compatible in-memory cache (hot data)      | 6379                 |

### Compute & Processing

| Technology         | Version | Role                                           | Port(s)                          |
| ------------------ | ------- | ---------------------------------------------- | -------------------------------- |
| **Apache Flink**   | 1.18.1  | Stream processing (speed layer)                | 8081 (UI)                        |
| **Apache Spark**   | 3.5.5   | Batch processing (batch layer)                 | 7077, 8082 (UI), 18080 (History) |
| **Apache Iceberg** | 1.5.2   | Table format for data lake (ACID, time-travel) | —                                |

### Query & Orchestration

| Technology  | Version | Role                                      | Port(s) |
| ----------- | ------- | ----------------------------------------- | ------- |
| **Trino**   | 442     | Distributed SQL query engine over Iceberg | 8083    |
| **Dagster** | 1.8.x   | Workflow orchestration & scheduling       | 3000    |

### Serving Layer (To Implement)

| Technology  | Version | Role                              | Port(s)             |
| ----------- | ------- | --------------------------------- | ------------------- |
| **FastAPI** | 0.110+  | REST + WebSocket API server       | 8000                |
| **Nginx**   | 1.25+   | Reverse proxy, static file server | 80                  |
| **React**   | 18.3.1  | Frontend SPA (Crypto Dashboard)   | — (served by Nginx) |

### Frontend Dependencies (Crypto-Dashboard)

| Package              | Version | Purpose                                 |
| -------------------- | ------- | --------------------------------------- |
| `lightweight-charts` | 5.1.0   | TradingView-standard candlestick charts |
| `recharts`           | 2.12.7  | Additional charting (overview)          |
| `lucide-react`       | 0.396.0 | UI icon library                         |
| `tailwindcss`        | 3.4.4   | Utility-first CSS framework             |

---

## 4. Project Structure

```
Lambda-Architecture-for-TradingView-Style-Platform/
│
├── docker-compose.yml              # 14 services across 4 layers
├── spark-defaults.conf             # Spark driver/executor config + Iceberg extensions
├── README.md                       # Original Vietnamese documentation
│
├── docker/                         # Dockerfiles & service configs
│   ├── backfill/
│   │   └── Dockerfile              # Python 3.11-slim for InfluxDB gap-filling
│   ├── dagster/
│   │   ├── Dockerfile              # Spark 3.5.5 base + Dagster 1.8
│   │   └── dagster.yaml            # PostgreSQL storage, daemon scheduler
│   ├── flink/
│   │   ├── Dockerfile              # Flink 1.18.1 + Python + Kafka connector
│   │   └── flink-conf.yaml         # Memory (1.6G JM / 1.7G TM), checkpointing
│   ├── postgres/
│   │   └── init.sql                # Creates iceberg_catalog + dagster databases
│   ├── producer/
│   │   └── Dockerfile              # Python 3.11-slim + kafka-python + websocket
│   ├── spark/
│   │   ├── Dockerfile              # Spark 3.5.5 + Python + custom entrypoint
│   │   └── entrypoint.sh           # master/worker/history mode switcher
│   └── trino/
│       └── etc/
│           ├── config.properties   # Coordinator config, 2GB query limit
│           ├── jvm.config          # G1GC, 2G heap
│           ├── log.properties      # INFO level
│           ├── node.properties     # Production env, data dir
│           └── catalog/
│               └── iceberg.properties  # JDBC catalog → PostgreSQL + MinIO
│
├── orchestration/                  # Dagster definitions
│   ├── assets.py                   # 3 Spark job assets with cron schedules
│   └── workspace.yaml              # Dagster workspace loader config
│
└── src/                            # Python source code
    ├── producer_binance.py         # Binance WebSocket → 4 Kafka topics
    ├── ingest_flink_crypto.py      # Flink: Kafka → KeyDB + InfluxDB + Iceberg
    ├── ingest_crypto.py            # (Legacy) Spark Kafka → Iceberg direct
    ├── backfill_historical.py      # Unified backfill: InfluxDB gaps + Iceberg incremental
    ├── backfill_influx.py          # (Legacy) InfluxDB-only gap detection
    ├── aggregate_candles.py        # 1m → 1h candle aggregation + retention
    ├── iceberg_maintenance.py      # Table compaction, snapshot expiry, orphan cleanup
    └── ingest_historical_iceberg.py # Standalone Binance → Iceberg historical loader

Crypto-Dashboard/                   # React frontend (separate project)
├── package.json                    # React 18.3.1 + lightweight-charts + tailwind
├── tailwind.config.js              # Dark theme, TradingView-style colors
├── public/
│   └── index.html                  # SPA entry point
└── src/
    ├── App.js                      # Main layout: header + chart + watchlist
    ├── index.js                    # Root: I18nProvider + AuthProvider
    ├── components/
    │   ├── CandlestickChart.js     # TradingView chart with SMA/EMA/RSI/MFI
    │   ├── ChartOverlay.js         # Drawing tools: trendline, fib, Elliott wave
    │   ├── DrawingToolbar.js       # Tool selection sidebar
    │   ├── MarketSelector.js       # Symbol dropdown with categories
    │   ├── OrderBook.js            # Bid/ask depth visualization
    │   ├── RecentTrades.js         # Recent trade feed table
    │   ├── OverviewChart.js        # OHLCV summary with sparkline
    │   ├── ToolSettingsPopup.js    # Drawing tool configuration
    │   ├── AuthModal.js            # Login/register modal
    │   └── LanguageSwitcher.js     # EN/VI language toggle
    ├── contexts/
    │   └── AuthContext.js          # Auth state (currently localStorage-based)
    ├── hooks/
    │   └── useCandlestickData.js   # Data fetching hook with live subscription
    ├── services/
    │   └── marketDataService.js    # API abstraction layer (mock/live toggle)
    └── i18n/
        ├── index.js                # I18n context provider
        └── translations.js         # EN + VI translation strings
```

---

## 5. Data Types & Schemas

This system processes **4 primary data types** from Binance, each flowing through a specific pipeline path.

### 5.1. Ticker Data (24h Market Statistics)

**Source**: Binance WebSocket `!ticker@arr` (all USDT pairs, batch update)

**Kafka Topic**: `crypto_ticker`

**Schema**:

```json
{
  "event_time": 1710000000000,
  "symbol": "BTCUSDT",
  "close": 67500.5,
  "bid": 67500.0,
  "ask": 67501.0,
  "24h_open": 66800.0,
  "24h_high": 68200.0,
  "24h_low": 66500.0,
  "24h_volume": 12345.678,
  "24h_quote_volume": 834567890.12,
  "24h_price_change": 700.5,
  "24h_price_change_pct": 1.048,
  "trade_count": 567890
}
```

**Storage Destinations**:

| Destination  | Format                               | Key/Measurement                                                                                              | TTL/Retention        |
| ------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------ | -------------------- |
| **KeyDB**    | Hash `ticker:latest:{symbol}`        | price, bid, ask, volume, event_time                                                                          | No TTL (overwritten) |
| **KeyDB**    | Sorted Set `ticker:history:{symbol}` | score=timestamp, member=price:volume                                                                         | Rolling window       |
| **InfluxDB** | Measurement `market_ticks`           | Tags: symbol, exchange, source; Fields: price, bid, ask, volume, quote_volume, price_change_pct, trade_count | Configurable         |
| **Iceberg**  | Table `coin_ticker`                  | Partitioned by `days(event_timestamp)`                                                                       | Permanent            |

---

### 5.2. Aggregate Trade Data

**Source**: Binance WebSocket `<symbol>@aggTrade`

**Kafka Topic**: `crypto_trades`

**Schema**:

```json
{
  "event_time": 1710000000123,
  "symbol": "BTCUSDT",
  "agg_trade_id": 3456789012,
  "price": 67500.5,
  "quantity": 0.0234,
  "trade_time": 1710000000100,
  "is_buyer_maker": false
}
```

**Storage Destinations**:

| Destination  | Format                                                      | Details              |
| ------------ | ----------------------------------------------------------- | -------------------- |
| **KeyDB**    | _(not stored — trades are high-volume)_                     | —                    |
| **InfluxDB** | _(not directly — only via ticker aggregation)_              | —                    |
| **Iceberg**  | Table `coin_trades`, partitioned by `days(trade_timestamp)` | All fields persisted |

---

### 5.3. Kline (Candlestick) Data

**Source**: Binance WebSocket `<symbol>@kline_1m` (1-minute interval)

**Kafka Topic**: `crypto_klines`

**Schema**:

```json
{
  "event_time": 1710000000000,
  "symbol": "BTCUSDT",
  "kline_start": 1709999940000,
  "kline_close": 1709999999999,
  "interval": "1m",
  "open": 67480.0,
  "high": 67520.0,
  "low": 67470.0,
  "close": 67500.5,
  "volume": 12.345,
  "quote_volume": 833456.78,
  "trade_count": 234,
  "is_closed": true
}
```

**Storage Destinations**:

| Destination  | Format                                                      | Details                                                       |
| ------------ | ----------------------------------------------------------- | ------------------------------------------------------------- |
| **KeyDB**    | Hash `candle:latest:{symbol}`                               | OHLCV + is_closed + interval                                  |
| **KeyDB**    | Sorted Set `candle:history:{symbol}`                        | score=kline_start, member=JSON candle                         |
| **InfluxDB** | Measurement `candles`                                       | Tags: symbol, exchange, interval; Fields: OHLCV + trade_count |
| **Iceberg**  | Table `coin_klines`, partitioned by `days(kline_timestamp)` | All fields                                                    |

**Aggregated Derivatives**:

| Table                | Source                             | Aggregation                                                |
| -------------------- | ---------------------------------- | ---------------------------------------------------------- |
| `coin_klines_hourly` | `coin_klines` (1m, is_closed=true) | OHLCV: first/max/min/last/sum per hour                     |
| `historical_hourly`  | Binance REST API (1h klines)       | Direct from API, partitioned by symbol + years(event_time) |

---

### 5.4. Order Book (Depth) Data

**Source**: Binance WebSocket `<symbol>@depth20@100ms` (top 20 levels, 100ms updates)

**Kafka Topic**: `crypto_depth`

**Schema**:

```json
{
  "event_time": 1710000000050,
  "symbol": "BTCUSDT",
  "bids": [["67500.00", "1.234"], ["67499.50", "2.567"], ...],
  "asks": [["67501.00", "0.890"], ["67501.50", "1.456"], ...],
  "lastUpdateId": 45678901234
}
```

**Storage Destinations**:

| Destination  | Format                                    | Details                                                                    |
| ------------ | ----------------------------------------- | -------------------------------------------------------------------------- |
| **KeyDB**    | Hash `orderbook:{symbol}`                 | bids (JSON), asks (JSON), best_bid, best_ask, spread, bid_depth, ask_depth |
| **InfluxDB** | _(not stored)_                            | —                                                                          |
| **Iceberg**  | _(not stored — high frequency, volatile)_ | —                                                                          |

> **TTL**: Order book entries in KeyDB expire after **60 seconds** and are refreshed every 100ms.

---

### 5.5. Derived Data: Technical Indicators

Computed by `ingest_flink_crypto.py` from kline data — **not** a raw Binance feed.

**Indicators Calculated**:

| Indicator  | Algorithm                       | Buffer    |
| ---------- | ------------------------------- | --------- |
| **SMA-20** | Mean of last 20 closing prices  | 60 closes |
| **SMA-50** | Mean of last 50 closing prices  | 60 closes |
| **EMA-12** | Exponential weighted (α = 2/13) | 60 closes |
| **EMA-26** | Exponential weighted (α = 2/27) | 60 closes |

**Storage**:

| Destination  | Format                           | Trigger                       |
| ------------ | -------------------------------- | ----------------------------- |
| **KeyDB**    | Hash `indicator:latest:{symbol}` | Every 200 events or 5 seconds |
| **InfluxDB** | Measurement `indicators`         | Same trigger                  |

---

### 5.6. Complete Data Schema Matrix

```
┌──────────────────────┬──────────┬──────────┬──────────┬──────────┐
│                      │  KeyDB   │ InfluxDB │ Iceberg  │  Trino   │
│    Data Type         │(hot/live)│(time-ser)│(cold/DL) │(SQL qry) │
├──────────────────────┼──────────┼──────────┼──────────┼──────────┤
│ Ticker (24h stats)   │    ✅    │    ✅    │    ✅    │    ✅    │
│ Aggregate Trades     │    —     │    —     │    ✅    │    ✅    │
│ Klines (1m candles)  │    ✅    │    ✅    │    ✅    │    ✅    │
│ Klines (1h agg.)     │    —     │    ✅    │    ✅    │    ✅    │
│ Order Book (depth)   │    ✅    │    —     │    —     │    —     │
│ Tech. Indicators     │    ✅    │    ✅    │    —     │    —     │
│ Historical (1h)      │    —     │    —     │    ✅    │    ✅    │
└──────────────────────┴──────────┴──────────┴──────────┴──────────┘
```

---

## 6. Data Flow — End to End

### 6.1. Real-Time Path (Speed Layer)

```
Binance WebSocket
       │
       ▼
producer_binance.py
├─ Stream A: !ticker@arr ──────────────▶ Kafka: crypto_ticker
├─ Stream B: <sym>@aggTrade ───────────▶ Kafka: crypto_trades
├─ Stream C: <sym>@kline_1m ──────────▶ Kafka: crypto_klines
└─ Stream D: <sym>@depth20@100ms ─────▶ Kafka: crypto_depth
       │
       ▼
ingest_flink_crypto.py (Flink Structured Streaming)
│
├─── Ticker Stream ────────────────────┬──▶ KeyDB: ticker:latest:{sym}, ticker:history:{sym}
│                                      ├──▶ InfluxDB: market_ticks
│                                      └──▶ Iceberg: coin_ticker
│
├─── Trades Stream ────────────────────────▶ Iceberg: coin_trades
│
├─── Klines Stream ────────────────────┬──▶ KeyDB: candle:latest:{sym}, candle:history:{sym}
│                                      ├──▶ InfluxDB: candles
│                                      └──▶ Iceberg: coin_klines
│
├─── Depth Stream ─────────────────────────▶ KeyDB: orderbook:{sym} (TTL 60s)
│
└─── Indicator Engine ─────────────────┬──▶ KeyDB: indicator:latest:{sym}
     (from kline closes, every 200     └──▶ InfluxDB: indicators
      events or 5 seconds)
```

**Latency Profile**:

- WebSocket → Kafka: < 10ms
- Kafka → KeyDB (via Flink): < 100ms
- Kafka → InfluxDB (via Flink): batched every 200 events or 5s
- Kafka → Iceberg (via Flink): batched every 1 minute

### 6.2. Batch Path (Batch Layer)

```
Dagster Scheduler
       │
       ├── Daily 02:00 AM ──▶ backfill_historical.py
       │                      ├── Mode: InfluxDB gap detection
       │                      │   └── Flux elapsed() → find gaps > 5min
       │                      │       └── Binance REST 1m klines → InfluxDB
       │                      └── Mode: Iceberg incremental
       │                          └── MAX(open_time) per symbol
       │                              └── Binance REST 1h klines → Iceberg
       │
       ├── Daily 04:00 AM ──▶ aggregate_candles.py
       │                      ├── InfluxDB: query 1m > 7 days old
       │                      │   └── Aggregate OHLCV → write 1h → delete old 1m
       │                      └── Iceberg: SQL aggregate coin_klines
       │                          └── INSERT INTO coin_klines_hourly
       │                              └── DELETE FROM coin_klines (old 1m)
       │
       └── Weekly Sun 03:00 ──▶ iceberg_maintenance.py
                                ├── Rewrite data files (binpack → 128MB target)
                                ├── Rewrite manifest files
                                ├── Expire snapshots (> 48h, keep 5)
                                └── Remove orphan files (> 72h unreferenced)
```

### 6.3. Query Path

```
Trino (SQL)
   │
   └── SELECT * FROM iceberg_catalog.crypto_lakehouse.coin_klines
       WHERE symbol = 'BTCUSDT'
       AND kline_timestamp > TIMESTAMP '2025-01-01'
       │
       └── Iceberg metadata (PostgreSQL) → Parquet files (MinIO/S3)
```

### 6.4. Serving Path (To Build)

```
React Frontend
   │
   ├── HTTP GET ──▶ Nginx ──▶ FastAPI ──▶ KeyDB   (latest price, orderbook)
   ├── HTTP GET ──▶ Nginx ──▶ FastAPI ──▶ InfluxDB (historical candles)
   ├── HTTP GET ──▶ Nginx ──▶ FastAPI ──▶ Trino    (deep analytics)
   └── WebSocket ─▶ Nginx ──▶ FastAPI ──▶ KeyDB    (real-time stream)
```

---

## 7. Component Deep Dives

### 7.1. Producer (`producer_binance.py`)

The producer manages 4 types of WebSocket connections to Binance and forwards data to Kafka.

**WebSocket Connection Strategy**:

- Fetches all USDT spot trading pairs from Binance `exchangeInfo` endpoint
- Caps at `MAX_SYMBOLS = 400` pairs (configurable)
- **Ticker** (Stream A): Single connection using `!ticker@arr` (all-tickers array)
- **Trades/Klines/Depth** (Streams B/C/D): Batched into groups of 200 symbols per connection using combined stream URLs

**Kafka Producer Config**:

```python
KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=json.dumps → bytes,
    key_serializer=str → bytes,
    compression_type='lz4',
    acks=1,
    batch_size=65536,        # 64KB batches
    linger_ms=5,             # 5ms linger for batching
    retries=3,
    retry_backoff_ms=200
)
```

**Reconnection**: Auto-reconnect with exponential backoff (5s base + random jitter).

---

### 7.2. Flink Streaming (`ingest_flink_crypto.py`)

The core speed layer, consuming 3 Kafka topics (ticker, trades, klines) plus depth, and writing to 3 sinks in parallel.

**Processing Pipeline per Stream**:

1. Read from Kafka as structured streaming DataFrame
2. Parse JSON with schema inference
3. Extract and cast fields (timestamps, decimals)
4. Add watermark (1–2 minutes) for late-arrival handling
5. Drop duplicates within watermark window
6. `foreachBatch` → parallel writes to KeyDB + InfluxDB + Iceberg

**KeyDB Write Strategy**:

```python
# Ticker: HSET (overwrite latest) + ZADD (append history)
pipe.hset(f"ticker:latest:{symbol}", mapping={...})
pipe.zadd(f"ticker:history:{symbol}", {f"{price}:{vol}": ts})

# Candles: similar dual-write pattern
pipe.hset(f"candle:latest:{symbol}", mapping={...})
pipe.zadd(f"candle:history:{symbol}", {json.dumps(candle): kline_start})

# Order Book: HSET with 60s TTL
pipe.hset(f"orderbook:{symbol}", mapping={
    "bids": json.dumps(top20_bids),
    "asks": json.dumps(top20_asks),
    "best_bid": ..., "best_ask": ..., "spread": ...
})
pipe.expire(f"orderbook:{symbol}", 60)
```

**InfluxDB Write Strategy**: Batched — flush every 200 events OR every 5 seconds.

**Iceberg Write Strategy**: Append mode, 1-minute trigger, checkpoint to MinIO.

---

### 7.3. Batch Jobs (Spark)

**backfill_historical.py** — Two modes:

- **InfluxDB Mode**: Detect gaps > 5min using Flux `elapsed()` → fetch Binance 1m klines → fill gaps. ThreadPoolExecutor with 5 workers.
- **Iceberg Mode**: Query `MAX(open_time)` per symbol → fetch Binance 1h klines from that point → batch write (10k rows) to Iceberg.

**aggregate_candles.py** — Dual-backend:

- **InfluxDB**: Query 1m candles older than `RETENTION_1M_DAYS` (default 7) → aggregate OHLCV → write 1h → delete old 1m.
- **Iceberg**: `INSERT INTO coin_klines_hourly SELECT ... FROM coin_klines WHERE is_closed = true GROUP BY date_trunc('hour', kline_timestamp), symbol`.

**iceberg_maintenance.py** — Four operations per table (`coin_ticker`, `coin_trades`, `coin_klines`, `coin_klines_hourly`):

1. `CALL rewrite_data_files(strategy => 'binpack', options => map('target-file-size-bytes', '134217728'))` — 128MB target
2. `CALL rewrite_manifests()` — optimize metadata
3. `CALL expire_snapshots(older_than => TIMESTAMP - 48h, retain_last => 5)`
4. `CALL remove_orphan_files(older_than => TIMESTAMP - 72h)`

---

### 7.4. React Dashboard (Crypto-Dashboard)

**Component Hierarchy**:

```
<I18nProvider>
  <AuthProvider>
    <App>
      ├── Header
      │   ├── Logo + Search
      │   ├── LanguageSwitcher
      │   └── Auth Button → AuthModal
      ├── Main Content
      │   ├── DrawingToolbar (left sidebar)
      │   └── CandlestickChart (center)
      │       ├── Tab: Candlestick → lightweight-charts + indicators
      │       ├── Tab: Overview → OverviewChart (recharts sparkline)
      │       ├── Tab: Order Book → OrderBook
      │       └── Tab: Recent Trades → RecentTrades
      │       └── ChartOverlay (drawing tools layer)
      └── Watchlist Sidebar (right)
    </App>
  </AuthProvider>
</I18nProvider>
```

**Key Abstraction — `marketDataService.js`**:

```javascript
// Toggle between mock data and live API
const DATA_SOURCE = "mock"; // Change to "api" when backend is ready

const API_BASE_URL =
  process.env.REACT_APP_API_BASE_URL || "http://localhost:8080/api";
```

This service is the **single point of integration** between the React frontend and the backend. It exports:

- `fetchCandles(symbol, timeframe, limit)` → OHLCV array
- `subscribeCandle(symbol, timeframe, onCandle)` → real-time WebSocket subscription
- `fetchSymbols()` → available trading pairs

---

## 8. Serving Layer Implementation Guide

This section details how to implement the missing serving layer: **FastAPI** (backend API), **Nginx** (reverse proxy), and adjustments to the **React** dashboard.

### 8.1. FastAPI Backend

#### Directory Structure

```
api/
├── main.py                    # FastAPI application entry point
├── requirements.txt           # Python dependencies
├── config.py                  # Environment-driven configuration
├── routers/
│   ├── klines.py              # Candlestick/OHLCV endpoints
│   ├── ticker.py              # Live price + 24h stats endpoints
│   ├── orderbook.py           # Order book endpoints
│   ├── trades.py              # Recent trades endpoints
│   ├── symbols.py             # Available symbols endpoint
│   ├── indicators.py          # Technical indicators endpoints
│   └── auth.py                # Authentication endpoints (optional)
├── services/
│   ├── keydb_service.py       # KeyDB (Redis) connection pool
│   ├── influx_service.py      # InfluxDB query client
│   └── trino_service.py       # Trino SQL client (for deep queries)
├── models/
│   ├── candle.py              # Pydantic models for OHLCV
│   ├── ticker.py              # Pydantic models for ticker
│   ├── orderbook.py           # Pydantic models for depth
│   └── trade.py               # Pydantic models for trades
└── ws/
    └── stream.py              # WebSocket manager for real-time push
```

#### Core Dependencies (`requirements.txt`)

```
fastapi==0.115.*
uvicorn[standard]==0.32.*
redis[hiredis]==5.2.*
influxdb-client==1.44.*
trino==0.330.*
pydantic==2.*
python-jose[cryptography]==3.3.*    # JWT auth (optional)
passlib[bcrypt]==1.7.*              # Password hashing (optional)
```

#### Application Entry Point (`main.py`)

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import redis.asyncio as redis

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: create connection pools
    app.state.redis = redis.ConnectionPool.from_url("redis://keydb:6379")
    yield
    # Shutdown: close pools
    await app.state.redis.aclose()

app = FastAPI(title="CryptoPrice API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
from routers import klines, ticker, orderbook, trades, symbols, indicators
app.include_router(klines.router, prefix="/api")
app.include_router(ticker.router, prefix="/api")
app.include_router(orderbook.router, prefix="/api")
app.include_router(trades.router, prefix="/api")
app.include_router(symbols.router, prefix="/api")
app.include_router(indicators.router, prefix="/api")
```

#### API Endpoints Specification

##### Klines (Candlestick Data)

```
GET /api/klines?symbol=BTCUSDT&interval=1m&limit=200
```

**Strategy**: Route to different backends based on timeframe:

- `1m`, `5m`, `15m` → **InfluxDB** (measurement: `candles`, aggregate on the fly for 5m/15m)
- `1h`, `4h` → **InfluxDB** (`candles` with interval tag) or **Iceberg** via Trino for older data
- `1d`, `1w` → **Trino** (query `coin_klines_hourly` or `historical_hourly` and aggregate)

```python
# Example: klines.py router
from fastapi import APIRouter, Query
router = APIRouter()

@router.get("/klines")
async def get_klines(
    symbol: str = Query(..., description="Trading pair, e.g. BTCUSDT"),
    interval: str = Query("1h", regex="^(1m|5m|15m|1h|4h|1d|1w)$"),
    limit: int = Query(200, ge=1, le=1500),
):
    if interval in ("1m", "5m", "15m"):
        return await query_influxdb_candles(symbol, interval, limit)
    elif interval in ("1h", "4h"):
        return await query_influxdb_or_iceberg(symbol, interval, limit)
    else:
        return await query_trino_historical(symbol, interval, limit)
```

**Response Format** (matches what React expects):

```json
[
  { "time": 1710000000, "open": 67480.0, "high": 67520.0, "low": 67470.0, "close": 67500.5, "volume": 12.345 },
  ...
]
```

##### Ticker (Live Prices)

```
GET /api/ticker/{symbol}
GET /api/ticker                  # All symbols (for watchlist)
```

**Strategy**: Read directly from **KeyDB** (`ticker:latest:{symbol}` hash).

```python
@router.get("/ticker/{symbol}")
async def get_ticker(symbol: str):
    data = await redis_client.hgetall(f"ticker:latest:{symbol}")
    return {
        "symbol": symbol,
        "price": float(data[b"price"]),
        "bid": float(data[b"bid"]),
        "ask": float(data[b"ask"]),
        "volume": float(data[b"volume"]),
        "event_time": int(data[b"event_time"]),
    }

@router.get("/ticker")
async def get_all_tickers():
    # SCAN for all ticker:latest:* keys
    tickers = []
    async for key in redis_client.scan_iter("ticker:latest:*"):
        symbol = key.decode().split(":")[-1]
        data = await redis_client.hgetall(key)
        tickers.append({...})
    return tickers
```

##### Order Book

```
GET /api/orderbook/{symbol}?depth=20
```

**Strategy**: Read from **KeyDB** (`orderbook:{symbol}` hash).

```python
@router.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str, depth: int = Query(20, ge=1, le=50)):
    data = await redis_client.hgetall(f"orderbook:{symbol}")
    bids = json.loads(data[b"bids"])[:depth]
    asks = json.loads(data[b"asks"])[:depth]
    return {
        "symbol": symbol,
        "bids": [{"price": b[0], "amount": b[1]} for b in bids],
        "asks": [{"price": a[0], "amount": a[1]} for a in asks],
        "spread": float(data.get(b"spread", 0)),
    }
```

##### Recent Trades

```
GET /api/trades/{symbol}?limit=50
```

**Strategy**: Query **Trino** against `coin_trades` Iceberg table (latest N trades).

```python
@router.get("/trades/{symbol}")
async def get_recent_trades(symbol: str, limit: int = Query(50, ge=1, le=200)):
    query = f"""
        SELECT trade_timestamp, price, quantity, is_buyer_maker
        FROM iceberg_catalog.crypto_lakehouse.coin_trades
        WHERE symbol = '{symbol}'
        ORDER BY trade_timestamp DESC
        LIMIT {limit}
    """
    # Use parameterized queries in production to prevent SQL injection
    rows = await trino_client.execute(query)
    return [format_trade(row) for row in rows]
```

> **Important**: Use parameterized queries or Trino's prepared statements in production. The example above is simplified for illustration.

##### Symbols

```
GET /api/symbols
```

**Strategy**: Derive from KeyDB keys (`ticker:latest:*`) or maintain a static config.

```python
@router.get("/symbols")
async def get_symbols():
    symbols = []
    async for key in redis_client.scan_iter("ticker:latest:*"):
        sym = key.decode().split(":")[-1]
        symbols.append({
            "symbol": sym,
            "name": sym.replace("USDT", "/USDT"),
            "type": "crypto"
        })
    return sorted(symbols, key=lambda s: s["symbol"])
```

##### Technical Indicators

```
GET /api/indicators/{symbol}
```

**Strategy**: Read from **KeyDB** (`indicator:latest:{symbol}` hash).

```python
@router.get("/indicators/{symbol}")
async def get_indicators(symbol: str):
    data = await redis_client.hgetall(f"indicator:latest:{symbol}")
    return {
        "symbol": symbol,
        "sma20": float(data.get(b"sma20", 0)),
        "sma50": float(data.get(b"sma50", 0)),
        "ema12": float(data.get(b"ema12", 0)),
        "ema26": float(data.get(b"ema26", 0)),
        "timestamp": data.get(b"ts", b"").decode(),
    }
```

#### WebSocket Endpoint (Real-Time Streaming)

```
WS /api/ws/stream?symbols=BTCUSDT,ETHUSDT
```

**Strategy**: Subscribe to KeyDB pub/sub or poll KeyDB keys and push updates to connected clients.

```python
from fastapi import WebSocket, WebSocketDisconnect

@app.websocket("/api/ws/stream")
async def websocket_stream(websocket: WebSocket, symbols: str = "BTCUSDT"):
    await websocket.accept()
    symbol_list = [s.strip().upper() for s in symbols.split(",")]

    try:
        while True:
            for sym in symbol_list:
                # Read latest data from KeyDB
                ticker = await redis_client.hgetall(f"ticker:latest:{sym}")
                candle = await redis_client.hgetall(f"candle:latest:{sym}")
                if ticker:
                    await websocket.send_json({
                        "type": "ticker",
                        "symbol": sym,
                        "data": format_ticker(ticker)
                    })
                if candle:
                    await websocket.send_json({
                        "type": "candle",
                        "symbol": sym,
                        "data": format_candle(candle)
                    })
            await asyncio.sleep(1)  # Push interval (1 second)
    except WebSocketDisconnect:
        pass
```

> **Optimization**: For production, use Redis/KeyDB pub/sub with `SUBSCRIBE` instead of polling. Have Flink publish to a dedicated channel after each write.

#### Dockerfile (`docker/api/Dockerfile`)

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ /app/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

---

### 8.2. Nginx Reverse Proxy

Nginx serves as the unified entry point, routing requests to either the React static files or the FastAPI backend.

#### Configuration (`docker/nginx/nginx.conf`)

```nginx
upstream fastapi {
    server api:8000;
}

server {
    listen 80;
    server_name _;

    # React SPA static files
    root /usr/share/nginx/html;
    index index.html;

    # API requests → FastAPI
    location /api/ {
        proxy_pass http://fastapi;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # WebSocket upgrade for /api/ws/
    location /api/ws/ {
        proxy_pass http://fastapi;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;
    }

    # React SPA fallback (client-side routing)
    location / {
        try_files $uri $uri/ /index.html;
    }

    # Cache static assets
    location /static/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    # Gzip
    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml;
    gzip_min_length 256;
}
```

#### Dockerfile (`docker/nginx/Dockerfile`)

```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY Crypto-Dashboard/package.json Crypto-Dashboard/package-lock.json* ./
RUN npm ci
COPY Crypto-Dashboard/ .
ENV REACT_APP_API_BASE_URL=/api
RUN npm run build

FROM nginx:1.25-alpine
COPY docker/nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
```

> **Note**: This is a multi-stage build. The first stage builds the React app, the second stage serves it via Nginx. The `REACT_APP_API_BASE_URL=/api` env var ensures the React app uses relative paths, which Nginx proxies to FastAPI.

---

### 8.3. React Dashboard Adjustments

The React dashboard is intentionally designed with a clean abstraction layer (`marketDataService.js`). Only a few files need modification to connect to the live backend.

#### 8.3.1. `marketDataService.js` — Switch to Live API

The main change is switching `DATA_SOURCE` from `"mock"` to `"api"` and updating the API functions:

```javascript
// Change this:
const DATA_SOURCE = "mock";
// To this:
const DATA_SOURCE = "api";

// API_BASE_URL will work as-is when served through Nginx:
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "/api";
```

The `fetchCandles` function in API mode should call:

```javascript
async function fetchCandlesFromAPI(symbol, timeframe, limit) {
  const res = await fetch(
    `${API_BASE_URL}/klines?symbol=${symbol}&interval=${timeframe}&limit=${limit}`,
  );
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json(); // Expects [{time, open, high, low, close, volume}, ...]
}
```

The `subscribeCandle` function should connect to the WebSocket:

```javascript
function subscribeCandleFromAPI(symbol, timeframe, onCandle) {
  const wsUrl = `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/api/ws/stream?symbols=${symbol}`;
  const ws = new WebSocket(wsUrl);

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "candle" && msg.symbol === symbol) {
      onCandle(msg.data);
    }
  };

  ws.onclose = () => {
    // Auto-reconnect after 3 seconds
    setTimeout(() => subscribeCandleFromAPI(symbol, timeframe, onCandle), 3000);
  };

  return () => ws.close(); // Return unsubscribe function
}
```

#### 8.3.2. `App.js` — Dynamic Watchlist

Replace the static `watchlistItems` array with a live API call:

```javascript
// Replace static watchlistItems with:
useEffect(() => {
  fetch("/api/ticker")
    .then((res) => res.json())
    .then((tickers) => {
      setWatchlistItems(
        tickers.map((t) => ({
          symbol: t.symbol,
          price: t.price,
          change: t.price_change_pct || 0,
          color: (t.price_change_pct || 0) >= 0 ? "green" : "red",
        })),
      );
    })
    .catch(console.error);
}, []);
```

#### 8.3.3. `OrderBook.js` — Live Order Book

Replace the `generateOrderBook` mock function with an API call:

```javascript
useEffect(() => {
  const fetchOrderBook = async () => {
    const res = await fetch(`/api/orderbook/${symbol}?depth=20`);
    const data = await res.json();
    setOrderBook(data);
  };
  fetchOrderBook();
  const interval = setInterval(fetchOrderBook, 1000); // Refresh every second
  return () => clearInterval(interval);
}, [symbol]);
```

#### 8.3.4. `RecentTrades.js` — Live Trades

Replace the `generateRecentTrades` mock with an API call:

```javascript
useEffect(() => {
  const fetchTrades = async () => {
    const res = await fetch(`/api/trades/${symbol}?limit=50`);
    const data = await res.json();
    setTrades(data);
  };
  fetchTrades();
  const interval = setInterval(fetchTrades, 2000); // Refresh every 2 seconds
  return () => clearInterval(interval);
}, [symbol]);
```

#### 8.3.5. `AuthContext.js` — Backend Authentication (Optional)

Replace localStorage-based auth with JWT-based API auth:

```javascript
const login = useCallback(async (email, password) => {
  const res = await fetch("/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    const err = await res.json();
    return { success: false, error: err.detail };
  }
  const { token, user } = await res.json();
  sessionStorage.setItem("token", token);
  setUser(user);
  return { success: true };
}, []);
```

#### 8.3.6. Symbol Naming Alignment

The React app currently uses symbols like `BTCUSD`, while Binance and the backend use `BTCUSDT`. Align the symbols:

- In `MarketSelector.js`: Update `SYMBOL_META` keys from `BTCUSD` → `BTCUSDT`
- In `App.js`: Update `watchlistItems` symbols
- In `CandlestickChart.js`: Update `SYMBOLS` array
- Or handle mapping in FastAPI (accept both `BTCUSD` and `BTCUSDT`)

---

## 9. Docker Compose Integration

### 9.1. New Services to Add

Add these services to the existing `docker-compose.yml`:

```yaml
# ══════════════════════════════════════════════════════════════════════════════
# SERVING LAYER
# ══════════════════════════════════════════════════════════════════════════════

api:
  build:
    context: .
    dockerfile: docker/api/Dockerfile
  container_name: api
  hostname: api
  restart: unless-stopped
  environment:
    REDIS_HOST: keydb
    REDIS_PORT: 6379
    INFLUX_URL: http://influxdb:8086
    INFLUX_TOKEN: ${INFLUX_TOKEN}
    INFLUX_ORG: vi
    INFLUX_BUCKET: crypto
    TRINO_HOST: trino
    TRINO_PORT: 8080
  networks:
    - crypto-net
  depends_on:
    keydb:
      condition: service_healthy
    influxdb:
      condition: service_healthy
    trino:
      condition: service_healthy
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8000/docs"]
    interval: 15s
    timeout: 5s
    retries: 5

nginx:
  build:
    context: .
    dockerfile: docker/nginx/Dockerfile
  container_name: nginx
  hostname: nginx
  ports:
    - "80:80"
  networks:
    - crypto-net
  depends_on:
    api:
      condition: service_healthy
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:80"]
    interval: 15s
    timeout: 5s
    retries: 5
```

### 9.2. Updated Service Map

After adding the serving layer, the full stack consists of **16 services**:

```
┌────────────────────────────────────────────────────────────────────┐
│                    docker-compose.yml (cryptoprice)                │
├──────────────┬────────────────────────────────────────────────────┤
│  DATA LAYER  │  kafka, minio, minio-init, influxdb,              │
│              │  postgres, keydb                                   │
├──────────────┼────────────────────────────────────────────────────┤
│  COMPUTE     │  flink-jobmanager, flink-taskmanager,             │
│              │  spark-master, spark-worker                        │
├──────────────┼────────────────────────────────────────────────────┤
│  QUERY/ORCH  │  trino, dagster-webserver, dagster-daemon         │
├──────────────┼────────────────────────────────────────────────────┤
│  PRODUCER    │  producer, influx-backfill                         │
├──────────────┼────────────────────────────────────────────────────┤
│  SERVING     │  api (FastAPI), nginx (React + Reverse Proxy)     │
│  (NEW)       │                                                    │
└──────────────┴────────────────────────────────────────────────────┘
```

### 9.3. Port Allocation

| Service         | Internal Port | External Port  | Purpose                 |
| --------------- | :-----------: | :------------: | ----------------------- |
| Kafka           |     9092      |      9092      | Broker (debugging)      |
| MinIO           |  9000, 9001   |   9000, 9001   | S3 API, Console         |
| InfluxDB        |     8086      |      8086      | Time-series UI          |
| PostgreSQL      |     5432      |      5432      | Database access         |
| KeyDB           |     6379      |      6379      | Redis CLI debugging     |
| Flink UI        |     8081      |      8081      | Flink Dashboard         |
| Spark Master UI |     8080      |      8082      | Spark Dashboard         |
| Spark History   |     18080     |     18080      | Job History             |
| Trino UI        |     8080      |      8083      | Trino Dashboard         |
| Dagster UI      |     3000      |      3000      | Orchestration Dashboard |
| **FastAPI**     |   **8000**    | **(internal)** | **API (via Nginx)**     |
| **Nginx**       |    **80**     |     **80**     | **Web UI + API Proxy**  |

> **Note**: FastAPI's port 8000 is **not exposed externally**. All traffic goes through Nginx on port 80. This simplifies CORS, provides a single entry point, and enables WebSocket proxying.

### 9.4. Startup Order

```bash
# Phase 1: Data layer
docker compose up -d kafka minio minio-init influxdb postgres keydb

# Phase 2: Compute layer (wait for healthy data services)
docker compose up -d flink-jobmanager flink-taskmanager spark-master spark-worker

# Phase 3: Query + Orchestration
docker compose up -d trino dagster-webserver dagster-daemon

# Phase 4: Producer + Backfill
docker compose up -d producer influx-backfill

# Phase 5: Submit Flink job
docker exec flink-jobmanager flink run -py /app/src/ingest_flink_crypto.py -d

# Phase 6: Serving layer
docker compose up -d api nginx

# Or simply:
docker compose up -d
# (Docker Compose resolves dependency order via depends_on + healthchecks)
```

### 9.5. Environment Variables (`.env` file)

```env
# Required — InfluxDB admin token (generate a unique one)
INFLUX_TOKEN=my-super-secret-influx-token

# Optional — override defaults if needed
# KAFKA_BOOTSTRAP=kafka:9092
# REDIS_HOST=keydb
# REDIS_PORT=6379
# MINIO_ENDPOINT=http://minio:9000
# MINIO_ACCESS_KEY=minioadmin
# MINIO_SECRET_KEY=minioadmin
```

### 9.6. Resource Requirements

| Service        | Memory (Config) | Memory (Actual) |       CPU        |
| -------------- | :-------------: | :-------------: | :--------------: |
| Kafka          |      ~1GB       |     ~800MB      |      1 core      |
| MinIO          |     ~512MB      |     ~300MB      |     0.5 core     |
| InfluxDB       |      ~1GB       |     ~600MB      |      1 core      |
| PostgreSQL     |     ~256MB      |     ~100MB      |     0.5 core     |
| KeyDB          |     ~256MB      |     ~100MB      |     0.5 core     |
| Flink JM       |      1.6GB      |     ~1.2GB      |      1 core      |
| Flink TM       |      1.7GB      |     ~1.4GB      |     2 slots      |
| Spark Master   |     ~512MB      |     ~300MB      |     0.5 core     |
| Spark Worker   |       3GB       |      ~2GB       |     2 cores      |
| Trino          |       2GB       |     ~1.5GB      |      1 core      |
| Dagster WS     |     ~512MB      |     ~300MB      |     0.5 core     |
| Dagster Daemon |     ~512MB      |     ~300MB      |     0.5 core     |
| Producer       |     ~128MB      |      ~80MB      |     0.5 core     |
| **FastAPI**    |   **~256MB**    |   **~150MB**    |   **0.5 core**   |
| **Nginx**      |    **~64MB**    |    **~30MB**    |  **0.25 core**   |
| **Total**      |   **~12.3GB**   |   **~9.2GB**    | **~11.25 cores** |

**Minimum System Requirements**: 16GB RAM, 6+ CPU cores, 50GB+ disk

---

## 10. AWS Cloud Deployment Considerations

### 10.1. Service Mapping to AWS

| Current (Docker) | AWS Equivalent                                             | Notes                                                                       |
| ---------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------- |
| Kafka            | **Amazon MSK** (Managed Kafka)                             | Serverless option available; handles replication, patching                  |
| MinIO            | **Amazon S3**                                              | Direct replacement; update `s3a://` endpoint. Iceberg-native S3 support     |
| InfluxDB         | **Amazon Timestream** or **self-hosted on EC2/ECS**        | Timestream has different query syntax; InfluxDB on EC2 is simpler migration |
| PostgreSQL       | **Amazon RDS for PostgreSQL**                              | Managed; handles backups, failover. Same schema                             |
| KeyDB            | **Amazon ElastiCache for Redis**                           | Drop-in Redis replacement; KeyDB is Redis-compatible                        |
| Flink            | **Amazon Managed Flink** (formerly Kinesis Data Analytics) | Upload PyFlink job as application                                           |
| Spark            | **Amazon EMR** or **EMR Serverless**                       | Submit Spark jobs; EMR Serverless for batch is cost-effective               |
| Trino            | **Amazon Athena** (Trino-based)                            | Athena uses Trino engine; query Iceberg on S3 natively                      |
| Dagster          | **Dagster Cloud** or **self-hosted on ECS**                | Dagster Cloud is turnkey; self-hosted on ECS Fargate works too              |
| FastAPI          | **ECS Fargate** or **Lambda + API Gateway**                | Fargate for persistent WebSocket; Lambda for REST-only                      |
| Nginx + React    | **CloudFront + S3** (static hosting)                       | Upload React build to S3, serve via CloudFront CDN                          |

### 10.2. Recommended Architecture on AWS

```
                    ┌──────────────────────────┐
                    │      Route 53 (DNS)       │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │     CloudFront (CDN)      │
                    │  Static: S3 (React build) │
                    │  API: ALB origin          │
                    └────────────┬─────────────┘
                                 │ /api/*
                    ┌────────────▼─────────────┐
                    │   ALB (Application LB)    │
                    │   HTTP + WebSocket         │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   ECS Fargate (FastAPI)    │
                    │   Auto-scaling, 2+ tasks   │
                    └────────────┬─────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
    ┌─────────────────┐ ┌───────────────┐ ┌────────────────┐
    │ ElastiCache     │ │ Timestream /  │ │ Athena (Trino) │
    │ (KeyDB/Redis)   │ │ InfluxDB EC2  │ │ → S3 Iceberg   │
    └─────────────────┘ └───────────────┘ └────────────────┘

    ┌─────────────────┐ ┌───────────────┐
    │ MSK (Kafka)     │ │ EMR Serverless│
    │ → Flink on MF   │ │ (Spark batch) │
    └─────────────────┘ └───────────────┘

    ┌─────────────────┐ ┌───────────────┐
    │ RDS PostgreSQL  │ │ S3 (Iceberg   │
    │ (catalog)       │ │  warehouse)   │
    └─────────────────┘ └───────────────┘
```

### 10.3. Migration Steps (High-Level)

1. **S3 Migration**: Replace MinIO endpoints with S3 bucket URL. Update Iceberg catalog `warehouse` property. Remove `s3.path-style-access` (not needed for real S3).

2. **Kafka → MSK**: Create MSK cluster with same topic configuration (3 partitions, 7-day retention). Update `KAFKA_BOOTSTRAP` env var to MSK bootstrap servers. Enable IAM auth or mTLS.

3. **PostgreSQL → RDS**: Create RDS PostgreSQL 16 instance. Run `init.sql` to create databases. Update connection strings in Dagster/Spark/Trino configs.

4. **KeyDB → ElastiCache**: Create ElastiCache Redis cluster (Redis 7+). Update `REDIS_HOST` to ElastiCache endpoint. Enable encryption in transit + at rest.

5. **Spark → EMR Serverless**: Package Spark jobs as EMR applications. Update Dagster assets to trigger EMR jobs instead of `spark-submit`.

6. **Flink → Managed Flink**: Package PyFlink application as a zip. Upload to Managed Flink with Kafka source configuration.

7. **React → CloudFront + S3**: Run `npm run build` → upload `build/` to S3 bucket. Create CloudFront distribution with S3 origin + ALB origin for `/api/*`.

8. **FastAPI → ECS Fargate**: Push Docker image to ECR. Create ECS service with Fargate launch type. Configure ALB target group with health check on `/docs`.

### 10.4. Cost Optimization Tips

- **MSK Serverless** — pay per throughput, not cluster hours
- **EMR Serverless** — pay only when Spark jobs run (ideal for daily batch)
- **Athena** — pay per query ($5/TB scanned); Iceberg pruning minimizes scanned data
- **ElastiCache** — use `cache.t3.small` (~$25/mo) for development
- **Fargate Spot** — up to 70% discount for fault-tolerant API tasks
- **S3 Intelligent-Tiering** — auto-optimizes storage costs for Iceberg data
- **Reserved Instances** — commit to RDS/ElastiCache for 30-40% savings in production

### 10.5. Networking & Security on AWS

- **VPC**: All services in private subnets. Only ALB and CloudFront in public subnets.
- **Security Groups**: Restrict inter-service traffic (e.g., FastAPI → ElastiCache on port 6379 only).
- **IAM Roles**: Each ECS task gets a task role with least-privilege S3/MSK/ElastiCache access.
- **Secrets Manager**: Store `INFLUX_TOKEN`, database passwords, API keys. Inject as env vars via ECS task definitions.
- **ACM + HTTPS**: Provision TLS certificate via ACM. Terminate HTTPS at ALB/CloudFront.
- **WAF**: Attach AWS WAF to CloudFront/ALB for DDoS protection and rate limiting.

---

## 11. Appendix

### 11.1. Service Access Credentials (Local Docker)

| Service            | URL                                | Credentials                                                  |
| ------------------ | ---------------------------------- | ------------------------------------------------------------ |
| Kafka              | `localhost:9092`                   | No auth                                                      |
| MinIO Console      | `http://localhost:9001`            | `minioadmin` / `minioadmin`                                  |
| MinIO API          | `http://localhost:9000`            | `minioadmin` / `minioadmin`                                  |
| InfluxDB           | `http://localhost:8086`            | `admin` / `adminpass123` (org: `vi`, bucket: `crypto`)       |
| PostgreSQL         | `localhost:5432`                   | `iceberg` / `iceberg123` (DBs: `iceberg_catalog`, `dagster`) |
| KeyDB              | `localhost:6379`                   | No auth                                                      |
| Flink UI           | `http://localhost:8081`            | No auth                                                      |
| Spark Master UI    | `http://localhost:8082`            | No auth                                                      |
| Spark History      | `http://localhost:18080`           | No auth                                                      |
| Trino UI           | `http://localhost:8083`            | No auth                                                      |
| Dagster UI         | `http://localhost:3000`            | No auth                                                      |
| **Web UI (Nginx)** | **`http://localhost:80`**          | **No auth**                                                  |
| **FastAPI Docs**   | **`http://localhost:80/api/docs`** | **No auth**                                                  |

### 11.2. Useful Diagnostic Commands

```bash
# Check Kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check Kafka consumer lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe

# Check KeyDB data
docker exec keydb keydb-cli KEYS "ticker:latest:*"
docker exec keydb keydb-cli HGETALL "ticker:latest:BTCUSDT"
docker exec keydb keydb-cli HGETALL "orderbook:BTCUSDT"
docker exec keydb keydb-cli HGETALL "candle:latest:BTCUSDT"
docker exec keydb keydb-cli HGETALL "indicator:latest:BTCUSDT"

# Query InfluxDB
docker exec influxdb influx query 'from(bucket:"crypto") |> range(start:-5m) |> filter(fn:(r) => r._measurement == "market_ticks" and r.symbol == "BTCUSDT") |> last()' --org vi --token $INFLUX_TOKEN

# Query Trino
docker exec -it trino trino --execute "SELECT COUNT(*) FROM iceberg_catalog.crypto_lakehouse.coin_klines"
docker exec -it trino trino --execute "SHOW TABLES FROM iceberg_catalog.crypto_lakehouse"

# Check Flink jobs
curl http://localhost:8081/jobs/overview

# View Spark events
docker exec spark-master ls -la /opt/spark-events/

# FastAPI docs (after serving layer is built)
curl http://localhost/api/docs
```

### 11.3. Iceberg Table Schemas (Spark SQL)

```sql
-- coin_ticker (partitioned by days(event_timestamp))
CREATE TABLE IF NOT EXISTS crypto_lakehouse.coin_ticker (
    event_timestamp TIMESTAMP,
    symbol          STRING,
    close_price     DOUBLE,
    bid_price       DOUBLE,
    ask_price       DOUBLE,
    open_24h        DOUBLE,
    high_24h        DOUBLE,
    low_24h         DOUBLE,
    volume_24h      DOUBLE,
    quote_volume_24h DOUBLE,
    price_change_pct DOUBLE,
    trade_count     LONG
) USING iceberg
PARTITIONED BY (days(event_timestamp));

-- coin_trades (partitioned by days(trade_timestamp))
CREATE TABLE IF NOT EXISTS crypto_lakehouse.coin_trades (
    trade_timestamp TIMESTAMP,
    symbol          STRING,
    agg_trade_id    LONG,
    price           DOUBLE,
    quantity        DOUBLE,
    is_buyer_maker  BOOLEAN
) USING iceberg
PARTITIONED BY (days(trade_timestamp));

-- coin_klines (partitioned by days(kline_timestamp))
CREATE TABLE IF NOT EXISTS crypto_lakehouse.coin_klines (
    kline_timestamp TIMESTAMP,
    symbol          STRING,
    interval        STRING,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    quote_volume    DOUBLE,
    trade_count     LONG,
    is_closed       BOOLEAN
) USING iceberg
PARTITIONED BY (days(kline_timestamp));

-- coin_klines_hourly (partitioned by days(kline_timestamp))
CREATE TABLE IF NOT EXISTS crypto_lakehouse.coin_klines_hourly (
    kline_timestamp TIMESTAMP,
    symbol          STRING,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    quote_volume    DOUBLE,
    trade_count     LONG
) USING iceberg
PARTITIONED BY (days(kline_timestamp));

-- historical_hourly (partitioned by symbol + years(event_time))
CREATE TABLE IF NOT EXISTS crypto_lakehouse.historical_hourly (
    open_time       LONG,
    symbol          STRING,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          DOUBLE,
    quote_volume    DOUBLE,
    trade_count     LONG,
    taker_buy_volume DOUBLE,
    taker_buy_quote DOUBLE,
    event_time      TIMESTAMP
) USING iceberg
PARTITIONED BY (symbol, years(event_time));
```

### 11.4. Data Retention Summary

| Data Type            | Store                        | Retention Policy                          |
| -------------------- | ---------------------------- | ----------------------------------------- |
| 1m candles           | InfluxDB                     | 7 days (then aggregated to 1h)            |
| 1h candles           | InfluxDB                     | Indefinite                                |
| 1m candles           | Iceberg `coin_klines`        | Purged after 1h aggregation               |
| 1h candles           | Iceberg `coin_klines_hourly` | Permanent                                 |
| Historical 1h        | Iceberg `historical_hourly`  | Permanent (from 2017)                     |
| Ticker               | Iceberg `coin_ticker`        | Permanent                                 |
| Trades               | Iceberg `coin_trades`        | Permanent                                 |
| Order Book           | KeyDB only                   | 60s TTL                                   |
| Indicators           | KeyDB + InfluxDB             | Latest in KeyDB, time-series in InfluxDB  |
| Kafka topics         | Kafka                        | 7 days (`KAFKA_LOG_RETENTION_HOURS: 168`) |
| Iceberg snapshots    | MinIO/S3                     | 48h (then expired)                        |
| Iceberg orphan files | MinIO/S3                     | 72h (then cleaned)                        |

### 11.5. Dagster Schedule Summary

| Asset                       | Schedule                      | Spark Job                                                      | Purpose                                     |
| --------------------------- | ----------------------------- | -------------------------------------------------------------- | ------------------------------------------- |
| `backfill_historical`       | `0 2 * * *` (daily 2 AM)      | `backfill_historical.py --mode all --iceberg-mode incremental` | Fill InfluxDB gaps + Iceberg incremental    |
| `aggregate_candles`         | `0 4 * * *` (daily 4 AM)      | `aggregate_candles.py --mode all`                              | 1m → 1h aggregation + retention             |
| `iceberg_table_maintenance` | `0 3 * * 0` (weekly Sun 3 AM) | `iceberg_maintenance.py`                                       | Compaction, snapshot expiry, orphan cleanup |

### 11.6. Frontend Feature Matrix

| Feature              | Component               | Data Source                           | Status                          |
| -------------------- | ----------------------- | ------------------------------------- | ------------------------------- |
| Candlestick Chart    | `CandlestickChart.js`   | `marketDataService.fetchCandles()`    | Ready (mock → API toggle)       |
| Live Price Updates   | `useCandlestickData.js` | `marketDataService.subscribeCandle()` | Ready (mock → WebSocket toggle) |
| Order Book           | `OrderBook.js`          | Mock `generateOrderBook()`            | Needs API integration           |
| Recent Trades        | `RecentTrades.js`       | Mock `generateRecentTrades()`         | Needs API integration           |
| Market Overview      | `OverviewChart.js`      | Derived from candle data              | Works with API data             |
| Technical Indicators | `CandlestickChart.js`   | Client-side calculation               | Works (SMA/EMA/RSI/MFI)         |
| Drawing Tools        | `ChartOverlay.js`       | Client-side state                     | No backend needed               |
| Symbol Selector      | `MarketSelector.js`     | Hardcoded `SYMBOL_META`               | Needs API + symbol alignment    |
| Watchlist            | `App.js`                | Hardcoded `watchlistItems`            | Needs API integration           |
| Auth                 | `AuthContext.js`        | localStorage (insecure)               | Needs FastAPI JWT backend       |
| i18n                 | `i18n/`                 | Static translations (EN/VI)           | No changes needed               |

---

_This documentation was generated to guide the integration of the serving layer into the existing Lambda Architecture platform. The backend data pipeline is fully implemented and operational. The primary remaining work is building the FastAPI bridge and connecting the React dashboard to live data sources._
