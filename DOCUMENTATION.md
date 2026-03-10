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
8. [Serving Layer](#8-serving-layer)
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
│  │  Auto-reconnect      │           │  │ (3 partitions each, 2-day retention)    │ │     │
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
│   │              SERVING LAYER                     │                                    │
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
| **Serving**       | FastAPI + Nginx + React                                                            | REST/WebSocket API + web UI                         |

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

### Serving Layer

| Technology  | Version | Role                              | Port(s)             |
| ----------- | ------- | --------------------------------- | ------------------- |
| **FastAPI** | 0.115+  | REST + WebSocket API server       | 8000 (internal)     |
| **Nginx**   | 1.25    | Reverse proxy, static file server | 80                  |
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
├── docker-compose.yml              # 16 services across 5 layers
├── spark-defaults.conf             # Spark driver/executor config + Iceberg extensions
├── README.md                       # Vietnamese documentation
├── DOCUMENTATION.md                # This file — full technical documentation
│
├── docker/                         # Dockerfiles & service configs
│   ├── backfill/
│   │   └── Dockerfile              # Python 3.11-slim for InfluxDB gap-filling
│   ├── dagster/
│   │   ├── Dockerfile              # Spark 3.5.5 base + Dagster 1.8
│   │   └── dagster.yaml            # PostgreSQL storage, daemon scheduler
│   ├── fastapi/
│   │   ├── Dockerfile              # Python 3.11-slim + FastAPI + uvicorn
│   │   └── requirements.txt        # fastapi, redis, influxdb-client, trino
│   ├── flink/
│   │   ├── Dockerfile              # Flink 1.18.1 + Python + Kafka connector
│   │   └── flink-conf.yaml         # Memory (1.6G JM / 1.7G TM), checkpointing
│   ├── nginx/
│   │   ├── Dockerfile              # Multi-stage: Node build React → Nginx alpine
│   │   └── nginx.conf              # Reverse proxy /api/ → FastAPI, SPA fallback
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
├── schemas/                        # Avro schemas for Kafka topics
│   ├── depth.avsc
│   ├── kline.avsc
│   ├── ticker.avsc
│   └── trade.avsc
│
├── serving/                        # FastAPI backend
│   ├── __init__.py
│   ├── main.py                     # App entry, lifespan, CORS, health check
│   ├── config.py                   # Environment-driven settings
│   ├── connections.py              # Lazy singletons: Redis, InfluxDB, Trino
│   └── routers/
│       ├── __init__.py
│       ├── klines.py               # OHLCV candle queries (InfluxDB + KeyDB)
│       ├── historical.py           # Cold-storage candles (Trino → Iceberg)
│       ├── ticker.py               # Live price from KeyDB
│       ├── orderbook.py            # Order book depth from KeyDB
│       ├── trades.py               # Recent price ticks from KeyDB
│       ├── symbols.py              # Active symbol list from KeyDB
│       ├── indicators.py           # SMA/EMA indicators from KeyDB
│       └── ws.py                   # WebSocket real-time candle streaming
│
├── src/                            # Python source code (pipeline)
│   ├── producer_binance.py         # Binance WebSocket → 4 Kafka topics
│   ├── ingest_flink_crypto.py      # Flink: Kafka → KeyDB + InfluxDB + Iceberg
│   ├── ingest_crypto.py            # (Legacy) Spark Kafka → Iceberg direct
│   ├── backfill_historical.py      # Unified backfill: InfluxDB gaps + Iceberg incremental
│   ├── backfill_influx.py          # (Legacy) InfluxDB-only gap detection
│   ├── aggregate_candles.py        # 1m → 1h candle aggregation + retention
│   ├── iceberg_maintenance.py      # Table compaction, snapshot expiry, orphan cleanup
│   └── ingest_historical_iceberg.py # Standalone Binance → Iceberg historical loader
│
└── frontend/                       # React SPA (Crypto Dashboard)
    ├── package.json                # React 18.3.1 + lightweight-charts + tailwind
    ├── tailwind.config.js          # Dark theme, TradingView-style colors
    ├── public/
    │   └── index.html              # SPA entry point
    └── src/
        ├── App.js                  # Main layout: header + chart + watchlist
        ├── index.js                # Root: ErrorBoundary + I18nProvider
        ├── components/
        │   ├── CandlestickChart.js # TradingView chart with indicators + tabs
        │   ├── ChartOverlay.js     # Drawing tools: trendline, fib, Elliott wave
        │   ├── DateRangePicker.js  # Historical date range selector
        │   ├── DrawingToolbar.js   # Tool selection sidebar
        │   ├── ErrorBoundary.js    # Global error catch with retry
        │   ├── Header.js           # Top bar + navigation drawer
        │   ├── LanguageSwitcher.js # EN/VI language toggle
        │   ├── MarketSelector.js   # Symbol dropdown with search & star
        │   ├── OrderBook.js        # Bid/ask depth visualization
        │   ├── OverviewChart.js    # OHLCV summary with sparkline
        │   ├── RecentTrades.js     # Recent trade feed table
        │   ├── ToolSettingsPopup.js# Drawing tool configuration
        │   ├── Watchlist.js        # Right sidebar: symbols + prices + stars
        │   └── chart/
        │       ├── chartConstants.js   # Theme, timeframes, tabs, indicator defaults
        │       ├── indicatorUtils.js   # calcSMA, calcEMA, calcRSI, calcMFI
        │       ├── IndicatorPanel.js   # Indicator settings panel
        │       ├── OscillatorPane.js   # RSI/MFI sub-chart
        │       └── OHLCVBar.js         # Crosshair OHLCV tooltip
        ├── hooks/
        │   └── useCandlestickData.js   # Data hook with live WebSocket subscription
        ├── services/
        │   └── marketDataService.js    # API abstraction: mock ↔ live toggle
        ├── utils/
        │   └── storageHelpers.js       # localStorage JSON helpers
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

### 6.4. Serving Path

```
React Frontend (Nginx :80)
   │
   ├── GET /api/ticker, /api/orderbook, /api/symbols
   │       └──▶ Nginx ──▶ FastAPI ──▶ KeyDB       (live prices, depth, symbol list)
   │
   ├── GET /api/klines
   │       └──▶ Nginx ──▶ FastAPI ──▶ InfluxDB     (OHLCV candles, server-side agg)
   │                                  └── KeyDB    (fallback for 1s/1m sorted sets)
   │
   ├── GET /api/klines/historical
   │       └──▶ Nginx ──▶ FastAPI ──▶ Trino        (Iceberg cold storage queries)
   │
   └── WS  /api/stream
           └──▶ Nginx ──▶ FastAPI ──▶ KeyDB        (real-time candle push, 500ms)
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

The core speed layer, consuming 4 Kafka topics (ticker, trades, klines, depth) and writing to KeyDB + InfluxDB in parallel via PyFlink `FlatMapFunction` sinks.

**Cluster Configuration** (optimized for t3a.2xlarge):

| Setting                             | Value   | Purpose                                            |
| ----------------------------------- | ------- | -------------------------------------------------- |
| `taskmanager.memory.process.size`   | 4096m   | JVM + off-heap for 12 tasks                        |
| `taskmanager.numberOfTaskSlots`     | 4       | Allows 12 tasks on 3 parallel pipelines            |
| `parallelism.default`              | 3       | Matches 3 Kafka partitions per topic               |
| `execution.checkpointing.interval` | 120s    | Balanced checkpoint frequency                      |

**Processing Pipeline**:

1. Read from Kafka topic via `FlinkKafkaConsumer` (JSON, 3 partitions)
2. Parse JSON in `FlatMapFunction.flat_map()`
3. Batch-buffer records (50–100 per flush, 0.3–0.5s interval)
4. Flush via Redis `pipeline()` to minimize round trips

**KeyDB Write Strategy** (all writers use batch-buffered pipelines):

```python
# Ticker (KeyDBWriter): batch 100 records, flush 0.5s
pipe.hset(f"ticker:latest:{symbol}", mapping={...})
pipe.zadd(f"ticker:history:{symbol}", {f"{price}:{vol}": ts})
# ZREMRANGEBYSCORE cleanup every 60 writes per symbol

# Candles (KeyDBKlineWriter): batch 50 records, flush 0.5s
pipe.hset(f"candle:latest:{symbol}", mapping={...})
pipe.zadd(f"candle:history:{symbol}", {json.dumps(candle): kline_start})

# Order Book (DepthWriter): batch 50 records, flush 0.3s
pipe.hset(f"orderbook:{symbol}", mapping={bids, asks, spread...})
pipe.expire(f"orderbook:{symbol}", 60)
```

**InfluxDB Write Strategy**: Batched via `write_api(SYNCHRONOUS)` — one write per record with large InfluxDB server-side batching.

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

### 7.4. React Dashboard (`frontend/`)

**Component Hierarchy**:

```
<ErrorBoundary>
  <I18nProvider>
    <TradingDashboard>
      ├── Header                   (logo, search, language switcher, nav drawer)
      ├── Connection Error Banner  (red bar with Retry when API unreachable)
      ├── DrawingToolbar           (left sidebar)
      ├── CandlestickChart         (center, tabbed)
      │   ├── MarketSelector       (symbol dropdown with search & star)
      │   ├── DateRangePicker      (historical mode trigger)
      │   ├── Timeframe buttons    (1s, 1m, 5m, 15m, 1H, 4H, 1D, 1W)
      │   ├── Tab: Candlestick → lightweight-charts + SMA/EMA/RSI/MFI
      │   │   ├── OHLCVBar         (crosshair tooltip)
      │   │   ├── OscillatorPane   (RSI/MFI sub-chart)
      │   │   ├── IndicatorPanel   (settings for all overlays/oscillators)
      │   │   └── ChartOverlay     (drawing tools layer)
      │   ├── Tab: Overview → OverviewChart (recharts sparkline)
      │   ├── Tab: Order Book → OrderBook
      │   └── Tab: Recent Trades → RecentTrades
      └── Watchlist                (right sidebar with price/change/star)
    </TradingDashboard>
  </I18nProvider>
</ErrorBoundary>
```

**Key Abstraction — `marketDataService.js`**:

```javascript
// Compile-time toggle: "api" for production, "mock" for frontend-only development
const DATA_SOURCE = process.env.REACT_APP_DATA_SOURCE || "api";
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "/api";
```

This service is the **single point of integration** between the React frontend and the backend. It exports:

- `fetchCandles(symbol, timeframe, limit)` → OHLCV array (InfluxDB via FastAPI)
- `fetchHistoricalCandles(symbol, startMs, endMs)` → cold-storage candles (Trino via FastAPI)
- `subscribeCandle(symbol, timeframe, onCandle)` → real-time WebSocket subscription
- `fetchSymbols()` → available trading pairs
- `fetchTickers()` → all live tickers for watchlist
- `fetchOrderBook(symbol)` → order book depth
- `fetchTrades(symbol, limit)` → recent price ticks

---

## 8. Serving Layer

The serving layer is the user-facing tier of the system, bridging the data pipeline to the browser. It consists of three components: a **FastAPI** REST/WebSocket backend, an **Nginx** reverse proxy, and a **React** single-page application.

```
Browser ──▶ Nginx:80
              ├── /api/*        ──▶ FastAPI:8000 ──▶ KeyDB / InfluxDB / Trino
              ├── /api/stream   ──▶ FastAPI:8000    (WebSocket, candle streaming)
              └── /*            ──▶ React SPA        (static build served by Nginx)
```

### 8.1. FastAPI Backend (`serving/`)

The API server connects to all three storage engines and exposes 8 routers under the `/api` prefix.

#### Directory Structure

```
serving/
├── __init__.py
├── main.py                    # FastAPI app, lifespan, CORS, health check
├── config.py                  # Environment-driven configuration
├── connections.py             # Lazy singletons: Redis, InfluxDB, Trino
└── routers/
    ├── __init__.py
    ├── klines.py              # GET /api/klines — OHLCV candles (InfluxDB + KeyDB)
    ├── historical.py          # GET /api/klines/historical — cold storage (Trino)
    ├── ticker.py              # GET /api/ticker[/{symbol}] — live prices (KeyDB)
    ├── orderbook.py           # GET /api/orderbook/{symbol} — depth (KeyDB)
    ├── trades.py              # GET /api/trades/{symbol} — recent ticks (KeyDB)
    ├── symbols.py             # GET /api/symbols — active trading pairs (KeyDB)
    ├── indicators.py          # GET /api/indicators/{symbol} — SMA/EMA (KeyDB)
    └── ws.py                  # WS /api/stream — real-time candle push (KeyDB)
```

#### Dependencies (`docker/fastapi/requirements.txt`)

```
fastapi
uvicorn[standard]
redis
influxdb-client
trino>=0.330.0
```

#### Application Entry Point (`serving/main.py`)

```python
app = FastAPI(title="CryptoDashboard API", version="1.0.0", lifespan=lifespan)

app.add_middleware(CORSMiddleware,
    allow_origins=CORS_ORIGINS, allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"])

# 8 routers registered under /api
for router in (ticker, klines, orderbook, trades,
               symbols, indicators, ws, historical):
    app.include_router(router)
```

The `/api/health` endpoint pings KeyDB, InfluxDB, and Trino, returning `"ok"` or `"degraded"` with per-service diagnostics.

#### Configuration (`serving/config.py`)

All settings are driven by environment variables with sensible defaults for Docker:

| Variable        | Default                | Purpose                    |
| --------------- | ---------------------- | -------------------------- |
| `REDIS_HOST`    | `keydb`                | KeyDB hostname             |
| `REDIS_PORT`    | `6379`                 | KeyDB port                 |
| `INFLUX_URL`    | `http://influxdb:8086` | InfluxDB endpoint          |
| `INFLUX_TOKEN`  | _(empty)_              | InfluxDB auth token        |
| `INFLUX_ORG`    | `vi`                   | InfluxDB organization      |
| `INFLUX_BUCKET` | `crypto`               | InfluxDB bucket            |
| `TRINO_HOST`    | `trino`                | Trino coordinator hostname |
| `TRINO_PORT`    | `8080`                 | Trino coordinator port     |
| `CORS_ORIGINS`  | `*`                    | Comma-separated CORS list  |

#### Connection Management (`serving/connections.py`)

Lazy singletons for each backend — created on first use, closed on shutdown:

- **KeyDB**: `redis.asyncio.Redis` with `decode_responses=True` and `socket_keepalive`.
- **InfluxDB**: `InfluxDBClient` (synchronous, wrapped in `asyncio.to_thread` at the call site).
- **Trino**: `trino.dbapi.connect()` with catalog `iceberg`, schema `crypto_lakehouse`, user `fastapi`.

The `close_all()` coroutine is called from the FastAPI `lifespan` shutdown hook.

#### API Endpoints

##### `GET /api/klines` — OHLCV Candles

| Parameter  | Type   | Default | Constraint    | Description                              |
| ---------- | ------ | ------- | ------------- | ---------------------------------------- |
| `symbol`   | string | _(req)_ | `^[A-Z0-9]+$` | Trading pair, e.g. `BTCUSDT`             |
| `interval` | string | `1m`    | see below     | `1s`,`1m`,`5m`,`15m`,`1h`,`4h`,`1d`,`1w` |
| `limit`    | int    | 200     | 1–1500        | Number of candles to return              |

**Data routing strategy**:

| Requested interval     | Data source                                                      |
| ---------------------- | ---------------------------------------------------------------- |
| `1s`                   | KeyDB sorted set `candle:1s:{symbol}`                            |
| `1m`                   | InfluxDB measurement `candles` (interval=1m)                     |
| `5m`, `15m`            | InfluxDB 1m candles → server-side OHLCV aggregation              |
| `1h`, `4h`, `1d`, `1w` | InfluxDB 1h candles → aggregation; 1m fallback if 1h unavailable |

When InfluxDB returns no data, the endpoint falls back to KeyDB sorted sets (`candle:1m:{symbol}`, then `candle:1s:{symbol}`).

Aggregation is performed server-side using a bucket-based OHLCV re-sampler: group candles by `floor(openTime / target_ms)`, take first open / max high / min low / last close / sum volume.

**Response shape**:

```json
[{"openTime": 1710000000000, "open": 67480.0, "high": 67520.0, "low": 67470.0, "close": 67500.5, "volume": 12.345}, ...]
```

##### `GET /api/klines/historical` — Cold-Storage Candles (Trino → Iceberg)

| Parameter   | Type | Default | Constraint        | Description  |
| ----------- | ---- | ------- | ----------------- | ------------ |
| `symbol`    | str  | _(req)_ | `^[A-Z0-9]+$`     | Trading pair |
| `startTime` | int  | _(req)_ | epoch ms          | Range start  |
| `endTime`   | int  | _(req)_ | epoch ms, > start | Range end    |
| `limit`     | int  | 500     | 1–5000            | Max rows     |

Queries `coin_klines_hourly` first; if empty, falls back to `historical_hourly`. Returns 1H candles. Range capped at 1 year. Uses parameterized SQL queries to prevent injection.

##### `GET /api/ticker/{symbol}` / `GET /api/ticker`

Reads `ticker:latest:{symbol}` hashes from KeyDB. The list endpoint (`/api/ticker`) scans all `ticker:latest:*` keys and returns a sorted array. Fields: `symbol`, `price`, `change24h`, `bid`, `ask`, `volume`, `event_time`.

##### `GET /api/orderbook/{symbol}`

Reads `orderbook:{symbol}` hash from KeyDB. Returns `bids`, `asks` (as `[[price, qty], ...]`), `spread`, `best_bid`, `best_ask`, `event_time`.

##### `GET /api/trades/{symbol}?limit=50`

Reads the `ticker:history:{symbol}` sorted set (reverse chronological). Derives `side` (buy/sell) by comparing successive prices. Returns `[{time, price, volume, side}]`.

##### `GET /api/symbols`

Scans `ticker:latest:*` keys in KeyDB. For each key, extracts the symbol name and derives a display name (e.g. `BTCUSDT` → `BTC / USDT`). Returns `[{symbol, name, type: "crypto"}]`.

##### `GET /api/indicators/{symbol}`

Reads `indicator:latest:{symbol}` hash from KeyDB. Returns `sma20`, `sma50`, `ema12`, `ema26`, `timestamp`.

##### `WS /api/stream?symbol=BTCUSDT&interval=1m` — Real-Time Candle Streaming

WebSocket endpoint that pushes OHLCV frames at ~500ms intervals. The server builds the current candle from KeyDB data, sending only when the value changes from the previous frame.

**Interval routing**:

| Interval | KeyDB source                                                                                       |
| -------- | -------------------------------------------------------------------------------------------------- |
| `1s`     | Latest entry from `candle:1s:{symbol}` sorted set                                                  |
| `1m`     | `candle:latest:{symbol}` hash (written by Flink)                                                   |
| `5m`+    | Aggregate entries from `candle:1m:{symbol}` within the current window; fallback to `candle:latest` |

**Frame shape**: `{"openTime": ms, "open": float, "high": float, "low": float, "close": float, "volume": float}`

#### Dockerfile (`docker/fastapi/Dockerfile`)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY docker/fastapi/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY serving/ /app/serving/
EXPOSE 8000
CMD ["uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

#### Docker Compose Service

```yaml
fastapi:
  build:
    context: .
    dockerfile: docker/fastapi/Dockerfile
  ports:
    - "8080:8000" # Also exposed directly for debugging
  environment:
    REDIS_HOST: keydb
    INFLUX_URL: http://influxdb:8086
    INFLUX_TOKEN: ${INFLUX_TOKEN}
    TRINO_HOST: trino
    TRINO_PORT: 8080
    CORS_ORIGINS: "*"
  depends_on:
    keydb: { condition: service_healthy }
    influxdb: { condition: service_healthy }
    trino: { condition: service_healthy }
  healthcheck:
    test:
      [
        "CMD",
        "python",
        "-c",
        "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')",
      ]
```

---

### 8.2. Nginx Reverse Proxy (`docker/nginx/`)

Nginx is the single entry point at port **80**. It serves the React build as static files and reverse-proxies `/api/*` requests to FastAPI.

#### Configuration Highlights (`docker/nginx/nginx.conf`)

```nginx
upstream fastapi_backend {
    server fastapi:8000;
}

server {
    listen 80;

    # API + WebSocket → FastAPI
    location /api/ {
        proxy_pass http://fastapi_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;          # 24h for WebSocket
    }

    # React SPA fallback
    location / {
        root /usr/share/nginx/html;
        index index.html;
        try_files $uri $uri/ /index.html;
    }

    # Long-lived cache for hashed static assets
    location /static/ {
        root /usr/share/nginx/html;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    gzip on;
    gzip_types text/plain text/css application/json application/javascript;
    gzip_min_length 256;
}
```

A single `location /api/` block handles both REST and WebSocket traffic. The `Upgrade` / `Connection` headers are always set; for plain HTTP requests they are simply ignored by FastAPI.

#### Dockerfile (`docker/nginx/Dockerfile`)

Multi-stage build: Node 20 builds the React app, then the output is copied into `nginx:1.25-alpine`.

```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --ignore-scripts
COPY frontend/ .
RUN npm run build

FROM nginx:1.25-alpine
COPY docker/nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
```

The React build uses `REACT_APP_API_BASE_URL=/api` (relative), so the SPA works seamlessly behind Nginx without any CORS issues.

---

### 8.3. React Dashboard (`frontend/`)

A React 18.3 SPA styled with Tailwind CSS and powered by `lightweight-charts` 5.1 for TradingView-style candlestick rendering.

#### Component Hierarchy

```
<ErrorBoundary>
  <I18nProvider>
    <TradingDashboard>
      ├── Header                   (logo, search, language switcher, nav drawer)
      ├── Connection Error Banner  (red bar when API unreachable, with Retry)
      ├── DrawingToolbar           (left sidebar: cursor, line, fib, wave tools)
      ├── CandlestickChart         (center, tabbed)
      │   ├── MarketSelector       (symbol dropdown with search & star)
      │   ├── DateRangePicker      (historical mode: start/end datetime → Trino)
      │   ├── Timeframe buttons    (1s, 1m, 5m, 15m, 1H, 4H, 1D, 1W)
      │   ├── Tab: Candlestick     (lightweight-charts + SMA/EMA overlays)
      │   │   ├── OHLCVBar         (crosshair tooltip: O/H/L/C/V)
      │   │   ├── OscillatorPane   (RSI / MFI sub-chart)
      │   │   ├── IndicatorPanel   (settings for SMA/EMA/RSI/MFI/Volume)
      │   │   └── ChartOverlay     (drawing tools layer: trendline, fib, Elliott)
      │   ├── Tab: Overview        (OverviewChart — recharts sparkline)
      │   ├── Tab: Order Book      (OrderBook — bid/ask depth bars)
      │   └── Tab: Recent Trades   (RecentTrades — price tick feed)
      └── Watchlist                (right sidebar: symbol list with price/change/star)
    </TradingDashboard>
  </I18nProvider>
</ErrorBoundary>
```

#### Data Service Abstraction (`marketDataService.js`)

All backend communication is isolated in a single service module. A compile-time toggle (`REACT_APP_DATA_SOURCE`) switches between mock data and the live API:

```javascript
const DATA_SOURCE = process.env.REACT_APP_DATA_SOURCE || "api"; // "mock" for development
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "/api";
```

**Exported functions**:

| Function                                      | API Endpoint                 | Purpose                        |
| --------------------------------------------- | ---------------------------- | ------------------------------ |
| `fetchCandles(sym, tf, limit)`                | `GET /api/klines`            | Historical OHLCV candles       |
| `fetchHistoricalCandles(sym, startMs, endMs)` | `GET /api/klines/historical` | Cold-storage candles via Trino |
| `subscribeCandle(sym, tf, onCandle)`          | `WS /api/stream`             | Real-time candle updates       |
| `fetchSymbols()`                              | `GET /api/symbols`           | Active trading pairs           |
| `fetchTickers()`                              | `GET /api/ticker`            | All tickers for watchlist      |
| `fetchOrderBook(sym)`                         | `GET /api/orderbook/{sym}`   | Order book depth               |
| `fetchTrades(sym, n)`                         | `GET /api/trades/{sym}`      | Recent price ticks             |

In mock mode, each function generates deterministic sample data with realistic price patterns, enabling full frontend development without running the backend.

#### Real-Time Data Flow

```
CandlestickChart mount
  │
  ├── fetchCandles(symbol, timeframe, 200) → REST → populate chart
  │
  └── subscribeCandle(symbol, timeframe, onCandle)
        │
        └── WebSocket /api/stream?symbol=BTCUSDT&interval=1m
              │
              └── onCandle({time, open, high, low, close, volume})
                    │
                    └── chart.update(candle)  ← live bar updates in-place
```

#### Error Handling Architecture

| Layer          | Component          | Behavior                                                                                |
| -------------- | ------------------ | --------------------------------------------------------------------------------------- |
| **Global**     | `ErrorBoundary`    | Catches unhandled render errors; shows a full-page error screen with "Try again" button |
| **Connection** | `App.js`           | `connError` state; red banner with Retry when `fetchSymbols` or `fetchTickers` fails    |
| **Chart data** | `CandlestickChart` | `fetchError` + `retryCount` state; red overlay "Failed to load candle data" with Retry  |
| **Order book** | `OrderBook`        | `error` state; inline error message when fetch fails and data is empty                  |
| **Trades**     | `RecentTrades`     | `error` state; inline error message, auto-clears on next successful fetch               |

#### Key Sub-Modules

| Module              | File                | Purpose                                                                   |
| ------------------- | ------------------- | ------------------------------------------------------------------------- |
| `chartConstants.js` | `components/chart/` | Theme colors, timeframe list, tab definitions, default indicator settings |
| `indicatorUtils.js` | `components/chart/` | Pure functions: `calcSMA`, `calcEMA`, `calcRSI`, `calcMFI`                |
| `IndicatorPanel.js` | `components/chart/` | Settings panel for overlays/oscillators                                   |
| `OscillatorPane.js` | `components/chart/` | RSI/MFI sub-chart below the main chart                                    |
| `OHLCVBar.js`       | `components/chart/` | Crosshair tooltip bar                                                     |
| `storageHelpers.js` | `utils/`            | `loadFromStorage` / `saveToStorage` (localStorage JSON wrappers)          |

#### Internationalization

`i18n/` provides an `I18nProvider` context with **English** and **Vietnamese** translations. The `LanguageSwitcher` component toggles between `en` and `vi`. All user-visible strings (buttons, labels, headings) use the `t()` translation function.

#### Historical Mode

When the user selects a date range via `DateRangePicker`, the chart enters **historical mode**:

1. Timeframe buttons are replaced with a "1H (historical)" label.
2. `fetchHistoricalCandles(symbol, startMs, endMs)` queries `GET /api/klines/historical`.
3. FastAPI routes the query through Trino to Iceberg cold storage (`coin_klines_hourly` → `historical_hourly` fallback).
4. Clearing the date range returns to live mode with WebSocket streaming.

---

## 9. Docker Compose Integration

### 9.1. Serving Layer Services

The serving layer adds two services to `docker-compose.yml`:

```yaml
fastapi:
  build:
    context: .
    dockerfile: docker/fastapi/Dockerfile
  ports:
    - "8080:8000" # 8080 exposed for direct debugging
  environment:
    REDIS_HOST: keydb
    REDIS_PORT: 6379
    INFLUX_URL: http://influxdb:8086
    INFLUX_TOKEN: ${INFLUX_TOKEN}
    INFLUX_ORG: vi
    INFLUX_BUCKET: crypto
    CORS_ORIGINS: "*"
    TRINO_HOST: trino
    TRINO_PORT: 8080
  depends_on:
    keydb: { condition: service_healthy }
    influxdb: { condition: service_healthy }
    trino: { condition: service_healthy }
  healthcheck:
    test:
      [
        "CMD",
        "python",
        "-c",
        "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')",
      ]

nginx:
  build:
    context: .
    dockerfile: docker/nginx/Dockerfile
  ports:
    - "80:80"
  depends_on:
    fastapi: { condition: service_healthy }
  healthcheck:
    test: ["CMD", "wget", "-qO-", "http://127.0.0.1:80/"]
```

### 9.2. Complete Service Map

The full stack consists of **16 services**:

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
│  SERVING     │  fastapi, nginx (React SPA + Reverse Proxy)       │
└──────────────┴────────────────────────────────────────────────────┘
```

### 9.3. Port Allocation

| Service         | Internal Port | External Port | Purpose                  |
| --------------- | :-----------: | :-----------: | ------------------------ |
| Kafka           |     9092      |     9092      | Broker (debugging)       |
| MinIO           |  9000, 9001   |  9000, 9001   | S3 API, Console          |
| InfluxDB        |     8086      |     8086      | Time-series UI           |
| PostgreSQL      |     5432      |     5432      | Database access          |
| KeyDB           |     6379      |     6379      | Redis CLI debugging      |
| Flink UI        |     8081      |     8081      | Flink Dashboard          |
| Spark Master UI |     8080      |     8082      | Spark Dashboard          |
| Spark History   |     18080     |     18080     | Job History              |
| Trino UI        |     8080      |     8083      | Trino Dashboard          |
| Dagster UI      |     3000      |     3000      | Orchestration Dashboard  |
| **FastAPI**     |   **8000**    |   **8080**    | **API (also via Nginx)** |
| **Nginx**       |    **80**     |    **80**     | **Web UI + API Proxy**   |

> **Note**: FastAPI is accessible both directly at port 8080 (for debugging) and through Nginx at port 80. In production, only Nginx on port 80 should be exposed.

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

# Phase 5: Submit Flink job (parallelism=3, 4 slots)
docker exec -d flink-jobmanager bash -c "cd /app && /opt/flink/bin/flink run -py src/ingest_flink_crypto.py -d"

# Phase 6: Serving layer (FastAPI waits for keydb + influxdb + trino healthy)
docker compose up -d fastapi nginx

# Or simply:
docker compose up -d --build
# (Docker Compose resolves dependency order via depends_on + healthchecks)
```

### 9.5. Environment Variables (`.env` file)

All credentials are loaded from a `.env` file at the project root (gitignored). Copy `.env.example` to `.env` and edit values.

```env
# ─── InfluxDB ────────────────────────────────────────────────────────────────
INFLUX_TOKEN=<strong-random-token>       # Required — API token for all InfluxDB access
INFLUX_ADMIN_USER=admin                 # InfluxDB initial admin username
INFLUX_ADMIN_PASSWORD=<change-me>       # InfluxDB initial admin password
INFLUX_ORG=vi                           # InfluxDB organization
INFLUX_BUCKET=crypto                    # InfluxDB bucket

# ─── MinIO ───────────────────────────────────────────────────────────────────
MINIO_ROOT_USER=minioadmin              # MinIO root username
MINIO_ROOT_PASSWORD=<change-me>         # MinIO root password

# ─── PostgreSQL ──────────────────────────────────────────────────────────────
POSTGRES_USER=iceberg                   # Postgres user (Iceberg catalog + Dagster)
POSTGRES_PASSWORD=<change-me>           # Postgres password
POSTGRES_DB=iceberg_catalog             # Default database

# ─── Redis ───────────────────────────────────────────────────────────────────
REDIS_HOST=localhost                    # Only needed for local dev outside Docker
REDIS_PORT=6379
```

> **Note**: `INFLUX_TOKEN` is the only strictly required secret (FastAPI will refuse to start without it). All other variables have working defaults in `.env.example` but should be changed in production.

### 9.6. Resource Requirements

| Service        | Memory (Config) | Memory (Actual) |       CPU        | Notes                                      |
| -------------- | :-------------: | :-------------: | :--------------: | ------------------------------------------ |
| Kafka          |      ~1GB       |      ~1GB       |     2 cores      | KRaft, LZ4, 48h retention                  |
| MinIO          |     ~512MB      |      ~86MB      |     0.1 core     | S3-compatible storage                      |
| InfluxDB       |      ~1GB       |     ~110MB      |     0.5 core     | Time-series                                |
| PostgreSQL     |     ~256MB      |      ~22MB      |     0.1 core     | Iceberg catalog + Dagster                  |
| KeyDB          |       1GB       |      ~26MB      |     0.2 core     | maxmemory 1GB, LRU eviction, hz 50         |
| Flink JM       |      1.6GB      |     ~448MB      |     0.5 core     | Container cap 1.75GB                       |
| Flink TM       |      4.1GB      |     ~2.2GB      |     3 cores      | 4 slots, parallelism=3, cap 4.5GB          |
| Spark Master   |     ~512MB      |     ~764MB      |     0.3 core     | Driver + scheduler                         |
| Spark Worker   |       4GB       |     ~244MB      |     0.1 core     | cores.max=2                                |
| Trino          |       1GB       |     ~1.1GB      |     0.5 core     | Xmx 1GB (infrequent historical queries)    |
| Dagster WS     |     ~512MB      |     ~203MB      |     0.5 core     | UI + metadata                              |
| Dagster Daemon |     ~512MB      |     ~274MB      |     0.5 core     | Scheduler loop                             |
| Producer       |     ~128MB      |      ~77MB      |     1.5 cores    | 4 WS threads to Binance                    |
| **FastAPI**    |   **~256MB**    |   **~143MB**    |  **0.3 core**    | 2 Uvicorn workers                          |
| **Nginx**      |    **~64MB**    |    **~7MB**     |  **0.1 core**    | Reverse proxy + gzip                       |
| **Total**      |  **~16.5GB**    |   **~6.7GB**    |  **~10 cores**   | On t3a.2xlarge (8 vCPU, 32GB, 100GB gp3)  |

**Current Deployment**: AWS EC2 t3a.2xlarge — 8 AMD vCPU, 32GB RAM, 100GB gp3, Singapore (ap-southeast-1)
**Minimum System Requirements**: 16GB RAM, 4+ CPU cores, 50GB+ disk

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

| Service            | URL                                | Credentials                                                   |
| ------------------ | ---------------------------------- | ------------------------------------------------------------- |
| Kafka              | `localhost:9092`                   | No auth                                                       |
| MinIO Console      | `http://localhost:9001`            | `$MINIO_ROOT_USER` / `$MINIO_ROOT_PASSWORD` (from `.env`)     |
| MinIO API          | `http://localhost:9000`            | `$MINIO_ROOT_USER` / `$MINIO_ROOT_PASSWORD` (from `.env`)     |
| InfluxDB           | `http://localhost:8086`            | `$INFLUX_ADMIN_USER` / `$INFLUX_ADMIN_PASSWORD` (from `.env`) |
| PostgreSQL         | `localhost:5432`                   | `$POSTGRES_USER` / `$POSTGRES_PASSWORD` (from `.env`)         |
| KeyDB              | `localhost:6379`                   | No auth                                                       |
| Flink UI           | `http://localhost:8081`            | No auth                                                       |
| Spark Master UI    | `http://localhost:8082`            | No auth                                                       |
| Spark History      | `http://localhost:18080`           | No auth                                                       |
| Trino UI           | `http://localhost:8083`            | No auth                                                       |
| Dagster UI         | `http://localhost:3000`            | No auth                                                       |
| **Web UI (Nginx)** | **`http://localhost:80`**          | **No auth**                                                   |
| **FastAPI Docs**   | **`http://localhost:80/api/docs`** | **No auth**                                                   |

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

# FastAPI health + API docs
curl http://localhost/api/health
curl http://localhost/api/docs
curl http://localhost/api/symbols
curl http://localhost/api/ticker/BTCUSDT
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
| Kafka topics         | Kafka                        | 2 days (`KAFKA_LOG_RETENTION_HOURS: 48`) |
| Iceberg snapshots    | MinIO/S3                     | 48h (then expired)                        |
| Iceberg orphan files | MinIO/S3                     | 72h (then cleaned)                        |

### 11.5. Dagster Schedule Summary

| Asset                       | Schedule                      | Spark Job                                                      | Purpose                                     |
| --------------------------- | ----------------------------- | -------------------------------------------------------------- | ------------------------------------------- |
| `backfill_historical`       | `0 2 * * *` (daily 2 AM)      | `backfill_historical.py --mode all --iceberg-mode incremental` | Fill InfluxDB gaps + Iceberg incremental    |
| `aggregate_candles`         | `0 4 * * *` (daily 4 AM)      | `aggregate_candles.py --mode all`                              | 1m → 1h aggregation + retention             |
| `iceberg_table_maintenance` | `0 3 * * 0` (weekly Sun 3 AM) | `iceberg_maintenance.py`                                       | Compaction, snapshot expiry, orphan cleanup |

### 11.6. Frontend Feature Matrix

| Feature              | Component               | Data Source                              | Status     |
| -------------------- | ----------------------- | ---------------------------------------- | ---------- |
| Candlestick Chart    | `CandlestickChart.js`   | `GET /api/klines` (InfluxDB/KeyDB)       | Integrated |
| Live Price Updates   | `useCandlestickData.js` | `WS /api/stream` (WebSocket)             | Integrated |
| Historical Candles   | `CandlestickChart.js`   | `GET /api/klines/historical` (Trino)     | Integrated |
| Order Book           | `OrderBook.js`          | `GET /api/orderbook/{sym}` (KeyDB)       | Integrated |
| Recent Trades        | `RecentTrades.js`       | `GET /api/trades/{sym}` (KeyDB)          | Integrated |
| Market Overview      | `OverviewChart.js`      | Derived from candle data                 | Integrated |
| Technical Indicators | `CandlestickChart.js`   | Client-side: SMA, EMA, RSI, MFI          | Integrated |
| Drawing Tools        | `ChartOverlay.js`       | Client-side state (trendline, fib, wave) | Integrated |
| Symbol Selector      | `MarketSelector.js`     | `GET /api/symbols` (dynamic)             | Integrated |
| Watchlist            | `Watchlist.js`          | `GET /api/ticker` (live, 5s refresh)     | Integrated |
| Error Boundary       | `ErrorBoundary.js`      | Global render-error catch, retry         | Integrated |
| Connection Errors    | `App.js`                | Banner + retry when API unreachable      | Integrated |
| i18n (EN/VI)         | `i18n/`                 | Static translations                      | Integrated |

---

_This documentation describes the complete Lambda Architecture platform, including all pipeline components and the fully integrated serving layer (FastAPI + Nginx + React)._
