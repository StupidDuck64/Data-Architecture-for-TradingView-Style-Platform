# Tài liệu Kỹ thuật — CryptoPrice Platform

## Nền tảng Theo dõi Giá Tiền điện tử Real-time

---

## Mục lục

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc Lambda](#2-kiến-trúc-lambda)
3. [Công nghệ sử dụng](#3-công-nghệ-sử-dụng)
4. [Cấu trúc thư mục](#4-cấu-trúc-thư-mục)
5. [Các loại dữ liệu & Schema](#5-các-loại-dữ-liệu--schema)
6. [Luồng dữ liệu End-to-End](#6-luồng-dữ-liệu-end-to-end)
7. [Chi tiết từng Component](#7-chi-tiết-từng-component)
8. [API Endpoints](#8-api-endpoints)
9. [Hướng dẫn triển khai](#9-hướng-dẫn-triển-khai)
10. [Xử lý sự cố](#10-xử-lý-sự-cố)

---

## 1. Tổng quan dự án

### Mục đích

CryptoPrice là nền tảng theo dõi giá tiền điện tử real-time, được thiết kế theo mô hình TradingView. Hệ thống thu thập dữ liệu trực tiếp từ Binance, xử lý qua 2 luồng (real-time và batch), lưu trữ trên nhiều database chuyên dụng, và phục vụ qua giao diện React.

### Chức năng chính

| Chức năng | Mô tả |
|-----------|-------|
| **Theo dõi real-time** | 400+ cặp giao dịch USDT, độ trễ ~500ms |
| **Biểu đồ nến** | 8 timeframe: 1s, 1m, 5m, 15m, 1H, 4H, 1D, 1W |
| **Scroll historical** | Kéo trái để load thêm dữ liệu lịch sử vô hạn |
| **Order Book** | Sổ lệnh top 20 cập nhật mỗi 100ms |
| **Indicators** | SMA20, SMA50, EMA12, EMA26, RSI, MFI |
| **Watchlist** | Danh sách theo dõi, đánh dấu yêu thích |
| **Backfill tự động** | Tự động bù đắp dữ liệu khi hệ thống downtime |

### Nguyên tắc Lambda Architecture

| Layer | Công nghệ | Vai trò |
|-------|-----------|---------|
| **Speed Layer** | Flink → KeyDB + InfluxDB | Xử lý real-time, độ trễ thấp |
| **Batch Layer** | Spark → Iceberg/MinIO | Xử lý lịch sử, aggregation |
| **Serving Layer** | FastAPI + Nginx + React | Phục vụ API và giao diện |

---

## 2. Kiến trúc Lambda

### Sơ đồ tổng quan

```
BINANCE EXCHANGE
    │
    │ WebSocket (wss://)
    │  ├─ !ticker@arr (ticker 24h)
    │  ├─ <symbol>@kline_1s (nến 1 giây)
    │  └─ <symbol>@depth20@100ms (order book)
    ▼
┌─────────────────────────────────────────────────────────────┐
│  DOCKER COMPOSE (18 containers)                             │
│                                                             │
│  ┌─────────────┐     ┌─────────────┐                        │
│  │  Producer   │────▶│   KAFKA     │                        │
│  │ (Python WS) │     │  (3 topics) │                        │
│  └─────────────┘     └──────┬──────┘                        │
│                             │                               │
│              ┌──────────────┴──────────────┐                │
│              │      FLINK CLUSTER          │                │
│              │  (Speed Layer - Real-time)  │                │
│              └──────┬───────────┬──────────┘                │
│                     │           │                           │
│         ┌──────────┴┐     ┌────┴────────┐                   │
│         ▼           ▼     ▼             ▼                   │
│   ┌──────────┐ ┌──────────┐ ┌─────────────────┐             │
│   │  KeyDB   │ │ InfluxDB │ │ ICEBERG/MinIO   │             │
│   │(hot cache)│ │(time-ser)│ │  (data lake)    │             │
│   └────┬─────┘ └────┬─────┘ └───────┬─────────┘             │
│        │            │               │                       │
│        │      ┌─────┴─────┐   ┌─────┴─────┐                 │
│        │      │  SPARK    │   │   TRINO   │                 │
│        │      │  (Batch)  │   │ (SQL qry) │                 │
│        │      └───────────┘   └─────┬─────┘                 │
│        │                            │                       │
│   ┌────┴────────────────────────────┴─────┐                 │
│   │            FASTAPI                     │                 │
│   │        (REST + WebSocket)             │                 │
│   └────────────────┬──────────────────────┘                 │
│                    │                                        │
│   ┌────────────────┴──────────────────────┐                 │
│   │  NGINX + REACT (Frontend Dashboard)   │                 │
│   └───────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

### Vai trò từng layer

| Layer | Component | Chức năng |
|-------|-----------|-----------|
| **Ingestion** | `producer_binance.py` | Thu thập WebSocket Binance → Kafka |
| **Speed** | Flink + `ingest_flink_crypto.py` | Xử lý real-time → KeyDB, InfluxDB |
| **Batch** | Spark + các script Python | Backfill, aggregation 1m→1h, maintenance |
| **Storage** | KeyDB, InfluxDB, Iceberg/MinIO, PostgreSQL | Hot cache, time-series, data lake, metadata |
| **Query** | Trino | SQL queries trên Iceberg tables |
| **Orchestration** | Dagster | Lập lịch và giám sát batch jobs |
| **Serving** | FastAPI + Nginx + React | REST/WebSocket API + giao diện web |

---

## 3. Công nghệ sử dụng

### Messaging & Storage

| Công nghệ | Version | Vai trò | Port |
|-----------|---------|---------|------|
| **Apache Kafka** | 3.9.0 (KRaft) | Message broker, event streaming | 9092 |
| **MinIO** | Latest | Object storage S3-compatible (Iceberg warehouse) | 9000, 9001 |
| **InfluxDB** | 2.7 | Time-series database | 8086 |
| **PostgreSQL** | 16-alpine | Metadata (Iceberg catalog + Dagster) | 5432 |
| **KeyDB** | Latest | In-memory cache Redis-compatible | 6379 |

### Processing

| Công nghệ | Version | Vai trò | Port |
|-----------|---------|---------|------|
| **Apache Flink** | 1.18.1 | Stream processing (speed layer) | 8081 |
| **Apache Spark** | 3.5.5 | Batch processing | 7077, 8082, 18080 |
| **Apache Iceberg** | 1.5.2 | Table format cho data lake (ACID) | — |
| **Trino** | 442 | SQL query engine trên Iceberg | 8083 |
| **Dagster** | 1.8.x | Workflow orchestration | 3000 |

### Serving

| Công nghệ | Version | Vai trò | Port |
|-----------|---------|---------|------|
| **FastAPI** | 0.115+ | REST + WebSocket API | 8000 (internal) |
| **Nginx** | 1.25 | Reverse proxy, static files | 80 |
| **React** | 18.3.1 | Frontend SPA | — (qua Nginx) |

### Frontend Libraries

| Package | Version | Mục đích |
|---------|---------|----------|
| `lightweight-charts` | 5.1.0 | Biểu đồ nến TradingView-style |
| `recharts` | 2.12.7 | Charting bổ sung |
| `lucide-react` | 0.396.0 | Icon library |
| `tailwindcss` | 3.4.4 | CSS framework |

---

## 4. Cấu trúc thư mục

```
cryptoprice_local/
│
├── docker-compose.yml          # Định nghĩa 18 services
├── .env                        # Biến môi trường (tokens, passwords)
├── .env.example                # Template biến môi trường
│
├── config/                     # Cấu hình
│   └── spark-defaults.conf     # Spark driver/executor + Iceberg
│
├── docs/                       # Tài liệu
│   ├── README.md               # Hướng dẫn nhanh
│   ├── DOCUMENTATION.md        # Tài liệu kỹ thuật (file này)
│   └── SYSTEM_ARCHITECTURE.md  # Kiến trúc chi tiết
│
├── scripts/                    # Scripts tiện ích
│   ├── auto_submit_jobs.sh     # Tự động submit Flink jobs
│   ├── setup_influx_retention.ps1  # Thiết lập retention InfluxDB (Windows)
│   └── setup_influx_retention.sh   # Thiết lập retention InfluxDB (Linux)
│
├── docker/                     # Dockerfiles & configs
│   ├── backfill/               # InfluxDB gap-filling
│   ├── dagster/                # Dagster + Spark base
│   ├── fastapi/                # FastAPI + uvicorn
│   ├── flink/                  # Flink + Python + Kafka connector
│   ├── nginx/                  # React build + Nginx
│   ├── postgres/               # Init script (databases)
│   ├── producer/               # Binance WebSocket producer
│   ├── spark/                  # Spark master/worker
│   └── trino/                  # Trino coordinator config
│
├── orchestration/              # Dagster definitions
│   ├── assets.py               # 3 Spark job assets + cron schedules
│   └── workspace.yaml          # Dagster workspace config
│
├── schemas/                    # Avro schemas cho Kafka
│   ├── ticker.avsc             # Ticker 24h data
│   ├── kline.avsc              # Candlestick/Kline data
│   ├── trade.avsc              # Trade data
│   └── depth.avsc              # Order book depth
│
├── serving/                    # FastAPI backend
│   ├── main.py                 # App entry point
│   ├── config.py               # Environment config
│   ├── connections.py          # Redis, InfluxDB, Trino connections
│   └── routers/                # API endpoints
│       ├── klines.py           # Candle queries
│       ├── historical.py       # Historical data (Trino)
│       ├── ticker.py           # Live ticker
│       ├── orderbook.py        # Order book
│       ├── trades.py           # Recent trades
│       ├── symbols.py          # Symbol list
│       ├── indicators.py       # SMA/EMA indicators
│       └── ws.py               # WebSocket streaming
│
├── src/                        # Python source code
│   ├── producer_binance.py     # Binance WS → Kafka
│   ├── ingest_flink_crypto.py  # Flink: Kafka → KeyDB + InfluxDB
│   ├── ingest_crypto.py        # Spark: Kafka → Iceberg
│   ├── backfill_historical.py  # Backfill InfluxDB + Iceberg
│   ├── aggregate_candles.py    # 1m → 1h aggregation
│   ├── iceberg_maintenance.py  # Table maintenance
│   └── candle_query_helper.py  # Query helper
│
└── frontend/                   # React SPA
    ├── package.json            # Dependencies
    ├── tailwind.config.js      # TailwindCSS config
    └── src/
        ├── App.js              # Main layout
        ├── components/         # React components
        │   ├── CandlestickChart.js
        │   ├── Watchlist.js
        │   ├── OrderBook.js
        │   └── ...
        ├── services/           # API abstraction
        ├── hooks/              # Custom hooks
        └── i18n/               # Internationalization
```

---

## 5. Các loại dữ liệu & Schema

### 5.1. Ticker (Thống kê 24h)

**Nguồn**: Binance WebSocket `!ticker@arr`  
**Kafka Topic**: `crypto_ticker`

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

**Lưu trữ**:
| Database | Key/Measurement | TTL |
|----------|-----------------|-----|
| KeyDB | `ticker:latest:{symbol}` (Hash) | Không (overwrite) |
| KeyDB | `ticker:history:{symbol}` (Sorted Set) | 5 phút |
| InfluxDB | `market_ticks` | Theo bucket retention |
| Iceberg | `coin_ticker` | Vĩnh viễn |

### 5.2. Kline (Dữ liệu nến)

**Nguồn**: Binance WebSocket `<symbol>@kline_1s`  
**Kafka Topic**: `crypto_klines`

```json
{
  "event_time": 1710000000000,
  "symbol": "BTCUSDT",
  "kline_start": 1709999940000,
  "kline_close": 1709999999999,
  "interval": "1s",
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

**Lưu trữ**:
| Database | Key/Measurement | Interval | TTL |
|----------|-----------------|----------|-----|
| KeyDB | `candle:1s:{symbol}` | 1s | 8 giờ |
| KeyDB | `candle:1m:{symbol}` | 1m | 7 ngày |
| KeyDB | `candle:latest:{symbol}` | latest | Không |
| InfluxDB | `candles` interval=1s | 1s | 7 ngày |
| InfluxDB | `candles` interval=1m | 1m | 90 ngày |
| InfluxDB | `candles` interval=1h | 1h | Vĩnh viễn |
| Iceberg | `coin_klines` | 1m | Vĩnh viễn |
| Iceberg | `coin_klines_hourly` | 1h | Vĩnh viễn |

### 5.3. Order Book (Sổ lệnh)

**Nguồn**: Binance WebSocket `<symbol>@depth20@100ms`  
**Kafka Topic**: `crypto_depth`

```json
{
  "event_time": 1710000000050,
  "symbol": "BTCUSDT",
  "bids": [["67500.00", "1.234"], ["67499.50", "2.567"]],
  "asks": [["67501.00", "0.890"], ["67501.50", "1.456"]],
  "lastUpdateId": 45678901234
}
```

**Lưu trữ**:
| Database | Key | TTL |
|----------|-----|-----|
| KeyDB | `orderbook:{symbol}` (Hash) | 60 giây |

> Order book chỉ lưu vào KeyDB vì tần suất cao và tính chất volatile.

### 5.4. Indicators (Chỉ báo kỹ thuật)

**Tính toán bởi**: `ingest_flink_crypto.py` từ dữ liệu kline đã đóng.

| Indicator | Thuật toán | Kỳ |
|-----------|------------|-----|
| SMA-20 | Trung bình 20 giá đóng cuối | 20 |
| SMA-50 | Trung bình 50 giá đóng cuối | 50 |
| EMA-12 | Exponential weighted (α = 2/13) | 12 |
| EMA-26 | Exponential weighted (α = 2/27) | 26 |

**Lưu trữ**:
| Database | Key/Measurement |
|----------|-----------------|
| KeyDB | `indicator:latest:{symbol}` |
| InfluxDB | `indicators` |

### 5.5. Ma trận lưu trữ

```
┌─────────────────────┬──────────┬──────────┬──────────┬──────────┐
│                     │  KeyDB   │ InfluxDB │ Iceberg  │  Trino   │
│    Loại dữ liệu     │(hot/live)│(time-ser)│(cold/DL) │(SQL qry) │
├─────────────────────┼──────────┼──────────┼──────────┼──────────┤
│ Ticker (24h stats)  │    ✅    │    ✅    │    ✅    │    ✅    │
│ Klines (nến 1s/1m)  │    ✅    │    ✅    │    ✅    │    ✅    │
│ Klines (nến 1h agg) │    —     │    ✅    │    ✅    │    ✅    │
│ Order Book (depth)  │    ✅    │    —     │    —     │    —     │
│ Indicators (SMA/EMA)│    ✅    │    ✅    │    —     │    —     │
└─────────────────────┴──────────┴──────────┴──────────┴──────────┘
```

---

## 6. Luồng dữ liệu End-to-End

### 6.1. Real-time Pipeline (Speed Layer)

```
Binance WS → Producer → Kafka → Flink → KeyDB + InfluxDB
     │                                      │
     │ ~100ms                               │ ~500ms
     ▼                                      ▼
  WebSocket                              FastAPI
  connection                             queries
```

**Chi tiết**:
1. **Producer** (`producer_binance.py`):
   - Mở 8+ WebSocket connections đến Binance
   - Chia 400 symbols thành groups 50 symbols/connection
   - Serialize thành Avro với Confluent wire format
   - Gửi vào 3 Kafka topics: `crypto_ticker`, `crypto_klines`, `crypto_depth`

2. **Flink** (`ingest_flink_crypto.py`):
   - Đọc từ 3 Kafka topics
   - **Ticker stream**: → KeyDB `ticker:latest:*` + InfluxDB `market_ticks`
   - **Kline stream**:
     - Raw 1s → KeyDB `candle:1s:*` + InfluxDB `candles` interval=1s
     - Aggregate 1s→1m (in-flight) → KeyDB `candle:1m:*` + InfluxDB `candles` interval=1m
     - Closed 1m → Calculate SMA/EMA → KeyDB `indicator:latest:*`
   - **Depth stream**: → KeyDB `orderbook:*`

### 6.2. Batch Pipeline

```
Dagster Schedule → Spark Jobs → InfluxDB / Iceberg
```

**Jobs**:
| Job | Tần suất | Chức năng |
|-----|----------|-----------|
| `aggregate_candles.py` | Hàng ngày 4AM | 1m→1h aggregation, xóa 1m cũ |
| `iceberg_maintenance.py` | Chủ nhật 3AM | Compact files, expire snapshots |
| `backfill_historical.py` | Theo yêu cầu | Bù đắp gaps, populate historical |

### 6.3. Query Flow

```
React → Nginx → FastAPI → {KeyDB, InfluxDB, Trino}
```

**Routing ưu tiên API `/api/klines`**:
1. **Check cache** (100ms TTL)
2. **KeyDB** (candle:1s hoặc candle:1m) — ưu tiên
3. **InfluxDB** — fallback nếu KeyDB không đủ
4. **Server-side aggregation** — nếu cần (1m→5m, 1h→4h)
5. **Merge live ticker** — vào candle cuối cùng
6. **Return + Cache**

---

## 7. Chi tiết từng Component

### 7.1. Producer (`src/producer_binance.py`)

**Chức năng**: Kết nối Binance WebSocket, serialize Avro, gửi Kafka.

| Tham số | Giá trị | Mô tả |
|---------|---------|-------|
| Symbols | ~400 | Tất cả USDT pairs đang TRADING |
| Connections/stream | 8 | 50 symbols/connection |
| Compression | lz4 | Kafka message compression |
| Batch size | 64KB | Kafka producer batch |
| Heartbeat | 5s | WebSocket ping interval |

### 7.2. Flink Pipeline (`src/ingest_flink_crypto.py`)

**Classes chính**:

| Class | Output | Batch/Flush |
|-------|--------|-------------|
| `KeyDBWriter` | `ticker:latest:*`, `ticker:history:*` | 100/0.5s |
| `InfluxDBWriter` | `market_ticks` measurement | 200/0.5s |
| `KeyDBKlineWriter` | `candle:1s:*`, `candle:1m:*` | 50/0.5s |
| `InfluxDBKlineWriter` | `candles` measurement | 500/3s |
| `KlineWindowAggregator` | 1s→1m aggregation (KeyedProcessFunction) | Window 60s |
| `IndicatorWriter` | `indicator:latest:*`, `indicators` | Per closed candle |
| `DepthWriter` | `orderbook:*` | 50/0.3s |

**KlineWindowAggregator** — Chi tiết:
- **Window**: 60 giây (processing time)
- **State**: MapState<kline_start_ms, JSON> per symbol
- **Dedup**: Cùng timestamp → overwrite
- **Gap-fill**: Forward-fill missing seconds với close price trước
- **Safety timer**: 65s sau window open để đảm bảo emit

### 7.3. Batch Jobs

**`aggregate_candles.py`**:
```bash
python aggregate_candles.py --mode all
```
- Modes: `influx`, `iceberg`, `all`
- Query 1m candles cũ hơn 90 ngày
- Aggregate theo giờ: first(open), max(high), min(low), last(close), sum(volume)
- Ghi 1h candles, xóa 1m cũ

**`backfill_historical.py`**:
```bash
python backfill_historical.py --mode populate --days 90
```
- Modes: `influx` (gap fill), `iceberg` (historical), `populate` (force N days), `all`
- ThreadPoolExecutor: 5 workers
- Rate limiting: 120ms/request

**`iceberg_maintenance.py`**:
```bash
python iceberg_maintenance.py
```
- Compact files (~128MB)
- Rewrite manifests
- Expire snapshots (>48h)
- Remove orphan files (>72h)

### 7.4. FastAPI Backend (`serving/`)

**Cấu trúc routers**:

| Router | Endpoint | Source | Mô tả |
|--------|----------|--------|-------|
| `klines.py` | `GET /api/klines` | KeyDB → InfluxDB | OHLCV candles |
| `historical.py` | `GET /api/klines/historical` | Trino | Historical từ Iceberg |
| `ticker.py` | `GET /api/ticker/{symbol}` | KeyDB | Live ticker |
| `orderbook.py` | `GET /api/orderbook/{symbol}` | KeyDB | Order book |
| `trades.py` | `GET /api/trades/{symbol}` | KeyDB | Recent ticks |
| `symbols.py` | `GET /api/symbols` | KeyDB | Symbol list |
| `indicators.py` | `GET /api/indicators/{symbol}` | KeyDB | SMA/EMA |
| `ws.py` | `WS /api/stream` | KeyDB | Real-time streaming |

### 7.5. Frontend React (`frontend/`)

**Components chính**:

| Component | File | Chức năng |
|-----------|------|-----------|
| `CandlestickChart` | `CandlestickChart.js` | Biểu đồ nến chính + indicators |
| `Watchlist` | `Watchlist.js` | Danh sách symbols + prices |
| `OrderBook` | `OrderBook.js` | Hiển thị sổ lệnh |
| `RecentTrades` | `RecentTrades.js` | Giao dịch gần đây |
| `MarketSelector` | `MarketSelector.js` | Dropdown chọn symbol |
| `DrawingToolbar` | `DrawingToolbar.js` | Công cụ vẽ |

**CandlestickChart** — Features:
- Multi-timeframe: 1s, 1m, 5m, 15m, 1H, 4H, 1D, 1W
- WebSocket real-time + incremental polling (không flicker)
- Scroll-left loading (load thêm 500 candles, cooldown 500ms)
- Indicators: SMA20, SMA50, EMA, RSI, MFI (toggle on/off)
- OHLCV tooltip on hover
- Export PNG

---

## 8. API Endpoints

### REST APIs

| Method | Path | Tham số | Mô tả |
|--------|------|---------|-------|
| GET | `/api/health` | — | Health check |
| GET | `/api/symbols` | — | Danh sách symbols |
| GET | `/api/ticker/{symbol}` | — | Ticker 1 symbol |
| GET | `/api/ticker` | — | Tất cả tickers |
| GET | `/api/klines` | symbol, interval, limit, endTime | OHLCV candles |
| GET | `/api/klines/historical` | symbol, startTime, endTime, limit | Historical (Iceberg) |
| GET | `/api/orderbook/{symbol}` | — | Order book |
| GET | `/api/trades/{symbol}` | limit | Recent trades |
| GET | `/api/indicators/{symbol}` | — | SMA/EMA indicators |

### WebSocket

| Path | Query Params | Mô tả |
|------|--------------|-------|
| `WS /api/stream` | symbol, interval | Real-time candle streaming |

**Ví dụ WebSocket**:
```javascript
const ws = new WebSocket('ws://localhost/api/stream?symbol=BTCUSDT&interval=1m');
ws.onmessage = (event) => {
  const candle = JSON.parse(event.data);
  // { time: 1710000000, open: 67500, high: 67520, low: 67480, close: 67510, volume: 12.34 }
};
```

### Response Examples

**GET /api/klines?symbol=BTCUSDT&interval=1m&limit=5**:
```json
[
  {"openTime": 1710000000000, "open": 67500, "high": 67520, "low": 67480, "close": 67510, "volume": 12.34},
  {"openTime": 1710000060000, "open": 67510, "high": 67550, "low": 67500, "close": 67540, "volume": 15.67},
  ...
]
```

**GET /api/orderbook/BTCUSDT**:
```json
{
  "bids": [["67500.00", "1.234"], ["67499.50", "2.567"]],
  "asks": [["67501.00", "0.890"], ["67501.50", "1.456"]],
  "spread": "1.00",
  "best_bid": "67500.00",
  "best_ask": "67501.00"
}
```

---

## 9. Hướng dẫn triển khai

### 9.1. Yêu cầu hệ thống

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 16GB | 32GB |
| CPU | 4 cores | 8 cores |
| Disk | 50GB SSD | 200GB SSD |
| Docker | 20.10+ | Latest |
| Docker Compose | 2.0+ | Latest |

### 9.2. Khởi động nhanh

```bash
# 1. Clone repository
git clone <repo-url>
cd cryptoprice_local

# 2. Tạo file .env
cp .env.example .env
# Chỉnh sửa: INFLUX_TOKEN, MINIO_ROOT_USER/PASSWORD, POSTGRES_USER/PASSWORD

# 3. Khởi động tất cả services
docker-compose up -d

# 4. Chờ services healthy (~2-3 phút)
docker-compose ps

# 5. Submit Flink job
docker exec flink-jobmanager flink run -d -py /app/src/ingest_flink_crypto.py
```

### 9.3. Thiết lập retention InfluxDB

```powershell
# Windows
.\scripts\setup_influx_retention.ps1

# Linux/Mac
./scripts/setup_influx_retention.sh
```

### 9.4. Backfill dữ liệu

```bash
# Populate 90 ngày 1m candles
docker run --rm --network cryptoprice_crypto-net \
  -e INFLUX_URL=http://influxdb:8086 \
  -e INFLUX_TOKEN=<your-token> \
  -e INFLUX_ORG=vi \
  -e INFLUX_BUCKET=crypto \
  cryptoprice-influx-backfill \
  python /app/backfill_historical.py --mode populate --days 90
```

### 9.5. Truy cập UI

| Service | URL |
|---------|-----|
| Frontend | http://localhost |
| Flink UI | http://localhost:8081 |
| Spark UI | http://localhost:8082 |
| Trino UI | http://localhost:8083 |
| InfluxDB | http://localhost:8086 |
| MinIO Console | http://localhost:9001 |
| Dagster UI | http://localhost:3000 |

---

## 10. Xử lý sự cố

### 10.1. Lỗi thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| **Cannot access 'q' before initialization** | TDZ trong JavaScript | Di chuyển function declaration lên trước useEffect |
| **Candles 1m+ không load** | Thiếu INFLUX_ORG/INFLUX_BUCKET | Thêm vào .env, restart fastapi |
| **Chart flickering** | Full-reload khi polling | Dùng incremental `.update()` thay vì `setData()` |
| **Scroll-left reset chart** | maxBars=500 truncate, useState cascade | Tăng maxBars=10000, dùng useRef cho isLoadingMore |
| **WebSocket reconnect loop** | Binance rate limit | Giảm số connections, tăng heartbeat interval |

### 10.2. Kiểm tra logs

```bash
# Producer logs
docker logs -f producer

# Flink logs
docker logs -f flink-taskmanager

# FastAPI logs
docker logs -f fastapi

# Nginx logs
docker logs -f nginx
```

### 10.3. Reset dữ liệu

```bash
# Xóa tất cả volumes (CẢNH BÁO: mất toàn bộ dữ liệu)
docker-compose down -v

# Khởi động lại
docker-compose up -d
```

### 10.4. Kiểm tra Flink job

```bash
# List running jobs
docker exec flink-jobmanager flink list

# Cancel job
docker exec flink-jobmanager flink cancel <job-id>

# Resubmit job
docker exec flink-jobmanager flink run -d -py /app/src/ingest_flink_crypto.py
```

---

## Phụ lục

### A. Biến môi trường

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `INFLUX_TOKEN` | InfluxDB API token | `abc123...` |
| `INFLUX_ORG` | InfluxDB organization | `vi` |
| `INFLUX_BUCKET` | InfluxDB bucket | `crypto` |
| `MINIO_ROOT_USER` | MinIO access key | `minioadmin` |
| `MINIO_ROOT_PASSWORD` | MinIO secret key | `minioadmin123` |
| `POSTGRES_USER` | PostgreSQL username | `postgres` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `postgres123` |

### B. Kafka Topics

| Topic | Partitions | Retention | Mô tả |
|-------|------------|-----------|-------|
| `crypto_ticker` | 3 | 48h | Ticker 24h data |
| `crypto_klines` | 3 | 48h | 1s candle data |
| `crypto_depth` | 3 | 48h | Order book depth |

### C. KeyDB Keys

| Pattern | Type | TTL | Mô tả |
|---------|------|-----|-------|
| `ticker:latest:{symbol}` | Hash | — | Latest ticker |
| `ticker:history:{symbol}` | Sorted Set | Auto-cleanup 5min | Price history |
| `candle:1s:{symbol}` | Sorted Set | 8h | 1s candles |
| `candle:1m:{symbol}` | Sorted Set | 7d | 1m candles |
| `candle:latest:{symbol}` | Hash | — | Latest candle |
| `orderbook:{symbol}` | Hash | 60s | Order book |
| `indicator:latest:{symbol}` | Hash | — | SMA/EMA indicators |

### D. InfluxDB Measurements

| Measurement | Tags | Fields |
|-------------|------|--------|
| `market_ticks` | symbol, exchange | price, bid, ask, volume, quote_volume, trade_count |
| `candles` | symbol, exchange, interval | open, high, low, close, volume, quote_volume, is_closed |
| `indicators` | symbol, exchange | sma20, sma50, ema12, ema26, close |

---

*Cập nhật: 2026-03-11*
