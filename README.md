# CryptoPrice — Real-time Crypto Streaming Platform

> **GitHub**: https://github.com/StupidDuck64/TradingView-Style-Crypto-Platform

Hệ thống streaming giá crypto real-time từ Binance, xử lý bằng Apache Flink + Spark, lưu trữ trên MinIO/Iceberg, query bằng Trino, orchestrate bằng Dagster. Thiết kế mô phỏng kiến trúc data platform kiểu TradingView trên nền lakehouse hiện đại.

---

## Kiến trúc tổng thể

```
  Binance WebSocket
        │
        ▼
  ┌─────────────┐
  │  Producer   │  (Python)  ─────────────────────────────────────────────┐
  │ (Binance WS)│                                                          │
  └─────────────┘                                                          │
        │ crypto_ticker / crypto_trades                                    │
        ▼                                                                  │
  ┌─────────────┐                                                          │
  │    Kafka    │  (KRaft, 3.9)                                            │
  │  broker+ctrl│                                                          │
  └──────┬──────┘                                                          │
         │                                                                 │
    ┌────┴─────────────────────────┐                                       │
    │                             │                                        │
    ▼                             ▼                                        │
┌────────────┐            ┌──────────────┐                                 │
│   Flink    │            │    Spark     │                                 │
│ 1.18.1     │            │   3.4.1      │                                 │
│ JobManager │            │ Master+Worker│                                 │
│ TaskManager│            └──────┬───────┘                                │
└─────┬──────┘                   │                                        │
      │                          │ append to Iceberg                      │
      │                          ▼                                        │
      │                  ┌──────────────┐                                 │
      │                  │   MinIO      │  ◄── Object Storage (S3-compat) │
      │                  │  + Iceberg   │                                 │
      │                  │  (JDBC→PG)   │                                 │
      │                  └──────┬───────┘                                 │
      │                         │                                         │
      │             ┌───────────┘                                         │
      │             ▼                                                      │
      │        ┌─────────┐                                                │
      │        │  Trino  │  442  (query Iceberg)                          │
      │        └─────────┘                                                │
      │                                                                   │
      ├──► KeyDB  (latest ticker cache → FastAPI)                         │
      └──► InfluxDB 2.7  (time-series metrics → dashboard)               │
                                                                          │
  ┌─────────────────────────────────────────────────────────┐            │
  │  Dagster  (orchestration)                               │            │
  │  - daily kline ingest (02:00 AM)                        │            │
  │  - weekly Iceberg maintenance (Sun 03:00 AM)            │            │
  └─────────────────────────────────────────────────────────┘            │
                                                                          │
  ┌──────────────────────────────────────────────────────────────────────┘
  │  PostgreSQL 16  (Iceberg JDBC catalog + Dagster metadata)
  └──────────────────────────────────────────────────────────
```

---

## Hướng dẫn chạy

### Yêu cầu

- **Docker Desktop** >= 4.x (WSL2 backend tren Windows)
- **RAM toi thieu**: 16 GB (khuyen nghi 20 GB+)
- **Disk**: 20 GB free
- **CPU**: 6 cores+

### 1. Clone repo

```bash
git clone https://github.com/StupidDuck64/TradingView-Style-Crypto-Platform.git
cd TradingView-Style-Crypto-Platform
```

### 2. Kiem tra file `.env`

```env
INFLUX_TOKEN=rOR4d3WHhRWiXF4MSvjM0Kg3yiB_omldxVarArm4R2hMIfu6e5JFx9E2ktgk_Qomj4giZLKbjC-stDelB9FvZw==
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
POSTGRES_USER=iceberg
POSTGRES_PASSWORD=iceberg123
POSTGRES_DB=iceberg_catalog
```

> **Quan trong**: File `.env` da co trong repo nhung **khong duoc commit lai sau khi thay token that**. Them `.env` vao `.gitignore` neu dung token production.

### 3. Build va start toan bo stack

```bash
# Lan dau — build image custom (Flink, Dagster, Producer)
docker compose up -d --build

# Tu lan sau
docker compose up -d
```

### 4. Kiem tra health

```bash
docker compose ps
# Tat ca services phai o trang thai "healthy" hoac "running"
```

### 5. Submit Flink job (real-time ingest)

Flink la **long-running streaming job** — khong qua Dagster, phai submit thu cong sau khi `flink-jobmanager` bao healthy:

```bash
docker exec flink-jobmanager \
  flink run -py /app/ingest_flink_crypto.py \
  -d \
  --jobmanager flink-jobmanager:8081
```

Kiem tra job dang chay tai http://localhost:8081.

### 6. Start Spark Streaming (batch to Iceberg)

Tuong tu, `ingest_crypto.py` la **long-running streaming job** — submit thu cong:

```bash
docker exec spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /app/ingest_crypto.py
```

### Dagster quan ly cai gi?

Dagster chi orchestrate **batch jobs co schedule** — khong quan ly cac streaming job (vi streaming job chay lien tuc, khong co start/end ro rang):

| Job | Chay bang gi | Cach start |
|---|---|---|
| `producer_binance.py` | Docker container (producer) | Tu dong khi `docker compose up` |
| `ingest_flink_crypto.py` | Flink cluster | Submit thu cong (buoc 5) |
| `ingest_crypto.py` | Spark cluster | Submit thu cong (buoc 6) |
| `ingest_historical_iceberg.py` | **Dagster** (02:00 AM) | Tu dong theo schedule, hoac manual trigger tren Dagster UI |
| `iceberg_maintenance.py` | **Dagster** (CN 03:00 AM) | Tu dong theo schedule, hoac manual trigger tren Dagster UI |

Vao http://localhost:3000 de xem status, trigger thu cong, hoac bat/tat schedule.

### Thu tu khoi dong container (docker compose up -d)

Day la thu tu Docker dam bao infrastructure san sang — tat ca chay bang 1 lenh, khong can bat tung cai:

```
postgres ──────────────────────────────────────────────┐
minio ─────────────────────────────────────────────────┤
  └── minio-init (one-shot: tao bucket cryptoprice,    │
                  flink-checkpoints)                   │
kafka ────────────────────────────────┐                │
influxdb ──────────────────────────── │ ──────┐        │
keydb ─────────────────────────────── │ ──────┤        │
                                      │       │        │
                            flink-jobmanager  │        │
                              └── flink-taskmanager    │
                                               │       │
                                         spark-master ─┘
                                           └── spark-worker
                                                     │
                                               trino ─┘
                                         dagster-webserver
                                           └── dagster-daemon
producer (cho kafka healthy)
```

### Port Map

| Service | Port (host) | URL |
|---|---|---|
| Kafka | `9092` | `kafka:9092` (internal) |
| MinIO API | `9000` | http://localhost:9000 |
| MinIO Console | `9001` | http://localhost:9001 |
| InfluxDB | `8086` | http://localhost:8086 |
| PostgreSQL | `5432` | `localhost:5432` |
| KeyDB | `6379` | `localhost:6379` |
| Flink Web UI | `8081` | http://localhost:8081 |
| Spark Master UI | `8082` | http://localhost:8082 |
| Spark History | `18080` | http://localhost:18080 |
| Trino UI | `8083` | http://localhost:8083 |
| Dagster UI | `3000` | http://localhost:3000 |

### Lenh huu ich

```bash
# Xem log mot service
docker compose logs -f kafka
docker compose logs -f flink-jobmanager

# Restart mot service
docker compose restart flink-taskmanager

# Dung toan bo (giu data volumes)
docker compose stop

# Dung va xoa hoan toan (ke ca volumes — mat data)
docker compose down -v

# Scale them Spark worker
docker compose up -d --scale spark-worker=3

# Vao shell container
docker exec -it kafka bash
docker exec -it flink-jobmanager bash
docker exec -it trino trino
```

---

## Qua trinh thu thap va xu ly du lieu

### 1. Thu thap — `producer_binance.py`

```
Binance WebSocket API
  wss://stream.binance.com/stream?streams=btcusdt@ticker/ethusdt@ticker/...
        │
        ▼
  producer_binance.py
  (KafkaProducer, kafka-python)
        │
        ├── topic: crypto_ticker   ← JSON ticker: symbol, price, volume, timestamp
        └── topic: crypto_trades   ← JSON trade: symbol, price, qty, side, tradeId
```

Producer duy tri mot WebSocket connection lien tuc voi Binance. Moi su kien ticker/trade duoc serialize thanh JSON va publish vao Kafka voi `key = symbol` (vi du `BTCUSDT`).

---

### 2. Xu ly real-time — `ingest_flink_crypto.py`

```
Kafka topic: crypto_ticker
        │
        ▼
  Flink Source (FlinkKafkaConsumer / Kafka SQL Table)
        │
        ├─── flatMap: KeyDBWriter
        │         └──► KeyDB  SET "ticker:BTCUSDT" → JSON latest price
        │              Key TTL: khong set → giu mai, overwrite lien tuc
        │
        └─── flatMap: InfluxDBWriter
                  └──► InfluxDB bucket: crypto
                       measurement: market_ticks
                       tags: symbol
                       fields: price (float), volume (float), quote_volume (float)
                       timestamp: event time tu Binance
```

**Checkpointing**: Flink ghi checkpoint vao `s3://flink-checkpoints/` (MinIO) moi 60s. Neu job crash, recovery tu dong tu checkpoint gan nhat — khong mat data.

**NoopSink**: Terminal sink trong Flink job graph duoc gan vao `NoopSink` (khong in ra stdout) thay vi `.print()` de tranh I/O overhead.

---

### 3. Xu ly batch / micro-batch — `ingest_crypto.py`

```
Kafka topics: crypto_ticker + crypto_trades
        │
        ▼
  Spark Structured Streaming
  (readStream tu Kafka, trigger moi 1 phut)
        │
        ├── parse JSON → DataFrame
        ├── cast types, add partition cols (date, hour)
        │
        ├──► Iceberg table: cryptoprice.market_data.ticker_stream
        │         storage: s3://cryptoprice/iceberg/ticker_stream/
        │         format: Parquet, partitioned by (date, hour)
        │
        └──► Iceberg table: cryptoprice.market_data.trade_stream
                  storage: s3://cryptoprice/iceberg/trade_stream/
                  format: Parquet, partitioned by (date, symbol)
```

Moi micro-batch append Parquet files moi vao MinIO va cap nhat Iceberg metadata (snapshot moi) trong PostgreSQL.

---

### 4. Ingest lich su — `ingest_historical_iceberg.py`

```
Binance REST API
  GET /api/v3/klines?symbol=BTCUSDT&interval=1h&limit=1000
        │
        ▼
  Spark batch job (trigger thu cong hoac Dagster 02:00 AM)
        │
        ├── fetch tat ca symbol theo danh sach config
        ├── convert sang DataFrame (open, high, low, close, volume, ...)
        │
        └──► Iceberg table: cryptoprice.market_data.historical_hourly
                  storage: s3://cryptoprice/iceberg/historical_hourly/
                  format: Parquet, partitioned by (symbol, year, month)
                  mode: MERGE INTO (upsert theo symbol + open_time)
```

---

### 5. Bao tri Iceberg — `iceberg_maintenance.py`

```
Spark batch job (Dagster, Chu nhat 03:00 AM)
        │
        ├── expire_snapshots()    → xoa snapshot cu hon 7 ngay
        │       └──► cap nhat iceberg_tables trong PostgreSQL
        │
        ├── remove_orphan_files() → quet MinIO, xoa Parquet khong co snapshot tham chieu
        │       └──► goi S3 API truc tiep tren MinIO
        │
        └── rewrite_data_files()  → compact small files thanh file lon hon
                └──► ghi file Parquet moi vao MinIO, cap nhat metadata pointer trong PostgreSQL
```

---

### 6. Query — Trino

```
Client SQL
        │
        ▼
  Trino coordinator (port 8083)
        │
        ├── doc Iceberg catalog tu PostgreSQL:
        │       DB iceberg_catalog → bang iceberg_tables
        │       → lay metadata_location (path toi file .avro tren MinIO)
        │
        ├── doc metadata files (.avro, .json) tu MinIO
        │
        └── doc data files (Parquet) tu MinIO, tra ket qua ve client
```

Trino **chi doc**, khong ghi vao InfluxDB hay KeyDB.

---

### 7. Orchestration — Dagster

```
Dagster daemon (scheduler loop moi 30s)
        │
        ├── Schedule: daily_kline_ingest (02:00 AM hang ngay)
        │       └── spark-submit ingest_historical_iceberg.py
        │               → ghi vao MinIO + PostgreSQL (Iceberg)
        │
        ├── Schedule: weekly_iceberg_maintenance (Chu nhat 03:00 AM)
        │       └── spark-submit iceberg_maintenance.py
        │               → compact + expire tren MinIO + PostgreSQL
        │
        └── Dagster metadata → DB dagster (PostgreSQL)
                    tables: runs, event_logs, schedules, sensors, partitions, ...
```

---

### So do luong ghi theo tung storage

```
┌─────────────────┬──────────────────────────────────────────────────────┐
│ Storage         │ Ai ghi vao?                  Noi dung                 │
├─────────────────┼──────────────────────────────────────────────────────┤
│ Kafka           │ producer_binance.py           raw ticker/trade JSON   │
│  crypto_ticker  │                                                        │
│  crypto_trades  │                                                        │
├─────────────────┼──────────────────────────────────────────────────────┤
│ KeyDB           │ ingest_flink_crypto.py        latest ticker per symbol│
│  key: ticker:XX │ (KeyDBWriter flatMap)         JSON, overwrite moi tick│
├─────────────────┼──────────────────────────────────────────────────────┤
│ InfluxDB        │ ingest_flink_crypto.py        OHLCV time-series       │
│  meas: market_  │ (InfluxDBWriter flatMap)      tag: symbol             │
│  ticks          │                               field: price, vol, ...  │
├─────────────────┼──────────────────────────────────────────────────────┤
│ MinIO           │ ingest_crypto.py (Spark)      Parquet data files      │
│  s3://cryptopri │ ingest_historical_iceberg.py  theo partition          │
│  ce/iceberg/    │ iceberg_maintenance.py        compact + orphan delete │
│                 │ Flink checkpoint              s3://flink-checkpoints/ │
├─────────────────┼──────────────────────────────────────────────────────┤
│ PostgreSQL      │ Spark (qua Iceberg JDBC)      iceberg_tables,         │
│  iceberg_catalog│ iceberg_maintenance.py        iceberg_namespace_props │
│                 │ Trino (read-only)             snapshot pointers       │
├─────────────────┼──────────────────────────────────────────────────────┤
│ PostgreSQL      │ dagster-webserver (auto)      runs, events, schedules │
│  dagster        │ dagster-daemon                sensors, partitions     │
└─────────────────┴──────────────────────────────────────────────────────┘
```

---

## Tech Stack, Phien ban & Config

### Stack

| Thanh phan | Image / Version | Vai tro |
|---|---|---|
| **Kafka** | `bitnami/kafka:3.9` (KRaft) | Message broker, 2 topics |
| **Flink** | `flink:1.18.1-java11` (custom) | Stream processing real-time |
| **Spark** | `bitnami/spark:3.4.1` | Batch / micro-batch to Iceberg |
| **MinIO** | `minio/minio:latest` | Object storage (S3-compatible) |
| **Iceberg** | runtime `1.5.0` (Scala 2.12) | Table format tren MinIO |
| **InfluxDB** | `influxdb:2.7` | Time-series DB — measurement `market_ticks` |
| **KeyDB** | `eqalpha/keydb:latest` | In-memory cache (Redis-compat) |
| **PostgreSQL** | `postgres:16-alpine` | Iceberg JDBC catalog + Dagster storage |
| **Trino** | `trinodb/trino:442` | Ad-hoc SQL query engine |
| **Dagster** | `1.9.*` (custom) | Workflow orchestration |
| **Producer** | Python 3.11 (custom) | Binance WS → Kafka |

### Key dependencies

| Package | Version | Dung trong |
|---|---|---|
| `apache-flink` | 1.18.1 | ingest_flink_crypto.py |
| `flink-sql-connector-kafka` | 3.1.0-1.18 (JAR) | Flink Kafka source |
| `influxdb-client` | 1.44.0 | ingest_flink_crypto.py |
| `redis` | 5.0.3 | ingest_flink_crypto.py (KeyDB) |
| `iceberg-spark-runtime-3.4_2.12` | 1.5.0 | Spark Iceberg jobs |
| `hadoop-aws` | 3.3.4 | Spark <-> MinIO (S3A) |
| `aws-java-sdk-bundle` | 1.12.262 | Spark <-> MinIO credential |
| `kafka-python` | 2.0.2 | producer_binance.py |
| `dagster` | 1.9.* | orchestration/assets.py |

### Cau truc thu muc

```
cryptoprice/
├── docker-compose.yml                   # Full stack, 13 services
├── .env                                 # Secrets (KHONG commit git)
├── spark-defaults.conf                  # Spark config (mounted vao spark-master)
├── README.md
│
├── producer_binance.py                  # Binance WebSocket → Kafka producer
├── ingest_flink_crypto.py               # Flink job: Kafka → KeyDB + InfluxDB
├── ingest_crypto.py                     # Spark job: Kafka → Iceberg (streaming)
├── ingest_historical_iceberg.py         # Spark job: Binance REST → Iceberg (batch)
├── iceberg_maintenance.py               # Spark job: compact / expire snapshots
│
├── orchestration/
│   ├── assets.py                        # Dagster assets (Spark job wrappers)
│   └── workspace.yaml                   # Dagster workspace config
│
└── docker/
    ├── flink/
    │   ├── Dockerfile                   # PyFlink 1.18.1 + Kafka connector JAR
    │   └── flink-conf.yaml              # Flink cluster config (bind 0.0.0.0)
    │
    ├── trino/
    │   └── etc/                         # Mount vao /etc/trino trong container
    │       ├── config.properties        # coordinator, port, memory
    │       ├── jvm.config               # -Xmx2G, G1GC
    │       ├── node.properties          # node.id, data-dir
    │       ├── log.properties           # log level
    │       └── catalog/
    │           └── iceberg.properties   # connector → JDBC/PG + MinIO
    │
    ├── dagster/
    │   ├── Dockerfile                   # Python 3.11 + Spark 3.4.1 + Dagster 1.9
    │   └── dagster.yaml                 # Storage: postgres, scheduler, launcher
    │
    ├── producer/
    │   └── Dockerfile                   # Python 3.11 + kafka-python
    │
    └── postgres/
        └── init.sql                     # Khoi tao 2 databases + Iceberg schema
```

### PostgreSQL phuc vu 2 he thong

```
postgres (port 5432)
├── DB: iceberg_catalog       ← Iceberg JDBC catalog (Spark + Trino)
│   ├── table: iceberg_tables               (metadata location cua moi table)
│   └── table: iceberg_namespace_properties (properties cua database/namespace)
│
└── DB: dagster               ← Dagster metadata (runs, events, schedules, sensors)
    └── (auto-created boi dagster-webserver khi start lan dau)
```

> Iceberg **khong luu data vao PostgreSQL** — PostgreSQL chi luu *con tro metadata* (path toi file `.avro` tren MinIO). Data thuc te (Parquet files) nam tren MinIO tai `s3://cryptoprice/iceberg/`.

### Phan bo tai nguyen

> Dua tren may khuyen nghi: **20 GB RAM**, **8 CPU cores**, **SSD**.

| Service | RAM (limit) | RAM (thuong dung) | CPU cores | Ghi chu |
|---|---|---|---|---|
| **kafka** | 1.5 GB | ~600 MB | 1.0 | JVM heap 1 GB, KRaft overhead |
| **minio** | 1 GB | ~300 MB | 0.5 | Tang neu co nhieu concurrent write |
| **minio-init** | 64 MB | ~30 MB | 0.1 | One-shot, thoat sau khi init |
| **influxdb** | 1.5 GB | ~500 MB | 0.5 | TSI index + write buffer |
| **postgres** | 512 MB | ~200 MB | 0.5 | Iceberg metadata + Dagster |
| **keydb** | 512 MB | ~150 MB | 0.5 | In-memory, ~100k keys latest ticker |
| **flink-jobmanager** | 1.8 GB | ~1.6 GB | 1.0 | `jobmanager.memory.process.size: 1600m` |
| **flink-taskmanager** | 2 GB | ~1.7 GB | 1.0 | `taskmanager.memory.process.size: 1728m` |
| **spark-master** | 1 GB | ~400 MB | 0.5 | Master chi schedule, khong chay task |
| **spark-worker** | 5 GB | ~4.5 GB | 2.0 | `SPARK_WORKER_MEMORY=4G`, driver ~512 MB overhead |
| **trino** | 2.5 GB | ~1.5 GB | 1.0 | JVM `-Xmx2G`, heap 2 GB |
| **dagster-webserver** | 1 GB | ~500 MB | 0.5 | UI + metadata queries |
| **dagster-daemon** | 512 MB | ~300 MB | 0.5 | Scheduler + sensor loops |
| **producer** | 256 MB | ~100 MB | 0.2 | Python WS client, I/O-bound |
| **Tong** | **~19.2 GB** | **~12.4 GB** | **~9.8 cores** | |

#### Goi y neu may chi co 16 GB RAM

```bash
# Tat Trino neu khong query batch
docker compose stop trino

# Giam Spark worker memory xuong 2G — sua trong docker-compose.yml:
# SPARK_WORKER_MEMORY: 2G

# Tat Spark History Server (port 18080) neu khong debug
# Xoa dong SPARK_HISTORY_OPTS trong spark-master
```

Tong RAM tieu thu khi tat Trino + Spark History: giam ~**2.5 GB**, con ~**10 GB** thuc te.

#### Resource limits (production)

```yaml
services:
  spark-worker:
    deploy:
      resources:
        limits:
          memory: 5g
          cpus: "2.0"
        reservations:
          memory: 4g
          cpus: "1.5"

  flink-taskmanager:
    deploy:
      resources:
        limits:
          memory: 2g
          cpus: "1.0"

  trino:
    deploy:
      resources:
        limits:
          memory: 2500m
          cpus: "1.0"
```

> `deploy.resources` chi co hieu luc voi Docker Swarm. Voi `docker compose up` thong thuong, dung `mem_limit` + `cpus` truc tiep o cap service.
