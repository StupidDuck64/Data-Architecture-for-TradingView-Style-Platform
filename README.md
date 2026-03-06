Dự án streaming giá crypto real-time từ Binance WebSocket, xử lý bằng Flink + Spark,chia thành 2 luồng streaming và batch, luồng streaming từ Flink đổ dữ liệu vào KeyDB và InfluxDB để phục vụ dữ liệu realtime, luồng batch lấy dữ liệu từ Spark và lưu trên MinIO (Iceberg), query bằng Trino, orchestrate bằng Dagster.

---

## Kiến trúc

![alt text](image.png) 

## Yêu cầu

- Docker Desktop >= 4.x (WSL2)
- RAM: 16 GB+ (khuyến nghị 20 GB)
- Disk: 20 GB free
- CPU: 6 cores+

## Khởi động

```powershell
# 1. Build & start
docker compose up -d --build

# 2. Submit Flink streaming job
docker exec flink-jobmanager flink run -d -py /app/src/ingest_flink_crypto.py

# 3. Submit Spark streaming job
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5" --conf spark.driver.memory=2g --conf spark.executor.memory=2g /app/src/ingest_crypto.py

# 4. (Tuỳ chọn) Lấy toàn bộ dữ liệu giá lịch sử từ Binance → Iceberg nếu gấp, còn không thì 2h sáng Dagster nó tự chạy
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2" --conf spark.driver.memory=2g --conf spark.executor.memory=2g /app/src/ingest_historical_iceberg.py
```

> Bước 2-3 submit streaming job thủ công (chạy liên tục, không qua Dagster). Bước 4 chạy 1 lần để nạp dữ liệu lịch sử, sau đó Dagster tự chạy lại lúc 2:00 AM hằng ngày.

## Tài khoản truy cập

| Service          | URL                      | Username     | Password       |
|:-----------------|:-------------------------|:-------------|:---------------|
| InfluxDB         | http://localhost:8086    | `admin`      | `adminpass123` |
| MinIO Console    | http://localhost:9001    | `minioadmin` | `minioadmin`   |
| PostgreSQL       | localhost:5432           | `iceberg`    | `iceberg123`   |
| Flink UI         | http://localhost:8081    | —            | —              |
| Spark Master UI  | http://localhost:8082    | —            | —              |
| Spark History    | http://localhost:18080   | —            | —              |
| Trino UI         | http://localhost:8083    | (điền bừa )  | —              |
| Dagster UI       | http://localhost:3000    | —            | —              |
| KeyDB            | localhost:6379           | —            | —              |
| Kafka            | localhost:9092           | —            | —              |

## Data pipeline

| Script                         | Engine | Trigger                      | Input           | Output                        |
|:-------------------------------|:-------|:-----------------------------|:----------------|:------------------------------|
| `producer_binance.py`          | Python | Auto (docker compose up)     | Binance WS      | Kafka topics                  |
| `ingest_flink_crypto.py`       | Flink  | Submit thủ công              | Kafka            | KeyDB + InfluxDB              |
| `ingest_crypto.py`             | Spark  | Submit thủ công              | Kafka            | MinIO/Iceberg                 |
| `ingest_historical_iceberg.py` | Spark  | Dagster schedule (2:00 AM)   | Binance REST API | MinIO/Iceberg                 |
| `iceberg_maintenance.py`       | Spark  | Dagster schedule (CN 3:00 AM)| Iceberg tables   | Compact + expire snapshots    |
| `backfill_influx.py`           | Python | Auto on startup (one-shot)   | Binance REST API | InfluxDB (fill gap khi tắt máy) |

## Phân bổ tài nguyên

| Service            | Image                        | RAM (config) | RAM (thực tế) | CPU  | Ghi chú                                      |
|:-------------------|:-----------------------------|-------------:|---------------:|-----:|:----------------------------------------------|
| kafka              | apache/kafka:3.9.0           |       1.5 GB |        ~600 MB |  1.0 | KRaft mode, JVM heap ~1 GB                    |
| minio              | minio/minio:latest           |       1.0 GB |        ~300 MB |  0.5 | S3-compatible object storage                  |
| minio-init         | minio/mc:latest              |      64.0 MB |         ~30 MB |  0.1 | One-shot, tạo bucket rồi thoát               |
| influxdb           | influxdb:2.7                 |       1.5 GB |        ~500 MB |  0.5 | Time-series, org=vi, bucket=crypto            |
| postgres           | postgres:16-alpine           |     512.0 MB |        ~200 MB |  0.5 | Iceberg catalog + Dagster metadata            |
| keydb              | eqalpha/keydb:latest         |     512.0 MB |        ~150 MB |  0.5 | Redis-compatible in-memory cache              |
| flink-jobmanager   | cryptoprice/flink:1.18.1     |       1.6 GB |         ~1.6 GB |  1.0 | `jobmanager.memory.process.size: 1600m`       |
| flink-taskmanager  | cryptoprice/flink:1.18.1     |       1.7 GB |         ~1.7 GB |  1.0 | `taskmanager.memory.process.size: 1728m`      |
| spark-master       | apache/spark:3.5.5           |       1.0 GB |        ~400 MB |  0.5 | Chỉ schedule, không chạy task                 |
| spark-worker       | apache/spark:3.5.5           |       4.0 GB |         ~3.5 GB |  2.0 | `SPARK_WORKER_MEMORY=3G`, 2 cores             |
| trino              | trinodb/trino:442            |       2.0 GB |         ~1.5 GB |  1.0 | JVM `-Xmx2G`, query Iceberg trên MinIO       |
| dagster-webserver  | cryptoprice/dagster:latest   |       1.0 GB |        ~500 MB |  0.5 | UI + metadata queries                         |
| dagster-daemon     | cryptoprice/dagster:latest   |     512.0 MB |        ~300 MB |  0.5 | Scheduler loop                                |
| producer           | cryptoprice-producer         |     256.0 MB |        ~100 MB |  0.2 | Python WebSocket client                       |
|                    |                              |              |                |      |                                               |
| **TỔNG**           |                              | **~17.2 GB** |  **~11.4 GB**  | **9.3** |                                            |

> Nếu máy chỉ có 16 GB RAM: tắt Trino (`docker compose stop trino`) giảm ~2 GB.

## Cấu trúc thư mục

```
cryptoprice_local/
├── docker-compose.yml          # 14 services
├── .env                        # Secrets (INFLUX_TOKEN, MINIO, POSTGRES)
├── spark-defaults.conf         # Spark config
├── src/
│   ├── producer_binance.py     # Binance WS → Kafka
│   ├── ingest_flink_crypto.py  # Flink: Kafka → KeyDB + InfluxDB
│   ├── ingest_crypto.py        # Spark Streaming: Kafka → Iceberg
│   ├── ingest_historical_iceberg.py  # Spark batch: REST API → Iceberg
│   └── iceberg_maintenance.py  # Spark: compact/expire Iceberg
├── orchestration/
│   ├── assets.py               # Dagster assets
│   └── workspace.yaml
└── docker/
    ├── flink/                  # Dockerfile + flink-conf.yaml
    ├── dagster/                # Dockerfile + dagster.yaml
    ├── producer/               # Dockerfile
    ├── trino/etc/              # Trino config + Iceberg catalog
    ├── spark/                  # Dockerfile
    └── postgres/init.sql       # Init iceberg_catalog + dagster DB
```

## Lệnh thường dùng

```bash
docker compose stop                  # Dừng (giữ data)
docker compose down -v               # Xoá hết (kể cả data)
docker compose logs -f <service>     # Xem log
docker compose restart <service>     # Restart 1 service
```
