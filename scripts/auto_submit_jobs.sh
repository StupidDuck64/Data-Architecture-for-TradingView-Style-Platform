#!/bin/bash

FLINK_HEALTH_URL="${FLINK_HEALTH_URL:-http://127.0.0.1:8081}"
SPARK_HEALTH_URL="${SPARK_HEALTH_URL:-http://127.0.0.1:8080}"

# ==========================================
# 1. START FLINK
# ==========================================
echo "Waiting for Flink Cluster to be ready on port 8081..."
# curl -s hides output; loop exits once the endpoint is reachable
until curl -s "$FLINK_HEALTH_URL" > /dev/null; do
    printf '.'
    sleep 5
done
echo " Flink is ready!"

# Wait 10 more seconds so TaskManager fully connects to JobManager
sleep 10

# Submit Flink job in detached mode (-d)
docker exec flink-jobmanager flink run -d -py /app/src/ingest_flink_crypto.py
echo "Submitted Flink job."


# ==========================================
# 2. START SPARK
# ==========================================
echo "Waiting for Spark Master to be ready on port 8080..."
# curl -s checks Spark master endpoint availability
until curl -s "$SPARK_HEALTH_URL" > /dev/null; do
    printf '.'
    sleep 5
done
echo " Spark Master is ready!"

# Submit Spark job in detached mode
docker exec -d spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5" \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  /app/src/ingest_crypto.py

echo "Submitted both streaming jobs successfully!"
