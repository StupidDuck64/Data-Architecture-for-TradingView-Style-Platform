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
# Create a proper Python package structure
docker exec flink-jobmanager bash -c "cd /app/src && python3 -c \"
import zipfile
import os

with zipfile.ZipFile('/tmp/deps.zip', 'w') as zf:
    # Add common module with proper structure
    for root, dirs, files in os.walk('common'):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                zf.write(filepath, filepath)

    # Add processing/writers module with proper structure
    # Include __init__.py to make it a proper package
    writers_init = 'processing/writers/__init__.py'
    if os.path.exists(writers_init):
        zf.write(writers_init, 'writers/__init__.py')
    else:
        # Create empty __init__.py if it doesn't exist
        zf.writestr('writers/__init__.py', '')

    for root, dirs, files in os.walk('processing/writers'):
        for file in files:
            if file.endswith('.py') and file != '__init__.py':
                filepath = os.path.join(root, file)
                # Write as writers/filename.py (flatten structure)
                zf.write(filepath, 'writers/' + file)
\"
"

# Submit with the zip file
docker exec flink-jobmanager bash -c "cd /app/src/processing && flink run -d -py pipeline.py --pyFiles /tmp/deps.zip"
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
  /app/src/lakehouse/pipeline.py

echo "Submitted both streaming jobs successfully!"
