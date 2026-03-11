#!/bin/bash

# ==========================================
# 1. KHÃ¡Â»Å¾I Ã„ÂÃ¡Â»ËœNG FLINK
# ==========================================
echo "Ã„Âang chÃ¡Â»Â Flink Cluster khÃ¡Â»Å¸i Ã„â€˜Ã¡Â»â„¢ng Ã¡Â»Å¸ cÃ¡Â»â€¢ng 8081..."
# LÃ¡Â»â€¡nh curl -s Ã¡ÂºÂ©n output, chÃ¡Â»â€° cÃ¡ÂºÂ§n kÃ¡ÂºÂ¿t nÃ¡Â»â€˜i TCP thÃƒÂ nh cÃƒÂ´ng lÃƒÂ  thoÃƒÂ¡t vÃƒÂ²ng lÃ¡ÂºÂ·p
until curl -s http://127.0.0.1:8081 > /dev/null; do
    printf '.'
    sleep 5
done
echo " Flink Ã„â€˜ÃƒÂ£ sÃ¡ÂºÂµn sÃƒÂ ng!"

# Ã„ÂÃ¡Â»Â£i thÃƒÂªm 10s Ã„â€˜Ã¡Â»Æ’ TaskManager kÃ¡ÂºÂ¿t nÃ¡Â»â€˜i hÃ¡ÂºÂ³n vÃƒÂ o JobManager
sleep 10

# Submit Flink Job (CÃ¡Â»Â -d chÃ¡ÂºÂ¡y ngÃ¡ÂºÂ§m)
docker exec flink-jobmanager flink run -d -py /app/src/ingest_flink_crypto.py
echo "Ã„ÂÃƒÂ£ submit Flink Job!"


# ==========================================
# 2. KHÃ¡Â»Å¾I Ã„ÂÃ¡Â»ËœNG SPARK
# ==========================================
echo "Ã„Âang chÃ¡Â»Â Spark Master khÃ¡Â»Å¸i Ã„â€˜Ã¡Â»â„¢ng Ã¡Â»Å¸ cÃ¡Â»â€¢ng 8080..."
# LÃ¡Â»â€¡nh curl -s kiÃ¡Â»Æ’m tra cÃ¡Â»â€¢ng 8080 cÃ¡Â»Â§a Spark
until curl -s http://127.0.0.1:8080 > /dev/null; do
    printf '.'
    sleep 5
done
echo " Spark Master Ã„â€˜ÃƒÂ£ sÃ¡ÂºÂµn sÃƒÂ ng!"

# Submit Spark Job chÃ¡ÂºÂ¡y ngÃ¡ÂºÂ§m
docker exec -d spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5" \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  /app/src/ingest_crypto.py

echo "Ã„ÂÃƒÂ£ submit thÃƒÂ nh cÃƒÂ´ng 2 Streaming Jobs!"
