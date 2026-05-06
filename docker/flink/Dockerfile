FROM flink:1.18.1-java11

# ── Python 3.11 ───────────────────────────────────────────────────────────────
RUN apt-get update -o Acquire::Retries=3 && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev wget curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────────────────
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    influxdb-client==1.44.0 \
    redis==5.0.3

# ── Kafka SQL connector JAR (Flink 1.18) ─────────────────────────────────────
# Avro Confluent Registry format JAR (for avro-confluent deserialization)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.18.1/flink-sql-avro-confluent-registry-1.18.1.jar

# ── Flink config ──────────────────────────────────────────────────────────────
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# ── Fix checkpoint directory permissions ──────────────────────────────────────
RUN mkdir -p /tmp/flink-checkpoints && chown -R flink:flink /tmp/flink-checkpoints

ENV PYFLINK_PYTHON=python3
