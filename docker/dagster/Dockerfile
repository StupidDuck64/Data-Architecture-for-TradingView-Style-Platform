# Rebase on apache/spark:3.5.5 — Java + Spark already included, no apt needed
FROM apache/spark:3.5.5

USER root

# ── Python packages ───────────────────────────────────────────────────────────
RUN python3 -m ensurepip --upgrade 2>/dev/null || true && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
        dagster==1.8.* \
        dagster-webserver==1.8.* \
        dagster-postgres \
        requests \
        pyarrow && \
    # Downgrade grpcio-health-checking to a version compatible with protobuf 4.x \
    pip3 install --no-cache-dir --force-reinstall \
        "grpcio-health-checking>=1.44.0,<1.62.0"

# ── Dagster home & config ─────────────────────────────────────────────────────
RUN mkdir -p /dagster-home
COPY dagster.yaml /dagster-home/dagster.yaml
ENV DAGSTER_HOME=/dagster-home
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
# Use pyspark bundled with the spark image (avoids pip download of 317MB)
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

WORKDIR /app
