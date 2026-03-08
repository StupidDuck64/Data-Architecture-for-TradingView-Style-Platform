FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.32.* \
    websocket-client==1.8.* \
    fastavro==1.9.* \
    lz4

WORKDIR /app

CMD ["python", "src/producer_binance.py"]
