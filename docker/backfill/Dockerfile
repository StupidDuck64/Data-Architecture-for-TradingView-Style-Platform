FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests==2.32.3 \
    influxdb-client==1.44.0

COPY src/backfill_historical.py /app/backfill_historical.py

CMD ["python", "/app/backfill_historical.py", "--mode", "influx"]
