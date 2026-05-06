FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests==2.32.3 \
    influxdb-client==1.44.0

COPY src/common /app/src/common
COPY src/exchanges /app/src/exchanges
COPY src/batch /app/src/batch

<<<<<<< HEAD
CMD ["python", "src/batch/backfill.py", "--mode", "influx"]
=======
# Default command: gap backfill
CMD ["python", "/app/backfill_historical.py", "--mode", "influx"]
>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA)
