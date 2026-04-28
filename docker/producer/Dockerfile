FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY docker/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/common /app/src/common
COPY src/exchanges /app/src/exchanges
COPY src/producer /app/src/producer
COPY schemas /app/schemas

CMD ["python", "src/producer/main.py"]
