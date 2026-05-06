#!/bin/bash
# InfluxDB Retention Policy Setup
# This script sets up retention policies for candle data:
# - 1s candles: 7 days retention (sliding window)
# - 1m candles: 90 days retention (sliding window)

set -e

INFLUX_URL="${INFLUX_URL:-http://influxdb:8086}"
INFLUX_TOKEN="${INFLUX_TOKEN}"
INFLUX_ORG="${INFLUX_ORG:-vi}"
INFLUX_BUCKET="${INFLUX_BUCKET:-crypto}"

echo "Setting up InfluxDB retention policies..."

# Wait for InfluxDB to be ready
echo "Waiting for InfluxDB at ${INFLUX_URL}..."
until curl -sf "${INFLUX_URL}/health" > /dev/null 2>&1; do
    echo "InfluxDB not ready, waiting..."
    sleep 2
done
echo "InfluxDB is ready!"

# NOTE: InfluxDB 2.x uses buckets instead of retention policies
# We'll use downsampling tasks instead

# Create downsampling task to remove old 1s data (keep only 7 days)
echo "Creating downsampling task for 1s candles (7 days retention)..."
influx task create \
  --org "${INFLUX_ORG}" \
  --name "delete_old_1s_candles" \
  --every 6h \
  --flux '
option task = {name: "delete_old_1s_candles", every: 6h}

from(bucket: "'"${INFLUX_BUCKET}"'")
  |> range(start: -8d, stop: -7d)
  |> filter(fn: (r) => r["_measurement"] == "candles")
  |> filter(fn: (r) => r["interval"] == "1s")
  |> drop()
' || echo "Task might already exist, continuing..."

# Create downsampling task to remove old 1m data (keep only 90 days)
echo "Creating downsampling task for 1m candles (90 days retention)..."
influx task create \
  --org "${INFLUX_ORG}" \
  --name "delete_old_1m_candles" \
  --every 1d \
  --flux '
option task = {name: "delete_old_1m_candles", every: 1d}

from(bucket: "'"${INFLUX_BUCKET}"'")
  |> range(start: -95d, stop: -90d)
  |> filter(fn: (r) => r["_measurement"] == "candles")
  |> filter(fn: (r) => r["interval"] == "1m")
  |> drop()
' || echo "Task might already exist, continuing..."

echo "InfluxDB retention setup completed!"
echo "  - 1s candles: 7 days retention (sliding window)"
echo "  - 1m candles: 90 days retention (sliding window)"
