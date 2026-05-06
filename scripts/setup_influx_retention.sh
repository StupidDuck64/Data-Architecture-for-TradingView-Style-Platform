#!/bin/bash
<<<<<<<< HEAD:scripts/setup_influx_retention.sh
# InfluxDB Retention Policy Setup
# This script sets up retention policies for candle data:
# - 1s candles: 7 days retention (sliding window)
# - 1m candles: 90 days retention (sliding window)
========
# setup_influx_retention.sh
# ─────────────────────────────────────────────────────────────────────────────
# Set InfluxDB bucket retention policy to 90 days for 1m candles
# Updated: Now keeps 90 days for all 440 USDT symbols
# ─────────────────────────────────────────────────────────────────────────────
>>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA):setup_influx_retention.sh

set -e

INFLUX_URL="${INFLUX_URL:-http://localhost:8086}"
INFLUX_TOKEN="${INFLUX_TOKEN}"
INFLUX_ORG="${INFLUX_ORG:-vi}"
INFLUX_BUCKET="${INFLUX_BUCKET:-crypto}"

RETENTION_SECONDS=$((90 * 24 * 3600))  # 90 days = 7,776,000 seconds

echo "=== InfluxDB Retention Policy Setup ==="
echo "Bucket: $INFLUX_BUCKET"
echo "Retention: 90 days (7,776,000 seconds)"
echo "Note: Chart 5m/15m/1h will be aggregated on-the-fly from 1m data"
echo ""

# Wait for InfluxDB to be ready
echo "Waiting for InfluxDB..."
for i in {1..30}; do
    if docker exec influxdb influx ping > /dev/null 2>&1; then
        echo "InfluxDB is ready!"
        break
    fi
    echo "Attempt $i/30..."
    sleep 2
done

# Update bucket retention policy
echo ""
echo "Setting retention policy for bucket '$INFLUX_BUCKET'..."
docker exec influxdb influx bucket update \
    --name "$INFLUX_BUCKET" \
    --retention "${RETENTION_SECONDS}s" \
    --org "$INFLUX_ORG" \
    --token "$INFLUX_TOKEN"

<<<<<<<< HEAD:scripts/setup_influx_retention.sh
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
========
echo ""
echo "✓ Retention policy set to 90 days!"
echo ""
echo "Verify with:"
echo "  docker exec influxdb influx bucket list --org $INFLUX_ORG --token $INFLUX_TOKEN"
echo ""
echo "Next steps:"
echo "  1. Run initial_populate_influx.py to populate 90 days for 440 symbols"
echo "  2. Higher timeframes (5m, 15m, 1h) will be calculated from 1m data"
>>>>>>>> 95fa5d0 (replace KeyDB by Redis sentinal HA & Kafka HA):setup_influx_retention.sh

from(bucket: "'"${INFLUX_BUCKET}"'")
  |> range(start: -95d, stop: -90d)
  |> filter(fn: (r) => r["_measurement"] == "candles")
  |> filter(fn: (r) => r["interval"] == "1m")
  |> drop()
' || echo "Task might already exist, continuing..."

echo "InfluxDB retention setup completed!"
echo "  - 1s candles: 7 days retention (sliding window)"
echo "  - 1m candles: 90 days retention (sliding window)"
