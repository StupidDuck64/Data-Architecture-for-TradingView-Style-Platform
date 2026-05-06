#!/bin/bash
set -e

echo "=== Creating Kafka topics with replication factor 3 ==="

BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# Wait for all brokers to be ready
echo "Waiting for Kafka brokers to be ready..."
sleep 10

# Topic configurations: name:partitions:replication
TOPICS=(
  "crypto_ticker:12:3"
  "crypto_klines:12:3"
  "crypto_trades:12:3"
  "crypto_depth:12:3"
)

for topic_config in "${TOPICS[@]}"; do
  IFS=':' read -r topic partitions replication <<< "$topic_config"

  echo ""
  echo "Creating topic: $topic (partitions=$partitions, replication=$replication)"

  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --create \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    --config min.insync.replicas=2 \
    --config compression.type=lz4 \
    --config retention.ms=172800000 \
    --config segment.ms=3600000 \
    --if-not-exists

  echo "✓ Created topic: $topic"
done

echo ""
echo "=== Verifying topics ==="
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server "$BOOTSTRAP_SERVERS"

echo ""
echo "=== Topic details ==="
for topic_config in "${TOPICS[@]}"; do
  IFS=':' read -r topic _ _ <<< "$topic_config"

  echo ""
  echo "=== $topic ==="
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --describe \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$topic"
done

echo ""
echo "✅ All topics created successfully!"
