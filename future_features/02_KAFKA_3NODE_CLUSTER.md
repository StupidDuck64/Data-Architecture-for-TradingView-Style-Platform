# Feature 2: Kafka 3-Node Cluster (High Availability)

## 📋 Tổng quan

**Mục tiêu:** Nâng cấp Kafka từ single-node lên 3-node cluster với replication factor 3 để đạt zero data loss.

**Lý do cần thiết:**
- Kafka hiện tại là single point of failure
- Không có replication → mất message khi broker sập
- Không scale được throughput

**Kiến trúc:**
```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Kafka-1     │   │  Kafka-2     │   │  Kafka-3     │
│  (Broker 1)  │   │  (Broker 2)  │   │  (Broker 3)  │
│  Controller  │   │              │   │              │
└──────────────┘   └──────────────┘   └──────────────┘
       │                  │                  │
       └──────────────────┴──────────────────┘
                          │
                   Replication Factor = 3
                   Min ISR = 2
                   12 Partitions/Topic
```

---

## 🔧 Implementation

### Step 1: Docker Compose - 3 Kafka Brokers

**File:** `docker-compose.yml`

```yaml
services:
  kafka-1:
    image: apache/kafka:3.9.0
    container_name: kafka-1
    hostname: kafka-1
    restart: unless-stopped
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 48
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_COMPRESSION_TYPE: lz4
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    networks:
      - crypto-net
    deploy:
      resources:
        limits:
          memory: 2G
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

  kafka-2:
    image: apache/kafka:3.9.0
    container_name: kafka-2
    hostname: kafka-2
    restart: unless-stopped
    ports:
      - "9093:9092"
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 48
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_COMPRESSION_TYPE: lz4
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    volumes:
      - kafka-2-data:/var/lib/kafka/data
    networks:
      - crypto-net
    deploy:
      resources:
        limits:
          memory: 2G
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

  kafka-3:
    image: apache/kafka:3.9.0
    container_name: kafka-3
    hostname: kafka-3
    restart: unless-stopped
    ports:
      - "9094:9092"
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 48
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_COMPRESSION_TYPE: lz4
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    volumes:
      - kafka-3-data:/var/lib/kafka/data
    networks:
      - crypto-net
    deploy:
      resources:
        limits:
          memory: 2G
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

volumes:
  kafka-1-data:
  kafka-2-data:
  kafka-3-data:
```

---

### Step 2: Create Topics Script

**File:** `scripts/create_kafka_topics.sh`

```bash
#!/bin/bash
set -e

echo "Creating Kafka topics with replication factor 3..."

BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# Topic configurations: name:partitions:replication
TOPICS=(
  "crypto_ticker:12:3"
  "crypto_klines:12:3"
  "crypto_trades:12:3"
  "crypto_depth:12:3"
)

for topic_config in "${TOPICS[@]}"; do
  IFS=':' read -r topic partitions replication <<< "$topic_config"
  
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
echo "Verifying topics..."
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server "$BOOTSTRAP_SERVERS"

echo ""
echo "Topic details:"
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
```

---

### Step 3: Producer - Multi-threaded với Partitioning

**File:** `src/common/kafka_client.py`

```python
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging
import hashlib

logger = logging.getLogger(__name__)

class KafkaClientHA:
    """
    High-availability Kafka client
    
    Features:
    - 3-broker cluster support
    - Consistent hashing for partitioning
    - Idempotent producer (exactly-once)
    - Automatic retries
    """
    
    def __init__(self):
        self.bootstrap_servers = [
            'kafka-1:9092',
            'kafka-2:9092',
            'kafka-3:9092'
        ]
    
    def create_producer(self):
        """
        Create Kafka producer with HA config
        
        Returns:
            KafkaProducer instance
        """
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks='all',  # Wait for all in-sync replicas
            retries=3,
            max_in_flight_requests_per_connection=5,
            compression_type='lz4',
            linger_ms=10,
            batch_size=32768,
            enable_idempotence=True,  # Exactly-once semantics
            on_send_error=self._on_send_error
        )
    
    def create_consumer(self, group_id: str, topics: list):
        """
        Create Kafka consumer with HA config
        
        Args:
            group_id: Consumer group ID
            topics: List of topics to subscribe
        
        Returns:
            KafkaConsumer instance
        """
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Manual commit for at-least-once
            max_poll_records=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
    
    def get_partition(self, key: str, num_partitions: int) -> int:
        """
        Consistent hashing for key → partition
        
        Args:
            key: Partition key (e.g., symbol)
            num_partitions: Total number of partitions
        
        Returns:
            Partition number (0 to num_partitions-1)
        """
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_value % num_partitions
    
    def _on_send_error(self, exc):
        """Callback for send errors"""
        logger.error("kafka_send_error", error=str(exc))
```

**File:** `src/producer/main.py`

```python
import asyncio
import queue
from concurrent.futures import ThreadPoolExecutor
from src.common.kafka_client import KafkaClientHA
from src.common.avro_serializer import AvroSerializer
import logging

logger = logging.getLogger(__name__)

class MultiThreadedProducer:
    """
    Multi-threaded Kafka producer
    
    Features:
    - 4 worker threads
    - Non-blocking queue
    - Consistent partitioning
    - Metrics tracking
    """
    
    def __init__(self, num_threads=4):
        self.num_threads = num_threads
        self.kafka_client = KafkaClientHA()
        
        # Create one producer per thread
        self.producers = [
            self.kafka_client.create_producer()
            for _ in range(num_threads)
        ]
        
        # Message queue
        self.message_queue = queue.Queue(maxsize=10000)
        
        # Thread pool
        self.executor = ThreadPoolExecutor(max_workers=num_threads)
        
        # Avro serializer
        self.avro = AvroSerializer()
        
        # Metrics
        self.messages_sent = 0
        self.messages_failed = 0
    
    def start(self):
        """Start worker threads"""
        for i in range(self.num_threads):
            self.executor.submit(self._worker, i)
        logger.info("producer_started", num_threads=self.num_threads)
    
    def _worker(self, worker_id: int):
        """
        Worker thread - consume from queue and send to Kafka
        
        Args:
            worker_id: Worker thread ID
        """
        producer = self.producers[worker_id]
        
        while True:
            try:
                # Get message from queue
                topic, key, value, partition = self.message_queue.get(timeout=1)
                
                # Send to Kafka
                future = producer.send(
                    topic,
                    key=key,
                    value=value,
                    partition=partition
                )
                
                # Wait for ack
                record_metadata = future.get(timeout=10)
                
                self.messages_sent += 1
                self.message_queue.task_done()
                
                logger.debug("kafka_send_success",
                    worker_id=worker_id,
                    topic=topic,
                    partition=record_metadata.partition,
                    offset=record_metadata.offset)
                
            except queue.Empty:
                continue
            except KafkaError as e:
                self.messages_failed += 1
                logger.error("kafka_send_failed", 
                    worker_id=worker_id, 
                    error=str(e))
            except Exception as e:
                logger.error("worker_error", 
                    worker_id=worker_id, 
                    error=str(e))
    
    async def send_async(self, topic: str, symbol: str, data: dict):
        """
        Non-blocking send - put to queue
        
        Args:
            topic: Kafka topic
            symbol: Symbol (used as partition key)
            data: Message data
        """
        try:
            # Serialize
            key = symbol.encode()
            value = self.avro.serialize(topic, data)
            
            # Get partition
            partition = self.kafka_client.get_partition(symbol, 12)
            
            # Put to queue (non-blocking)
            self.message_queue.put_nowait((topic, key, value, partition))
            
        except queue.Full:
            logger.warning("queue_full", topic=topic, symbol=symbol)
            # Fallback: block until space available
            self.message_queue.put((topic, key, value, partition))
    
    def get_metrics(self) -> dict:
        """Get producer metrics"""
        return {
            "messages_sent": self.messages_sent,
            "messages_failed": self.messages_failed,
            "queue_depth": self.message_queue.qsize(),
            "queue_capacity": self.message_queue.maxsize
        }

# Global instance
producer = MultiThreadedProducer(num_threads=4)
producer.start()

async def send_kline(symbol: str, kline: dict):
    """Send kline message"""
    await producer.send_async('crypto_klines', symbol, kline)

async def send_ticker(symbol: str, ticker: dict):
    """Send ticker message"""
    await producer.send_async('crypto_ticker', symbol, ticker)

async def send_trade(symbol: str, trade: dict):
    """Send trade message"""
    await producer.send_async('crypto_trades', symbol, trade)

async def send_depth(symbol: str, depth: dict):
    """Send depth message"""
    await producer.send_async('crypto_depth', symbol, depth)
```

---

### Step 4: Flink - Increase Parallelism

**File:** `src/processing/pipeline.py`

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

def main():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism = number of partitions
    env.set_parallelism(12)
    
    # Checkpoint configuration
    env.enable_checkpointing(120000)  # 2 minutes
    
    # Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka-1:9092,kafka-2:9092,kafka-3:9092') \
        .set_topics('crypto_klines') \
        .set_group_id('flink-kline-processor') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()
    
    # Create stream
    kline_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10)),
        "kline_source"
    )
    
    # Process with parallelism
    kline_stream \
        .key_by(lambda x: x['symbol']) \
        .process(KlineWindowAggregator()) \
        .set_parallelism(12) \
        .add_sink(KeyDBKlineWriter())
    
    env.execute("Crypto Kline Processing")

if __name__ == '__main__':
    main()
```

**File:** `docker-compose.yml` (Update Flink TaskManager)

```yaml
services:
  flink-taskmanager:
    environment:
      FLINK_TASKMANAGER_SLOTS: 12  # Increase from 1 to 12
      FLINK_TASKMANAGER_MEMORY: 8192m
    deploy:
      resources:
        limits:
          memory: 10G  # Increase from 7G to 10G
```

---

## 🧪 Testing

### Test 1: Verify Cluster

```bash
# Create topics
bash scripts/create_kafka_topics.sh

# Verify replication
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic crypto_klines

# Expected output:
# Topic: crypto_klines  PartitionCount: 12  ReplicationFactor: 3
# Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
# Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
# ...
```

### Test 2: Broker Failover

```bash
# Stop broker 2
docker stop kafka-2

# Producer should continue (min.insync.replicas=2)
# Check producer metrics
curl http://localhost:8080/api/metrics | jq '.producer'

# Consumer should continue (rebalance < 30s)
# Check Flink job
curl http://localhost:8081/jobs

# Restart broker 2
docker start kafka-2

# Replicas should sync automatically
```

### Test 3: Partition Distribution

```bash
# Check consumer lag per partition
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --group flink-kline-processor \
  --describe

# Expected: lag should be balanced across 12 partitions
```

---

## 📊 Expected Improvements

| Metric | Before (1 broker) | After (3 brokers) | Improvement |
|--------|-------------------|-------------------|-------------|
| Throughput | 10k msg/s | 30k msg/s | 3x |
| Availability | 95% | 99.9% | +4.9% |
| Data loss risk | High | Zero | 100% |
| Failover time | Manual | Auto (< 30s) | Instant |
| Parallelism | 1 task | 12 tasks | 12x |
