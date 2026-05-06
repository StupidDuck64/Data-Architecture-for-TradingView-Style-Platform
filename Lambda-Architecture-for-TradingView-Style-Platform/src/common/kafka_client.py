"""
Thread-safe Kafka producer with automatic reconnection and consistent hashing.

Extracted from the monolithic ``producer_binance.py`` to be reusable
across any exchange producer implementation.
"""

import hashlib
import json
import logging
import threading
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from common.config import KAFKA_BOOTSTRAP

log = logging.getLogger(__name__)

_producer: KafkaProducer | None = None
_producer_lock = threading.Lock()

# Number of partitions per topic (must match Kafka topic config)
NUM_PARTITIONS = 12


def get_partition(key: str, num_partitions: int = NUM_PARTITIONS) -> int:
    """
    Consistent hashing for key → partition mapping.

    Args:
        key: Partition key (e.g., symbol)
        num_partitions: Total number of partitions

    Returns:
        Partition number (0 to num_partitions-1)
    """
    hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return hash_value % num_partitions


def create_kafka_producer() -> KafkaProducer:
    """Create a Kafka producer with LZ4 compression and retry logic."""
    while True:
        try:
            log.info("Connecting to Kafka at %s ...", KAFKA_BOOTSTRAP)
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP.split(','),  # Support multiple brokers
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                acks='all',  # Wait for all in-sync replicas
                compression_type="lz4",
                linger_ms=10,
                batch_size=32 * 1024,
                buffer_memory=64 * 1024 * 1024,
                retries=3,
                max_in_flight_requests_per_connection=5,
            )
            p.bootstrap_connected()
            log.info("Successfully connected to Kafka cluster.")
            return p
        except NoBrokersAvailable as e:
            log.error("Kafka unavailable (%s). Retrying in 5s...", e)
            time.sleep(5)


def get_producer() -> KafkaProducer:
    """Return the global producer, creating it if necessary (thread-safe)."""
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = create_kafka_producer()
    return _producer


def init_producer() -> None:
    """Eagerly initialize the global producer at startup."""
    global _producer
    _producer = create_kafka_producer()


def _on_send_error(topic: str, symbol: str, exc: Exception) -> None:
    log.error("[KAFKA] Async send failed | topic=%s symbol=%s error=%s", topic, symbol, exc)


def send_to_kafka(topic: str, record: dict, avro_serializer=None) -> None:
    """
    Serialize and send a record to Kafka with consistent partitioning (thread-safe).

    Uses consistent hashing to ensure same symbol always goes to same partition.
    """
    producer = get_producer()
    key: str = record.get("symbol", "")

    # Calculate partition using consistent hashing
    partition = get_partition(key) if key else None

    try:
        value_bytes = (
            avro_serializer.serialize(topic, record)
            if avro_serializer
            else json.dumps(record).encode("utf-8")
        )
        future = producer.send(topic, key=key, value=value_bytes, partition=partition)
        future.add_errback(_on_send_error, topic, key)
    except KafkaError as e:
        log.error("[KAFKA] Sync send error (dropped message) | topic=%s symbol=%s partition=%s error=%s",
                  topic, key, partition, e)
    except Exception as e:
        log.error("[KAFKA] Unexpected send error | topic=%s symbol=%s partition=%s error=%s",
                  topic, key, partition, e)


def flush_and_close() -> None:
    """Gracefully flush and close the global producer."""
    global _producer
    if _producer is not None:
        _producer.flush(timeout=10)
        _producer.close()
        _producer = None
        log.info("Kafka producer closed.")
