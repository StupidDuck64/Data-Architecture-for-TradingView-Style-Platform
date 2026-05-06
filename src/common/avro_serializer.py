"""
Avro serializer with Confluent Schema Registry integration.

Handles Confluent wire format (magic byte 0x00 + 4-byte schema ID + Avro binary)
and schema registration with retry logic.
"""

import json
import logging
import struct
import time
from io import BytesIO

import fastavro
import requests

log = logging.getLogger(__name__)


class AvroSerializer:
    """Serialize dicts to Confluent wire-format Avro bytes.

    Wire format: ``[magic 0x00][4-byte schema ID][Avro binary]``
    """

    MAGIC = b'\x00'

    def __init__(self, registry_url: str):
        self._url = registry_url.rstrip('/')
        self._cache: dict[str, tuple] = {}   # topic → (parsed_schema, schema_id)

    def register(self, topic: str, schema_path: str) -> None:
        """Register an Avro schema file with the Schema Registry."""
        with open(schema_path) as f:
            schema_dict = json.load(f)
        parsed = fastavro.parse_schema(schema_dict)
        schema_id = self._register_with_retry(f"{topic}-value", schema_dict)
        self._cache[topic] = (parsed, schema_id)
        log.info("[AVRO] Registered %s-value  schema_id=%d", topic, schema_id)

    def serialize(self, topic: str, record: dict) -> bytes:
        """Serialize a record to Confluent wire-format Avro bytes."""
        parsed, sid = self._cache[topic]
        buf = BytesIO()
        buf.write(self.MAGIC)
        buf.write(struct.pack('>I', sid))
        fastavro.schemaless_writer(buf, parsed, record)
        return buf.getvalue()

    def _register_with_retry(self, subject: str, schema_dict: dict,
                             retries: int = 30) -> int:
        for attempt in range(retries):
            try:
                resp = requests.post(
                    f"{self._url}/subjects/{subject}/versions",
                    json={"schema": json.dumps(schema_dict)},
                    headers={"Content-Type":
                             "application/vnd.schemaregistry.v1+json"},
                    timeout=10,
                )
                resp.raise_for_status()
                return resp.json()["id"]
            except Exception as e:
                if attempt < retries - 1:
                    log.warning("[AVRO] %s register failed (%s), "
                                "retry %d/%d ...", subject, e,
                                attempt + 1, retries)
                    time.sleep(2)
                else:
                    raise RuntimeError(
                        f"Schema registration failed for '{subject}' "
                        f"after {retries} attempts: {e}"
                    ) from e
        return -1  # unreachable, keeps linter quiet
