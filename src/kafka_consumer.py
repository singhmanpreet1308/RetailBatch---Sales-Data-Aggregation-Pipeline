"""
KafkaConsumerClient (LLD 2. Class/Interface Overview)

Responsibility:
  - consume_messages(): read sales transaction messages from the Kafka topic (FR-1)
  - write_to_hdfs():    persist raw transactions to HDFS as raw files (FR-2)

Same dual-mode approach as KafkaProducerClient: uses a real Kafka consumer
when a broker is reachable, otherwise reads from the local simulation queue.
HDFS is represented locally under config.HDFS_RAW_PATH; pointing
HDFS_RAW_PATH at a real "hdfs://..." URI (with PySpark handling the write)
requires no other code changes.
"""

import json
import os
import time
from datetime import datetime, timezone

from src import config
from src.logger_setup import get_logger

logger = get_logger("KafkaConsumerClient")

try:
    from kafka import KafkaConsumer
    _KAFKA_PY_AVAILABLE = True
except ImportError:
    _KAFKA_PY_AVAILABLE = False


REQUIRED_FIELDS = {"transaction_id", "timestamp", "store_id", "product_id", "quantity", "price"}


class KafkaConsumerClient:
    def __init__(self, broker: str = None, topic: str = None):
        self.broker = broker or config.KAFKA_BROKER
        self.topic = topic or config.KAFKA_TOPIC
        self.simulation_mode = True
        self._consumer = None

        if _KAFKA_PY_AVAILABLE:
            try:
                self._consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.broker,
                    group_id=config.KAFKA_CONSUMER_GROUP,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    consumer_timeout_ms=config.KAFKA_CONSUMER_TIMEOUT_MS,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                )
                self.simulation_mode = False
                logger.info(
                    f"Connected consumer group '{config.KAFKA_CONSUMER_GROUP}' "
                    f"to Kafka broker at {self.broker}"
                )
            except Exception as exc:
                logger.warning(
                    f"Could not reach Kafka broker at {self.broker} ({exc}). "
                    f"Reading from local simulation queue instead."
                )
        else:
            logger.warning("kafka-python not installed; reading local simulation queue.")

    def _validate(self, record: dict) -> bool:
        """FR-7 / LLD error handling: validate schema, skip invalid records."""
        missing = REQUIRED_FIELDS - record.keys()
        if missing:
            logger.warning(f"Skipping record with missing fields {missing}: {record}")
            return False
        return True

    def consume_messages(self) -> list:
        """Read all currently-available messages and return the valid ones."""
        records = []
        if not self.simulation_mode:
            for msg in self._consumer:
                if self._validate(msg.value):
                    records.append(msg.value)
        else:
            if not os.path.exists(config.LOCAL_KAFKA_QUEUE):
                logger.warning(f"No messages found at {config.LOCAL_KAFKA_QUEUE}")
                return records
            with open(config.LOCAL_KAFKA_QUEUE) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning(f"Skipping malformed record: {exc}")
                        continue
                    if self._validate(record):
                        records.append(record)

        logger.info(f"Consumed {len(records)} valid transactions from topic '{self.topic}'")
        return records

    def write_to_hdfs(self, records: list) -> str:
        """Write a batch of raw transactions to HDFS raw storage (FR-2).

        Idempotency (NFR-4): each batch file is named with a timestamp +
        record count, so re-running does not overwrite/duplicate prior
        batches, and downstream jobs read the full raw directory.
        """
        retries = config.HDFS_MAX_RETRIES
        batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        out_dir = config.HDFS_RAW_PATH
        out_path = os.path.join(out_dir, f"raw_batch_{batch_ts}_{len(records)}.json")

        for attempt in range(1, retries + 1):
            try:
                os.makedirs(out_dir, exist_ok=True)
                with open(out_path, "w") as f:
                    json.dump(records, f)
                logger.info(f"Wrote {len(records)} raw records to {out_path}")
                return out_path
            except Exception as exc:
                logger.error(f"HDFS write failed (attempt {attempt}/{retries}): {exc}")
                time.sleep(1)
        raise IOError(f"Failed to write batch to HDFS raw storage after {retries} attempts")

    def close(self):
        if self._consumer is not None:
            self._consumer.close()


if __name__ == "__main__":
    consumer = KafkaConsumerClient()
    batch = consumer.consume_messages()
    if batch:
        consumer.write_to_hdfs(batch)
    consumer.close()
