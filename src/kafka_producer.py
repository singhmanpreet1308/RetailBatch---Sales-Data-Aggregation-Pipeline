"""
KafkaProducerClient (LLD 2. Class/Interface Overview)

Responsibility: Send sales transaction messages onto the Kafka topic (FR-1,
HLD Component 'Kafka Producer').

Real Kafka is used when a broker is reachable at config.KAFKA_BROKER. If no
broker can be reached (e.g. this sandbox has no live Kafka cluster), the
client transparently falls back to appending each message to a local
JSON-lines file that plays the role of the topic, so the rest of the
pipeline can be exercised end-to-end without a live cluster. No other code
in the pipeline needs to know which mode is active.
"""

import json
import os
import time

from src import config
from src.logger_setup import get_logger

logger = get_logger("KafkaProducerClient")

try:
    from kafka import KafkaProducer
    _KAFKA_PY_AVAILABLE = True
except ImportError:  # kafka-python not installed
    _KAFKA_PY_AVAILABLE = False


class KafkaProducerClient:
    def __init__(self, broker: str = None, topic: str = None):
        self.broker = broker or config.KAFKA_BROKER
        self.topic = topic or config.KAFKA_TOPIC
        self._producer = None
        self.simulation_mode = True

        if _KAFKA_PY_AVAILABLE:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.broker,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    request_timeout_ms=3000,
                )
                self.simulation_mode = False
                logger.info(f"Connected to Kafka broker at {self.broker}")
            except Exception as exc:  # NoBrokersAvailable, etc.
                logger.warning(
                    f"Could not reach Kafka broker at {self.broker} ({exc}). "
                    f"Falling back to local simulation queue: {config.LOCAL_KAFKA_QUEUE}"
                )
        else:
            logger.warning("kafka-python not installed; using local simulation queue.")

        if self.simulation_mode:
            os.makedirs(os.path.dirname(config.LOCAL_KAFKA_QUEUE), exist_ok=True)

    def send_message(self, sale_record: dict) -> bool:
        """Send a single sales transaction record. Returns True on success."""
        retries = config.KAFKA_MAX_RETRIES
        for attempt in range(1, retries + 1):
            try:
                if not self.simulation_mode:
                    future = self._producer.send(self.topic, value=sale_record)
                    future.get(timeout=5)  # block for send acknowledgment
                else:
                    with open(config.LOCAL_KAFKA_QUEUE, "a") as f:
                        f.write(json.dumps(sale_record) + "\n")
                return True
            except Exception as exc:
                backoff = config.KAFKA_BACKOFF_BASE_SECONDS ** attempt
                logger.error(
                    f"send_message failed (attempt {attempt}/{retries}): {exc}. "
                    f"Retrying in {backoff}s."
                )
                time.sleep(min(backoff, 10))
        logger.error(f"Giving up on record after {retries} attempts: {sale_record}")
        return False

    def stream_transactions(self, source_path: str = None) -> int:
        """Stream every transaction from a JSON file (list of records) to Kafka."""
        source_path = source_path or config.SAMPLE_DATA_PATH
        with open(source_path) as f:
            records = json.load(f)

        sent = 0
        for record in records:
            if self.send_message(record):
                sent += 1
        logger.info(f"Streamed {sent}/{len(records)} transactions to topic '{self.topic}'")
        return sent

    def close(self):
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()


if __name__ == "__main__":
    producer = KafkaProducerClient()
    producer.stream_transactions()
    producer.close()
