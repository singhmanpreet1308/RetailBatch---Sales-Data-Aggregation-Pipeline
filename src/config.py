"""
Central configuration for the RetailBatch - Sales Data Aggregation Pipeline.

In a real deployment:
  - KAFKA_BROKER   -> points at your real Kafka broker(s), e.g. "broker1:9092"
  - HDFS_RAW_PATH  -> an actual HDFS URI, e.g. "hdfs://namenode:8020/retailbatch/raw"
  - HDFS_OUTPUT_PATH -> "hdfs://namenode:8020/retailbatch/output"

For this environment (no live Kafka broker / HDFS cluster reachable), the same
code paths are used but Kafka + HDFS are represented locally on disk so the
full pipeline is runnable and testable end-to-end. Swapping KAFKA_BROKER /
HDFS_*_PATH to real endpoints requires no code changes elsewhere.
"""

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- Kafka settings ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sales_transactions")
KAFKA_CONSUMER_GROUP = os.environ.get(
    "KAFKA_CONSUMER_GROUP", "retailbatch-consumer-group"
)
KAFKA_CONSUMER_TIMEOUT_MS = int(
    os.environ.get("KAFKA_CONSUMER_TIMEOUT_MS", 30000)
)
# Local file standing in for the Kafka topic when no broker is reachable.
LOCAL_KAFKA_QUEUE = os.path.join(BASE_DIR, "local_kafka_queue", f"{KAFKA_TOPIC}.jsonl")

# --- HDFS settings (represented locally as directories in this sandbox) ---
HDFS_RAW_PATH = os.environ.get("HDFS_RAW_PATH", os.path.join(BASE_DIR, "data", "hdfs_raw"))
HDFS_OUTPUT_PATH = os.environ.get("HDFS_OUTPUT_PATH", os.path.join(BASE_DIR, "data", "hdfs_output"))

# --- Batch / pipeline settings ---
MIN_BATCH_SIZE = int(os.environ.get("MIN_BATCH_SIZE", 10000))  # NFR-2
BATCH_TIMEOUT_MINUTES = int(os.environ.get("BATCH_TIMEOUT_MINUTES", 30))  # NFR-3
AGGREGATION_PERIODS = ["daily", "weekly"]

# --- Retry settings (see LLD section 5, Error Handling) ---
KAFKA_MAX_RETRIES = 5
KAFKA_BACKOFF_BASE_SECONDS = 2
HDFS_MAX_RETRIES = 3

# --- Sample data (generated from Master_Sales_data.xlsx) ---
SAMPLE_DATA_PATH = os.path.join(BASE_DIR, "sample_data", "sales_transactions.json")

# --- Logging ---
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "pipeline.log")
