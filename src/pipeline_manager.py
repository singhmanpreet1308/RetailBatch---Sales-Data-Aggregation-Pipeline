"""
PipelineManager (LLD 2. Class/Interface Overview)

Responsibility: orchestrate and schedule the end-to-end pipeline (FR-6),
matching the pseudocode in LLD section 4:

    def run_pipeline():
        consumer = KafkaConsumerClient()
        consumer.consume_messages()
        consumer.write_to_hdfs()
        processor = SalesBatchProcessor()
        for period in ['daily', 'weekly']:
            summary_df = processor.aggregate_sales(period)
            processor.save_csv(summary_df, period)
"""

import time

from src import config
from src.kafka_consumer import KafkaConsumerClient
from src.batch_processor import SalesBatchProcessor
from src.logger_setup import get_logger

logger = get_logger("PipelineManager")


class PipelineManager:
    def __init__(self):
        self.consumer = KafkaConsumerClient()
        self.processor = None  # created lazily (starts a Spark session)

    def run_pipeline(self) -> dict:
        """Run one full batch cycle: ingest -> store raw -> aggregate -> export."""
        start = time.time()
        outputs = {}
        try:
            logger.info("Pipeline run started")

            # Step 1: consume from Kafka, persist raw to HDFS
            records = self.consumer.consume_messages()
            if not records:
                logger.warning("No new transactions to process; pipeline run ending early.")
                return outputs
            if len(records) < config.MIN_BATCH_SIZE:
                logger.warning(
                    f"Batch contains {len(records)} transactions, below the "
                    f"{config.MIN_BATCH_SIZE}-transaction target; processing "
                    "continues so incremental Kafka messages are not discarded."
                )
            else:
                logger.info(
                    f"Batch-size target met: {len(records)} transactions "
                    f"(minimum {config.MIN_BATCH_SIZE})."
                )
            self.consumer.write_to_hdfs(records)

            # Step 2: batch-aggregate with PySpark and export CSV
            self.processor = SalesBatchProcessor()
            for period in config.AGGREGATION_PERIODS:
                summary_df = self.processor.aggregate_sales(period)
                out_path = self.processor.save_csv(summary_df, period)
                outputs[period] = out_path

            elapsed = time.time() - start
            logger.info(f"Pipeline run completed in {elapsed:.1f}s. Outputs: {outputs}")

            if elapsed > config.BATCH_TIMEOUT_MINUTES * 60:
                logger.warning(
                    f"Run exceeded target of {config.BATCH_TIMEOUT_MINUTES} minutes (NFR-3)"
                )
            return outputs

        except Exception as exc:
            logger.error(f"Pipeline run failed: {exc}", exc_info=True)
            raise
        finally:
            if self.processor is not None:
                self.processor.stop()

    def schedule_jobs(self, interval_minutes: int = 60, max_runs: int = None):
        """Simple recurring scheduler (FR-6). For production, prefer cron / Airflow."""
        run_count = 0
        while max_runs is None or run_count < max_runs:
            self.run_pipeline()
            run_count += 1
            if max_runs is None or run_count < max_runs:
                logger.info(f"Sleeping {interval_minutes} minutes until next run.")
                time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    manager = PipelineManager()
    manager.run_pipeline()
