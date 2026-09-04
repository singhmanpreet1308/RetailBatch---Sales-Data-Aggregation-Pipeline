#!/usr/bin/env python3
"""
Entry point for the RetailBatch - Sales Data Aggregation Pipeline.

Usage:
    python run_pipeline.py                 # run once
    python run_pipeline.py --schedule 60    # run every 60 minutes, forever
    python run_pipeline.py --produce        # (re)stream sample_data first, then run once
"""

import argparse

from src.pipeline_manager import PipelineManager
from src.kafka_producer import KafkaProducerClient


def main():
    parser = argparse.ArgumentParser(description="Run the RetailBatch pipeline")
    parser.add_argument(
        "--produce", action="store_true",
        help="Stream sample_data/sales_transactions.json onto the topic before running",
    )
    parser.add_argument(
        "--schedule", type=int, default=None, metavar="MINUTES",
        help="Run repeatedly on this interval instead of once",
    )
    args = parser.parse_args()

    if args.produce:
        producer = KafkaProducerClient()
        producer.stream_transactions()
        producer.close()

    manager = PipelineManager()
    if args.schedule:
        manager.schedule_jobs(interval_minutes=args.schedule)
    else:
        manager.run_pipeline()


if __name__ == "__main__":
    main()
