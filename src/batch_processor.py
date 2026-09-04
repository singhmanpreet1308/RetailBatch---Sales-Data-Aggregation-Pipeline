"""
SalesBatchProcessor (LLD 2. Class/Interface Overview)

Responsibility:
  - aggregate_sales(period): read raw data from HDFS, aggregate by
    store_id + day/week (FR-3, FR-4)
  - save_csv(df, period): export the aggregated summary as CSV (FR-5)

Implements HLD component 'PySpark Processor' and the pseudocode in
LLD section 4 (Algorithms/Logic).
"""

import os
import shutil

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src import config
from src.logger_setup import get_logger

logger = get_logger("SalesBatchProcessor")


class SalesBatchProcessor:
    def __init__(self, spark: SparkSession = None):
        self.spark = spark or (
            SparkSession.builder
            .appName("RetailBatch-SalesAggregation")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

    def _read_raw(self) -> DataFrame:
        """Read all raw transaction batches from HDFS raw storage (FR-3)."""
        raw_dir = config.HDFS_RAW_PATH
        if not os.path.isdir(raw_dir) or not os.listdir(raw_dir):
            raise FileNotFoundError(
                f"No raw data found at {raw_dir}. Run the Kafka consumer first."
            )

        df = self.spark.read.option("multiLine", True).json(raw_dir)

        df = (
            df.withColumn("timestamp", F.to_timestamp("timestamp"))
              .withColumn("quantity", F.col("quantity").cast("int"))
              .withColumn("unit_cost", F.col("unit_cost").cast("double"))
              .withColumn("sales_value", F.col("sales_value").cast("double"))
              .withColumn("date", F.to_date("timestamp"))
              .withColumn(
                  "_week_start",
                  F.to_date(F.date_trunc("week", F.col("timestamp"))),
              )
              .withColumn(
                  "week",
                  F.concat_ws(
                      "-W",
                      # The Thursday identifies the correct ISO week-year.
                      F.year(F.date_add(F.col("_week_start"), 3)),
                      F.lpad(F.weekofyear("timestamp").cast("string"), 2, "0"),
                  ),
              )
        )
        # Data integrity (NFR-4): drop rows that failed to parse into a valid timestamp
        clean_df = df.filter(F.col("timestamp").isNotNull())
        dropped = df.count() - clean_df.count()
        if dropped:
            logger.warning(f"Dropped {dropped} raw records with invalid timestamps")
        return clean_df

    def aggregate_sales(self, period: str) -> DataFrame:
        """Aggregate raw sales into daily or weekly summaries (FR-4).

        period: "daily" or "weekly"
        Output schema matches LLD 3. Data Structure Overview -
        'Aggregated Summary': date/week, store_id, total_sales, sales_value,
        total_quantity
        """
        if period not in ("daily", "weekly"):
            raise ValueError("period must be 'daily' or 'weekly'")

        df = self._read_raw()
        period_col = "date" if period == "daily" else "week"

        summary = (
            df.withColumn("line_total", F.col("quantity") * F.col("unit_cost"))
              .groupBy(period_col, "store_id")
              .agg(
                  F.round(F.sum("line_total"), 2).alias("total_sales"),
                  F.round(F.sum("sales_value"), 2).alias("sales_value"),
                  F.sum("quantity").alias("total_quantity"),
              )
              .withColumnRenamed(period_col, "date" if period == "daily" else "week")
              .orderBy(period_col, "store_id")
        )
        logger.info(f"Computed {period} aggregation: {summary.count()} rows")
        return summary

    def save_csv(self, df: DataFrame, period: str) -> str:
        """Write the aggregated summary as a single CSV file (FR-5)."""
        tmp_dir = os.path.join(config.HDFS_OUTPUT_PATH, f"_tmp_{period}")
        final_path = os.path.join(config.HDFS_OUTPUT_PATH, f"{period}_sales_summary.csv")

        df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

        part_file = next(
            f for f in os.listdir(tmp_dir) if f.startswith("part-") and f.endswith(".csv")
        )
        os.makedirs(config.HDFS_OUTPUT_PATH, exist_ok=True)
        shutil.move(os.path.join(tmp_dir, part_file), final_path)
        shutil.rmtree(tmp_dir)

        logger.info(f"Saved {period} summary to {final_path}")
        return final_path

    def stop(self):
        self.spark.stop()

if __name__ == "__main__":
    processor = SalesBatchProcessor()

    try:
        for period in config.AGGREGATION_PERIODS:
            summary = processor.aggregate_sales(period)
            output_path = processor.save_csv(summary, period)
            print(f"{period.capitalize()} summary saved to: {output_path}")
    finally:
        processor.stop()
