# RetailBatch - Sales Data Aggregation Pipeline

RetailBatch is a batch data pipeline for retail sales analysis. Transactions
are generated from an Excel workbook, published to Kafka, consumed into a raw
storage area, and aggregated by PySpark into daily and weekly CSV summaries.

## Pipeline flow

```text
Master_Sales_data.xlsx
          |
          v
generate_sample_data.py
          |
          v
sales_transactions.json
          |
          v
Kafka producer -> Docker Kafka -> Kafka consumer -> data/hdfs_raw
                         |                              |
                         v                              v
                     Kafka UI                       PySpark
                                                        |
                                                        v
                                            data/hdfs_output/*.csv
```

The main components are:

1. `scripts/generate_sample_data.py` creates transaction JSON from the Excel
   worksheets.
2. `src/kafka_producer.py` publishes the transactions to the
   `sales_transactions` Kafka topic.
3. `src/kafka_consumer.py` validates and consumes messages, then writes raw
   batch files to `data/hdfs_raw/`.
4. `src/batch_processor.py` aggregates the raw transactions by day and week.
5. `src/pipeline_manager.py` orchestrates the consumer and batch processor.
6. `run_pipeline.py` provides the command-line entry point.

## Requirements

- Python 3
- Java for PySpark
- Docker Desktop and Docker Compose for real Kafka and Kafka UI

Install the Python dependencies:

```powershell
pip install -r requirements.txt
```

## Generate the sample data

The input workbook is `data/Master_Sales_data.xlsx`. The generator reads these
worksheets:

| Worksheet    | Generated month |
| ------------ | --------------- |
| `Nov_file` | November 2025   |
| `Dec_file` | December 2025   |
| `Jan_file` | January 2026    |

Run the generator with:

```powershell
python scripts/generate_sample_data.py
```

The output is written to `sample_data/sales_transactions.json`.

### Transaction field mapping

| Transaction field  | Excel source or rule                                 |
| ------------------ | ---------------------------------------------------- |
| `transaction_id` | Generated sequential ID                              |
| `timestamp`      | Worksheet year/month plus a distributed calendar day |
| `store_id`       | `BELFAST`                                          |
| `product_id`     | `Stock Code`                                       |
| `quantity`       | `Sold Period`                                      |
| `price`          | `Unit Price`                                       |
| `unit_cost`      | `Unit Cost`                                        |
| `cost_sales`     | `Unit Cost * Sold Period`                          |
| `sales_value`    | `Sales Value`                                      |

The generator gets the actual number of days in each month and spreads valid
transactions evenly across those days:

```python
days_in_month = calendar.monthrange(year, month)[1]
month_tx_counter += 1
day = ((month_tx_counter - 1) % days_in_month) + 1
```

The monthly counter resets for each worksheet. This includes day 29, 30, and
31 where applicable. With the current 966 source records, the distribution is:

| Month         | Transactions | Days | Distribution               |
| ------------- | -----------: | ---: | -------------------------- |
| November 2025 |          381 |   30 | 21 days x 13, 9 days x 12  |
| December 2025 |          323 |   31 | 13 days x 11, 18 days x 10 |
| January 2026  |          262 |   31 | 14 days x 9, 17 days x 8   |

## Start Kafka and Kafka UI with Docker

`run_pipeline.py` and `src.pipeline_manager` do not create or start Docker
containers. Kafka must be started separately before any producer, consumer,
or pipeline command is run:

```powershell
docker compose up -d
docker compose ps
```

`docker compose up -d` is safe to use whenever the project starts:

- if the containers do not exist, Docker creates them;
- if they exist but are stopped, Docker starts them; and
- if they are already running, Docker leaves them running.

The Compose configuration creates:

| Container           | Purpose                              | Address                   |
| ------------------- | ------------------------------------ | ------------------------- |
| `retail-kafka`    | Kafka broker and transaction storage | `localhost:9092`        |
| `retail-kafka-ui` | Browser interface for Kafka          | `http://localhost:8080` |

Confirm that Kafka's external port is available:

```powershell
Test-NetConnection localhost -Port 9092
```

Wait until `TcpTestSucceeded` is `True` before starting the producer.

To let Docker restart these services automatically after Docker Desktop or
the computer restarts, add `restart: unless-stopped` to both the `kafka` and
`kafka-ui` services in `docker-compose.yml`. The containers must still be
created once with `docker compose up -d`.

## Publish transactions to Kafka

After Docker Kafka is running, publish the generated sample data:

```powershell
python -m src.kafka_producer
```

A successful connection produces logs similar to:

```text
Connected to Kafka broker at localhost:9092
Streamed 966/966 transactions to topic 'sales_transactions'
```

If the log contains `Falling back to local simulation queue`, the records were
not sent to Docker Kafka. Check Docker, wait for Kafka to become ready, and run
the producer again. Running the producer multiple times publishes duplicate
copies of the source records.

## Visualize messages with Kafka UI

Open [http://localhost:8080](http://localhost:8080), then navigate to:

```text
RetailBatch -> Topics -> sales_transactions -> Messages
```

Choose the earliest offset and search to display the messages. Kafka UI reads
messages from the Kafka broker; it does not store a separate copy.

Kafka UI can display brokers, topic partitions, offsets, JSON messages, and
consumer groups. It visualizes Kafka infrastructure and messages, not charts
from the daily or weekly sales summaries.

## Consume messages

Run the consumer after messages have been published to Docker Kafka:

```powershell
python -m src.kafka_consumer
```

The consumer joins the named group `retailbatch-consumer-group`, commits its
offsets, and remains connected for 30 seconds after the last available
message. While it is running, open Kafka UI and navigate to:

```text
RetailBatch -> Consumers -> retailbatch-consumer-group
```

This page shows the active member, assigned partitions, committed offsets,
and consumer lag. The group and its committed offsets remain trackable after
the process exits, although it will no longer have an active member.

Expected output is similar to:

```text
Connected consumer group 'retailbatch-consumer-group' to Kafka broker at localhost:9092
Consumed 966 valid transactions from topic 'sales_transactions'
Wrote 966 raw records to data/hdfs_raw/raw_batch_..._966.json
```

Consuming a message does not immediately delete it from Kafka or Kafka UI.
Kafka retains messages according to its retention settings.

Because offsets are committed for the named group, subsequent runs consume
only messages published after its last committed offset. To process existing
messages independently, use a different group name before starting Python:

```powershell
$env:KAFKA_CONSUMER_GROUP = "retailbatch-replay-group"
python -m src.kafka_consumer
```

## Run the complete pipeline

### Initial load or full demonstration

Once Docker Kafka is ready, use `--produce` for the initial load. It publishes
the complete sample JSON, consumes the messages, and runs the daily and weekly
aggregations:

```powershell
python run_pipeline.py --produce
```

Do not use `--produce` merely to check for new messages. Every use republishes
all 966 sample records, and Kafka assigns them new offsets even when their
`transaction_id` values are unchanged.

### Normal processing of new messages

After the initial load, use the command without `--produce`:

```powershell
python run_pipeline.py
```

This is the command-line entry-point equivalent of:

```powershell
python -m src.pipeline_manager
```

Both commands consume only messages after the named consumer group's last
committed offset. They do not publish data or create Docker containers.

If 966 messages were previously consumed and 34 new messages arrive, the next
normal run consumes only those 34 messages, writes a new raw batch, and
regenerates the summaries from the raw directory.

If no new messages are present, this is expected behavior:

```text
Consumed 0 valid transactions from topic 'sales_transactions'
No new transactions to process; pipeline run ending early.
```

To regenerate summaries from raw files that already exist without checking
Kafka, run:

```powershell
python -m src.batch_processor
```

### Scheduled checks

To run the pipeline every 60 minutes:

```powershell
python run_pipeline.py --schedule 60
```

The scheduler keeps the Python process running and checks Kafka every 60
minutes. Docker Kafka must remain available separately.

## Test the pipeline with one new transaction

Use this procedure to verify that the named consumer group detects and
processes only a newly arrived Kafka message.

### 1. Produce one message with Kafka UI

Open [http://localhost:8080](http://localhost:8080), then navigate to:

```text
RetailBatch -> Topics -> sales_transactions -> Produce Message
```

Use `TXN-TEST-000967` as the optional message key and enter this JSON as the
message value:

```json
{
  "transaction_id": "TXN-TEST-000967",
  "timestamp": "2026-02-01T12:00:00Z",
  "store_id": "BELFAST",
  "product_id": "TEST-PRODUCT-001",
  "quantity": 2,
  "price": 10.0,
  "unit_cost": 4.0,
  "cost_sales": 8.0,
  "sales_value": 20.0
}
```

Click **Produce Message**. Use a different unique `transaction_id` for each
additional test transaction.

### 2. Check the waiting message

Navigate to:

```text
RetailBatch -> Consumers -> retailbatch-consumer-group
```

Before the pipeline consumes the record, the group lag should increase to
`1`. Kafka UI may require a refresh before the updated lag appears.

### 3. Process the new message

Run the normal pipeline manager:

```powershell
python -m src.pipeline_manager
```

Do not use `python run_pipeline.py --produce` for this test because it
republishes all 966 sample transactions. The consumer waits for 30 seconds
after the last available message, so the raw file is written after that wait.

Expected logs include:

```text
Consumed 1 valid transactions from topic 'sales_transactions'
Wrote 1 raw records to data/hdfs_raw/raw_batch_..._1.json
```

### 4. Verify the raw and aggregated outputs

A new file should appear with one record:

```text
data/hdfs_raw/raw_batch_<timestamp>_1.json
```

The daily CSV should contain a row for `2026-02-01` with these values when no
other transaction exists for that date and store:

```text
date,store_id,total_sales,sales_value,total_quantity
2026-02-01,BELFAST,8.0,20.0,2
```

The calculation is:

```text
total_sales    = quantity * unit_cost = 2 * 4 = 8
sales_value    = 20
total_quantity = 2
```

The weekly CSV should include the transaction in `2026-W05`. After successful
consumption, refresh the Kafka UI consumer-group page and confirm that its lag
has returned to `0`.

Outputs are written to:

```text
data/hdfs_output/daily_sales_summary.csv
data/hdfs_output/weekly_sales_summary.csv
```

Logs are written to `logs/pipeline.log` and standard output.

## Kafka persistence and local fallback

Docker Kafka stores broker data at `/var/lib/kafka/data` inside the container.
The Compose file maps that directory to the named `kafka-data` Docker volume,
so messages survive normal container restarts.

This preserves Kafka data:

```powershell
docker compose down
docker compose up -d
```

This deletes the Kafka volume and its messages:

```powershell
docker compose down -v
```

Do not use `-v` when Kafka messages must be retained.

When Kafka cannot be reached, the producer switches to simulation mode and
appends messages to `local_kafka_queue/sales_transactions.jsonl`.

The local JSONL queue and Docker Kafka are separate storage systems. Records
written to JSONL are not automatically copied to Docker Kafka. Start Docker
and rerun the producer to publish the original sample JSON to the real broker.

The project currently simulates HDFS with local directories:

| Location                                       | Purpose                             |
| ---------------------------------------------- | ----------------------------------- |
| `sample_data/sales_transactions.json`        | Generated producer input            |
| Docker`kafka-data` volume                    | Real Kafka messages                 |
| `local_kafka_queue/sales_transactions.jsonl` | Kafka-unavailable fallback          |
| `data/hdfs_raw/`                             | Raw batches written by the consumer |
| `data/hdfs_output/`                          | Daily and weekly CSV results        |

## Troubleshooting: consumer reads zero messages

`Consumed 0 valid transactions` has two common explanations.

### The consumer group is already caught up

The named group `retailbatch-consumer-group` commits its Kafka offsets. After
it consumes the first 966 messages, another pipeline run correctly returns
zero until a producer publishes newer messages. No action is required; run
the pipeline again when new messages have arrived.

### The producer used the local fallback

Inspect the earlier producer log. This sequence means the producer and
consumer used different storage systems:

```text
Producer: Could not reach Kafka broker
Producer: Falling back to local simulation queue
Consumer: Connected consumer to Kafka broker
Consumer: Consumed 0 valid transactions
```

The producer wrote to local JSONL while the consumer later read from a real
but empty Docker Kafka topic. Recover with:

```powershell
docker compose up -d
Test-NetConnection localhost -Port 9092
python -m src.kafka_producer
python -m src.kafka_consumer
```

The correct execution order is:

```text
1. Start Docker Kafka
2. Generate or regenerate the sample JSON
3. Run the Kafka producer
4. Inspect messages in Kafka UI (optional)
5. Run the Kafka consumer
6. Run the batch aggregation
```

Useful Docker diagnostics:

```powershell
docker compose ps
docker compose logs kafka
docker compose logs kafka-ui
docker volume ls
```

## External cluster configuration

Settings in `src/config.py` can be overridden with environment variables. For
example, in PowerShell:

```powershell
$env:KAFKA_BROKER = "broker1:9092"
$env:HDFS_RAW_PATH = "hdfs://namenode:8020/retailbatch/raw"
$env:HDFS_OUTPUT_PATH = "hdfs://namenode:8020/retailbatch/output"
```

## Error handling

| Scenario                 | Handling                                          |
| ------------------------ | ------------------------------------------------- |
| Kafka connection failure | Fall back to the local JSONL queue                |
| Message send failure     | Retry with exponential backoff                    |
| HDFS write/read error    | Retry a limited number of times and log the error |
| Data schema mismatch     | Skip invalid records and log the missing fields   |
| PySpark failure          | Log and propagate the error for investigation     |
| CSV export failure       | Log and propagate the failure                     |

## Project structure

```text
retailbatch/
|-- docker-compose.yml
|-- run_pipeline.py
|-- requirements.txt
|-- src/
|   |-- config.py
|   |-- kafka_producer.py
|   |-- kafka_consumer.py
|   |-- batch_processor.py
|   |-- pipeline_manager.py
|   `-- logger_setup.py
|-- scripts/
|   `-- generate_sample_data.py
|-- sample_data/
|   `-- sales_transactions.json
|-- local_kafka_queue/
|   `-- sales_transactions.jsonl
|-- data/
|   |-- hdfs_raw/
|   `-- hdfs_output/
`-- logs/
    `-- pipeline.log
```
