# Phase 2: Spark Structured Streaming

## Status

Kafka-to-Parquet event and product streaming was verified locally on
2026-10-01. PostgreSQL loading is planned for a later phase.

## Flow

Historical Synerise Parquet records are replayed through Kafka. Spark 4.2.0
reads the event and product topics, parses their versioned JSON envelopes,
validates fields and Kafka keys, then writes valid and rejected records to
separate local Parquet directories.

| Kafka topic | Valid output | Rejected output |
|---|---|---|
| `ecommerce.events.v1` | `data/processed/bronze/events` | `data/processed/quarantine/events` |
| `ecommerce.products.v1` | `data/processed/bronze/products` | `data/processed/quarantine/products` |

Each output has its own checkpoint under `data/checkpoints/spark/`. The valid
and quarantine writers are independent streaming queries. A failure can
temporarily leave one writer ahead of the other; each resumes from its own
checkpoint.

The producer's `ecommerce.events.dlq.v1` topic is separate from Spark's
quarantine directories and is not yet landed by these jobs.

## Run locally

Start the required services:

```powershell
docker compose up -d kafka kafka-init spark-ivy-init spark
```

Process the currently available Kafka records:

```powershell
$phase2KafkaPackage = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

docker compose exec -T spark `
  /opt/spark/bin/spark-submit `
  --master "local[2]" `
  --conf spark.jars.ivy=/tmp/spark-ivy `
  --packages $phase2KafkaPackage `
  /opt/project/src/ecommerce_intelligence/streaming/event_stream.py `
  --available-now

docker compose exec -T spark `
  /opt/spark/bin/spark-submit `
  --master "local[2]" `
  --conf spark.jars.ivy=/tmp/spark-ivy `
  --packages $phase2KafkaPackage `
  /opt/project/src/ecommerce_intelligence/streaming/product_stream.py `
  --available-now
```

Omit `--available-now` to keep a job running for new Kafka messages.
Stop a foreground job with Ctrl+C. When restarted with the same checkpoint,
each writer resumes from its recorded Kafka offsets.

## Verification

- Event bronze contained 10 cart additions and 3 page visits.
- Product bronze contained 3 records.
- Both quarantine outputs contained 0 rows for the normal topics.
- Rerunning the available-now jobs did not increase the bronze row counts.
- Isolated malformed-message tests placed one record in each quarantine
  output with `validation_error=malformed_json` and the raw JSON preserved.
- The Spark parsing and validation smoke checks passed for both topics.

## Data semantics and limitations

- This is historical Synerise event replay, not live retailer traffic.
- Replay follows physical Parquet row order, not global event-time order.
- Original event timestamps have no known timezone. `event_time_local`
  is a parsed wall-clock value, not a UTC instant.
- `published_at_utc` is replay publication time.
- Product `price_bucket` is anonymized; it is not a monetary price.
- Bronze output is append-only. Replaying source rows can create duplicate
  record IDs, so downstream models must deduplicate.
- The event and product valid/quarantine queries have separate checkpoints;
  their outputs are not one atomic transaction.
- The producer DLQ Kafka topic is not yet landed by these Spark jobs.
- Local Kafka retention and product-topic compaction limit historical
  recovery. This single-node Docker setup is for development.
