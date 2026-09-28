# Phase 1: Synerise Data Foundation and Historical Replay

## Status

Completed on 2026-09-28.

## Objective

Build a reproducible ingestion foundation for replaying historical, real-world Synerise e-commerce interactions through Kafka.

This project does not claim to consume live production traffic.

## Dataset

- Provider: Synerise
- Release: RecSys Challenge 2025
- Underlying event period: 2022-06-23 through 2022-12-08
- Behavioral events: 225,224,262
- Product records: 1,534,050
- Source format: Parquet
- Streaming classification: historical event replay

The source archive is identified by the SHA-256 checksum recorded in `data/source_manifest.yaml`.

## Source tables

| Table | Rows | Purpose |
|---|---:|---|
| `page_visit` | 199,451,980 | Anonymized page visits |
| `search_query` | 13,223,769 | Searches with encoded query vectors |
| `add_to_cart` | 7,541,117 | Product additions to carts |
| `remove_from_cart` | 2,688,894 | Product removals from carts |
| `product_buy` | 2,318,502 | Product purchase interactions |
| `product_properties` | 1,534,050 | Product category, price bucket and encoded name |

## Data-quality observations

- Parquet footer statistics report no null values.
- Event timestamps are stored as timezone-free strings.
- Product and search text is represented by string-serialized integer vectors.
- Encoded vectors require validation before being used for machine learning.
- Product prices are anonymized buckets from 0 through 99.
- Page visits contain URL identifiers but no direct product identifiers.
- Product SKU identifiers are not contiguous.
- Exact revenue, payment and geographic analysis is not supported.

## Kafka contracts

- Behavioral topic: `ecommerce.events.v1`
- Product reference topic: `ecommerce.products.v1`
- Dead-letter topic: `ecommerce.events.dlq.v1`
- Behavioral partition key: `client_id`
- Product partition key: `sku`
- Dead-letter partition key: deterministic `dlq_id`

Source values are preserved in the event envelope. Records that cannot be
transformed or validated are routed to the dead-letter topic instead of being
silently discarded or corrected.

## Local Kafka topology

The development environment uses the official Apache Kafka 4.3.1 image in
single-node KRaft combined mode. The broker exposes `localhost:9092` to host
applications and `kafka:19092` to other Compose services.

| Topic | Partitions | Local policy |
|---|---:|---|
| `ecommerce.events.v1` | 6 | Seven-day retention |
| `ecommerce.products.v1` | 3 | Log compaction |
| `ecommerce.events.dlq.v1` | 3 | Thirty-day retention |

The single broker and replication factor of one are appropriate only for local
development. A production deployment would require multiple brokers,
replication, authentication, encryption, access control, and monitoring.

## Historical replay producer

The `ecommerce-replay` command reads one Synerise Parquet source file in bounded
batches, transforms each row into its versioned JSON contract, validates the
record, and publishes it with an idempotent Kafka producer. Publishing can be
rate-limited, and the required `--limit` argument prevents an accidental replay
of the full dataset.

Install the project command:

```powershell
python -m pip install --editable ".[dev]"
```

Perform a dry run without publishing:

```powershell
ecommerce-replay `
    --source-file add_to_cart.parquet `
    --limit 3 `
    --dry-run
```

Publish ten behavioral events:

```powershell
ecommerce-replay `
    --source-file add_to_cart.parquet `
    --limit 10 `
    --messages-per-second 10
```

Publish three product reference records:

```powershell
ecommerce-replay `
    --source-file product_properties.parquet `
    --limit 3 `
    --messages-per-second 10
```

Each invocation has a replay run UUID. Event and product identifiers are
deterministic UUIDv5 values derived from the source filename and zero-based
physical row number, which supports reproducible reprocessing and downstream
deduplication.

## Ordering semantics

- Parquet records are read in physical source-file order.
- Behavioral records use `client_id` as the Kafka key. Records for the same
  client therefore reach the same partition and retain producer order within
  that partition.
- Product records use `sku` as the key, enabling log compaction.
- Kafka does not guarantee a global order across partitions.
- Source files are replayed separately and are not merged into global
  event-time order.
- Source timestamps do not identify a timezone, so the original strings are
  preserved and `event_time_timezone` remains null.

These constraints mean the pipeline is a reproducible historical streaming
workload, not a simulation of the exact chronology of a live production store.

## Dead-letter handling

The producer distinguishes between transformation failures and JSON Schema
contract-validation failures. Both are converted into
`ecommerce_dlq_v1.schema.json` records containing:

- a deterministic DLQ identifier;
- the intended destination topic;
- source file and physical row number;
- replay run identifier and failure timestamp;
- error stage, exception type, and message; and
- the unmodified raw source record.

Kafka transport failures are not routed to Kafka itself because an unavailable
broker would also prevent DLQ delivery. The command instead exits unsuccessfully
when messages remain undelivered after its flush timeout.

## Verification evidence

Phase 1 was verified locally on 2026-09-28:

- 44 automated tests passed across contract validation, transformations, replay
  behavior, and DLQ routing.
- Ten `add_to_cart` events were published to and consumed from
  `ecommerce.events.v1`.
- Three product records were published to and consumed from the compacted
  `ecommerce.products.v1` topic with their SKU keys.
- One intentionally invalid timestamp was classified as a transformation
  failure, delivered to `ecommerce.events.dlq.v1`, and consumed with its raw
  record and error context intact.
- Kafka output demonstrated that records for one client retain their production
  sequence within a partition, while output across partitions is not globally
  ordered.

## Deliverables

- [x] Select and document the real-world dataset
- [x] Protect local data from Git and Docker build contexts
- [x] Download and checksum the source archive
- [x] Validate files, schemas and row counts
- [x] Profile timestamps, nulls and identifier ranges
- [x] Define versioned event, product, and dead-letter contracts
- [x] Implement contract validation tests
- [x] Implement deterministic event identifiers
- [x] Implement historical replay producer
- [x] Add Kafka services to Docker Compose
- [x] Verify replay, partition ordering, and dead-letter handling

## Completion criteria

Phase 1 is complete: the reproducible producer publishes validated historical
interactions to Kafka, preserves per-customer producer order within Kafka
partitions, routes invalid source records to a dead-letter topic, and passes its
automated and local integration checks.
