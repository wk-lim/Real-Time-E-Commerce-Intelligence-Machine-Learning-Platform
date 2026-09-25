# Phase 1: Synerise Data Foundation and Historical Replay

## Status

In progress.

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

## Planned Kafka contracts

- Behavioral topic: `ecommerce.events.v1`
- Product reference topic: `ecommerce.products.v1`
- Behavioral partition key: `client_id`
- Product partition key: `sku`

Source values will be preserved in the event envelope. Invalid records will be routed to a dead-letter topic instead of silently discarded or corrected.

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

## Deliverables

- [x] Select and document the real-world dataset
- [x] Protect local data from Git and Docker build contexts
- [x] Download and checksum the source archive
- [x] Validate files, schemas and row counts
- [x] Profile timestamps, nulls and identifier ranges
- [x] Define versioned event and product contracts
- [x] Implement contract validation tests
- [x] Implement deterministic event identifiers
- [ ] Implement historical replay producer
- [x] Add Kafka services to Docker Compose
- [ ] Verify replay, ordering and dead-letter handling

## Completion criteria

Phase 1 is complete when a reproducible producer can publish validated historical interactions to Kafka, maintain per-customer ordering, route invalid records to a dead-letter topic and pass automated tests.
