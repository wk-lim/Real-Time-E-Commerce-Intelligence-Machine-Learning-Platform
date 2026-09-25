# Real-Time E-Commerce Intelligence & Machine Learning Platform

A portfolio project demonstrating how to build a reproducible, production-style
data and machine-learning platform from real-world e-commerce behavior.

## Project goal

The completed platform will replay historical Synerise customer interactions,
process them through a streaming pipeline, transform them into analytics models
and ML features, serve predictions through an API, and display useful business
metrics.

The data is real and anonymized, but the source events occurred from June through
December 2022. The project therefore presents this workload as historical event
replay, not as live production traffic.

## Target architecture

```text
Synerise Parquet Dataset
      |
      v
Historical Replay Producer
      |
      v
    Kafka
      |
      v
Spark Structured Streaming
      |
      v
  PostgreSQL
      |
      v
     dbt
      |
      v
Feature Engineering
      |
      v
Machine Learning Model
      |
      v
   FastAPI
      |
      v
  Streamlit Dashboard
```

Apache Airflow will later orchestrate appropriate batch and ML workflows.
Docker Compose will provide reproducible local services.

## Current phase

**Phase 1 - Synerise data foundation and historical replay**

Completed objectives:

- [x] Acquire and checksum the full Synerise dataset
- [x] Validate 225,224,262 behavioral events and 1,534,050 products
- [x] Define versioned behavioral-event and product contracts
- [x] Implement deterministic identifiers and automated contract tests
- [x] Run Apache Kafka 4.3.1 locally in KRaft mode
- [x] Create event, product-reference, and dead-letter topics
- [ ] Implement and verify the historical replay producer

See the [Phase 0 guide](docs/phase-0.md) and
[Phase 1 guide](docs/phase-1.md) for detailed decisions and verification.

## Development approach

This project is built one verified phase at a time. Each technology is introduced
only when the previous phase works and its role in the architecture is understood.

## Local setup

Prerequisites:

- Python 3.11-3.13
- Git
- Docker Desktop with Docker Compose

Activate the Windows virtual environment from the repository root:

```powershell
.\.venv\Scripts\Activate.ps1
```

Verify the local Python environment:

```powershell
python --version
python -m pip --version
```

Run the Phase 0 container smoke test:

```powershell
docker compose config
docker compose run --rm -T --interactive=false phase0-check
```

Expected output:

```text
Phase 0 container is ready
Python 3.13.x
```

Start Kafka and initialize the project topics:

```powershell
docker compose up -d kafka kafka-init
docker compose ps -a
```

Describe the topics:

```powershell
docker compose exec -T kafka `
    /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:9092 `
    --describe
```

Stop the local services while preserving Kafka data:

```powershell
docker compose down
```

## Roadmap

1. Project foundation
2. Real-world data acquisition, contracts, and Kafka replay
3. PySpark Structured Streaming
4. PostgreSQL and analytical storage
5. dbt transformations and data quality
6. Apache Airflow orchestration
7. Feature engineering and machine learning
8. MLflow experiment and model management
9. FastAPI model serving
10. Streamlit dashboard
11. Testing, CI/CD, observability, and production hardening
