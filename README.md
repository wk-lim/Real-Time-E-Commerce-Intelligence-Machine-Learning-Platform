# Real-Time E-Commerce Intelligence & Machine Learning Platform

A portfolio project demonstrating how to build a reproducible, production-style
data and machine-learning platform for simulated e-commerce activity.

## Project goal

The completed platform will generate realistic user events, process them through
a real-time streaming pipeline, transform them into analytics models and ML
features, serve predictions through an API, and display useful business metrics.

## Target architecture

```text
Event Generator
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

**Phase 0 - Project setup**

Completed objectives:

- [x] Connect the local project to GitHub
- [x] Create an isolated Python virtual environment
- [x] Add an initial `.gitignore`
- [x] Add Python project metadata
- [x] Add environment-variable conventions
- [x] Add the Docker and Docker Compose foundation
- [x] Verify the complete Phase 0 setup

No Kafka, Spark, database, API, dashboard, or ML services have been added yet.

Detailed setup, verification, troubleshooting, and interview questions are
available in the [Phase 0 guide](docs/phase-0.md).

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
docker compose config --quiet
docker compose run --rm -T --interactive=false phase0-check
```

Expected output:

```text
Phase 0 container is ready
Python 3.13.x
```

## Roadmap

1. Project setup
2. E-commerce event generator
3. Apache Kafka
4. PySpark Structured Streaming
5. PostgreSQL
6. dbt transformations
7. Apache Airflow
8. Feature engineering and machine learning
9. MLflow
10. FastAPI
11. Streamlit dashboard
12. Containerization, testing, CI/CD, and monitoring
