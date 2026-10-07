# Phase 5: manual Airflow orchestration

## Status and scope

Verified locally on 2026-10-07 with Apache Airflow 3.3.2 and LocalExecutor.
The API server, scheduler, and DAG processor reported healthy heartbeats.
The `phase5_airflow_smoke` DAG completed its task, and the manually triggered
`phase5_bronze_to_analytics` DAG completed all three tasks successfully.

This is a bounded batch workflow over **existing Spark bronze files**. Airflow
does not yet start the historical Kafka replay or submit the Spark streams.
Both Phase 5 DAGs use `schedule=None`; creating or unpausing a DAG does not
schedule automatic runs.

## Workflow

```text
Existing bronze Parquet
    -> load_products (at most one new file, at most 10,000 rows)
    -> load_events   (at most one new file, at most 10,000 rows)
    -> dbt build     (staging views, mart, and data tests)
```

The DAG is defined in `dags/phase5_bronze_to_analytics.py`. It allows only
one active run at a time. The first two tasks invoke the Phase 3 incremental
loader with `--apply`; each completed file and its inserted rows commit in one
PostgreSQL transaction. Previously recorded files are skipped, and a changed
file at the same path fails rather than being silently reloaded. The dbt task
runs after both loaders, including when neither finds a new file. A failed
subprocess causes its Airflow task to fail and prevents downstream tasks from
running.

The separate `phase5_airflow_smoke` DAG performs no data load. It verifies
that a manually triggered Python task can run under LocalExecutor.

## Local setup

Keep `POSTGRES_PASSWORD`, `AIRFLOW_DB_PASSWORD`, and `AIRFLOW_JWT_SECRET` in the
ignored `.env` file; `.env.example` contains names only. Airflow's metadata
database (`airflow-db`) is separate from the project's PostgreSQL landing
database (`postgres`). The API server binds to `127.0.0.1:18080` on the host.
Simple Auth Manager is for local development only; do not expose this setup
as a production Airflow deployment.

Build the project-specific image before starting the scheduler and DAG
processor. It installs the project and dbt while keeping Airflow pinned to
the version in the base image:

```powershell
docker build -f Dockerfile.airflow -t ecommerce-airflow:3.3.2 .
docker compose config --quiet
docker compose up -d airflow-scheduler airflow-dag-processor
docker compose ps -a airflow-db airflow-init airflow-api-server airflow-scheduler airflow-dag-processor postgres
```

`airflow-init` exiting with code 0 after its database migration is expected.
Compose waits for the API server and project PostgreSQL to be healthy before
starting the scheduler. The scheduler and DAG processor use the custom image;
the API server and initialization job use the base Airflow image. The DAG
directory is mounted read-only into the scheduler and DAG processor. Only the
bronze directory is mounted read-only into the scheduler. The ignored `.env`
file and raw Synerise dataset are not copied into the custom image.

Verify the two Airflow heartbeats and the project database/dbt connections:

```powershell
$health = Invoke-RestMethod http://127.0.0.1:18080/api/v2/monitor/health
$health.scheduler
$health.dag_processor

docker compose exec -T airflow-scheduler python -m ecommerce_intelligence.storage.postgres
docker compose exec -T airflow-scheduler `
  dbt debug --project-dir /opt/project --profiles-dir /opt/project
```

The unused triggerer may have a blank health status. Before triggering the
data-writing DAG, check that Airflow has parsed it without import errors:

```powershell
docker compose exec -T airflow-dag-processor airflow dags list-import-errors
docker compose exec -T airflow-api-server airflow dags list
```

New DAGs may take a short time to appear in the UI. They start paused, so use
the **All** or **Paused** DAG view at `http://127.0.0.1:18080` and clear any
search or tag filters. Unpause and trigger `phase5_bronze_to_analytics` only
when a bounded PostgreSQL write and dbt build are intended. The generated
Simple Auth Manager password is private; never commit it or paste it into an
issue or log excerpt.

## Verification

At the 2026-10-07 verification point, the real DAG's `load_products`,
`load_events`, and `build_analytics` tasks all succeeded. The file-load ledger
and database reconciliation reported:

| Source | Tracked files | File rows | Cumulative rows inserted by tracked loads |
| --- | ---: | ---: | ---: |
| Events | 18 | 6,601 | 6,596 |
| Products | 2 | 3 | 0 |

The difference reflects 5 event rows and 3 product rows already present from
the earlier bounded sample load. The ledger's inserted counts are cumulative
across file loads; they are **not** the insertion counts of this Airflow run.
`landing.events` contained 6,601 rows, equal to the sum of
`analytics.fct_daily_event_activity.event_count`. Both `landing.products` and
`analytics.stg_products` contained 3 rows. The 6,601 landing events include
earlier smoke-test records as well as the separate 6,588-event historical
window documented in [the coherent sample guide](coherent-sample.md).

Recheck current state after later DAG runs with:

```powershell
docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT source_name, count(*) AS tracked_files, sum(row_count) AS file_rows, sum(inserted_count) AS inserted_rows FROM landing.bronze_file_loads GROUP BY source_name ORDER BY source_name;"

docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT (SELECT count(*) FROM landing.events) AS landing_events, (SELECT coalesce(sum(event_count), 0) FROM analytics.fct_daily_event_activity) AS mart_events, (SELECT count(*) FROM landing.products) AS landing_products, (SELECT count(*) FROM analytics.stg_products) AS staged_products;"
```

## Limitations and next work

- Runs are manual. Airflow does not yet orchestrate Kafka replay, Spark
  streaming, or continuous bronze ingestion.
- One new file per source per run is a local safety bound, not a throughput
  design for the full Synerise dataset. Repeated manual runs are needed to
  process a backlog.
- The dbt models are views over landing data; the build verifies the current
  bounded dataset but does not establish business trends or conversions.
- This development Compose deployment uses Simple Auth Manager and a local
  Docker image. Production deployment needs stronger authentication, secret
  management, resource controls, and durable task logging.
