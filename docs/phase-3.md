# Phase 3: PostgreSQL landing storage

## Status

The initial bounded PostgreSQL landing load was verified locally on
2026-10-05. The landing tables contain 5 events and 3 products. This is
not yet a full-dataset load or a continuously running database sink.

## Flow

Valid Spark bronze Parquet files are read by a host-side Python loader
using PyArrow. Psycopg inserts selected rows into PostgreSQL:

```text
Bronze Parquet -> bounded Python loader -> PostgreSQL landing tables
```

Rejected Spark records remain in the quarantine Parquet directories.
The producer's Kafka dead-letter topic is not loaded by this job.

## Local setup

Set a unique `POSTGRES_PASSWORD` in the ignored `.env` file. Never commit
or share that file.

```powershell
docker compose config --quiet
docker compose up -d postgres
docker compose ps postgres
```

PostgreSQL uses the named `postgres-data` volume. The host port is bound
to `127.0.0.1`; other containers use the internal PostgreSQL port 5432.

Create the landing schema:

```powershell
docker compose cp sql/001_create_landing.sql postgres:/tmp/001_create_landing.sql

docker compose exec -T postgres `
  psql -X -1 -v ON_ERROR_STOP=1 `
  -U ecommerce_admin -d ecommerce_intelligence `
  -f /tmp/001_create_landing.sql
```

## Bounded load

Preview records without a database connection:

```powershell
python -m ecommerce_intelligence.storage.load_bronze `
  --source events --limit 5 --dry-run

python -m ecommerce_intelligence.storage.load_bronze `
  --source products --limit 3 --dry-run
```

Load the same bounded samples by omitting `--dry-run`:

```powershell
python -m ecommerce_intelligence.storage.load_bronze `
  --source events --limit 5

python -m ecommerce_intelligence.storage.load_bronze `
  --source products --limit 3
```

The required `--limit` is capped at 10,000 rows per invocation to prevent
an accidental full-dataset load. Rerun the same commands and check the
`Inserted` and `Already present` summaries.

## Verification

The local landing tables contained 5 events and 3 products:

```powershell
docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT 'events' AS source, count(*) AS rows FROM landing.events UNION ALL SELECT 'products', count(*) FROM landing.products;"
```

The Python test suite passed with 49 tests, including loader row-limit,
timestamp-conversion, and dry-run checks.

## Data semantics and limitations

- `event_id` and `record_id` are the respective table primary keys.
  Repeated IDs are skipped on insert.
- Each table also enforces uniqueness on dataset, source file, and source
  row number. An inconsistent ID for the same source row raises an error.
- Product `sku` is not unique in the landing table; later models must
  choose a product-record policy.
- Original event times have no known timezone and remain
  `TIMESTAMP WITHOUT TIME ZONE`. Replay and processing times are stored
  as UTC `TIMESTAMPTZ`.
- Product `price_bucket` is anonymized and is not a monetary price.
- The loader scans local Parquet files in scan order, not event-time order.
  It has no incremental file checkpoint or scheduler yet.
- Per-row inserts and a 10,000-row cap make this an initial correctness
  checkpoint, not a design for loading all 225 million events.
- The automated tests do not require a live PostgreSQL instance; the
  database load was verified separately with local commands.