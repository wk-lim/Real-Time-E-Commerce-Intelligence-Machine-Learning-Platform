# Phase 3: PostgreSQL landing storage

## Status

The initial bounded PostgreSQL landing load was verified locally on
2026-10-05. The landing tables contain 5 events and 3 products. This is
not yet a full-dataset load or a continuously running database sink.

On 2026-10-06, file-level resume was verified with two product files
(3 rows) and one event file (2 rows) recorded in the load ledger. Those
files inserted no new landing rows because the bounded loader had already
loaded their records.

## Flow

Valid Spark bronze Parquet files are read by host-side Python loaders
using PyArrow. Psycopg inserts selected rows into PostgreSQL:

```text
Bronze Parquet -> bounded sample loader -----> PostgreSQL landing tables
               -> file-level resume loader --> PostgreSQL landing tables
                                         +----> landing.bronze_file_loads ledger
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

Create the file-load ledger before running the incremental loader:

```powershell
docker compose cp sql/002_create_bronze_file_loads.sql postgres:/tmp/002_create_bronze_file_loads.sql

docker compose exec -T postgres `
  psql -X -1 -v ON_ERROR_STOP=1 `
  -U ecommerce_admin -d ecommerce_intelligence `
  -f /tmp/002_create_bronze_file_loads.sql
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

## Incremental file-level load

The separate incremental loader discovers visible, non-temporary bronze
Parquet files, hashes each file, and records completed loads in
`landing.bronze_file_loads`.
It requires an explicit `--apply` flag and caps each run at 10 newly loaded
files and each file at 10,000 rows. For a bounded local run:

```powershell
python -m ecommerce_intelligence.storage.incremental_loader `
  --source products --max-new-files 1 --max-file-rows 10000 --apply

python -m ecommerce_intelligence.storage.incremental_loader `
  --source events --max-new-files 1 --max-file-rows 10000 --apply
```

Rerunning the same command skips files whose relative path, size, SHA-256,
and row count match the ledger. `--max-new-files` counts newly loaded files,
not skipped files: a rerun can skip completed files and continue to the
next pending file. A changed file at a previously loaded path raises an
error rather than silently replacing data. For each new file, landing-row
inserts and the ledger entry commit in one PostgreSQL transaction; a failure
rolls back both. This is file-level resume, not a continuous database sink.

## Verification

The local landing tables contained 5 events and 3 products:

```powershell
docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT 'events' AS source, count(*) AS rows FROM landing.events UNION ALL SELECT 'products', count(*) FROM landing.products;"
```

Inspect file-level progress:

```powershell
docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT source_name, count(*) AS files, sum(row_count) AS file_rows, sum(inserted_count) AS inserted_rows FROM landing.bronze_file_loads GROUP BY source_name ORDER BY source_name;"
```

The default Python suite passed with 58 tests and one skipped
PostgreSQL-dependent integration test. The integration test passed when
enabled explicitly with `RUN_POSTGRES_INTEGRATION=1`; it verified rollback
after a partial row load, successful retry, and subsequent skip inside an
outer rollback-only transaction. A separate row-limit failure left the
event table at 5 rows and the event ledger at 0 rows before the successful
event-file load.

Run the optional PostgreSQL integration test only when the local database
and bronze event files are available:

```powershell
$env:RUN_POSTGRES_INTEGRATION = "1"
python -m pytest tests/test_incremental_postgres.py -q
Remove-Item Env:RUN_POSTGRES_INTEGRATION
```

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
- The bounded loader scans Parquet rows in scan order; the incremental
  loader scans file paths in sorted order. Neither guarantees event-time order.
- The file ledger tracks completed files, but there is no scheduler,
  continuous database sink, or reconciliation of deleted source files.
- Per-row inserts, repeated file hashing, and the 10,000-row-per-file cap
  make this a correctness checkpoint, not a design for loading all
  225 million events.
- The default automated tests do not require a live PostgreSQL instance;
  the optional integration test and local load verification do.
