# Phase 4: dbt analytics over bounded PostgreSQL data

## Status

The initial dbt slice was verified locally on 2026-10-06. The PostgreSQL
landing tables contained 5 events and 3 products at verification time. The
full dbt build created 3 views and passed 15 data tests. This demonstrates
source-to-mart lineage and data-quality checks, not meaningful business trends
or full-dataset analytics.

## Model flow and grain

```text
landing.events   -> analytics.stg_events   -> analytics.fct_daily_event_activity
landing.products -> analytics.stg_products
```

`models/staging/_landing.yml` declares the PostgreSQL landing tables as dbt
sources. The staging views select explicit columns and preserve one row per
landing record, including source and replay lineage. They do not filter or
deduplicate data. `stg_products` preserves the anonymized `price_bucket` and
`name_vector_raw` fields; it does not interpret them as monetary price or
human-readable product name.

`fct_daily_event_activity` has one row per source-reported calendar date and
event type. It reports `event_count` and `distinct_clients` within each group.
Distinct client counts must not be summed across groups because a client can
occur in multiple dates or event types. The source event timestamp has no
verified timezone: `event_date_source` is not a UTC date. Replay publication
and processing timestamps remain separate from the historical event time.

No event-to-product join or conversion metric is modeled yet. `sku` is not
unique in `landing.products`, and the current event sample has no purchases.

## Local setup and build

Start PostgreSQL and create and populate the landing tables as described in
the [Phase 3 guide](phase-3.md). Activate the repository's virtual environment
and install the dbt optional dependencies:

```powershell
python -m pip install --editable ".[dev,analytics]"
docker compose up -d postgres
docker compose ps postgres
```

`dbt-core` and `dbt-postgres` are pinned to the 1.11 release line in
`pyproject.toml`. The project's `profiles.yml` refers to `POSTGRES_PASSWORD`
through `env_var`; the actual password belongs in the ignored `.env` file.
The generated `.user.yml`, dbt `target/`, `logs/`, and `dbt_packages/` are also
ignored. Run the commands below from the repository root. The Python wrapper
loads `.env` into the dbt process because dbt 1.11 does not load it
automatically:

```powershell
python -c "from dotenv import load_dotenv; import subprocess; load_dotenv('.env', override=False); raise SystemExit(subprocess.call(['dbt', 'debug', '--profiles-dir', '.']))"

python -c "from dotenv import load_dotenv; import subprocess; load_dotenv('.env', override=False); raise SystemExit(subprocess.call(['dbt', 'build', '--profiles-dir', '.']))"
```

The profile targets the local `ecommerce_intelligence` database and writes
dbt views to the `analytics` schema. `dbt_project.yml` keeps dbt SQL tests in
`dbt_tests/`, separate from the Python `tests/` directory.

## Verification

The full local build reported `PASS=18`: 3 views and 15 data tests. The tests
include source and staging identifier checks, mart not-null checks, and two
singular assertions:

- `assert_daily_event_totals` requires the mart's summed event counts to
  equal the staged event row count.
- `assert_daily_event_grain` rejects duplicate date/event-type groups.

The default Python suite reported 58 passed and one skipped optional
PostgreSQL integration test. That integration test is described in the
[Phase 3 guide](phase-3.md).

At verification time, the staging views matched their landing tables (5
events and 3 products), and the mart counts summed to 5. The event sample
contained 3 `add_to_cart` records and 2 `page_visit` records. Check current
counts after any additional loads with:

```powershell
docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT (SELECT count(*) FROM landing.events) AS landing_events, (SELECT count(*) FROM analytics.stg_events) AS staged_events, (SELECT count(*) FROM landing.products) AS landing_products, (SELECT count(*) FROM analytics.stg_products) AS staged_products;"

docker compose exec -T postgres `
  psql -U ecommerce_admin -d ecommerce_intelligence `
  -c "SELECT (SELECT count(*) FROM analytics.stg_events) AS staged_events, (SELECT coalesce(sum(event_count), 0) FROM analytics.fct_daily_event_activity) AS mart_events;"
```

These views reflect landing-table changes when queried, but the dbt tests do
not run automatically after a load. Run `dbt build` again to validate the new
data and keep model definitions synchronized.

## Current limitations and next work

- Five events and three products are a pipeline-verification sample, not a
  representative basis for trend, revenue, or conversion claims.
- `price_bucket` is anonymized and cannot be used as a currency amount.
- The source event timezone is unknown. Do not mix source event dates with
  UTC replay or processing dates without an explicit policy.
- Replaying an equal fixed number of rows from each event-type file would
  distort the natural event mix and may select unrelated customers and time
  periods. A future bounded analytical sample should preserve a coherent
  cohort or time window and the observed event-type proportions before
  conversion metrics are introduced.
- The local PostgreSQL loader and dbt models have not been exercised on all
  225 million behavioral events; continuous loading and orchestration remain
  future work.
