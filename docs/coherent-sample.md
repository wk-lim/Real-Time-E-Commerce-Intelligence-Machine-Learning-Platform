# Coherent historical event-replay sample

## Scope

On 2026-10-07, a bounded Synerise sample was selected from all five
behavioral source files using the half-open source timestamp window
`[2022-09-01 12:00:00, 2022-09-01 12:05:00)`. The source timestamps have no
verified timezone. This is a reproducible historical replay, not live store
traffic or a globally event-time-ordered stream.

The window preserves the observed event-type mix rather than taking an equal
number of physical rows from each file. The Parquet files are scanned in
physical row order; filtering retains each original zero-based source row
number. Deterministic event IDs therefore remain stable across replays.

| Event type | Source file | Events | Distinct clients in daily mart | Replay run ID |
|---|---|---:|---:|---|
| `add_to_cart` | `add_to_cart.parquet` | 195 | 144 | `e9e9b80b-6402-47b2-b833-b401190eeac4` |
| `page_visit` | `page_visit.parquet` | 5,826 | 2,073 | `2cbb6d2f-9b06-4366-8ba7-32220d46d321` |
| `product_buy` | `product_buy.parquet` | 118 | 47 | `b9090525-78c0-49ec-97e8-49895f0b78a8` |
| `remove_from_cart` | `remove_from_cart.parquet` | 76 | 38 | `1726dba2-6b61-4e37-8418-ffd82a73059b` |
| `search_query` | `search_query.parquet` | 373 | 140 | `aedec0d0-c3d0-49da-8bf1-65cef2375cb8` |
| **Total** | | **6,588** | **Not additive** | |

The client counts are distinct *within each event type*. One client can appear
in several rows, so summing those counts would not give total unique clients.

## Selection and verification

Profile the raw files before replaying:

```powershell
python -m ecommerce_intelligence.data_quality.profile_window `
  --start "2022-09-01 12:00:00" `
  --end "2022-09-01 12:05:00"
```

The profile reported 6,588 matching source rows. The producer accepts
`--window-start` (inclusive) and `--window-end` (exclusive) for behavioral
files. Its required `--limit` caps **selected records**, not the number of
source rows scanned. A dry run validates transformations without publishing:

```powershell
python -m ecommerce_intelligence.ingestion.replay `
  --source-file product_buy.parquet `
  --window-start "2022-09-01 12:00:00" `
  --window-end "2022-09-01 12:05:00" `
  --limit 200 `
  --messages-per-second 0 `
  --dry-run
```

All five source-file dry runs matched the profile counts and had zero
dead-lettered records. Separate bounded producer runs then delivered all
6,588 records to `ecommerce.events.v1` with zero reported delivery failures.
The Spark available-now event job, using its existing checkpoints, landed
6,588 records in event bronze across those five run IDs. The PostgreSQL
file-level loader processed the new bronze files in two bounded invocations;
`landing.events` contained the same 6,588 records for those run IDs. Earlier
smoke-test records were retained and were not counted as part of this sample.

Use the run IDs above when reconciling this sample with other data in bronze
or PostgreSQL. For example, the following SQL isolates these producer runs:

```sql
SELECT event_type, count(*) AS event_count
FROM landing.events
WHERE replay_run_id IN (
    'e9e9b80b-6402-47b2-b833-b401190eeac4',
    '2cbb6d2f-9b06-4366-8ba7-32220d46d321',
    'b9090525-78c0-49ec-97e8-49895f0b78a8',
    '1726dba2-6b61-4e37-8418-ffd82a73059b',
    'aedec0d0-c3d0-49da-8bf1-65cef2375cb8'
)
GROUP BY event_type
ORDER BY event_type;
```

The full dbt build then completed with 3 views and 15 passing data tests.
At that verification point, `analytics.fct_daily_event_activity` reported
the same five event counts for source date `2022-09-01`. This mart groups by
the **whole source-reported day**, not by the five-minute replay window: its
counts can change if additional September 1 events are loaded. The run-ID
filter above is the reproducible way to isolate this particular replay.

## Interpretation limits

- Source event timestamps are timezone-free. Historical event dates must not
  be presented as UTC dates or confused with 2026 publication timestamps.
- Separate source files were published in separate runs and physical Parquet
  order. Kafka preserves no global event-time ordering across partitions.
- A five-minute window censors activity before and after it. The source has
  no order or session IDs, so these counts do not establish a purchase
  conversion rate or a complete customer journey.
- Repeated client, SKU, and timestamp values can occur at different original
  row numbers. They remain distinct source records; whether they represent
  duplicate business actions is not established.
- Replaying a successful run again appends Kafka messages. Deterministic
  event IDs support PostgreSQL deduplication, but bronze is append-only and
  may contain repeated IDs if the producer is rerun. Avoid repeating the
  publish commands against this populated local topic.
- Product reference data was not expanded for this window. Product SKU is not
  unique in the landing table, and anonymized price buckets are not currency.
