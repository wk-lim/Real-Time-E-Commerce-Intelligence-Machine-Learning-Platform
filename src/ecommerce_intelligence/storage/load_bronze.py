from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pyarrow.dataset as ds
from psycopg import sql

from ecommerce_intelligence.storage.postgres import (
    PROJECT_ROOT,
    connect_postgres
)


COMMON_COLUMNS = (
    "message_key",
    "raw_json",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "schema_version",
)

LINEAGE_COLUMNS = (
    "source_dataset",
    "source_file",
    "source_row_number",
    "replay_run_id",
    "published_at_raw",
    "published_at_utc",
    "processed_at_utc",
)

TABLE_COLUMNS = {
    "events": COMMON_COLUMNS
    + (
        "event_id",
        "event_type",
        "event_time_raw",
        "event_time_local",
        "event_time_timezone",
        "client_id",
        "sku",
        "url_id",
        "query_vector_raw",
    )
    + LINEAGE_COLUMNS,
    "products": COMMON_COLUMNS
    + (
        "record_id",
        "sku",
        "category_id",
        "price_bucket",
        "name_vector_raw",
    )
    + LINEAGE_COLUMNS,
}

PRIMARY_KEYS = {
    "events": "event_id",
    "products": "record_id",
}

UUID_COLUMNS = {"event_id","record_id","replay_run_id"}
UTC_COLUMNS = {"kafka_timestamp","published_at_utc","processed_at_utc"}
MAX_LIMIT = 10_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=tuple(TABLE_COLUMNS),
        required=True,
    )
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bronze-root", type=Path, default=PROJECT_ROOT / "data" / "processed" / "bronze",)

    args = parser.parse_args()
    if not 1 <= args.limit <= MAX_LIMIT:
        parser.error(f"--limit must be between 1 and {MAX_LIMIT}")
    return args


def iter_rows(path: Path, columns: tuple[str, ...], limit: int):
    dataset = ds.dataset(path, format="parquet")
    missing = set(columns) - set(dataset.schema.names)
    if missing:
        raise ValueError(f"Missing bronze columns: {sorted(missing)}")

    selected = 0
    for batch in dataset.to_batches(
        columns=list(columns),
        batch_size=min(limit, 512),
    ):
        for row in batch.to_pylist():
            yield row
            selected += 1
            if selected >= limit:
                return

def normalize_value(column: str, value: object) -> object:
    if value is None:
        return None

    if column in UUID_COLUMNS:
        return UUID(str(value))

    if column in UTC_COLUMNS:
        if not isinstance(value, datetime):
            raise TypeError(f"{column} must be a datetime")
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if column == "event_time_local":
        if not isinstance(value, datetime) or value.tzinfo is not None:
            raise TypeError("event_time_local must be a naive datetime")

    return value

def insert_statement(source: str, columns: tuple[str, ...]):
    return sql.SQL(
        "INSERT INTO {table} ({columns}) VALUES ({values}) "
        "ON CONFLICT ({primary_key}) DO NOTHING RETURNING 1"
    ).format(
        table=sql.Identifier("landing", source),
        columns=sql.SQL(", ").join(
            sql.Identifier(column) for column in columns
        ),
        values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        primary_key=sql.Identifier(PRIMARY_KEYS[source]),
    )

def main() -> None:
    args = parse_args()
    path = args.bronze_root / args.source
    if not path.is_dir():
        raise FileNotFoundError(f"Bronze directory not found: {path}")

    columns = TABLE_COLUMNS[args.source]
    primary_key = PRIMARY_KEYS[args.source]

    print(f"Source: {path}")
    print(f"Limit: {args.limit}")
    print(f"Mode: {'dry run' if args.dry_run else 'PostgreSQL insert'}")
    print("Selection: Parquet scan order, not event-time order")

    rows = iter_rows(path, columns, args.limit)
    processed = 0

    if args.dry_run:
        for row in rows:
            processed += 1
            # Exercise type conversion without writing to PostgreSQL
            for column in columns:
                normalize_value(column, row[column])
            if processed <= 3:
                print(
                    f"{primary_key}={row[primary_key]} "
                    f"source_row_number={row['source_row_number']}"
                )

        print(f"Previewed: {processed}")
        return

    inserted = 0
    statement = insert_statement(args.source, columns)

    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            for row in rows:
                values = tuple(
                    normalize_value(column, row[column]) for column in columns
                )
                result = cursor.execute(statement, values).fetchone()
                processed += 1
                if result is not None:
                    inserted += 1

    print(f"Processed: {processed}")
    print(f"Inserted: {inserted}")
    print(f"Already present: {processed - inserted}")

if __name__ == "__main__":
    main()
