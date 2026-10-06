from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow.parquet as pq
import psycopg

import argparse

from ecommerce_intelligence.storage.bronze_files import iter_bronze_files
from ecommerce_intelligence.storage.postgres import PROJECT_ROOT, connect_postgres

from ecommerce_intelligence.storage.bronze_files import (
    BronzeFile,
    classify_file,
    sha256_file,
)
from ecommerce_intelligence.storage.load_bronze import (
    TABLE_COLUMNS,
    insert_statement,
    normalize_value,
)


@dataclass(frozen=True)
class LoadResult:
    status: Literal["loaded", "skipped"]
    row_count: int
    inserted_count: int


def assert_file_unchanged(bronze_file: BronzeFile) -> None:
    if (
        bronze_file.path.stat().st_size != bronze_file.file_size_bytes
        or sha256_file(bronze_file.path) != bronze_file.file_sha256
    ):
        raise RuntimeError(
            f"Bronze file changed during loading: {bronze_file.path}"
        )


def load_file(
    connection: psycopg.Connection,
    bronze_file: BronzeFile,
    max_file_rows: int = 10_000,
) -> LoadResult:
    if not connection.autocommit:
        raise ValueError("Connection must use autocommit=True")
    if max_file_rows < 1:
        raise ValueError("max_file_rows must be positive")

    with connection.transaction():
        with connection.cursor() as cursor:
            claim = cursor.execute(
                """
                INSERT INTO landing.bronze_file_loads (
                    source_name, relative_path, file_size_bytes,
                    file_sha256, row_count, inserted_count
                )
                VALUES (%s, %s, %s, %s, %s, 0)
                ON CONFLICT (source_name, relative_path) DO NOTHING
                RETURNING 1
                """,
                (
                    bronze_file.source_name,
                    bronze_file.relative_path,
                    bronze_file.file_size_bytes,
                    bronze_file.file_sha256,
                    bronze_file.row_count,
                ),
            ).fetchone()

            if claim is None:
                recorded = cursor.execute(
                    """
                    SELECT file_size_bytes, file_sha256, row_count
                    FROM landing.bronze_file_loads
                    WHERE source_name = %s AND relative_path = %s
                    """,
                    (bronze_file.source_name, bronze_file.relative_path),
                ).fetchone()
                if recorded is None:
                    raise RuntimeError("Conflicting ledger row disappeared")

                classify_file(bronze_file, recorded)
                assert_file_unchanged(bronze_file)
                return LoadResult("skipped", bronze_file.row_count, 0)

            if bronze_file.row_count > max_file_rows:
                raise ValueError(
                    f"{bronze_file.relative_path} has {bronze_file.row_count} "
                    f"rows; maximum is {max_file_rows}"
                )

            columns = TABLE_COLUMNS[bronze_file.source_name]
            statement = insert_statement(bronze_file.source_name, columns)
            processed = 0
            inserted = 0

            with bronze_file.path.open("rb") as stream:
                parquet_file = pq.ParquetFile(stream)
                missing = set(columns) - set(parquet_file.schema_arrow.names)
                if missing:
                    raise ValueError(
                        f"Missing bronze columns: {sorted(missing)}"
                    )

                for batch in parquet_file.iter_batches(
                    batch_size=512,
                    columns=list(columns),
                ):
                    for row in batch.to_pylist():
                        values = tuple(
                            normalize_value(column, row[column])
                            for column in columns
                        )
                        result = cursor.execute(statement, values).fetchone()
                        processed += 1
                        if result is not None:
                            inserted += 1

            if processed != bronze_file.row_count:
                raise RuntimeError(
                    f"Parquet row count changed: {bronze_file.path}"
                )
            assert_file_unchanged(bronze_file)

            cursor.execute(
                """
                UPDATE landing.bronze_file_loads
                SET inserted_count = %s, completed_at_utc = now()
                WHERE source_name = %s AND relative_path = %s
                """,
                (
                    inserted,
                    bronze_file.source_name,
                    bronze_file.relative_path,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Could not finalize bronze file ledger")

            return LoadResult("loaded", processed, inserted)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally load committed bronze Parquet files."
    )
    parser.add_argument("--source", choices=("events", "products"), required=True)
    parser.add_argument("--max-new-files", type=int, required=True)
    parser.add_argument("--max-file-rows", type=int, default=10_000)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not 1 <= args.max_new_files <= 10:
        parser.error("--max-new-files must be between 1 and 10")
    if not 1 <= args.max_file_rows <= 10_000:
        parser.error("--max-file-rows must be between 1 and 10000")
    if not args.apply:
        parser.error("Pass --apply to permit PostgreSQL writes")

    bronze_root = PROJECT_ROOT / "data" / "processed" / "bronze"
    loaded = 0
    skipped = 0
    inserted = 0

    with connect_postgres() as connection:
        connection.autocommit = True

        for bronze_file in iter_bronze_files(bronze_root, args.source):
            if loaded >= args.max_new_files:
                break

            result = load_file(
                connection,
                bronze_file,
                max_file_rows=args.max_file_rows,
            )
            print(
                f"{result.status}: {bronze_file.relative_path} "
                f"rows={result.row_count} inserted={result.inserted_count}"
            )

            if result.status == "loaded":
                loaded += 1
                inserted += result.inserted_count
            else:
                skipped += 1

    print(f"New files loaded: {loaded}")
    print(f"Previously loaded files skipped: {skipped}")
    print(f"Rows inserted: {inserted}")


if __name__ == "__main__":
    main()