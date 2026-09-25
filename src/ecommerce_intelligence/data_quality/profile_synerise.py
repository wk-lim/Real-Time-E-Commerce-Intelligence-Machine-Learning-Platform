from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


TABLES = (
    "add_to_cart.parquet",
    "page_visit.parquet",
    "product_buy.parquet",
    "remove_from_cart.parquet",
    "search_query.parquet",
    "product_properties.parquet",
)

RANGE_COLUMNS = {
    "client_id",
    "timestamp",
    "sku",
    "url",
    "category",
    "price",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile Synerise using Parquet footer statistics."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/synerise"),
    )
    return parser.parse_args()


def normalize_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def column_statistics(
    parquet_file: pq.ParquetFile,
    column_index: int,
) -> tuple[int | None, Any | None, Any | None]:
    null_count = 0
    null_count_available = True
    minimums: list[Any] = []
    maximums: list[Any] = []

    for row_group_index in range(parquet_file.metadata.num_row_groups):
        column = parquet_file.metadata.row_group(
            row_group_index
        ).column(column_index)
        statistics = column.statistics

        if statistics is None:
            null_count_available = False
            continue

        if statistics.null_count is None:
            null_count_available = False
        else:
            null_count += statistics.null_count

        if statistics.has_min_max:
            minimums.append(normalize_value(statistics.min))
            maximums.append(normalize_value(statistics.max))

    return (
        null_count if null_count_available else None,
        min(minimums) if minimums else None,
        max(maximums) if maximums else None,
    )


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir.resolve()

    if not data_dir.is_dir():
        print(f"ERROR: Dataset directory does not exist: {data_dir}")
        return 1

    for filename in TABLES:
        path = data_dir / filename

        if not path.is_file():
            print(f"ERROR: Missing file: {path}")
            return 1

        parquet_file = pq.ParquetFile(path)

        print(f"\n{filename}")
        print(f"  rows: {parquet_file.metadata.num_rows:,}")

        for index, field in enumerate(parquet_file.schema_arrow):
            null_count, minimum, maximum = column_statistics(
                parquet_file,
                index,
            )

            null_display = (
                f"{null_count:,}"
                if null_count is not None
                else "unavailable"
            )

            print(f"  {field.name}: nulls={null_display}")

            if field.name in RANGE_COLUMNS:
                print(f"    min={minimum}")
                print(f"    max={maximum}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())