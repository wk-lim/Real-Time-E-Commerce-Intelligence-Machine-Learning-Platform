from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow.parquet as pq


EXPECTED_EVENT_ROWS = {
    "add_to_cart.parquet": 7_541_117,
    "page_visit.parquet": 199_451_980,
    "product_buy.parquet": 2_318_502,
    "remove_from_cart.parquet": 2_688_894,
    "search_query.parquet": 13_223_769,
}

EXPECTED_PRODUCT_ROWS = 1_534_050

EXPECTED_FILES = (
    *EXPECTED_EVENT_ROWS,
    "product_properties.parquet",
)

EXPECTED_EVENT_TOTAL = 225_224_262


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the local Synerise Parquet dataset."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/synerise"),
        help="Directory containing the extracted Synerise Parquet files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir.resolve()

    if not data_dir.is_dir():
        print(f"ERROR: Dataset directory does not exist: {data_dir}")
        return 1

    missing_files = [
        filename
        for filename in EXPECTED_FILES
        if not (data_dir / filename).is_file()
    ]

    if missing_files:
        print("ERROR: Missing required files:")
        for filename in missing_files:
            print(f"  - {filename}")
        return 1

    errors: list[str] = []
    event_total = 0

    print(f"Dataset directory: {data_dir}")

    for filename in EXPECTED_FILES:
        path = data_dir / filename
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        row_count = metadata.num_rows

        print(f"\n{filename}")
        print(f"  rows: {row_count:,}")
        print(f"  row groups: {metadata.num_row_groups}")
        print("  schema:")

        for field in parquet_file.schema_arrow:
            print(f"    - {field.name}: {field.type}")

        if filename == "product_properties.parquet":
            if row_count != EXPECTED_PRODUCT_ROWS:
                errors.append(
                    f"{filename}: expected {EXPECTED_PRODUCT_ROWS:,} rows, "
                    f"found {row_count:,}"
                )
            continue

        expected_rows = EXPECTED_EVENT_ROWS[filename]
        event_total += row_count

        if row_count != expected_rows:
            errors.append(
                f"{filename}: expected {expected_rows:,} rows, "
                f"found {row_count:,}"
            )

    print(f"\nTotal behavioral events: {event_total:,}")

    if event_total != EXPECTED_EVENT_TOTAL:
        errors.append(
            f"Expected {EXPECTED_EVENT_TOTAL:,} behavioral events, "
            f"found {event_total:,}"
        )

    if errors:
        print("\nVALIDATION FAILED")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("\nVALIDATION PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())