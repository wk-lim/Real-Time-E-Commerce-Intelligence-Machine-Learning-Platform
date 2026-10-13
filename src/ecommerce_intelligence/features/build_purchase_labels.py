import argparse
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", required=True, type=Path)
    parser.add_argument(
        "--purchase-file",
        type=Path,
        default=Path("data/raw/synerise/product_buy.parquet"),
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    if args.output.exists():
        raise SystemExit(f"Output already exists: {args.output}")

    feature_rows = pq.read_table(args.features).to_pylist()
    if not feature_rows:
        raise SystemExit("Feature file contains no rows")

    as_of_dates = {row["feature_as_of_date_source"] for row in feature_rows}
    if len(as_of_dates) != 1:
        raise SystemExit("This builder requires one as-of date per feature file")

    as_of_date = next(iter(as_of_dates))
    if isinstance(as_of_date, str):
        as_of_date = date.fromisoformat(as_of_date)

    window_start = datetime.combine(as_of_date, time.min)
    window_end = window_start + timedelta(days=7)

    candidates = {
        (int(row["client_id"]), int(row["sku"]))
        for row in feature_rows
    }
    if len(candidates) != len(feature_rows):
        raise SystemExit("Feature file has duplicate client-SKU pairs")

    first_purchase: dict[tuple[int, int], datetime] = {}
    matching_purchase_rows = 0
    invalid_candidate_timestamps = 0

    purchase_file = pq.ParquetFile(args.purchase_file)
    for batch in purchase_file.iter_batches(
        batch_size=65_536,
        columns=["client_id", "sku", "timestamp"],
    ):
        values = batch.to_pydict()
        for client_id, sku, raw_timestamp in zip(
            values["client_id"],
            values["sku"],
            values["timestamp"],
        ):
            if client_id is None or sku is None:
                continue

            key = (int(client_id), int(sku))
            if key not in candidates:
                continue

            if isinstance(raw_timestamp, bytes):
                raw_timestamp = raw_timestamp.decode("utf-8")

            try:
                purchased_at = datetime.strptime(
                    str(raw_timestamp), "%Y-%m-%d %H:%M:%S"
                )
            except (TypeError, ValueError):
                invalid_candidate_timestamps += 1
                continue

            if not window_start <= purchased_at < window_end:
                continue

            matching_purchase_rows += 1
            if key not in first_purchase or purchased_at < first_purchase[key]:
                first_purchase[key] = purchased_at

    if invalid_candidate_timestamps:
        raise SystemExit(
            f"Audit incomplete: {invalid_candidate_timestamps} invalid "
            "candidate timestamps"
        )

    label_rows = []
    for row in feature_rows:
        client_id = int(row["client_id"])
        sku = int(row["sku"])
        purchased_at = first_purchase.get((client_id, sku))

        label_rows.append(
            {
                "feature_as_of_date_source": as_of_date,
                "client_id": client_id,
                "sku": sku,
                "purchase_observed_within_7d": purchased_at is not None,
                "first_purchase_at_source": purchased_at,
                "hours_to_first_purchase": (
                    (purchased_at - window_start).total_seconds() / 3600
                    if purchased_at is not None
                    else None
                ),
                "observation_window_end_source": window_end,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(label_rows),
        args.output,
        compression="snappy",
    )

    print(f"Window: [{window_start}, {window_end})")
    print(f"Label rows: {len(label_rows)}")
    print(f"Matching purchase rows: {matching_purchase_rows}")
    print(f"Pairs with purchase observed: {len(first_purchase)}")
    print(f"Pairs without purchase observed: {len(candidates) - len(first_purchase)}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()