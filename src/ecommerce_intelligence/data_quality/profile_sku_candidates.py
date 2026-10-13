import argparse
from collections import Counter
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow.parquet as pq


SKU_FILES = {
    "add_to_cart": "add_to_cart.parquet",
    "product_buy": "product_buy.parquet",
    "remove_from_cart": "remove_from_cart.parquet",
}

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--activity-date", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/synerise"),
    )
    args = parser.parse_args()

    start = datetime.combine(args.activity_date, time.min)
    end = start + timedelta(days=1)
    start_text = str(start)
    end_text = str(end)

    pair_counts: Counter[tuple[int, int]] = Counter()
    event_counts: Counter[str] = Counter()
    missing_ids = 0

    for event_type, filename in SKU_FILES.items():
        parquet_file = pq.ParquetFile(args.data_dir / filename)
        for batch in parquet_file.iter_batches(
            batch_size=65_536,
            columns=["client_id", "sku", "timestamp"],
        ):
            values = batch.to_pydict()
            for client_id, sku, raw_timestamp in zip(
                values["client_id"],
                values["sku"],
                values["timestamp"],
            ):
                if raw_timestamp is None:
                    continue
                if isinstance(raw_timestamp, bytes):
                    raw_timestamp = raw_timestamp.decode("utf-8")

                timestamp_text = str(raw_timestamp)
                if not start_text <= timestamp_text < end_text:
                    continue

                datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")
                event_counts[event_type] += 1

                if client_id is None or sku is None:
                    missing_ids += 1
                    continue

                pair_counts[(int(client_id), int(sku))] += 1

    print(f"Activity date: {args.activity_date}")
    for event_type in SKU_FILES:
        print(f"{event_type}: {event_counts[event_type]}")
    print(f"Total SKU events: {sum(event_counts.values())}")
    print(f"Rows missing client_id or sku: {missing_ids}")
    print(f"Distinct client-SKU candidates: {len(pair_counts)}")
    print(f"Distinct clients: {len({client for client, _ in pair_counts})}")

    outcome_start = end
    outcome_end = end + timedelta(days=7)
    first_purchase: dict[tuple[int, int], datetime] = {}
    matching_purchase_rows = 0
    invalid_candidate_timestamps = 0

    purchase_file = pq.ParquetFile(args.data_dir / "product_buy.parquet")
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
            if key not in pair_counts:
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

            if not outcome_start <= purchased_at < outcome_end:
                continue

            matching_purchase_rows += 1
            if key not in first_purchase or purchased_at < first_purchase[key]:
                first_purchase[key] = purchased_at

    print(f"Outcome window: [{outcome_start}, {outcome_end})")
    print(f"Matching purchase rows: {matching_purchase_rows}")
    print(f"Pairs with purchase observed: {len(first_purchase)}")
    print(
        "Pairs with no purchase observed in this file: "
        f"{len(pair_counts) - len(first_purchase)}"
    )
    print(f"Invalid candidate timestamps: {invalid_candidate_timestamps}")

    if invalid_candidate_timestamps:
        raise SystemExit("Audit incomplete: invalid candidate timestamps")



if __name__ == "__main__":
    main()