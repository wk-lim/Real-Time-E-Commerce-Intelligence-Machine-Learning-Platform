import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SKU_FILES = {
    "add_to_cart": "add_to_cart.parquet",
    "product_buy": "product_buy.parquet",
    "remove_from_cart": "remove_from_cart.parquet",
}

CLIENT_FILES = {
    "page_visit": "page_visit.parquet",
    "search_query": "search_query.parquet",
}


def iter_day_records(
    path: Path,
    columns: list[str],
    start_text: str,
    end_text: str,
):
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(
        batch_size=65_536,
        columns=columns,
    ):
        values = batch.to_pydict()
        for row in zip(*(values[column] for column in columns)):
            record = dict(zip(columns, row))
            raw_timestamp = record["timestamp"]
            if raw_timestamp is None:
                continue
            if isinstance(raw_timestamp, bytes):
                raw_timestamp = raw_timestamp.decode("utf-8")

            timestamp_text = str(raw_timestamp)
            if not start_text <= timestamp_text < end_text:
                continue

            record["_event_time"] = datetime.strptime(
                timestamp_text, "%Y-%m-%d %H:%M:%S"
            )
            yield record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--activity-date", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/synerise"),
    )
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    if args.output.exists():
        raise SystemExit(f"Output already exists: {args.output}")

    start = datetime.combine(args.activity_date, time.min)
    end = start + timedelta(days=1)
    as_of_date = args.activity_date + timedelta(days=1)

    client_counts = defaultdict(Counter)
    pair_counts = defaultdict(Counter)
    client_skus = defaultdict(set)
    last_client_event = {}
    last_sku_event = {}
    skipped_sku_rows = 0

    for event_type, filename in SKU_FILES.items():
        for record in iter_day_records(
            args.data_dir / filename,
            ["client_id", "sku", "timestamp"],
            str(start),
            str(end),
        ):
            if record["client_id"] is None or record["sku"] is None:
                skipped_sku_rows += 1
                continue

            client_id = int(record["client_id"])
            sku = int(record["sku"])
            event_time = record["_event_time"]
            last_client_event[client_id] = max(
                event_time, last_client_event.get(client_id, event_time)
            )
            last_sku_event[(client_id, sku)] = max(
                event_time, last_sku_event.get((client_id, sku), event_time)
            )
            client_counts[client_id][event_type] += 1
            pair_counts[(client_id, sku)][event_type] += 1
            client_skus[client_id].add(sku)

    candidate_clients = set(client_counts)

    for event_type, filename in CLIENT_FILES.items():
        for record in iter_day_records(
            args.data_dir / filename,
            ["client_id", "timestamp"],
            str(start),
            str(end),
        ):
            if record["client_id"] is None:
                continue

            client_id = int(record["client_id"])
            if client_id in candidate_clients:
                event_time = record["_event_time"]
                last_client_event[client_id] = max(
                    event_time, last_client_event.get(client_id, event_time)
                )
                client_counts[client_id][event_type] += 1

    rows = []
    for (client_id, sku), sku_counts in sorted(pair_counts.items()):
        counts = client_counts[client_id]
        rows.append(
            {
                "feature_as_of_date_source": as_of_date,
                "client_id": client_id,
                "sku": sku,
                "events_previous_day": sum(counts.values()),
                "page_visits_previous_day": counts["page_visit"],
                "search_previous_day": counts["search_query"],
                "client_cart_additions_previous_day": counts["add_to_cart"],
                "client_cart_removals_previous_day": counts["remove_from_cart"],
                "client_purchases_previous_day": counts["product_buy"],
                "distinct_skus_previous_day": len(client_skus[client_id]),
                "sku_interactions_previous_day": sum(sku_counts.values()),
                "sku_cart_additions_previous_day": sku_counts["add_to_cart"],
                "sku_cart_removals_previous_day": sku_counts["remove_from_cart"],
                "sku_purchases_previous_day": sku_counts["product_buy"],
                "hours_since_last_client_event": (
                    end - last_client_event[client_id]
                ).total_seconds() / 3600,
                "hours_since_last_sku_event": (
                    end - last_sku_event[(client_id, sku)]
                ).total_seconds() / 3600,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), args.output, compression="snappy")

    print(f"As-of date: {as_of_date}")
    print(f"Feature rows: {len(rows)}")
    print(f"Distinct clients: {len(candidate_clients)}")
    print(f"SKU interactions: {sum(row['sku_interactions_previous_day'] for row in rows)}")
    print(f"Skipped SKU rows: {skipped_sku_rows}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
