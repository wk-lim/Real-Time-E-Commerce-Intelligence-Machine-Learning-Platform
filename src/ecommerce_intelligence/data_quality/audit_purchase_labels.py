import argparse
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv

from ecommerce_intelligence.storage.postgres import connect_postgres


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of-date", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--source-file",
        type=Path,
        default=Path("data/raw/synerise/product_buy.parquet"),
    )
    args = parser.parse_args()

    load_dotenv(".env", override=False)

    window_start = datetime.combine(args.as_of_date, time.min)
    window_end = window_start + timedelta(days=7)

    with connect_postgres() as connection:
        rows = connection.execute(
            """
            select client_id, sku
            from analytics.client_product_feature_snapshot
            where feature_as_of_date_source = %s
            """,
            (args.as_of_date,),
        ).fetchall()

    candidates = {(int(client_id), int(sku)) for client_id, sku in rows}
    if not candidates:
        raise SystemExit("No candidate pairs found for that as-of date")

    first_purchase: dict[tuple[int, int], datetime] = {}
    matching_purchase_rows = 0
    invalid_candidate_timestamps = 0

    parquet_file = pq.ParquetFile(args.source_file)
    for batch in parquet_file.iter_batches(
        batch_size=65_536,
        columns=["client_id", "sku", "timestamp"],
    ):
        clients = batch.column(0).to_pylist()
        skus = batch.column(1).to_pylist()
        timestamps = batch.column(2).to_pylist()

        for client_id, sku, raw_timestamp in zip(clients, skus, timestamps):
            if client_id is None or sku is None:
                continue

            key = (int(client_id), int(sku))
            if key not in candidates:
                continue

            if isinstance(raw_timestamp, bytes):
                raw_timestamp = raw_timestamp.decode("utf-8")

            try:
                purchase_at = datetime.strptime(
                    str(raw_timestamp), "%Y-%m-%d %H:%M:%S"
                )
            except (TypeError, ValueError):
                invalid_candidate_timestamps += 1
                continue

            if not window_start <= purchase_at < window_end:
                continue

            matching_purchase_rows += 1
            if key not in first_purchase or purchase_at < first_purchase[key]:
                first_purchase[key] = purchase_at

    print(f"Window: [{window_start}, {window_end})")
    print(f"Candidate pairs: {len(candidates)}")
    print(f"Matching purchase rows: {matching_purchase_rows}")
    print(f"Pairs with a purchase observed: {len(first_purchase)}")
    print(
        "Pairs with no purchase observed in this file: "
        f"{len(candidates) - len(first_purchase)}"
    )
    print(f"Invalid candidate timestamps: {invalid_candidate_timestamps}")

    for (client_id, sku), purchased_at in sorted(
        first_purchase.items(), key=lambda item: item[1]
    )[:5]:
        print(f"client_id={client_id} sku={sku} first_purchase={purchased_at}")

    if invalid_candidate_timestamps:
        raise SystemExit("Audit incomplete: candidate purchases have invalid timestamps")


if __name__ == "__main__":
    main()