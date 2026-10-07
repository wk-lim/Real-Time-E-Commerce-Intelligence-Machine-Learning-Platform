import argparse
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


EVENT_FILES = (
    "add_to_cart.parquet",
    "page_visit.parquet",
    "product_buy.parquet",
    "remove_from_cart.parquet",
    "search_query.parquet",
)
DATA_DIR = Path("data/raw/synerise")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument(
        "--source-file",
        choices=("all", *EVENT_FILES),
        default="all",
    )
    args = parser.parse_args()

    time_format = "%Y-%m-%d %H:%M:%S"
    start = datetime.strptime(args.start, time_format)
    end = datetime.strptime(args.end, time_format)
    if start >= end:
        parser.error("--start must be earlier than --end")

    lower = pa.scalar(args.start, type=pa.string())
    upper = pa.scalar(args.end, type=pa.string())
    files = EVENT_FILES if args.source_file == "all" else (args.source_file,)
    total = 0

    for filename in files:
        path = DATA_DIR / filename
        parquet_file = pq.ParquetFile(path)
        matched = 0

        for batch in parquet_file.iter_batches(
            batch_size=65_536,
            columns=["timestamp"],
        ):
            timestamps = batch.column(0)
            in_window = pc.and_(
                pc.greater_equal(timestamps, lower),
                pc.less(timestamps, upper),
            )
            matched += pc.sum(pc.cast(in_window, pa.int64())).as_py() or 0

        print(f"{filename}: {matched:,} matching events")
        total += matched

    print(f"Total matching events: {total:,}")


if __name__ == "__main__":
    main()