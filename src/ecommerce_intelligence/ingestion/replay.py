from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Literal
from uuid import UUID, uuid4, uuid5

import pyarrow.parquet as pq
from confluent_kafka import KafkaError, Message, Producer
import pyarrow as pa
import pyarrow.compute as pc

from ecommerce_intelligence.contract_validation import (
    validate_dlq,
    validate_event,
    validate_product,
)
from ecommerce_intelligence.ingestion.transform import (
    DATASET_NAME,
    DATASET_NAMESPACE,
    EVENT_TYPE_BY_FILE,
    build_event,
    build_product,
    utc_timestamp,
)


EVENT_TOPIC = "ecommerce.events.v1"
PRODUCT_TOPIC = "ecommerce.products.v1"
PRODUCT_SOURCE_FILE = "product_properties.parquet"
DLQ_TOPIC = "ecommerce.events.dlq.v1"

SOURCE_FILES = (
    *EVENT_TYPE_BY_FILE.keys(),
    PRODUCT_SOURCE_FILE,
)


@dataclass
class DeliveryTracker:
    delivered: int = 0
    failed: int = 0
    last_error: str | None = None

    def callback(
        self,
        error: KafkaError | None,
        message: Message,
    ) -> None:
        if error is not None:
            self.failed += 1
            self.last_error = str(error)
            print(
                f"Delivery failed for {message.topic()}: {error}",
                file=sys.stderr,
            )
            return

        self.delivered += 1


def positive_integer(value: str) -> int:
    try:
        parsed_value = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be an integer"
        ) from error

    if parsed_value <= 0:
        raise argparse.ArgumentTypeError(
            "value must be greater than zero"
        )

    return parsed_value


def nonnegative_float(value: str) -> float:
    try:
        parsed_value = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a number"
        ) from error

    if parsed_value < 0:
        raise argparse.ArgumentTypeError(
            "value must be zero or greater"
        )

    return parsed_value


def parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "value must be a valid UUID"
        ) from error


def iter_source_rows(
    source_path: Path,
    *,
    batch_size: int,
    limit: int,
) -> Iterator[tuple[int, dict[str, Any]]]:
    parquet_file = pq.ParquetFile(source_path)
    row_number = 0

    for batch in parquet_file.iter_batches(
        batch_size=batch_size,
    ):
        for row in batch.to_pylist():
            yield row_number, row

            row_number += 1

            if row_number >= limit:
                return


def build_record(
    *,
    source_file: str,
    row_number: int,
    row: dict[str, Any],
    run_id: UUID,
) -> tuple[dict[str, Any], str, str]:
    published_at = datetime.now(timezone.utc)

    if source_file == PRODUCT_SOURCE_FILE:
        record = build_product(
            row_number=row_number,
            row=row,
            run_id=run_id,
            published_at=published_at,
        )

        return record, PRODUCT_TOPIC, str(record["sku"])

    record = build_event(
        source_file=source_file,
        row_number=row_number,
        row=row,
        run_id=run_id,
        published_at=published_at,
    )

    # Kafka uses client_id as the event key so events belonging
    # to the same client are routed to the same partition.
    return record, EVENT_TOPIC, str(record["client_id"])


def build_dlq_record(
    *,
    source_file: str,
    row_number: int,
    row: dict[str, Any],
    run_id: UUID,
    original_topic: str,
    stage: Literal[
        "transformation",
        "contract_validation",
    ],
    error: Exception,
    failed_at: datetime,
) -> tuple[dict[str, Any], str, str]:
    dlq_id = str(
        uuid5(
            DATASET_NAMESPACE,
            f"dlq:{source_file}:{row_number}",
        )
    )

    record = {
        "schema_version": "1.0.0",
        "dlq_id": dlq_id,
        "original_topic": original_topic,
        "source": {
            "dataset": DATASET_NAME,
            "file": source_file,
            "row_number": row_number,
        },
        "replay": {
            "run_id": str(run_id),
            "failed_at": utc_timestamp(failed_at),
        },
        "error": {
            "stage": stage,
            "type": type(error).__name__,
            "message": (
                str(error)
                or "Unspecified processing error"
            ),
        },
        "raw_record": dict(row),
    }

    validate_dlq(record)

    return record, DLQ_TOPIC, dlq_id


def validate_record(
    record: dict[str, Any],
    *,
    source_file: str,
) -> None:
    if source_file == PRODUCT_SOURCE_FILE:
        validate_product(record)
    else:
        validate_event(record)


def serialize_record(record: dict[str, Any]) -> bytes:
    return json.dumps(
        record,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def create_producer(
    bootstrap_servers: str,
) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "synerise-replay-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 20,
            "batch.num.messages": 10_000,
        }
    )


def publish_message(
    *,
    producer: Producer,
    tracker: DeliveryTracker,
    topic: str,
    key: str,
    value: bytes,
) -> None:
    while True:
        try:
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                headers=[
                    ("content-type", b"application/json"),
                    ("schema-version", b"1.0.0"),
                ],
                on_delivery=tracker.callback,
            )
            break
        except BufferError:
            # Let completed delivery callbacks run and free space
            # in the producer's local queue.
            producer.poll(0.1)

    producer.poll(0)


def apply_rate_limit(
    *,
    started_at: float,
    processed_count: int,
    messages_per_second: float,
) -> None:
    if messages_per_second == 0:
        return

    target_time = (
        started_at
        + processed_count / messages_per_second
    )
    remaining_seconds = target_time - monotonic()

    if remaining_seconds > 0:
        sleep(remaining_seconds)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replay Synerise Parquet records through Kafka. "
            "Records are read in physical source-file order."
        )
    )

    parser.add_argument(
        "--source-file",
        required=True,
        choices=SOURCE_FILES,
        help="Synerise Parquet file to replay.",
    )
    parser.add_argument(
        "--limit",
        required=True,
        type=positive_integer,
        help=(
            "Maximum records to process. This required safety "
            "guard prevents accidental full-dataset replay."
        ),
    )
    parser.add_argument(
        "--window-start",
        help="Inclusive source timestamp (YYYY-MM-DD HH:MM:SS).",
    )
    parser.add_argument(
        "--window-end",
        help="Exclusive source timestamp (YYYY-MM-DD HH:MM:SS).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw/synerise"),
        help="Directory containing the extracted Parquet files.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--batch-size",
        type=positive_integer,
        default=10_000,
        help="Number of Parquet rows loaded per batch.",
    )
    parser.add_argument(
        "--messages-per-second",
        type=nonnegative_float,
        default=1_000.0,
        help=(
            "Maximum publishing rate. Use 0 for no rate limit."
        ),
    )
    parser.add_argument(
        "--run-id",
        type=parse_uuid,
        default=None,
        help=(
            "Optional replay run UUID. A new UUID is generated "
            "when omitted."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Transform and validate records without publishing "
            "them to Kafka."
        ),
    )
    parser.add_argument(
        "--validate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable or disable JSON Schema validation.",
    )

    return parser

def iter_source_rows_in_window(
        source_path: Path,
        *,
        batch_size: int,
        max_selected: int,
        start_time: str,
        end_time: str,
) -> Iterator[tuple[int, dict[str, Any]]]:
    if batch_size <= 0 or max_selected <= 0:
        raise ValueError("batch_size and max_selected must be positive")

    time_format = "%Y-%m-%d %H:%M:%S"
    start = datetime.strptime(start_time, time_format)
    end = datetime.strptime(end_time, time_format)

    if (
        start.strftime(time_format) != start_time
        or end.strftime(time_format) != end_time
        or start >= end
    ):
        raise ValueError("Expected an increasing canonical timestamp window")

    lower = pa.scalar(start_time, type=pa.string())
    upper = pa.scalar(end_time, type=pa.string())
    parquet_file = pq.ParquetFile(source_path)
    source_row_number = 0
    selected_count = 0

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        timestamps = batch.column("timestamp")
        in_window = pc.and_(
            pc.greater_equal(timestamps, lower),
            pc.less(timestamps, upper),
        )
        indices = pc.indices_nonzero(in_window)
        selected_rows = batch.take(indices).to_pylist()

        for batch_index, row in zip(indices.to_pylist(), selected_rows):
            yield source_row_number + batch_index, row
            selected_count += 1

            if selected_count >= max_selected:
                return

        source_row_number += batch.num_rows



def run(args: argparse.Namespace) -> int:
    source_path = args.data_dir / args.source_file

    if not source_path.is_file():
        raise FileNotFoundError(
            f"Source file does not exist: {source_path}"
        )

    window_start = getattr(args, "window_start", None)
    window_end = getattr(args, "window_end", None)

    if (window_start is None) != (window_end is None):
        raise ValueError("--window-start and --window-end must be provided together")

    if window_start is not None and args.source_file == PRODUCT_SOURCE_FILE:
        raise ValueError("Time windows are only supported for behavioral events")

    run_id = args.run_id or uuid4()
    tracker = DeliveryTracker()
    producer = (
        None
        if args.dry_run
        else create_producer(args.bootstrap_servers)
    )

    original_topic = (
        PRODUCT_TOPIC
        if args.source_file == PRODUCT_SOURCE_FILE
        else EVENT_TOPIC
    )

    processed_count = 0
    dlq_count = 0
    started_at = monotonic()

    print(f"Replay run ID: {run_id}")
    print(f"Source: {source_path}")
    print(f"Limit: {args.limit:,}")
    print(
        "Mode: "
        + ("dry run" if args.dry_run else "Kafka publish")
    )
    print(
        "Ordering: physical Parquet row order "
        "(not global event-time order)"
    )

    if window_start is None:
        source_rows = iter_source_rows(
            source_path,
            batch_size=args.batch_size,
            limit=args.limit,
        )
    else:
        print(f"Window: [{window_start}, {window_end}) (source timezone unknown)")
        print("Limit applies to matching records; the source scan may be larger")
        source_rows = iter_source_rows_in_window(
            source_path,
            batch_size=args.batch_size,
            max_selected=args.limit,
            start_time=window_start,
            end_time=window_end,
        )

    for row_number, row in source_rows:
        try:
            record, topic, key = build_record(
                source_file=args.source_file,
                row_number=row_number,
                row=row,
                run_id=run_id,
            )
        except Exception as error:
            record, topic, key = build_dlq_record(
                source_file=args.source_file,
                row_number=row_number,
                row=row,
                run_id=run_id,
                original_topic=original_topic,
                stage="transformation",
                error=error,
                failed_at=datetime.now(timezone.utc),
            )
            dlq_count += 1
        else:
            if args.validate:
                try:
                    validate_record(
                        record,
                        source_file=args.source_file,
                    )
                except Exception as error:
                    record, topic, key = build_dlq_record(
                        source_file=args.source_file,
                        row_number=row_number,
                        row=row,
                        run_id=run_id,
                        original_topic=original_topic,
                        stage="contract_validation",
                        error=error,
                        failed_at=datetime.now(timezone.utc),
                    )
                    dlq_count += 1

        value = serialize_record(record)

        if args.dry_run:
            # Avoid flooding the terminal during large dry runs.
            if processed_count < 5:
                print(value.decode("utf-8"))
        else:
            assert producer is not None

            publish_message(
                producer=producer,
                tracker=tracker,
                topic=topic,
                key=key,
                value=value,
            )

        processed_count += 1

        apply_rate_limit(
            started_at=started_at,
            processed_count=processed_count,
            messages_per_second=args.messages_per_second,
        )

    if producer is not None:
        undelivered_count = producer.flush(30)

        if undelivered_count:
            raise RuntimeError(
                f"{undelivered_count} Kafka messages were "
                "not delivered before the flush timeout"
            )

        if tracker.failed:
            raise RuntimeError(
                f"{tracker.failed} Kafka deliveries failed. "
                f"Last error: {tracker.last_error}"
            )

    elapsed_seconds = monotonic() - started_at

    print()
    print("Replay completed")
    print(f"Processed: {processed_count:,}")
    print(f"Dead-lettered: {dlq_count:,}")
    print(f"Delivered: {tracker.delivered:,}")
    print(f"Failed: {tracker.failed:,}")
    print(f"Elapsed seconds: {elapsed_seconds:.2f}")

    return 0


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    try:
        exit_code = run(args)
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        parser.exit(1, f"Error: {error}\n")

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
