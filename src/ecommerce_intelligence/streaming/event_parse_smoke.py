from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions

from ecommerce_intelligence.streaming.event_transform import (
    transform_event_kafka_records,
)


def positive_integer(value: str) -> int:
    parsed_value = int(value)

    if parsed_value <= 0:
        raise argparse.ArgumentTypeError(
            "value must be greater than zero"
        )

    return parsed_value


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Transform and validate a bounded Kafka "
            "event snapshot."
        )
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:19092",
    )
    parser.add_argument(
        "--topic",
        default="ecommerce.events.v1",
    )
    parser.add_argument(
        "--max-records",
        type=positive_integer,
        default=5,
    )

    return parser


def main() -> None:
    args = create_parser().parse_args()

    spark = (
        SparkSession.builder
        .appName("ecommerce-event-transform-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_records = (
            spark.read
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                args.bootstrap_servers,
            )
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "true")
            .load()
        )

        transformed = transform_event_kafka_records(
            kafka_records
        )

        total_records = transformed.count()

        if total_records == 0:
            raise RuntimeError(
                f"No records found in topic {args.topic}"
            )

        valid_records = transformed.where(
            functions.col("is_valid")
        )
        invalid_records = transformed.where(
            ~functions.col("is_valid")
        )

        valid_count = valid_records.count()
        invalid_count = invalid_records.count()

        print("Spark event transformation smoke test")
        print(f"Topic: {args.topic}")
        print(f"Total records: {total_records}")
        print(f"Valid records: {valid_count}")
        print(f"Invalid records: {invalid_count}")

        if invalid_count:
            print("Validation failures:")

            for row in (
                invalid_records
                .groupBy("validation_error")
                .count()
                .orderBy("validation_error")
                .collect()
            ):
                print(row.asDict())

            raise RuntimeError(
                f"{invalid_count} records failed validation"
            )

        sample_records = (
            valid_records
            .orderBy(
                "kafka_partition",
                "kafka_offset",
            )
            .limit(args.max_records)
            .select(
                "kafka_partition",
                "kafka_offset",
                "message_key",
                "event_id",
                "event_type",
                "event_time_raw",
                "event_time_local",
                "client_id",
                "sku",
                "url_id",
                "query_vector_raw",
                "source_file",
                "source_row_number",
                "replay_run_id",
                "published_at_utc",
            )
            .collect()
        )

        for row in sample_records:
            print(row.asDict(recursive=True))

        print(
            "Spark event transformation smoke test passed"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()