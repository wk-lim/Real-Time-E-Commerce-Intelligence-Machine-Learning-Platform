from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions

from ecommerce_intelligence.streaming.schemas import EVENT_SCHEMA


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
            "Parse a bounded Kafka event snapshot "
            "with the canonical Spark schema."
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
        .appName("ecommerce-event-parse-smoke")
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

        parsed_records = kafka_records.select(
            functions.col("key")
            .cast("string")
            .alias("message_key"),
            functions.col("value")
            .cast("string")
            .alias("raw_json"),
            functions.col("topic")
            .alias("kafka_topic"),
            functions.col("partition")
            .alias("kafka_partition"),
            functions.col("offset")
            .alias("kafka_offset"),
            functions.col("timestamp")
            .alias("kafka_timestamp"),
            functions.from_json(
                functions.col("value").cast("string"),
                EVENT_SCHEMA,
            ).alias("event"),
        )

        total_records = parsed_records.count()

        parse_failures = parsed_records.where(
            functions.col("event.event_id").isNull()
        ).count()

        key_mismatches = parsed_records.where(
            functions.col("message_key")
            != functions.col("event.client_id").cast("string")
        ).count()

        if total_records == 0:
            raise RuntimeError(
                f"No records found in topic {args.topic}"
            )

        if parse_failures:
            raise RuntimeError(
                f"{parse_failures} records failed schema parsing"
            )

        if key_mismatches:
            raise RuntimeError(
                f"{key_mismatches} records have an invalid Kafka key"
            )

        sample_records = (
            parsed_records
            .orderBy("kafka_partition", "kafka_offset")
            .limit(args.max_records)
            .select(
                "kafka_partition",
                "kafka_offset",
                "message_key",
                "event.event_id",
                "event.event_type",
                "event.event_time",
                "event.client_id",
                "event.payload",
            )
            .collect()
        )

        print("Spark event parsing smoke test passed")
        print(f"Topic: {args.topic}")
        print(f"Total records: {total_records}")
        print(f"Parse failures: {parse_failures}")
        print(f"Kafka key mismatches: {key_mismatches}")

        for row in sample_records:
            print(row.asDict(recursive=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()