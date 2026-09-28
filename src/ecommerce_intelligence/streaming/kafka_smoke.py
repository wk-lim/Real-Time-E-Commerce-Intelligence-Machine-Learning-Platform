from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


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
            "Read a bounded snapshot from Kafka using Spark."
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
        .appName("ecommerce-kafka-smoke")
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
            .option("includeHeaders", "true")
            .load()
            .selectExpr(
                "CAST(key AS STRING) AS message_key",
                "CAST(value AS STRING) AS message_value",
                "topic",
                "partition",
                "offset",
                "timestamp AS kafka_timestamp",
                "timestampType AS kafka_timestamp_type",
                "headers",
            )
        )

        total_records = kafka_records.count()

        if total_records == 0:
            raise RuntimeError(
                f"No records found in Kafka topic {args.topic}"
            )

        sample_records = (
            kafka_records
            .orderBy("partition", "offset")
            .limit(args.max_records)
            .collect()
        )

        print("Spark Kafka smoke test passed")
        print(f"Topic: {args.topic}")
        print(f"Records available: {total_records}")
        print(f"Sample records: {len(sample_records)}")

        for row in sample_records:
            print(
                "partition="
                f"{row['partition']} "
                "offset="
                f"{row['offset']} "
                "key="
                f"{row['message_key']} "
                "value="
                f"{row['message_value'][:200]}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()