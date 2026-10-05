"""Write validated Kafka products and rejected records to Parquet."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce_intelligence.streaming.product_transform import (
    transform_product_kafka_records,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="ecommerce.products.v1")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("/opt/project/data"),
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=10_000,
    )
    parser.add_argument("--processing-time", default="10 seconds")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process the current Kafka backlog, then exit.",
    )

    args = parser.parse_args()

    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger must be positive")

    return args


def start_sink(
    records,
    name: str,
    output_path: Path,
    checkpoint_path: Path,
    args: argparse.Namespace,
):
    writer = (
        records.writeStream
        .queryName(name)
        .format("parquet")
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
    )

    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.processing_time)

    return writer.start()


def main() -> None:
    args = parse_args()
    data_root = args.data_root.resolve()

    bronze_path = data_root / "processed" / "bronze" / "products"
    quarantine_path = data_root / "processed" / "quarantine" / "products"

    bronze_checkpoint = (
        data_root / "checkpoints" / "spark" / "products" / "bronze"
    )
    quarantine_checkpoint = (
        data_root / "checkpoints" / "spark" / "products" / "quarantine"
    )

    spark = (
        SparkSession.builder
        .appName("ecommerce-product-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    try:
        kafka_records = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option(
                "maxOffsetsPerTrigger",
                args.max_offsets_per_trigger,
            )
            .load()
        )

        transformed = transform_product_kafka_records(kafka_records)

        valid_records = (
            transformed
            .where(F.col("is_valid"))
            .drop("is_valid", "validation_error")
        )
        invalid_records = (
            transformed
            .where(~F.col("is_valid"))
            .drop("is_valid")
        )

        queries.append(
            start_sink(
                valid_records,
                "ecommerce-products-bronze",
                bronze_path,
                bronze_checkpoint,
                args,
            )
        )

        queries.append(
            start_sink(
                invalid_records,
                "ecommerce-products-quarantine",
                quarantine_path,
                quarantine_checkpoint,
                args,
            )
        )

        print(f"Topic: {args.topic}", flush=True)
        print(f"Bronze output: {bronze_path}", flush=True)
        print(f"Quarantine output: {quarantine_path}", flush=True)
        print(
            f"Mode: {'available now' if args.available_now else 'continuous'}",
            flush=True,
        )

        if args.available_now:
            for query in queries:
                query.awaitTermination()
            print("Available-now product stream completed", flush=True)
        else:
            spark.streams.awaitAnyTermination()
            raise RuntimeError("A product stream stopped unexpectedly")

    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()