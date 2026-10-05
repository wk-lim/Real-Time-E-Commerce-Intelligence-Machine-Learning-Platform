"""Check product parsing against a bounded Kafka snapshot."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ecommerce_intelligence.streaming.product_transform import (
    transform_product_kafka_records,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:19092")
    parser.add_argument("--topic", default="ecommerce.products.v1")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("ecommerce-product-parse-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_records = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "true")
            .load()
        )

        transformed = transform_product_kafka_records(kafka_records)
        total_count = transformed.count()

        if total_count == 0:
            raise RuntimeError(f"No records found in {args.topic}")

        invalid = transformed.where(~F.col("is_valid"))
        invalid_count = invalid.count()

        print("Spark product parsing smoke test")
        print(f"Topic: {args.topic}")
        print(f"Total records: {total_count}")
        print(f"Valid records: {total_count - invalid_count}")
        print(f"Invalid records: {invalid_count}")

        if invalid_count:
            for row in (
                invalid
                .select("kafka_partition", "kafka_offset", "validation_error")
                .limit(5)
                .collect()
            ):
                print(row.asDict())

            raise RuntimeError(
                f"{invalid_count} product records failed validation"
            )

        samples = (
            transformed
            .orderBy("kafka_partition", "kafka_offset")
            .limit(3)
            .select(
                "kafka_partition",
                "kafka_offset",
                "message_key",
                "record_id",
                "sku",
                "category_id",
                "price_bucket",
                "name_vector_raw",
                "source_row_number",
                "published_at_utc",
            )
            .collect()
        )

        for row in samples:
            print(row.asDict())

        print("Spark product parsing smoke test passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()