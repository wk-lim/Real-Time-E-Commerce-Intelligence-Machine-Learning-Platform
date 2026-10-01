"""Check product validation with in-memory Kafka-shaped records."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from ecommerce_intelligence.streaming.product_transform import (
    transform_product_kafka_records,
)


BASE_PRODUCT = {
    "schema_version": "1.0.0",
    "record_id": "761c36b7-f88b-5424-8b96-6c4443baa5ec",
    "sku": 1263699,
    "category_id": 780,
    "price_bucket": 26,
    "name_vector_raw": "[193 102 221]",
    "source": {
        "dataset": "synerise_recsys_2025",
        "file": "product_properties.parquet",
        "row_number": 0,
    },
    "replay": {
        "run_id": "1b30109f-14e0-424c-9b77-e12a5ef8f575",
        "published_at": "2026-09-25T07:23:23Z",
    },
}


def kafka_row(offset: int, product: dict | str, key: str = "1263699") -> Row:
    value = product if isinstance(product, str) else json.dumps(product)

    return Row(
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        topic="ecommerce.products.v1",
        partition=0,
        offset=offset,
        timestamp=datetime(2026, 9, 25, 7, 23, 23, tzinfo=timezone.utc),
        timestampType=0,
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("ecommerce-product-validation-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        records = [
            kafka_row(0, BASE_PRODUCT),
            kafka_row(1, "{invalid-json"),
            kafka_row(2, BASE_PRODUCT, key="999"),
            kafka_row(3, {**BASE_PRODUCT, "price_bucket": 100}),
            kafka_row(
                4,
                {**BASE_PRODUCT, "name_vector_raw": "not-a-vector"},
            ),
        ]

        transformed = transform_product_kafka_records(
            spark.createDataFrame(records)
        )

        actual = {
            row.kafka_offset: row.validation_error
            for row in transformed
            .select("kafka_offset", "validation_error")
            .collect()
        }
        expected = {
            0: None,
            1: "malformed_json",
            2: "invalid_message_key",
            3: "invalid_price_bucket",
            4: "invalid_name_vector",
        }

        assert actual == expected, f"Expected {expected}, got {actual}"

        print("Spark product validation smoke test passed")
        for offset, error in sorted(actual.items()):
            print(f"offset={offset} validation_error={error}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()