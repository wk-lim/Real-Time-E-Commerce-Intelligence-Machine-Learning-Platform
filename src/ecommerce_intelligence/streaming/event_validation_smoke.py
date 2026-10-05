"""Check Spark event validation with bounded in-memory Kafka-shaped records."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecommerce_intelligence.streaming.event_transform import (
    transform_event_kafka_records,
)


KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("timestampType", IntegerType(), False),
    ]
)


def base_event(offset: int) -> dict:
    return {
        "schema_version": "1.0.0",
        "event_id": str(UUID(int=offset + 1)),
        "event_type": "add_to_cart",
        "event_time": "2022-09-22 06:26:40",
        "event_time_timezone": None,
        "client_id": 10551361,
        "payload": {"sku": 728970},
        "source": {
            "dataset": "synerise_recsys_2025",
            "file": "add_to_cart.parquet",
            "row_number": offset,
        },
        "replay": {
            "run_id": "4ebde710-aed1-41c4-a50e-612c1c5c326c",
            "published_at": "2026-09-25T07:01:17Z",
        },
    }


def make_record(
    offset: int,
    event: dict | None,
    *,
    key: str = "10551361",
    raw_json: str | None = None,
) -> tuple:
    value = raw_json if raw_json is not None else json.dumps(event)
    return (
        key.encode("utf-8"),
        value.encode("utf-8"),
        "ecommerce.events.v1",
        0,
        offset,
        datetime(2026, 9, 25, 7, 1, 17, tzinfo=timezone.utc),
        0,
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("ecommerce-event-validation-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        valid = base_event(0)
        wrong_key = base_event(2)
        bad_time = base_event(3)
        bad_time["event_time"] = "2022-02-30 12:00:00"
        missing_sku = base_event(4)
        missing_sku["payload"] = {}
        page_visit = base_event(5)
        page_visit["event_type"] = "page_visit"
        page_visit["payload"] = {"url_id": 42}
        page_visit["source"]["file"] = "page_visit.parquet"
        search_query = base_event(6)
        search_query["event_type"] = "search_query"
        search_query["payload"] = {"query_vector_raw": "[1 2]"}
        search_query["source"]["file"] = "search_query.parquet"

        records = [
            make_record(0, valid),
            make_record(1, None, raw_json="{"),
            make_record(2, wrong_key, key="999"),
            make_record(3, bad_time),
            make_record(4, missing_sku),
            make_record(5, page_visit),
            make_record(6, search_query),
        ]
        expected = {
            0: None,
            1: "malformed_json",
            2: "invalid_message_key",
            3: "invalid_event_time",
            4: "missing_sku",
            5: None,
            6: None,
        }

        kafka_records = spark.createDataFrame(records, KAFKA_SCHEMA)
        actual = {
            row.kafka_offset: row
            for row in transform_event_kafka_records(kafka_records)
            .select(
                "kafka_offset",
                "validation_error",
                "is_valid",
                "raw_json",
            )
            .collect()
        }

        assert set(actual) == set(expected), actual
        for offset, error in expected.items():
            row = actual[offset]
            assert row.validation_error == error, (offset, row)
            assert row.is_valid is (error is None), (offset, row)
            assert row.raw_json, (offset, row)

        print("Spark event validation smoke test passed")
        print(f"Records checked: {len(actual)}")
        for offset in sorted(expected):
            print(f"offset={offset} validation_error={expected[offset]}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
