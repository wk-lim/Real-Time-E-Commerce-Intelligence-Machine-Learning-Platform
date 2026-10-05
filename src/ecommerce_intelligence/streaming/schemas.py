from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


SOURCE_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("file", StringType(), False),
        StructField("row_number", LongType(), False),
    ]
)

EVENT_REPLAY_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("published_at", StringType(), False),
    ]
)

DLQ_REPLAY_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("failed_at", StringType(), False),
    ]
)

EVENT_PAYLOAD_SCHEMA = StructType(
    [
        StructField("sku", LongType(), True),
        StructField("url_id", LongType(), True),
        StructField("query_vector_raw", StringType(), True),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("event_time_timezone", StringType(), True),
        StructField("client_id", LongType(), False),
        StructField("payload", EVENT_PAYLOAD_SCHEMA, False),
        StructField("source", SOURCE_SCHEMA, False),
        StructField("replay", EVENT_REPLAY_SCHEMA, False),
    ]
)

EVENT_PARSING_SCHEMA = StructType(
    [
        *EVENT_SCHEMA.fields,
        StructField("_corrupt_record", StringType(), True),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), False),
        StructField("record_id", StringType(), False),
        StructField("sku", LongType(), False),
        StructField("category_id", LongType(), False),
        StructField("price_bucket", IntegerType(), False),
        StructField("name_vector_raw", StringType(), False),
        StructField("source", SOURCE_SCHEMA, False),
        StructField("replay", EVENT_REPLAY_SCHEMA, False),
    ]
)

PRODUCT_PARSING_SCHEMA = StructType(
    [
        *PRODUCT_SCHEMA.fields,
        StructField("_corrupt_record", StringType(), True),
    ]
)

DLQ_ERROR_SCHEMA = StructType(
    [
        StructField("stage", StringType(), False),
        StructField("type", StringType(), False),
        StructField("message", StringType(), False),
    ]
)

DLQ_RAW_RECORD_SCHEMA = StructType(
    [
        StructField("client_id", LongType(), False),
        StructField("timestamp", StringType(), False),
        StructField("sku", LongType(), True),
        StructField("url", LongType(), True),
        StructField("query", StringType(), True),
        StructField("category", LongType(), True),
        StructField("price", LongType(), True),
        StructField("name", StringType(), True),
    ]
)

DLQ_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), False),
        StructField("dlq_id", StringType(), False),
        StructField("original_topic", StringType(), False),
        StructField("source", SOURCE_SCHEMA, False),
        StructField("replay", DLQ_REPLAY_SCHEMA, False),
        StructField("error", DLQ_ERROR_SCHEMA, False),
        StructField("raw_record", DLQ_RAW_RECORD_SCHEMA, False),
    ]
)

def main() -> None:
    schemas = {
        "event": EVENT_SCHEMA,
        "product": PRODUCT_SCHEMA,
        "dlq": DLQ_SCHEMA,
    }

    for name, schema in schemas.items():
        print(f"{name}: {schema.simpleString()}")

if __name__ == "__main__":
    main()
