from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as functions

from ecommerce_intelligence.streaming.schemas import (
    EVENT_PARSING_SCHEMA,
)


VALID_EVENT_TYPES = (
    "add_to_cart",
    "page_visit",
    "product_buy",
    "remove_from_cart",
    "search_query",
)

SKU_EVENT_TYPES = (
    "add_to_cart",
    "product_buy",
    "remove_from_cart",
)

VALID_SOURCE_FILES = (
    "add_to_cart.parquet",
    "page_visit.parquet",
    "product_buy.parquet",
    "remove_from_cart.parquet",
    "search_query.parquet",
)

UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)


def event_validation_error(
    event_time_local: Column,
    published_at_utc: Column,
) -> Column:
    event_type = functions.col("_event.event_type")

    return (
        functions.when(
            functions.col(
                "_event._corrupt_record"
            ).isNotNull(),
            "malformed_json",
        )
        .when(
            functions.col(
                "_event.schema_version"
            ).isNull()
            | (
                functions.col(
                    "_event.schema_version"
                )
                != "1.0.0"
            ),
            "invalid_schema_version",
        )
        .when(
            functions.col("_event.event_id").isNull()
            | ~functions.col(
                "_event.event_id"
            ).rlike(UUID_PATTERN),
            "invalid_event_id",
        )
        .when(
            event_type.isNull()
            | ~event_type.isin(*VALID_EVENT_TYPES),
            "invalid_event_type",
        )
        .when(
            event_time_local.isNull(),
            "invalid_event_time",
        )
        .when(
            functions.col(
                "_event.event_time_timezone"
            ).isNotNull(),
            "unexpected_event_time_timezone",
        )
        .when(
            functions.col("_event.client_id").isNull()
            | (
                functions.col("_event.client_id")
                < 0
            ),
            "invalid_client_id",
        )
        .when(
            functions.col("message_key").isNull()
            | (
                functions.col("message_key")
                != functions.col(
                    "_event.client_id"
                ).cast("string")
            ),
            "invalid_message_key",
        )
        .when(
            event_type.isin(*SKU_EVENT_TYPES)
            & functions.col(
                "_event.payload.sku"
            ).isNull(),
            "missing_sku",
        )
        .when(
            (event_type == "page_visit")
            & functions.col(
                "_event.payload.url_id"
            ).isNull(),
            "missing_url_id",
        )
        .when(
            (event_type == "search_query")
            & functions.col(
                "_event.payload.query_vector_raw"
            ).isNull(),
            "missing_query_vector",
        )
        .when(
            functions.col(
                "_event.source.dataset"
            ).isNull()
            | (
                functions.col(
                    "_event.source.dataset"
                )
                != "synerise_recsys_2025"
            ),
            "invalid_source_dataset",
        )
        .when(
            functions.col(
                "_event.source.file"
            ).isNull()
            | ~functions.col(
                "_event.source.file"
            ).isin(*VALID_SOURCE_FILES),
            "invalid_source_file",
        )
        .when(
            functions.col(
                "_event.source.row_number"
            ).isNull()
            | (
                functions.col(
                    "_event.source.row_number"
                )
                < 0
            ),
            "invalid_source_row_number",
        )
        .when(
            functions.col(
                "_event.replay.run_id"
            ).isNull()
            | ~functions.col(
                "_event.replay.run_id"
            ).rlike(UUID_PATTERN),
            "invalid_replay_run_id",
        )
        .when(
            published_at_utc.isNull(),
            "invalid_published_at",
        )
        .otherwise(
            functions.lit(None).cast("string")
        )
    )


def transform_event_kafka_records(
    kafka_records: DataFrame,
) -> DataFrame:
    kafka_metadata = kafka_records.select(
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
        functions.col("timestampType")
        .alias("kafka_timestamp_type"),
    )

    parsed = kafka_metadata.withColumn(
        "_event",
        functions.from_json(
            functions.col("raw_json"),
            EVENT_PARSING_SCHEMA,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": (
                    "_corrupt_record"
                ),
            },
        ),
    )

    event_time_local = functions.try_to_timestamp(
        functions.col("_event.event_time"),
        functions.lit("yyyy-MM-dd HH:mm:ss"),
    )

    published_at_utc = functions.try_to_timestamp(
        functions.col(
            "_event.replay.published_at"
        ),
        functions.lit(
            "yyyy-MM-dd'T'HH:mm:ssX"
        ),
    )

    validation_error = event_validation_error(
        event_time_local,
        published_at_utc,
    )

    return parsed.select(
        "message_key",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_timestamp_type",
        functions.col(
            "_event.schema_version"
        ).alias("schema_version"),
        functions.col(
            "_event.event_id"
        ).alias("event_id"),
        functions.col(
            "_event.event_type"
        ).alias("event_type"),
        functions.col(
            "_event.event_time"
        ).alias("event_time_raw"),
        event_time_local.alias(
            "event_time_local"
        ),
        functions.col(
            "_event.event_time_timezone"
        ).alias("event_time_timezone"),
        functions.col(
            "_event.client_id"
        ).alias("client_id"),
        functions.col(
            "_event.payload.sku"
        ).alias("sku"),
        functions.col(
            "_event.payload.url_id"
        ).alias("url_id"),
        functions.col(
            "_event.payload.query_vector_raw"
        ).alias("query_vector_raw"),
        functions.col(
            "_event.source.dataset"
        ).alias("source_dataset"),
        functions.col(
            "_event.source.file"
        ).alias("source_file"),
        functions.col(
            "_event.source.row_number"
        ).alias("source_row_number"),
        functions.col(
            "_event.replay.run_id"
        ).alias("replay_run_id"),
        functions.col(
            "_event.replay.published_at"
        ).alias("published_at_raw"),
        published_at_utc.alias(
            "published_at_utc"
        ),
        validation_error.alias(
            "validation_error"
        ),
        validation_error.isNull().alias(
            "is_valid"
        ),
        functions.current_timestamp().alias(
            "processed_at_utc"
        ),
    )