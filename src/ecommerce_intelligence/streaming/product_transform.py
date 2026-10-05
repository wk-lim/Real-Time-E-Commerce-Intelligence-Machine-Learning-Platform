"""Parse and validate product records read from Kafka."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ecommerce_intelligence.streaming.schemas import PRODUCT_PARSING_SCHEMA


UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def product_validation_error(published_at_utc: Column) -> Column:
    sku = F.col("_product.sku")
    category = F.col("_product.category_id")
    price = F.col("_product.price_bucket")
    name = F.col("_product.name_vector_raw")
    source = F.col("_product.source")

    return (
        F.when(
            F.col("raw_json").isNull()
            | F.col("_product._corrupt_record").isNotNull(),
            "malformed_json",
        )
        .when(
            F.col("_product.schema_version").isNull()
            | (F.col("_product.schema_version") != "1.0.0"),
            "invalid_schema_version",
        )
        .when(
            F.col("_product.record_id").isNull()
            | ~F.col("_product.record_id").rlike(UUID_PATTERN),
            "invalid_record_id",
        )
        .when(sku.isNull() | (sku < 0), "invalid_sku")
        .when(
            F.col("message_key").isNull()
            | (F.col("message_key") != sku.cast("string")),
            "invalid_message_key",
        )
        .when(
            category.isNull() | (category < 0),
            "invalid_category_id",
        )
        .when(
            price.isNull() | (price < 0) | (price > 99),
            "invalid_price_bucket",
        )
        .when(
            name.isNull() | ~name.rlike(r"^\[.*\]$"),
            "invalid_name_vector",
        )
        .when(
            source["dataset"].isNull()
            | (source["dataset"] != "synerise_recsys_2025"),
            "invalid_source_dataset",
        )
        .when(
            source["file"].isNull()
            | (source["file"] != "product_properties.parquet"),
            "invalid_source_file",
        )
        .when(
            source["row_number"].isNull()
            | (source["row_number"] < 0),
            "invalid_source_row_number",
        )
        .when(
            F.col("_product.replay.run_id").isNull()
            | ~F.col("_product.replay.run_id").rlike(UUID_PATTERN),
            "invalid_replay_run_id",
        )
        .when(
            published_at_utc.isNull(),
            "invalid_published_at",
        )
        .otherwise(F.lit(None).cast("string"))
    )


def transform_product_kafka_records(kafka_records: DataFrame) -> DataFrame:
    kafka_metadata = kafka_records.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
    )

    parsed = kafka_metadata.withColumn(
        "_product",
        F.from_json(
            F.col("raw_json"),
            PRODUCT_PARSING_SCHEMA,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ),
    )

    published_at_utc = F.try_to_timestamp(
        F.col("_product.replay.published_at"),
        F.lit("yyyy-MM-dd'T'HH:mm:ssX"),
    )
    validation_error = product_validation_error(published_at_utc)

    return parsed.select(
        "message_key",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_timestamp_type",
        F.col("_product.schema_version").alias("schema_version"),
        F.col("_product.record_id").alias("record_id"),
        F.col("_product.sku").alias("sku"),
        F.col("_product.category_id").alias("category_id"),
        F.col("_product.price_bucket").alias("price_bucket"),
        F.col("_product.name_vector_raw").alias("name_vector_raw"),
        F.col("_product.source.dataset").alias("source_dataset"),
        F.col("_product.source.file").alias("source_file"),
        F.col("_product.source.row_number").alias("source_row_number"),
        F.col("_product.replay.run_id").alias("replay_run_id"),
        F.col("_product.replay.published_at").alias("published_at_raw"),
        published_at_utc.alias("published_at_utc"),
        validation_error.alias("validation_error"),
        validation_error.isNull().alias("is_valid"),
        F.current_timestamp().alias("processed_at_utc"),
    )