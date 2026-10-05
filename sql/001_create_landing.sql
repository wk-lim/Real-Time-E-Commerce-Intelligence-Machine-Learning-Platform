CREATE SCHEMA IF NOT EXISTS landing;

CREATE TABLE IF NOT EXISTS landing.events (
    event_id UUID PRIMARY KEY,
    schema_version TEXT NOT NULL CHECK (schema_version = '1.0.0'),
    message_key TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    kafka_timestamp TIMESTAMPTZ,
    kafka_timestamp_type INTEGER,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'add_to_cart', 'page_visit', 'product_buy',
            'remove_from_cart', 'search_query'
        )
    ),
    event_time_raw TEXT NOT NULL,
    event_time_local TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_time_timezone TEXT,
    client_id BIGINT NOT NULL CHECK (client_id >= 0),
    sku BIGINT,
    url_id BIGINT,
    query_vector_raw TEXT,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL CHECK (source_row_number >= 0),
    replay_run_id UUID NOT NULL,
    published_at_raw TEXT NOT NULL,
    published_at_utc TIMESTAMPTZ NOT NULL,
    processed_at_utc TIMESTAMPTZ NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_source_row_unique
        UNIQUE (source_dataset, source_file, source_row_number)
);

CREATE TABLE IF NOT EXISTS landing.products (
    record_id UUID PRIMARY KEY,
    schema_version TEXT NOT NULL CHECK (schema_version = '1.0.0'),
    message_key TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    kafka_timestamp TIMESTAMPTZ,
    kafka_timestamp_type INTEGER,
    sku BIGINT NOT NULL CHECK (sku >= 0),
    category_id BIGINT NOT NULL CHECK (category_id >= 0),
    price_bucket INTEGER NOT NULL CHECK (price_bucket BETWEEN 0 AND 99),
    name_vector_raw TEXT NOT NULL,
    source_dataset TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL CHECK (source_row_number >= 0),
    replay_run_id UUID NOT NULL,
    published_at_raw TEXT NOT NULL,
    published_at_utc TIMESTAMPTZ NOT NULL,
    processed_at_utc TIMESTAMPTZ NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT products_source_row_unique
        UNIQUE (source_dataset, source_file, source_row_number)
);