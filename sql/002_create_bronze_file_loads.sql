CREATE TABLE IF NOT EXISTS landing.bronze_file_loads (
    source_name TEXT NOT NULL
        CHECK (source_name IN ('events', 'products')),
    relative_path TEXT NOT NULL
        CHECK (relative_path <> ''),
    file_size_bytes BIGINT NOT NULL
        CHECK (file_size_bytes > 0),
    file_sha256 TEXT NOT NULL
        CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    row_count BIGINT NOT NULL
        CHECK (row_count >= 0),
    inserted_count BIGINT NOT NULL
        CHECK (inserted_count >= 0 AND inserted_count <= row_count),
    completed_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_name, relative_path)
);