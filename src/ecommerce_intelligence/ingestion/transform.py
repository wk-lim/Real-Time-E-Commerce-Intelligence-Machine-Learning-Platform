from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5


DATASET_NAME = "synerise_recsys_2025"

DATASET_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://github.com/wk-lim/"
    "Real-Time-E-Commerce-Intelligence-Machine-Learning-Platform/"
    "synerise/v1",
)

EVENT_TYPE_BY_FILE = {
    "add_to_cart.parquet": "add_to_cart",
    "page_visit.parquet": "page_visit",
    "product_buy.parquet": "product_buy",
    "remove_from_cart.parquet": "remove_from_cart",
    "search_query.parquet": "search_query",
}

SKU_EVENTS = {
    "add_to_cart",
    "product_buy",
    "remove_from_cart",
}

SOURCE_TIMESTAMP_PATTERN = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2} "
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}$"
)


def deterministic_record_id(
    source_file: str,
    row_number: int,
) -> str:
    if row_number < 0:
        raise ValueError("row_number must be non-negative")

    return str(
        uuid5(
            DATASET_NAMESPACE,
            f"{source_file}:{row_number}",
        )
    )


def utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("published_at must contain timezone information")

    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def require_nonnegative_integer(
    row: Mapping[str, Any],
    field: str,
) -> int:
    value = row.get(field)

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")

    if value < 0:
        raise ValueError(f"{field} must be non-negative")

    return value


def require_string(
    row: Mapping[str, Any],
    field: str,
) -> str:
    value = row.get(field)

    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")

    return value


def build_event(
    *,
    source_file: str,
    row_number: int,
    row: Mapping[str, Any],
    run_id: UUID,
    published_at: datetime,
) -> dict[str, Any]:
    try:
        event_type = EVENT_TYPE_BY_FILE[source_file]
    except KeyError as error:
        raise ValueError(
            f"Unsupported behavioral source file: {source_file}"
        ) from error

    client_id = require_nonnegative_integer(row, "client_id")
    event_time = require_string(row, "timestamp")

    if not SOURCE_TIMESTAMP_PATTERN.fullmatch(event_time):
        raise ValueError(
            "timestamp must use YYYY-MM-DD HH:MM:SS format"
        )

    try:
        datetime.strptime(event_time, "%Y-%m-%d %H:%M:%S")
    except ValueError as error:
        raise ValueError(
            "timestamp is not a valid calendar timestamp"
        ) from error

    if event_type in SKU_EVENTS:
        payload = {
            "sku": require_nonnegative_integer(row, "sku"),
        }

    if event_type in SKU_EVENTS:
        payload = {
            "sku": require_nonnegative_integer(row, "sku"),
        }
    elif event_type == "page_visit":
        payload = {
            "url_id": require_nonnegative_integer(row, "url"),
        }
    else:
        query_vector_raw = require_string(row, "query")

        if not (
            query_vector_raw.startswith("[")
            and query_vector_raw.endswith("]")
        ):
            raise ValueError(
                "query must be a bracketed serialized vector"
            )

        payload = {
            "query_vector_raw": query_vector_raw,
        }

    return {
        "schema_version": "1.0.0",
        "event_id": deterministic_record_id(
            source_file,
            row_number,
        ),
        "event_type": event_type,
        "event_time": event_time,
        "event_time_timezone": None,
        "client_id": client_id,
        "payload": payload,
        "source": {
            "dataset": DATASET_NAME,
            "file": source_file,
            "row_number": row_number,
        },
        "replay": {
            "run_id": str(run_id),
            "published_at": utc_timestamp(published_at),
        },
    }

def build_product(
    *,
    row_number: int,
    row: Mapping[str, Any],
    run_id: UUID,
    published_at: datetime,
) -> dict[str, Any]:
    source_file = "product_properties.parquet"
    name_vector_raw = require_string(row, "name")

    if not (
        name_vector_raw.startswith("[")
        and name_vector_raw.endswith("]")
    ):
        raise ValueError(
            "name must be a bracketed serialized vector"
        )

    price_bucket = require_nonnegative_integer(row, "price")

    if price_bucket > 99:
        raise ValueError("price must be between 0 and 99")

    return {
        "schema_version": "1.0.0",
        "record_id": deterministic_record_id(
            source_file,
            row_number,
        ),
        "sku": require_nonnegative_integer(row, "sku"),
        "category_id": require_nonnegative_integer(
            row,
            "category",
        ),
        "price_bucket": price_bucket,
        "name_vector_raw": name_vector_raw,
        "source": {
            "dataset": DATASET_NAME,
            "file": source_file,
            "row_number": row_number,
        },
        "replay": {
            "run_id": str(run_id),
            "published_at": utc_timestamp(published_at),
        },
    }