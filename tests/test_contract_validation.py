from __future__ import annotations

from typing import Any

import pytest
from jsonschema.exceptions import ValidationError

from ecommerce_intelligence.contract_validation import (
    validate_event,
    validate_product,
)


EVENT_ID = "0b4ce971-dbc8-5c4b-a9c7-f58d29ac37fc"
RUN_ID = "ae7330fd-d17f-4e61-9c50-d74ab71d0a56"
PRODUCT_RECORD_ID = "bd511224-aed8-54b3-a4b7-5ea75a72c46e"


def make_event(
    event_type: str,
    source_file: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "event_id": EVENT_ID,
        "event_type": event_type,
        "event_time": "2022-08-06 15:17:25",
        "event_time_timezone": None,
        "client_id": 18080713,
        "payload": payload,
        "source": {
            "dataset": "synerise_recsys_2025",
            "file": source_file,
            "row_number": 0,
        },
        "replay": {
            "run_id": RUN_ID,
            "published_at": "2026-09-24T08:00:00Z",
        },
    }


def make_product() -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "record_id": PRODUCT_RECORD_ID,
        "sku": 1263699,
        "category_id": 780,
        "price_bucket": 26,
        "name_vector_raw": (
            "[193 102 221  13  57 176  57  99 "
            "169  14 242  45  29  91 135 191]"
        ),
        "source": {
            "dataset": "synerise_recsys_2025",
            "file": "product_properties.parquet",
            "row_number": 0,
        },
        "replay": {
            "run_id": RUN_ID,
            "published_at": "2026-09-24T08:00:00Z",
        },
    }


@pytest.mark.parametrize(
    ("event_type", "source_file", "payload"),
    [
        (
            "add_to_cart",
            "add_to_cart.parquet",
            {"sku": 219064},
        ),
        (
            "product_buy",
            "product_buy.parquet",
            {"sku": 219064},
        ),
        (
            "remove_from_cart",
            "remove_from_cart.parquet",
            {"sku": 219064},
        ),
        (
            "page_visit",
            "page_visit.parquet",
            {"url_id": 12345},
        ),
        (
            "search_query",
            "search_query.parquet",
            {"query_vector_raw": "[240 170 240 207]"},
        ),
    ],
)
def test_valid_events(
    event_type: str,
    source_file: str,
    payload: dict[str, Any],
) -> None:
    validate_event(make_event(event_type, source_file, payload))


def test_valid_product() -> None:
    validate_product(make_product())


def test_page_visit_rejects_product_payload() -> None:
    record = make_event(
        "page_visit",
        "page_visit.parquet",
        {"sku": 219064},
    )

    with pytest.raises(ValidationError):
        validate_event(record)


def test_search_rejects_non_vector_string() -> None:
    record = make_event(
        "search_query",
        "search_query.parquet",
        {"query_vector_raw": "not-a-vector"},
    )

    with pytest.raises(ValidationError):
        validate_event(record)


def test_event_rejects_invalid_uuid() -> None:
    record = make_event(
        "add_to_cart",
        "add_to_cart.parquet",
        {"sku": 219064},
    )
    record["event_id"] = "not-a-uuid"

    with pytest.raises(ValidationError):
        validate_event(record)


def test_event_rejects_timezone_in_source_timestamp() -> None:
    record = make_event(
        "add_to_cart",
        "add_to_cart.parquet",
        {"sku": 219064},
    )
    record["event_time"] = "2022-08-06T15:17:25Z"

    with pytest.raises(ValidationError):
        validate_event(record)


def test_replay_timestamp_requires_timezone() -> None:
    record = make_event(
        "add_to_cart",
        "add_to_cart.parquet",
        {"sku": 219064},
    )
    record["replay"]["published_at"] = "2026-09-24T08:00:00"

    with pytest.raises(ValidationError):
        validate_event(record)


def test_product_rejects_price_bucket_outside_range() -> None:
    record = make_product()
    record["price_bucket"] = 100

    with pytest.raises(ValidationError):
        validate_product(record)


def test_product_rejects_unknown_property() -> None:
    record = make_product()
    record["currency"] = "USD"

    with pytest.raises(ValidationError):
        validate_product(record)