from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pytest

from ecommerce_intelligence.contract_validation import (
    validate_event,
    validate_product,
)
from ecommerce_intelligence.ingestion.transform import (
    DATASET_NAMESPACE,
    build_event,
    build_product,
    deterministic_record_id,
)


RUN_ID = UUID("ae7330fd-d17f-4e61-9c50-d74ab71d0a56")
PUBLISHED_AT = datetime(
    2026,
    9,
    24,
    8,
    0,
    0,
    tzinfo=timezone.utc,
)


@pytest.mark.parametrize(
    ("source_file", "row", "event_type", "payload"),
    [
        (
            "add_to_cart.parquet",
            {
                "client_id": 18080713,
                "timestamp": "2022-08-06 15:17:25",
                "sku": 219064,
            },
            "add_to_cart",
            {"sku": 219064},
        ),
        (
            "product_buy.parquet",
            {
                "client_id": 33,
                "timestamp": "2022-08-06 15:17:25",
                "sku": 219064,
            },
            "product_buy",
            {"sku": 219064},
        ),
        (
            "remove_from_cart.parquet",
            {
                "client_id": 18080713,
                "timestamp": "2022-08-06 15:17:25",
                "sku": 219064,
            },
            "remove_from_cart",
            {"sku": 219064},
        ),
        (
            "page_visit.parquet",
            {
                "client_id": 0,
                "timestamp": "2022-08-06 15:17:25",
                "url": 12345,
            },
            "page_visit",
            {"url_id": 12345},
        ),
        (
            "search_query.parquet",
            {
                "client_id": 18080713,
                "timestamp": "2022-08-06 15:17:25",
                "query": "[240 170 240 207]",
            },
            "search_query",
            {"query_vector_raw": "[240 170 240 207]"},
        ),
    ],
)
def test_build_event(
    source_file: str,
    row: dict[str, Any],
    event_type: str,
    payload: dict[str, Any],
) -> None:
    event = build_event(
        source_file=source_file,
        row_number=7,
        row=row,
        run_id=RUN_ID,
        published_at=PUBLISHED_AT,
    )

    assert event["event_type"] == event_type
    assert event["payload"] == payload
    assert event["event_time_timezone"] is None
    assert event["replay"]["published_at"] == (
        "2026-09-24T08:00:00Z"
    )

    validate_event(event)


def test_dataset_namespace_is_stable() -> None:
    assert str(DATASET_NAMESPACE) == (
        "44e4330d-8108-5d1f-b93b-65eb8c58a82f"
    )


def test_record_ids_are_deterministic_and_row_specific() -> None:
    first = deterministic_record_id(
        "add_to_cart.parquet",
        7,
    )
    repeated = deterministic_record_id(
        "add_to_cart.parquet",
        7,
    )
    next_row = deterministic_record_id(
        "add_to_cart.parquet",
        8,
    )

    assert first == repeated
    assert first != next_row


def test_build_product() -> None:
    product = build_product(
        row_number=3,
        row={
            "sku": 1263699,
            "category": 780,
            "price": 26,
            "name": "[193 102 221 13]",
        },
        run_id=RUN_ID,
        published_at=PUBLISHED_AT,
    )

    assert product["sku"] == 1263699
    assert product["category_id"] == 780
    assert product["price_bucket"] == 26

    validate_product(product)


def test_rejects_invalid_calendar_timestamp() -> None:
    with pytest.raises(
        ValueError,
        match="valid calendar timestamp",
    ):
        build_event(
            source_file="add_to_cart.parquet",
            row_number=0,
            row={
                "client_id": 1,
                "timestamp": "2022-99-99 15:17:25",
                "sku": 1,
            },
            run_id=RUN_ID,
            published_at=PUBLISHED_AT,
        )


def test_rejects_unsupported_source_file() -> None:
    with pytest.raises(
        ValueError,
        match="Unsupported behavioral source file",
    ):
        build_event(
            source_file="unknown.parquet",
            row_number=0,
            row={},
            run_id=RUN_ID,
            published_at=PUBLISHED_AT,
        )


def test_rejects_naive_published_timestamp() -> None:
    with pytest.raises(
        ValueError,
        match="timezone information",
    ):
        build_event(
            source_file="add_to_cart.parquet",
            row_number=0,
            row={
                "client_id": 1,
                "timestamp": "2022-08-06 15:17:25",
                "sku": 1,
            },
            run_id=RUN_ID,
            published_at=datetime(2026, 9, 24, 8, 0, 0),
        )


def test_rejects_product_price_outside_bucket_range() -> None:
    with pytest.raises(
        ValueError,
        match="between 0 and 99",
    ):
        build_product(
            row_number=0,
            row={
                "sku": 1,
                "category": 2,
                "price": 100,
                "name": "[1 2 3 4]",
            },
            run_id=RUN_ID,
            published_at=PUBLISHED_AT,
        )


def test_rejects_unbracketed_query_vector() -> None:
    with pytest.raises(
        ValueError,
        match="bracketed serialized vector",
    ):
        build_event(
            source_file="search_query.parquet",
            row_number=0,
            row={
                "client_id": 1,
                "timestamp": "2022-08-06 15:17:25",
                "query": "1 2 3 4",
            },
            run_id=RUN_ID,
            published_at=PUBLISHED_AT,
        )