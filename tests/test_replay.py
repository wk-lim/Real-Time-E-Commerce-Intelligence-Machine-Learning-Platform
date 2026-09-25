from __future__ import annotations

import argparse
import json
from argparse import Namespace
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ecommerce_intelligence.ingestion.replay import (
    EVENT_TOPIC,
    PRODUCT_TOPIC,
    build_record,
    iter_source_rows,
    nonnegative_float,
    positive_integer,
    run,
    serialize_record,
    validate_record,
)


RUN_ID = UUID("12345678-1234-5678-1234-567812345678")


def write_parquet(
    path: Path,
    rows: list[dict[str, object]],
) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def test_positive_integer_accepts_positive_value() -> None:
    assert positive_integer("25") == 25


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_positive_integer_rejects_invalid_value(
    value: str,
) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        positive_integer(value)


def test_nonnegative_float_accepts_zero() -> None:
    assert nonnegative_float("0") == 0.0


@pytest.mark.parametrize("value", ["-0.1", "invalid"])
def test_nonnegative_float_rejects_invalid_value(
    value: str,
) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        nonnegative_float(value)


def test_iter_source_rows_preserves_row_numbers(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "add_to_cart.parquet"

    write_parquet(
        source_path,
        [
            {
                "client_id": 10,
                "timestamp": "2022-06-23 00:10:20",
                "sku": 100,
            },
            {
                "client_id": 20,
                "timestamp": "2022-06-23 00:10:25",
                "sku": 200,
            },
            {
                "client_id": 30,
                "timestamp": "2022-06-23 00:10:30",
                "sku": 300,
            },
        ],
    )

    rows = list(
        iter_source_rows(
            source_path,
            batch_size=1,
            limit=2,
        )
    )

    assert [row_number for row_number, _ in rows] == [0, 1]
    assert rows[0][1]["client_id"] == 10
    assert rows[1][1]["client_id"] == 20


def test_builds_behavioral_event() -> None:
    record, topic, key = build_record(
        source_file="add_to_cart.parquet",
        row_number=0,
        row={
            "client_id": 18080713,
            "timestamp": "2022-08-06 15:17:25",
            "sku": 219064,
        },
        run_id=RUN_ID,
    )

    validate_record(
        record,
        source_file="add_to_cart.parquet",
    )

    assert topic == EVENT_TOPIC
    assert key == "18080713"
    assert record["event_type"] == "add_to_cart"
    assert record["payload"] == {"sku": 219064}
    assert record["replay"]["run_id"] == str(RUN_ID)


def test_builds_product_record() -> None:
    record, topic, key = build_record(
        source_file="product_properties.parquet",
        row_number=0,
        row={
            "sku": 1263699,
            "category": 780,
            "price": 26,
            "name": "[193 102 221]",
        },
        run_id=RUN_ID,
    )

    validate_record(
        record,
        source_file="product_properties.parquet",
    )

    assert topic == PRODUCT_TOPIC
    assert key == "1263699"
    assert record["sku"] == 1263699
    assert record["price_bucket"] == 26
    assert record["replay"]["run_id"] == str(RUN_ID)


def test_serialize_record_returns_utf8_json() -> None:
    encoded_record = serialize_record(
        {
            "message": "hello",
            "number": 10,
        }
    )

    assert isinstance(encoded_record, bytes)
    assert json.loads(encoded_record) == {
        "message": "hello",
        "number": 10,
    }


def test_run_in_dry_run_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_path = tmp_path / "add_to_cart.parquet"

    write_parquet(
        source_path,
        [
            {
                "client_id": 10,
                "timestamp": "2022-06-23 00:10:20",
                "sku": 100,
            },
            {
                "client_id": 20,
                "timestamp": "2022-06-23 00:10:25",
                "sku": 200,
            },
        ],
    )

    args = Namespace(
        source_file="add_to_cart.parquet",
        limit=2,
        data_dir=tmp_path,
        bootstrap_servers="localhost:9092",
        batch_size=1,
        messages_per_second=0.0,
        run_id=RUN_ID,
        dry_run=True,
        validate=True,
    )

    assert run(args) == 0

    output = capsys.readouterr().out

    assert "Mode: dry run" in output
    assert "Processed: 2" in output
    assert '"client_id":10' in output
    assert '"client_id":20' in output