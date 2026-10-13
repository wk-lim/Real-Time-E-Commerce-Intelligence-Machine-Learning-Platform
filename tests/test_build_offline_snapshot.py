import sys
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from ecommerce_intelligence.features.build_offline_snapshot import main


def write_rows(path, rows):
    pq.write_table(pa.Table.from_pylist(rows), path)


def test_full_day_features_and_candidate_grain(tmp_path, monkeypatch):
    write_rows(
        tmp_path / "add_to_cart.parquet",
        [
            {"client_id": 1, "sku": 10, "timestamp": "2022-09-01 12:00:00"},
            {"client_id": 1, "sku": 10, "timestamp": "2022-09-01 13:00:00"},
            {"client_id": 2, "sku": 20, "timestamp": "2022-09-01 14:00:00"},
            {"client_id": 1, "sku": 99, "timestamp": "2022-09-02 00:00:00"},
        ],
    )
    write_rows(
        tmp_path / "product_buy.parquet",
        [
            {"client_id": 1, "sku": 10, "timestamp": "2022-08-31 23:59:59"},
            {"client_id": 1, "sku": 10, "timestamp": "2022-09-01 18:00:00"},
        ],
    )
    write_rows(
        tmp_path / "remove_from_cart.parquet",
        [{"client_id": 1, "sku": 10, "timestamp": "2022-09-01 19:00:00"}],
    )
    write_rows(
        tmp_path / "page_visit.parquet",
        [
            {"client_id": 1, "timestamp": "2022-09-01 11:00:00"},
            {"client_id": 1, "timestamp": "2022-09-01 15:00:00"},
            {"client_id": 2, "timestamp": "2022-09-01 16:00:00"},
            {"client_id": 3, "timestamp": "2022-09-01 17:00:00"},
        ],
    )
    write_rows(
        tmp_path / "search_query.parquet",
        [{"client_id": 1, "timestamp": "2022-09-01 10:00:00"}],
    )

    output = tmp_path / "features.parquet"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_offline_snapshot",
            "--activity-date",
            "2022-09-01",
            "--data-dir",
            str(tmp_path),
            "--output",
            str(output),
        ],
    )

    main()

    rows = pq.read_table(output).to_pylist()
    assert [(row["client_id"], row["sku"]) for row in rows] == [
        (1, 10),
        (2, 20),
    ]
    assert all(
        row["feature_as_of_date_source"] == date(2022, 9, 2)
        for row in rows
    )

    first = rows[0]
    assert first["events_previous_day"] == 7
    assert first["page_visits_previous_day"] == 2
    assert first["search_previous_day"] == 1
    assert first["distinct_skus_previous_day"] == 1
    assert first["sku_interactions_previous_day"] == 4
    assert first["sku_cart_additions_previous_day"] == 2
    assert first["sku_cart_removals_previous_day"] == 1
    assert first["sku_purchases_previous_day"] == 1
    assert first["hours_since_last_client_event"] == 5.0
    assert first["hours_since_last_sku_event"] == 5.0

    second = rows[1]
    assert second["events_previous_day"] == 2
    assert second["sku_interactions_previous_day"] == 1
    assert second["hours_since_last_client_event"] == 8.0
    assert second["hours_since_last_sku_event"] == 10.0
