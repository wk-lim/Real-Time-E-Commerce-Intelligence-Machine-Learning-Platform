import sys
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from ecommerce_intelligence.features.build_purchase_labels import main


def test_purchase_labels_use_earliest_same_sku_purchase(tmp_path, monkeypatch, capsys):
    features_path = tmp_path / "features.parquet"
    purchases_path = tmp_path / "purchases.parquet"
    labels_path = tmp_path / "labels.parquet"

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "feature_as_of_date_source": date(2022, 9, 2),
                    "client_id": 1,
                    "sku": 100,
                    "sku_purchases_previous_day": 1,  # Repeat purchase is eligible.
                },
                {
                    "feature_as_of_date_source": date(2022, 9, 2),
                    "client_id": 2,
                    "sku": 200,
                    "sku_purchases_previous_day": 0,
                },
                {
                    "feature_as_of_date_source": date(2022, 9, 2),
                    "client_id": 3,
                    "sku": 300,
                    "sku_purchases_previous_day": 0,
                },
            ]
        ),
        features_path,
    )

    pq.write_table(
        pa.Table.from_pylist(
            [
                {"client_id": 1, "sku": 100, "timestamp": "2022-09-01 23:59:59"},
                {"client_id": 1, "sku": 100, "timestamp": "2022-09-03 12:00:00"},
                {"client_id": 1, "sku": 100, "timestamp": "2022-09-02 00:00:00"},
                {"client_id": 1, "sku": 100, "timestamp": "2022-09-09 00:00:00"},
                {"client_id": 2, "sku": 999, "timestamp": "2022-09-03 00:00:00"},
                {"client_id": 2, "sku": 200, "timestamp": "2022-09-08 23:59:59"},
                {"client_id": 3, "sku": 300, "timestamp": "2022-09-09 00:00:00"},
            ]
        ),
        purchases_path,
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_purchase_labels",
            "--features",
            str(features_path),
            "--purchase-file",
            str(purchases_path),
            "--output",
            str(labels_path),
        ],
    )

    main()

    labels = pq.read_table(labels_path).to_pylist()
    by_pair = {(row["client_id"], row["sku"]): row for row in labels}

    assert len(labels) == 3
    assert by_pair[(1, 100)]["purchase_observed_within_7d"] is True
    assert by_pair[(1, 100)]["first_purchase_at_source"] == datetime(2022, 9, 2)
    assert by_pair[(1, 100)]["hours_to_first_purchase"] == 0

    assert by_pair[(2, 200)]["purchase_observed_within_7d"] is True
    assert by_pair[(2, 200)]["first_purchase_at_source"] == datetime(
        2022, 9, 8, 23, 59, 59
    )

    assert by_pair[(3, 300)]["purchase_observed_within_7d"] is False
    assert by_pair[(3, 300)]["first_purchase_at_source"] is None
    assert by_pair[(3, 300)]["hours_to_first_purchase"] is None

    assert "Matching purchase rows: 3" in capsys.readouterr().out