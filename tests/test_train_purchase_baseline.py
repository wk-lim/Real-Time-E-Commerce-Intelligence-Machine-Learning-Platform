from datetime import date

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ecommerce_intelligence.ml.train_purchase_baseline import (
    FEATURE_COLUMNS,
    load_day,
)


AS_OF = date(2022, 9, 2)


def write_inputs(tmp_path):
    features_dir = tmp_path / "features_v2"
    labels_dir = tmp_path / "features"
    features_dir.mkdir()
    labels_dir.mkdir()

    features = [
        {
            "feature_as_of_date_source": AS_OF,
            "client_id": client_id,
            "sku": sku,
            **dict.fromkeys(FEATURE_COLUMNS, 1),
            "hours_since_last_client_event": client_hours,
            "hours_since_last_sku_event": sku_hours,
        }
        for client_id, sku, client_hours, sku_hours in [
            (1, 100, 1.5, 2.5),
            (2, 200, 3.5, 4.5),
        ]
    ]
    labels = [
        {
            "feature_as_of_date_source": AS_OF,
            "client_id": 2,
            "sku": 200,
            "purchase_observed_within_7d": False,
            "hours_to_first_purchase": None,
        },
        {
            "feature_as_of_date_source": AS_OF,
            "client_id": 1,
            "sku": 100,
            "purchase_observed_within_7d": True,
            "hours_to_first_purchase": 12.0,
        },
    ]

    pq.write_table(
        pa.Table.from_pylist(features),
        features_dir / "client_product_2022-09-02.parquet",
    )
    label_path = labels_dir / "client_product_labels_2022-09-02.parquet"
    pq.write_table(pa.Table.from_pylist(labels), label_path)
    return features_dir, labels_dir, label_path, labels


def test_load_day_uses_separate_labels_and_optional_recency(tmp_path):
    features_dir, labels_dir, _, _ = write_inputs(tmp_path)

    x, y, hours = load_day(
        features_dir, AS_OF, labels_dir=labels_dir, include_recency=True
    )
    assert x.shape == (2, len(FEATURE_COLUMNS) + 2)
    assert x[:, -2:].tolist() == [[1.5, 2.5], [3.5, 4.5]]
    assert y.tolist() == [1, 0]  # Labels were stored in reverse order.
    assert hours[0] == 12.0
    assert np.isnan(hours[1])

    x_counts, _, _ = load_day(features_dir, AS_OF, labels_dir=labels_dir)
    assert x_counts.shape == (2, len(FEATURE_COLUMNS))


def test_load_day_rejects_mismatched_keys(tmp_path):
    features_dir, labels_dir, label_path, labels = write_inputs(tmp_path)
    labels[0]["sku"] = 999
    pq.write_table(pa.Table.from_pylist(labels), label_path)

    with pytest.raises(ValueError, match="Feature and label keys differ"):
        load_day(
            features_dir, AS_OF, labels_dir=labels_dir, include_recency=True
        )