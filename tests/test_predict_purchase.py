import numpy as np
import pytest

from ecommerce_intelligence.ml.predict_purchase import score_feature_row
from ecommerce_intelligence.ml.train_purchase_baseline import (
    FEATURE_COLUMNS,
    RECENCY_COLUMNS,
)


class FakeModel:
    def __init__(self, classes, probabilities):
        self.classes_ = np.asarray(classes)
        self.probabilities = np.asarray([probabilities])
        self.received = None

    def predict_proba(self, values):
        self.received = values.copy()
        return self.probabilities


@pytest.fixture
def feature_row():
    columns = FEATURE_COLUMNS + RECENCY_COLUMNS
    # Reverse insertion order to prove scoring uses FEATURE_COLUMNS order.
    return {name: float(index) for index, name in reversed(
        list(enumerate(columns, start=1))
    )}


def test_scores_in_training_order_and_maps_classes(feature_row):
    purchase_model = FakeModel([1, 0], [0.25, 0.75])
    timing_model = FakeModel([2, 0, 1], [0.20, 0.50, 0.30])

    result = score_feature_row(feature_row, purchase_model, timing_model)

    np.testing.assert_array_equal(
        purchase_model.received,
        np.asarray([list(range(1, 12))]),
    )
    np.testing.assert_array_equal(
        timing_model.received,
        np.asarray([list(range(1, 14))]),
    )
    assert result["purchase_within_7d_probability"] == pytest.approx(0.25)
    assert result["timing_given_purchase"] == {
        "under_24h": pytest.approx(0.50),
        "hours_24_to_72": pytest.approx(0.30),
        "hours_72_to_168": pytest.approx(0.20),
    }


@pytest.mark.parametrize("bad_value", [None, "not-a-number", np.nan, np.inf])
def test_rejects_invalid_feature(feature_row, bad_value):
    feature_row[FEATURE_COLUMNS[0]] = bad_value
    purchase_model = FakeModel([0, 1], [0.75, 0.25])
    timing_model = FakeModel([0, 1, 2], [0.50, 0.30, 0.20])

    with pytest.raises(ValueError, match="Features must"):
        score_feature_row(feature_row, purchase_model, timing_model)


def test_rejects_missing_feature(feature_row):
    del feature_row[RECENCY_COLUMNS[0]]
    purchase_model = FakeModel([0, 1], [0.75, 0.25])
    timing_model = FakeModel([0, 1, 2], [0.50, 0.30, 0.20])

    with pytest.raises(ValueError, match="Missing features"):
        score_feature_row(feature_row, purchase_model, timing_model)


def test_rejects_unexpected_model_classes(feature_row):
    purchase_model = FakeModel([0, 2], [0.75, 0.25])
    timing_model = FakeModel([0, 1, 2], [0.50, 0.30, 0.20])

    with pytest.raises(ValueError, match="Unexpected model classes"):
        score_feature_row(feature_row, purchase_model, timing_model)


import sys
from types import ModuleType, SimpleNamespace

from ecommerce_intelligence.ml.predict_purchase import load_candidate_models


def fake_mlflow(monkeypatch, purchase_width=11, timing_width=13):
    calls = []
    mlflow = ModuleType("mlflow")
    mlflow.__path__ = []
    sklearn = ModuleType("mlflow.sklearn")

    purchase = SimpleNamespace(n_features_in_=purchase_width)
    timing = SimpleNamespace(n_features_in_=timing_width)
    models = {
        "models:/ecommerce-purchase-binary@candidate": purchase,
        "models:/ecommerce-purchase-timing@candidate": timing,
    }

    def set_tracking_uri(uri):
        calls.append(("tracking_uri", uri))

    def load_model(uri):
        calls.append(("load_model", uri))
        return models[uri]

    mlflow.set_tracking_uri = set_tracking_uri
    mlflow.sklearn = sklearn
    sklearn.load_model = load_model
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setitem(sys.modules, "mlflow.sklearn", sklearn)
    return purchase, timing, calls


def test_loads_both_candidate_models(monkeypatch):
    purchase, timing, calls = fake_mlflow(monkeypatch)

    assert load_candidate_models("sqlite:///test.db") == (purchase, timing)
    assert calls == [
        ("tracking_uri", "sqlite:///test.db"),
        ("load_model", "models:/ecommerce-purchase-binary@candidate"),
        ("load_model", "models:/ecommerce-purchase-timing@candidate"),
    ]


@pytest.mark.parametrize(
    ("purchase_width", "timing_width"),
    [(10, 13), (11, 12)],
)
def test_rejects_wrong_model_feature_count(
    monkeypatch, purchase_width, timing_width
):
    fake_mlflow(monkeypatch, purchase_width, timing_width)

    with pytest.raises(ValueError, match="feature count does not match"):
        load_candidate_models("sqlite:///test.db")


def test_loader_requires_tracking_uri():
    with pytest.raises(ValueError, match="tracking URI is required"):
        load_candidate_models("")