import numpy as np
import pytest
from fastapi.testclient import TestClient

import ecommerce_intelligence.api.app as api_module
from ecommerce_intelligence.ml.train_purchase_baseline import (
    FEATURE_COLUMNS,
    RECENCY_COLUMNS,
)


class FakeModel:
    def __init__(self, classes, probabilities, feature_count):
        self.classes_ = np.asarray(classes)
        self.probabilities = np.asarray([probabilities])
        self.feature_count = feature_count

    def predict_proba(self, values):
        assert values.shape == (1, self.feature_count)
        return self.probabilities


@pytest.fixture
def client(monkeypatch):
    purchase = FakeModel([0, 1], [0.7, 0.3], 11)
    timing = FakeModel([0, 1, 2], [0.2, 0.5, 0.3], 13)
    loads = []

    def fake_loader(uri):
        loads.append(uri)
        return purchase, timing

    monkeypatch.setenv("MLFLOW_TRACKING_URI", "sqlite:///test.db")
    monkeypatch.setattr(api_module, "load_candidate_models", fake_loader)

    # The context manager runs FastAPI's startup lifespan.
    with TestClient(api_module.app) as test_client:
        yield test_client, loads


def valid_features():
    return {
        **{name: 1 for name in FEATURE_COLUMNS},
        **{name: 1.0 for name in RECENCY_COLUMNS},
    }


def test_health_and_prediction(client):
    test_client, loads = client

    assert test_client.get("/health").json() == {"status": "ready"}

    response = test_client.post("/predict", json=valid_features())
    assert response.status_code == 200
    result = response.json()
    assert result["purchase_within_7d_probability"] == pytest.approx(0.3)
    assert result["timing_given_purchase"] == {
        "under_24h": pytest.approx(0.2),
        "hours_24_to_72": pytest.approx(0.5),
        "hours_72_to_168": pytest.approx(0.3),
    }
    assert loads == ["sqlite:///test.db"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("events_previous_day", -1),
        ("hours_since_last_client_event", -0.1),
        ("sku_purchases_previous_day", None),
    ],
)
def test_rejects_invalid_features(client, field, value):
    test_client, _ = client
    features = valid_features()
    features[field] = value

    assert test_client.post("/predict", json=features).status_code == 422


def test_rejects_missing_or_extra_features(client):
    test_client, _ = client
    missing = valid_features()
    del missing["sku_interactions_previous_day"]
    extra = valid_features()
    extra["client_id"] = 123

    assert test_client.post("/predict", json=missing).status_code == 422
    assert test_client.post("/predict", json=extra).status_code == 422


def test_startup_requires_tracking_uri(monkeypatch):
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)

    with pytest.raises(RuntimeError, match="MLFLOW_TRACKING_URI is required"):
        with TestClient(api_module.app):
            pass