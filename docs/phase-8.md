# Phase 8: local feature-input purchase scoring

## Scope

This phase exposes the two [Phase 7](phase-7.md) MLflow `candidate` models
through a local FastAPI application. `POST /predict` accepts one already
computed client-product feature snapshot. It does **not** look up a client or
SKU, build features from live events, or serve a production decision system.
No client ID, SKU, raw event, or label is required in the request.

The binary model uses 11 previous-day count features to estimate the chance
of an observed same-SKU purchase within seven days. The timing model uses
those 11 counts plus two recency features. Its three probabilities describe
**when a purchase occurs, conditional on purchase**: under 24 hours, 24-72
hours, or 72-168 hours. They do not predict a purchase time for a pair that
will not purchase. Neither model explains why a purchase does not occur.
The labels and temporal evaluation limits are described in
[Phase 6](phase-6.md).

## Local setup and run

From the repository root, with the project virtual environment activated:

```powershell
python -m pip install --editable ".[dev,ml,tracking,serving]"
python -m pip check
$env:MLFLOW_TRACKING_URI = "sqlite:///data/processed/mlflow.db"
python -m uvicorn ecommerce_intelligence.api.app:app --host 127.0.0.1 --port 18081
```

Run the server from the repository root because the SQLite tracking URI is
relative. The local database and MLflow artifacts are ignored by Git; see
[Phase 7](phase-7.md) for how the models were logged and registered. Startup
requires `MLFLOW_TRACKING_URI` and loads both `candidate` aliases once before
accepting requests. It rejects a model whose input feature count does not
match the expected 11 or 13. If an alias is changed, restart the server to
load the new version. Use only model artifacts from a trusted registry.

Keep this unauthenticated development server bound to `127.0.0.1`. If port
18081 is occupied, choose another free local port and use it in the requests
below. Stop the server with Ctrl+C.

## Request and response

In a second PowerShell terminal, check readiness and send a synthetic feature
row. The count totals in this example are internally consistent; the row is
not a real customer or a representative candidate.

```powershell
Invoke-RestMethod http://127.0.0.1:18081/health

$body = @{
    events_previous_day = 5
    page_visits_previous_day = 2
    search_previous_day = 1
    client_cart_additions_previous_day = 1
    client_cart_removals_previous_day = 0
    client_purchases_previous_day = 1
    distinct_skus_previous_day = 1
    sku_interactions_previous_day = 2
    sku_cart_additions_previous_day = 1
    sku_cart_removals_previous_day = 0
    sku_purchases_previous_day = 1
    hours_since_last_client_event = 2.5
    hours_since_last_sku_event = 3.0
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri http://127.0.0.1:18081/predict `
    -ContentType "application/json" `
    -Body $body | ConvertTo-Json -Depth 3
```

The verified local `/health` response was `{"status":"ready"}`. The
synthetic `/predict` request returned a seven-day purchase probability of
`0.01668841772322957` and conditional timing probabilities of approximately
`0.4751`, `0.2671`, and `0.2578`; the timing probabilities sum to one. These
numbers verify the API-to-model path, not predictive quality for a real pair.
Results can change if the mutable `candidate` aliases point to new versions.

The API rejects missing, extra, negative, null, and non-finite feature values.
It does not currently verify relationships among count fields or prove that
the supplied row was computed at the correct as-of time. Callers must use
the same feature definitions and ordering as the offline training pipeline.

## Verification and limits

```powershell
python -m pytest tests/test_predict_purchase.py tests/test_purchase_api.py -q
python -m pytest -q
```

The unit tests cover feature ordering, class-to-probability mapping, invalid
inputs, model loading and feature-count checks, API startup, and HTTP request
validation. The full host suite reported 81 passed and one expected skip for
the optional PostgreSQL integration test. The real local `candidate` models
loaded with 11 and 13 input features, and the loopback health and prediction
smoke requests succeeded.

This API is not production-ready: it has no authentication, TLS, rate
limiting, online feature store, monitoring, or deployment policy. The local
registry is not shared or durable infrastructure. The binary score has not
been established as calibrated for operational decisions, and an unobserved
purchase is not a causal explanation for non-purchase. Do not expose this
service beyond loopback or use its output as an automated customer decision.
