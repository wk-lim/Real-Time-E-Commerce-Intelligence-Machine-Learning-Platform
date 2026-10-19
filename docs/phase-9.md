# Phase 9: local purchase-scoring dashboard

## Scope

This phase adds a local Streamlit form for the Phase 8 FastAPI scoring endpoint.
The operator enters one **already-computed** client-product feature snapshot:
11 previous-day count features and two recency features. The dashboard sends
these 13 values to `POST /predict` and displays the seven-day purchase score
and three timing probabilities conditional on a purchase.

The dashboard does not look up a client or SKU, calculate features from raw
events, retrain models, or explain why a purchase does not occur. See the
[Phase 8 guide](phase-8.md) for request semantics and model-serving limits.

## Local setup and run

From the repository root, activate the virtual environment and install the
optional dashboard dependencies:

```powershell
python -m pip install --editable ".[dev,ml,tracking,serving,dashboard]"
python -m pip check
```

Start the Phase 8 API in one PowerShell terminal from the repository root:

```powershell
$env:MLFLOW_TRACKING_URI = "sqlite:///data/processed/mlflow.db"
python -m uvicorn ecommerce_intelligence.api.app:app --host 127.0.0.1 --port 18081
```

In another terminal, start the dashboard:

```powershell
python -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 18501
```

Open `http://127.0.0.1:18501`. The dashboard uses
`http://127.0.0.1:18081` for the API by default. To use a different local API
address, set `PURCHASE_API_URL` in the dashboard terminal before starting it.
Keep both development servers bound to loopback; neither provides access
control for public use. Stop each server with Ctrl+C.

## Manual smoke check

Enter this synthetic row into the form, in display order:

| Feature | Value |
| --- | ---: |
| Events | 5 |
| Page visits | 2 |
| Searches | 1 |
| Client cart additions | 1 |
| Client cart removals | 0 |
| Client purchases | 1 |
| Distinct SKUs | 1 |
| SKU interactions | 2 |
| SKU cart additions | 1 |
| SKU cart removals | 0 |
| SKU purchases | 1 |
| Hours since last client event | 2.5 |
| Hours since last SKU event | 3.0 |

Click **Score purchase**. The verified local candidate models displayed
**1.67%** for a purchase observed within seven days and conditional timing
probabilities of **47.51%** under 24 hours, **26.71%** at 24–72 hours, and
**25.78%** at 72–168 hours. These values verify the dashboard-to-API path,
not predictive quality for a real customer; they can change if the mutable
MLflow `candidate` aliases are updated.

Stop the API and submit the form again. The dashboard should display a
connection error instead of crashing. Restart the API for further scoring.

## Automated checks and limitations

```powershell
python -m pytest tests/test_dashboard.py -q
python -m pytest -q
```

The dashboard tests simulate a successful API response and an unavailable
API without starting either server. The host suite reported 83 passed and
one expected skip for the optional PostgreSQL integration test.

Inputs must use the same definitions and as-of timing as the offline feature
pipeline. The form enforces nonnegative values, but does not check count
relationships or feature provenance. The binary score is experimental and
has not been established as calibrated for operational decisions. Timing
probabilities describe *when, conditional on a purchase*; they are not a
promise of a purchase or a causal explanation for non-purchase. This
dashboard is a local demonstration, not an authenticated production UI.
