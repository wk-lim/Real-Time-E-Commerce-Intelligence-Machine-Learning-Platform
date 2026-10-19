# Phase 7: local MLflow tracking and candidate models

## Scope

MLflow records the two selected Phase 6 baselines as separate experiments:
seven-day same-SKU purchase ranking and timing **conditional on an observed
purchase**. This is a local experiment and model-management setup, not a
production model deployment. The September 13, 2022 date remains the model
selection date; the September 21 holdout results in [Phase 6](phase-6.md)
must not be used to tune or promote a different model.

The training commands still work without MLflow. Passing `--tracking-uri`
opts into logging the fitted scikit-learn pipeline, parameters, feature-set
description, evaluation metrics, and a synthetic all-zero numeric input
example. No client IDs, SKU IDs, raw events, or Parquet datasets are logged
as model artifacts. The model example documents input shape, not a real
customer or a representative candidate.

## Local setup

Install the separate tracking extra in the activated project environment:

```powershell
python -m pip install --editable ".[dev,ml,tracking]"
python -c "import mlflow; print(mlflow.__version__)"
python -m pip check
```

The verified local installation used MLflow 3.17.0 and reported no broken
requirements. Tracking metadata and the model registry use
`sqlite:///data/processed/mlflow.db`; model artifacts are stored under
`mlruns/`. Both locations are ignored by Git. The database is **local state**,
not part of the repository or a portable record of the models. Run commands
from the repository root so the relative SQLite URI resolves consistently.
The parent `data/processed/` directory must exist before the first run.

The first SQLite smoke run created the database and logged a parameter and a
metric under `ecommerce-phase7-smoke`. The selected model runs were then
logged with explicit tracking URIs:

```powershell
python -m ecommerce_intelligence.ml.train_purchase_baseline `
  --tracking-uri sqlite:///data/processed/mlflow.db `
  --train-as-of 2022-09-02 2022-09-03 2022-09-04 2022-09-05 `
  --validation-as-of 2022-09-13

python -m ecommerce_intelligence.ml.train_purchase_timing `
  --data-dir data/processed/features_v2 `
  --labels-dir data/processed/features `
  --include-recency `
  --tracking-uri sqlite:///data/processed/mlflow.db `
  --train-as-of 2022-09-02 2022-09-03 2022-09-04 2022-09-05 `
  --validation-as-of 2022-09-13
```

The binary run recorded 0.0465 average precision and 0.6828 ROC AUC, and its
logged pipeline was reloaded with 11 input features and a two-column
`predict_proba` result. The conditional timing run recorded 1.0507 model log
loss versus 1.0577 for its prior-only baseline; its logged pipeline was
reloaded with 13 input features and a three-column `predict_proba` result.
These are the same September 13 validation metrics reported in Phase 6, not
new holdout results.

## Registry and inspection

The two verified logged models were registered as version 1 under
`ecommerce-purchase-binary` and `ecommerce-purchase-timing`. Each has the
local alias `candidate`. Both aliases were resolved and reloaded. `candidate`
means available for further evaluation; it is **not** a production approval
or evidence that either model is calibrated for operational decisions.
Registration is deliberately separate from training so an experimental run
does not automatically create or promote a model version. Re-registering the
same URI creates another version, so inspect the registry before doing so.

To inspect the local UI, start a server in a separate terminal from the
repository root:

```powershell
mlflow server `
  --backend-store-uri sqlite:///data/processed/mlflow.db `
  --host 127.0.0.1 `
  --port 15000
```

Then open `http://127.0.0.1:15000`. Keep this development server bound to
loopback; it has not been configured as an authenticated shared service.
The experiments show the run parameters and metrics, while the Models view
shows registered versions and their aliases. To inspect candidate aliases
without starting the UI:

```powershell
$env:MLFLOW_TRACKING_URI = "sqlite:///data/processed/mlflow.db"
python -c "import mlflow, mlflow.sklearn; print(mlflow.sklearn.load_model('models:/ecommerce-purchase-binary@candidate').n_features_in_); print(mlflow.sklearn.load_model('models:/ecommerce-purchase-timing@candidate').n_features_in_)"
```

Expected feature counts are 11 and 13. The native scikit-learn flavor exposes
`predict_proba`; the generic pyfunc `predict` output is a class label, not a
purchase probability. Use the right model and feature ordering when building
future inference code. The binary model predicts purchase within seven days;
the timing model applies only after a purchase is assumed or observed. Neither
explains a non-purchase.

`--evaluation-role` defaults to `validation`. A separately logged holdout
assessment must pass `--evaluation-role holdout`; that flag changes metadata,
not the model or the evaluation calculation. Do not repeatedly inspect the
holdout to choose settings.

## Verification and remaining work

The local SQLite smoke, both tracked training runs, both model reloads, and
both `candidate` alias reloads succeeded. The host suite reported 64 passed
and one expected PostgreSQL integration-test skip. Tracking is currently
verified by these local integration checks, not by an automated MLflow test.

The local registry and artifacts are not durable team infrastructure. The
runs record feature columns, date windows, class counts, hyperparameters, and
metrics, but not source-file checksums or a fully portable data snapshot.
The local feature-input scoring API is documented in [Phase 8](phase-8.md).
Access control, monitoring, calibration checks, live feature retrieval, and a
policy for changing the candidate alias remain future work.
