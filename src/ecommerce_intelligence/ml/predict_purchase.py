import numpy as np

from ecommerce_intelligence.ml.train_purchase_baseline import (
    FEATURE_COLUMNS,
    RECENCY_COLUMNS,
)


def score_feature_row(row, purchase_model, timing_model):
    columns = FEATURE_COLUMNS + RECENCY_COLUMNS
    missing = [name for name in columns if name not in row]
    if missing:
        raise ValueError(f"Missing features: {', '.join(missing)}")

    try:
        values = np.asarray(
            [[float(row[name]) for name in columns]], dtype=np.float64
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("Features must be numeric") from exc

    if not np.isfinite(values).all():
        raise ValueError("Features must be finite")

    purchase_classes = list(purchase_model.classes_)
    timing_classes = list(timing_model.classes_)
    if set(purchase_classes) != {0, 1} or set(timing_classes) != {0, 1, 2}:
        raise ValueError("Unexpected model classes")

    purchase_probs = purchase_model.predict_proba(
        values[:, :len(FEATURE_COLUMNS)]
    )[0]
    timing_probs = timing_model.predict_proba(values)[0]

    return {
        "purchase_within_7d_probability": float(
            purchase_probs[purchase_classes.index(1)]
        ),
        "timing_given_purchase": {
            "under_24h": float(timing_probs[timing_classes.index(0)]),
            "hours_24_to_72": float(timing_probs[timing_classes.index(1)]),
            "hours_72_to_168": float(timing_probs[timing_classes.index(2)]),
        },
    }

def load_candidate_models(tracking_uri: str):
    if not tracking_uri:
        raise ValueError("An MLflow tracking URI is required")

    import mlflow
    import mlflow.sklearn

    mlflow.set_tracking_uri(tracking_uri)
    purchase_model = mlflow.sklearn.load_model(
        "models:/ecommerce-purchase-binary@candidate"
    )
    timing_model = mlflow.sklearn.load_model(
        "models:/ecommerce-purchase-timing@candidate"
    )

    if purchase_model.n_features_in_ != len(FEATURE_COLUMNS):
        raise ValueError("Purchase model feature count does not match")
    if timing_model.n_features_in_ != len(FEATURE_COLUMNS + RECENCY_COLUMNS):
        raise ValueError("Timing model feature count does not match")

    return purchase_model, timing_model