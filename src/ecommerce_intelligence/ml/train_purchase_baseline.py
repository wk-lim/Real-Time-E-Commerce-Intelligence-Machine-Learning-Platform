import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    roc_auc_score,
)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


KEY_COLUMNS = (
    "feature_as_of_date_source",
    "client_id",
    "sku",
)

FEATURE_COLUMNS = (
    "events_previous_day",
    "page_visits_previous_day",
    "search_previous_day",
    "client_cart_additions_previous_day",
    "client_cart_removals_previous_day",
    "client_purchases_previous_day",
    "distinct_skus_previous_day",
    "sku_interactions_previous_day",
    "sku_cart_additions_previous_day",
    "sku_cart_removals_previous_day",
    "sku_purchases_previous_day",
)

RECENCY_COLUMNS = (
    "hours_since_last_client_event",
    "hours_since_last_sku_event",
)


def load_day(
    data_dir: Path,
    as_of: date,
    *,
    labels_dir: Path | None = None,
    include_recency: bool = False,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    day = as_of.isoformat()
    feature_path = data_dir / f"client_product_{day}.parquet"
    label_root = data_dir if labels_dir is None else labels_dir
    label_path = label_root / f"client_product_labels_{day}.parquet"
    feature_columns = FEATURE_COLUMNS + (
        RECENCY_COLUMNS if include_recency else ()
    )

    features = pq.read_table(
        feature_path,
        columns=[*KEY_COLUMNS, *feature_columns],
    ).to_pydict()
    labels = pq.read_table(
        label_path,
        columns=[
            *KEY_COLUMNS,
            "purchase_observed_within_7d",
            "hours_to_first_purchase",
        ],
    ).to_pydict()

    feature_keys = list(zip(*(features[column] for column in KEY_COLUMNS)))
    label_keys = list(zip(*(labels[column] for column in KEY_COLUMNS)))

    if not feature_keys:
        raise ValueError(f"No feature rows for {day}")
    if len(set(feature_keys)) != len(feature_keys):
        raise ValueError(f"Duplicate feature keys for {day}")
    if len(set(label_keys)) != len(label_keys):
        raise ValueError(f"Duplicate label keys for {day}")
    if set(feature_keys) != set(label_keys):
        raise ValueError(f"Feature and label keys differ for {day}")
    if any(key[0] != as_of for key in feature_keys):
        raise ValueError(f"Unexpected as-of date in {feature_path}")

    labels_by_key = {
        key: (observed, hours)
        for key, observed, hours in zip(
            label_keys,
            labels["purchase_observed_within_7d"],
            labels["hours_to_first_purchase"],
        )
    }
    if any(observed is None for observed, _ in labels_by_key.values()):
        raise ValueError(f"Missing purchase labels for {day}")

    x = np.column_stack(
    [
        np.asarray(features[column], dtype=np.float64)
        for column in feature_columns
    ]
)
    if not np.isfinite(x).all():
        raise ValueError(f"Missing or non-finite feature values for {day}")

    y = np.asarray(
        [int(labels_by_key[key][0]) for key in feature_keys],
        dtype=np.int8,
    )
    hours = np.asarray(
        [
            np.nan if labels_by_key[key][1] is None
            else float(labels_by_key[key][1])
            for key in feature_keys
        ],
        dtype=np.float64,
    )

    if np.any((y == 1) & ~np.isfinite(hours)):
        raise ValueError(f"Positive labels need finite purchase times for {day}")
    if np.any((y == 0) & ~np.isnan(hours)):
        raise ValueError(f"Negative labels must have no purchase time for {day}")
    if np.any((y == 1) & ((hours < 0) | (hours >= 168))):
        raise ValueError(f"Purchase times must be within seven days for {day}")

    return x, y, hours


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/processed/features"),
    )
    parser.add_argument(
        "--train-as-of",
        type=date.fromisoformat,
        nargs="+",
        required=True,
    )
    parser.add_argument(
        "--validation-as-of",
        type=date.fromisoformat,
        required=True,
    )
    parser.add_argument("--labels-dir", type=Path)
    parser.add_argument("--include-recency", action="store_true")
    args = parser.parse_args()

    if len(set(args.train_as_of)) != len(args.train_as_of):
        parser.error("Training dates must be unique")
    if max(args.train_as_of) + timedelta(days=7) > args.validation_as_of:
        parser.error("Training and validation outcome windows overlap")

    training_days = [
        load_day(
            args.data_dir,
            day,
            labels_dir=args.labels_dir,
            include_recency=args.include_recency,
        )
        for day in args.train_as_of
    ]
    x_train = np.vstack([x for x, _, _ in training_days])
    y_train = np.concatenate([y for _, y, _ in training_days])
    x_valid, y_valid, _ = load_day(
        args.data_dir,
        args.validation_as_of,
        labels_dir=args.labels_dir,
        include_recency=args.include_recency,
    )

    if len(np.unique(y_train)) != 2 or len(np.unique(y_valid)) != 2:
        raise ValueError("Training and validation each need both label classes")

    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=2000),
    )
    model.fit(x_train, y_train)
    probabilities = model.predict_proba(x_valid)[:, 1]

    k = min(1000, len(y_valid))
    top_indices = np.argsort(probabilities)[-k:]
    top_hits = int(y_valid[top_indices].sum())

    print(f"Training: {len(y_train):,} rows, {int(y_train.sum()):,} positives")
    print(f"Validation: {len(y_valid):,} rows, {int(y_valid.sum()):,} positives")
    print(f"Random-ranking AP baseline: {y_valid.mean():.4f}")
    print(f"Average precision: {average_precision_score(y_valid, probabilities):.4f}")
    print(f"ROC AUC: {roc_auc_score(y_valid, probabilities):.4f}")
    print(f"Brier score: {brier_score_loss(y_valid, probabilities):.4f}")
    print(f"Precision at top {k:,}: {top_hits / k:.4f}")
    print(f"Recall at top {k:,}: {top_hits / int(y_valid.sum()):.4f}")


if __name__ == "__main__":
    main()
