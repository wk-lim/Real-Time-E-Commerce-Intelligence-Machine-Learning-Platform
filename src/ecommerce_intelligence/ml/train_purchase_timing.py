import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    log_loss,
)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ecommerce_intelligence.ml.train_purchase_baseline import (
    FEATURE_COLUMNS,
    RECENCY_COLUMNS,
    load_day,
)


BUCKET_NAMES = ["under 24h", "24-72h", "72-168h"]


def timing_buckets(hours: np.ndarray) -> np.ndarray:
    return np.searchsorted([24.0, 72.0], hours, side="right")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/processed/features"),
    )
    parser.add_argument("--labels-dir", type=Path)
    parser.add_argument("--include-recency", action="store_true")
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
    parser.add_argument(
        "--tracking-uri",
        help="MLflow tracking URI; enables experiment and model logging",
    )
    parser.add_argument(
        "--experiment-name",
        default="ecommerce-purchase-timing",
    )
    parser.add_argument(
        "--evaluation-role",
        choices=("validation", "holdout"),
        default="validation",
    )
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

    x_valid, y_valid, hours_valid = load_day(
        args.data_dir,
        args.validation_as_of,
        labels_dir=args.labels_dir,
        include_recency=args.include_recency,
    )
    x_train = np.vstack([x[y == 1] for x, y, _ in training_days])
    hours_train = np.concatenate(
        [hours[y == 1] for _, y, hours in training_days]
    )
    x_valid = x_valid[y_valid == 1]
    hours_valid = hours_valid[y_valid == 1]

    train_buckets = timing_buckets(hours_train)
    valid_buckets = timing_buckets(hours_valid)

    if set(np.unique(train_buckets)) != {0, 1, 2}:
        raise ValueError("Training needs examples in all three timing buckets")
    if len(valid_buckets) == 0:
        raise ValueError("Validation has no observed purchases")

    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(max_iter=2000, C=0.01),
    )
    model.fit(x_train, train_buckets)

    predictions = model.predict(x_valid)
    probabilities = model.predict_proba(x_valid)

    train_counts = np.bincount(train_buckets, minlength=3)
    valid_counts = np.bincount(valid_buckets, minlength=3)
    majority_bucket = int(train_counts.argmax())
    prior = train_counts / train_counts.sum()
    prior_probabilities = np.tile(prior, (len(valid_buckets), 1))
    train_prior_probabilities = np.tile(prior, (len(train_buckets), 1))
    train_probabilities = model.predict_proba(x_train)
    metrics = {
        "training_prior_log_loss": float(
            log_loss(train_buckets, train_prior_probabilities, labels=[0, 1, 2])
        ),
        "training_model_log_loss": float(
            log_loss(train_buckets, train_probabilities, labels=[0, 1, 2])
        ),
        "majority_bucket_accuracy": float(
            accuracy_score(
                valid_buckets,
                np.full_like(valid_buckets, majority_bucket),
            )
        ),
        "model_accuracy": float(accuracy_score(valid_buckets, predictions)),
        "model_macro_f1": float(
            f1_score(valid_buckets, predictions, average="macro")
        ),
        "prior_log_loss": float(
            log_loss(valid_buckets, prior_probabilities, labels=[0, 1, 2])
        ),
        "model_log_loss": float(
            log_loss(valid_buckets, probabilities, labels=[0, 1, 2])
        ),
    }

    print(
        "Training prior log loss:",
        f"{metrics['training_prior_log_loss']:.4f}",
    )
    print(
        "Training model log loss:",
        f"{metrics['training_model_log_loss']:.4f}",
    )

    print("Training timing counts:", train_counts.tolist())
    print("Validation timing counts:", valid_counts.tolist())
    print(
        "Majority-bucket accuracy:",
        f"{metrics['majority_bucket_accuracy']:.4f}",
    )
    print("Model accuracy:", f"{metrics['model_accuracy']:.4f}")
    print("Model macro F1:", f"{metrics['model_macro_f1']:.4f}")
    print(
        "Prior-only log loss:",
        f"{metrics['prior_log_loss']:.4f}",
    )
    print(
        "Model log loss:",
        f"{metrics['model_log_loss']:.4f}",
    )
    print(
        classification_report(
            valid_buckets,
            predictions,
            labels=[0, 1, 2],
            target_names=BUCKET_NAMES,
            zero_division=0,
        )
    )

    if args.tracking_uri:
        import mlflow
        import mlflow.sklearn

        mlflow.set_tracking_uri(args.tracking_uri)
        mlflow.set_experiment(args.experiment_name)
        with mlflow.start_run(
            run_name=f"timing-{args.validation_as_of.isoformat()}"
        ) as run:
            mlflow.log_params(
                {
                    "model": "StandardScaler+LogisticRegression",
                    "feature_set": (
                        "counts+recency" if args.include_recency else "counts"
                    ),
                    "feature_columns": ",".join(
                        FEATURE_COLUMNS
                        + (RECENCY_COLUMNS if args.include_recency else ())
                    ),
                    "train_as_of": ",".join(
                        day.isoformat() for day in args.train_as_of
                    ),
                    "evaluation_as_of": args.validation_as_of.isoformat(),
                    "training_positive_pairs": len(train_buckets),
                    "evaluation_positive_pairs": len(valid_buckets),
                    "training_bucket_counts": ",".join(
                        str(count) for count in train_counts
                    ),
                    "evaluation_bucket_counts": ",".join(
                        str(count) for count in valid_counts
                    ),
                    "bucket_boundaries_hours": "24,72,168",
                    "C": 0.01,
                    "max_iter": 2000,
                }
            )
            mlflow.log_metrics(metrics)
            mlflow.set_tags(
                {
                    "evaluation_role": args.evaluation_role,
                    "conditional_on_purchase": "true",
                }
            )
            model_info = mlflow.sklearn.log_model(
                sk_model=model,
                name="purchase_timing_classifier",
                input_example=np.zeros((1, x_train.shape[1]), dtype=np.float64),
            )
        print(f"MLflow run ID: {run.info.run_id}")
        print(f"MLflow model URI: {model_info.model_uri}")


if __name__ == "__main__":
    main()
