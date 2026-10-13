# Phase 6: client-product features and purchase baselines

## Scope

This phase builds an offline, source-time snapshot for each candidate
`(feature_as_of_date_source, client_id, sku)` and asks two separate questions:

1. Was a purchase of that SKU observed for that client in the following seven
   days, including a repeat purchase?
2. For pairs with an observed purchase, which time bucket contained the first
   purchase: under 24 hours, 24-72 hours, or 72-168 hours?

These are local baselines on selected historical Synerise dates, not live
predictions or a full-dataset training pipeline. The offline builders scan the
raw Synerise Parquet files directly. They do **not** train on the small,
five-minute sample in PostgreSQL or the dbt feature views. Source timestamps
have an unknown timezone; dates and midnight boundaries below are in source
time, with no UTC conversion.

## Snapshot and label definitions

`build_offline_snapshot.py` scans one activity day, `[D 00:00, D+1 00:00)`,
and sets the as-of date to `D+1`. A client-SKU pair becomes a candidate when
it has at least one `add_to_cart`, `remove_from_cart`, or `product_buy` event on
the activity day. Prior purchases do not remove a candidate: repeat purchases
are eligible. The snapshot has one row per candidate pair and contains
previous-day client and SKU event counts. The v2 snapshot adds hours since
the client's latest event and hours since the pair's latest SKU event,
measured back from the as-of midnight. Page visits and searches contribute
to client-level counts and recency but do not create SKU candidates.

`build_purchase_labels.py` matches each candidate to `product_buy` events in
the half-open outcome window `[as-of 00:00, as-of + 7 days 00:00)`. The binary
label is true when at least one same-client, same-SKU purchase is observed;
`first_purchase_at_source` and `hours_to_first_purchase` use the earliest
matching event. The label is false, with null purchase time, otherwise. A
false label means **no purchase observed in this file during this window**;
it does not establish why the client did not buy or rule out later purchases.

Feature and label files are joined by the full as-of/client/SKU key when
training. The loader rejects duplicate or mismatched keys and invalid label
times. Keys, outcome columns, and future purchase timestamps are not model
features. Generated Parquet lives under ignored `data/processed/` and must
not be committed.

## Reproduce a snapshot and labels

From the repository root, install the development and ML dependencies:

```powershell
python -m pip install --editable ".[dev,ml]"
```

For example, build the September 2 as-of snapshot and its seven-day labels:

```powershell
python -m ecommerce_intelligence.features.build_offline_snapshot `
  --activity-date 2022-09-01 `
  --output data/processed/features/client_product_2022-09-02.parquet

python -m ecommerce_intelligence.features.build_purchase_labels `
  --features data/processed/features/client_product_2022-09-02.parquet `
  --output data/processed/features/client_product_labels_2022-09-02.parquet
```

Both builders refuse to overwrite existing output. Other as-of dates used
below are September 3, 4, 5, 13, and 21. For v2, run the snapshot builder
with the same activity dates and an output path under `features_v2/`. The
existing labels under `features/` are reused: the candidate keys and count
features were checked against v1, and the v2 loader checks feature-label keys
again for every loaded date.

| As-of date | Role | Candidate pairs | Observed-purchase pairs |
| --- | --- | ---: | ---: |
| 2022-09-02 | Train | 43,407 | 742 |
| 2022-09-03 | Train | 35,152 | 770 |
| 2022-09-04 | Train | 31,150 | 695 |
| 2022-09-05 | Train | 47,075 | 1,038 |
| 2022-09-13 | Model selection | 46,427 | 1,093 |
| 2022-09-21 | Final holdout | 52,731 | 1,394 |

The four training dates total 156,784 candidate rows and 3,245 positives.
Their last outcome window ends on September 12, before the September 13
selection date. The September 13 outcome window ends on September 20, before
the September 21 holdout. The scripts reject overlapping training and
evaluation outcome windows. Repeated clients or SKUs across dates are possible;
the split is temporal, not a client-disjoint split.

## Models and selection

`train_purchase_baseline.py` fits a standardized logistic regression for the
binary seven-day outcome. On September 13, the count-only v1 features were
selected over v2 counts plus recency:

| September 13 metric | v1 counts | v2 counts + recency |
| --- | ---: | ---: |
| Average precision | 0.0465 | 0.0424 |
| ROC AUC | 0.6828 | 0.6648 |
| Precision at top 1,000 | 0.0620 | 0.0570 |

The September 13 random-ranking average-precision baseline was 0.0235.
These values show modest ranking ability, not a calibrated purchase decision
threshold or a causal explanation of non-purchase.

`train_purchase_timing.py` fits a standardized three-class logistic regression
with `C=0.01` **only on pairs labeled as purchased**. On September 13, v2
counts plus recency achieved 1.0507 log loss, versus 1.0577 for a training
class-prior predictor and 1.0743 for the count-only timing model at the same
regularization. The v2 model's accuracy was 0.4355, below the majority-bucket
accuracy of 0.4767. Thus v2 was selected for its small probability-quality
gain, not for better hard-class accuracy. The two selected configurations
were locked before evaluating September 21.

Run the selected configurations against the September 13 selection date:

```powershell
python -m ecommerce_intelligence.ml.train_purchase_baseline `
  --train-as-of 2022-09-02 2022-09-03 2022-09-04 2022-09-05 `
  --validation-as-of 2022-09-13

python -m ecommerce_intelligence.ml.train_purchase_timing `
  --data-dir data/processed/features_v2 `
  --labels-dir data/processed/features `
  --include-recency `
  --train-as-of 2022-09-02 2022-09-03 2022-09-04 2022-09-05 `
  --validation-as-of 2022-09-13
```

## Locked holdout result

The same training dates and selected configurations were evaluated once on
September 21. The CLI calls this date `--validation-as-of`, but in this
assessment it is the untouched final holdout, not another tuning date.

| September 21 measure | Result | Reference |
| --- | ---: | ---: |
| Binary average precision | 0.0563 | Random-ranking baseline 0.0264 |
| Binary ROC AUC | 0.6848 | — |
| Binary Brier score | 0.0256 | — |
| Precision at top 1,000 | 0.0880 | 88 observed purchases |
| Recall at top 1,000 | 0.0631 | 1,394 positive pairs |
| Conditional timing log loss | 1.0424 | Prior-only 1.0505 |
| Conditional timing accuracy | 0.4864 | Majority bucket 0.4950 |
| Conditional timing macro F1 | 0.3468 | — |

Timing was evaluated on the 1,394 holdout pairs with an observed purchase,
not on all 52,731 candidates. The holdout timing buckets contained 690
under-24-hour, 398 24-72-hour, and 306 72-168-hour pairs. Recall for the
middle and late buckets remained low (0.11 and 0.17). The small log-loss
improvement does not justify a claim of reliable exact purchase timing.
Do not tune the model against these holdout results.

To reproduce the locked holdout evaluation after creating the corresponding
v1 features/labels and v2 feature file:

```powershell
python -m ecommerce_intelligence.ml.train_purchase_baseline `
  --train-as-of 2022-09-02 2022-09-03 2022-09-04 2022-09-05 `
  --validation-as-of 2022-09-21

python -m ecommerce_intelligence.ml.train_purchase_timing `
  --data-dir data/processed/features_v2 `
  --labels-dir data/processed/features `
  --include-recency `
  --train-as-of 2022-09-02 2022-09-03 2022-09-04 2022-09-05 `
  --validation-as-of 2022-09-21
```

## Verification and limits

```powershell
python -m pytest -q
```

At this checkpoint the host suite reported 64 passed and one skipped
PostgreSQL-dependent integration test. Focused tests cover snapshot counts
and recency, earliest same-SKU purchase labeling, optional recency loading,
separate feature/label directories, and key mismatch rejection.

These scripts report metrics but do not persist a trained model, run online
inference, or provide explanations for individual predictions. Candidate
generation is restricted to prior-day SKU interactions, which excludes
client-SKU pairs without one. More dates and monitoring are needed before
generalizing beyond these historical splits. The next roadmap step is
experiment tracking and model management, followed by a serving interface.
