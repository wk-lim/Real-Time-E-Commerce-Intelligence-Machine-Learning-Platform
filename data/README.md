# Local data

This directory stores local datasets and generated streaming state. Large data files are intentionally excluded from Git and Docker image build contexts.

## Selected dataset

This project uses the full Synerise RecSys Challenge 2025 dataset:

- Official dataset page: https://recsys.synerise.com/data-set
- Official repository: https://github.com/Synerise/recsys2025
- Selected download: Synerise dataset, approximately 1.9 GB compressed
- Access checked: 2026-09-24

The smaller 1.3 GB competition-preprocessed version is not the primary source for this project.

## Data classification

The dataset contains anonymized, real-world interactions recorded by an online retailer over six months.

The project replays these historical events through Kafka according to event timestamps. This is a reproducible historical streaming workload, not a true live production data feed.

Although released for the 2025 challenge, the verified event-time window is 2022-06-23 through 2022-12-08.

## Directory layout

- `raw/`: Original archive and extracted Parquet files
- `processed/`: Locally generated normalized or sampled data
- `checkpoints/`: PySpark Structured Streaming checkpoints

Expected source tables include:

- `product_buy.parquet`
- `add_to_cart.parquet`
- `remove_from_cart.parquet`
- `page_visit.parquet`
- `search_query.parquet`
- `product_properties.parquet`

## Data handling

Do not commit dataset files, processed outputs, checkpoints, credentials, or personal information.

The project repository contains only code, schemas, metadata, documentation, and reproducible download or processing instructions.

The repository code license does not automatically establish a license for redistributing the dataset. Verify the dataset provider's terms before publishing or redistributing any source or derived data.