import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ecommerce_intelligence.storage.bronze_files import iter_bronze_files, BronzeFile, classify_file


def write_parquet(path: Path, values: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": values}), path)


def test_inventory_has_stable_paths_counts_and_hashes(tmp_path: Path) -> None:
    source_dir = tmp_path / "bronze" / "events"
    write_parquet(source_dir / "b.parquet", [1, 2])
    write_parquet(source_dir / "a.parquet", [3])

    files = list(iter_bronze_files(tmp_path / "bronze", "events"))

    assert [item.relative_path for item in files] == [
        "a.parquet",
        "b.parquet",
    ]
    assert [item.row_count for item in files] == [1, 2]
    assert all(item.source_name == "events" for item in files)
    assert files[0].file_sha256 == hashlib.sha256(
        (source_dir / "a.parquet").read_bytes()
    ).hexdigest()


def test_inventory_ignores_temporary_directories(tmp_path: Path) -> None:
    source_dir = tmp_path / "bronze" / "products"
    write_parquet(source_dir / "part.parquet", [1])
    write_parquet(source_dir / ".spark-staging" / "part.parquet", [2])
    write_parquet(source_dir / "_temporary" / "part.parquet", [3])

    files = list(iter_bronze_files(tmp_path / "bronze", "products"))

    assert [item.relative_path for item in files] == ["part.parquet"]


def test_inventory_requires_existing_source(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Bronze directory"):
        list(iter_bronze_files(tmp_path, "events"))


@pytest.fixture
def sample_file(tmp_path: Path) -> BronzeFile:
    path = tmp_path / "bronze" / "events" / "part.parquet"
    write_parquet(path, [1, 2])
    return next(iter_bronze_files(tmp_path / "bronze", "events"))


def test_unrecorded_file_needs_loading(sample_file: BronzeFile) -> None:
    assert classify_file(sample_file, None) == "load"


def test_matching_file_is_skipped(sample_file: BronzeFile) -> None:
    recorded = (
        sample_file.file_size_bytes,
        sample_file.file_sha256,
        sample_file.row_count,
    )
    assert classify_file(sample_file, recorded) == "skip"


def test_changed_file_is_rejected(sample_file: BronzeFile) -> None:
    recorded = (
        sample_file.file_size_bytes,
        sample_file.file_sha256,
        sample_file.row_count + 1,
    )
    with pytest.raises(ValueError, match="has changed"):
        classify_file(sample_file, recorded)