import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from ecommerce_intelligence.storage.bronze_files import BronzeFile
from ecommerce_intelligence.storage.incremental_loader import (
    assert_file_unchanged,
    load_file,
)


def sample_file(path: Path) -> BronzeFile:
    path.write_bytes(b"abc")
    return BronzeFile(
        source_name="events",
        relative_path="part.parquet",
        path=path,
        file_size_bytes=3,
        file_sha256=hashlib.sha256(b"abc").hexdigest(),
        row_count=1,
    )


def test_unchanged_file_passes(tmp_path: Path) -> None:
    assert_file_unchanged(sample_file(tmp_path / "part.parquet"))


def test_same_size_changed_content_is_rejected(tmp_path: Path) -> None:
    bronze_file = sample_file(tmp_path / "part.parquet")
    bronze_file.path.write_bytes(b"xyz")

    with pytest.raises(RuntimeError, match="changed during loading"):
        assert_file_unchanged(bronze_file)

def test_load_requires_autocommit(tmp_path: Path) -> None:
    bronze_file = sample_file(tmp_path / "part.parquet")
    connection = SimpleNamespace(autocommit=False)

    with pytest.raises(ValueError, match="autocommit=True"):
        load_file(connection, bronze_file)