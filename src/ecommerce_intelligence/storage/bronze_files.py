from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal

import pyarrow.parquet as pq



@dataclass(frozen=True)
class BronzeFile:
    source_name: str
    relative_path: str
    path: Path
    file_size_bytes: int
    file_sha256: str
    row_count: int


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_bronze_files(
    bronze_root: Path,
    source_name: str,
) -> Iterator[BronzeFile]:
    if source_name not in ('events', 'products'):
        raise ValueError(f"Unknown bronze source: {source_name}")

    source_dir = bronze_root / source_name
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Bronze directory not found: {source_dir}")

    paths = sorted(
        (
            path
            for path in source_dir.rglob("*.parquet")
            if path.is_file()
            and not path.is_symlink()
            and all(
                not part.startswith((".", "_")) for part in path.relative_to(source_dir).parts
            )
        ),
        key=lambda path: path.relative_to(source_dir).as_posix(),
    )

    for path in paths:
        before = path.stat()
        row_count = pq.read_metadata(path).num_rows
        file_hash = sha256_file(path)
        after = path.stat()

        if (
            before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            raise RuntimeError(f"Bronze file changed during inspection: {path}")

        yield BronzeFile(
            source_name=source_name,
            relative_path=path.relative_to(source_dir).as_posix(),
            path=path,
            file_size_bytes=after.st_size,
            file_sha256=file_hash,
            row_count=row_count,
        )

def classify_file(
    bronze_file: BronzeFile,
    recorded: tuple[int, str, int] | None,
) -> Literal["load", "skip"]:
    if recorded is None:
        return "load"

    expected = (
        bronze_file.file_size_bytes,
        bronze_file.file_sha256,
        bronze_file.row_count,
    )
    if recorded != expected:
        raise ValueError(
            f"Previously loaded bronze file has changed: "
            f"{bronze_file.source_name}/{bronze_file.relative_path}"
        )

    return "skip"