from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ecommerce_intelligence.storage.load_bronze as loader

def test_iter_rows_respects_limit(tmp_path: Path) -> None:
    folder = tmp_path / "events"
    folder.mkdir()

    table = pa.table({
        "event_id": ["first", "second", "third"],
        "source_row_number": [0,1,2],
    })
    pq.write_table(table, folder / "part.parquet")

    rows = list(
        loader.iter_rows(
            folder,
            ("event_id", "source_row_number"),
            limit=2
        )
    )

    assert [row["event_id"] for row in rows] == ["first", "second"]


def test_normalize_uuid() -> None:
    value = "12345678-1234-5678-1234-567812345678"

    assert loader.normalize_value("event_id", value) == UUID(value)


def test_naive_replay_timestamp_becomes_utc() -> None:
    value = datetime(2026, 9, 25, 7, 1, 17)

    result = loader.normalize_value("published_at_utc", value)

    assert result == value.replace(tzinfo=timezone.utc)

def test_event_time_remains_timezone_unknown() -> None:
    value = datetime(2022, 9, 22, 6, 26, 40)

    assert loader.normalize_value("event_time_local", value) is value

    with pytest.raises(TypeError, match="naive datetime"):
        loader.normalize_value("event_time_local", value.replace(tzinfo=timezone.utc))

def test_dry_run_does_not_connect(
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "events").mkdir()
    args = Namespace(
        source="events",
        limit=1,
        dry_run=True,
        bronze_root=tmp_path,
    )
    monkeypatch.setattr(loader, "parse_args", lambda: args)

    row = {column: None for column in loader.TABLE_COLUMNS["events"]}
    row.update({
        "event_id": "12345678-1234-5678-1234-567812345678",
        "replay_run_id": "12345678-1234-5678-1234-567812345678",
        "event_time_local": datetime(2022, 9, 22, 6, 26, 40),
        "published_at_utc": datetime(2026, 9, 25, 7, 1, 17),
        "processed_at_utc": datetime(2026, 9, 25, 7, 1, 18),
        "source_row_number": 0,
    })
    monkeypatch.setattr(loader, "iter_rows", lambda path, columns, limit: iter([row]))

    def forbidden_connection() -> None:
        pytest.fail("Dry run must not connect to PostgreSQL")

    monkeypatch.setattr(loader, "connect_postgres", forbidden_connection)

    loader.main()

    assert "Previewed: 1" in capsys.readouterr().out