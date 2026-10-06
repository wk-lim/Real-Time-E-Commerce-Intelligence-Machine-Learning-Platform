import os
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ecommerce_intelligence.storage.incremental_loader as loader
from ecommerce_intelligence.storage.bronze_files import iter_bronze_files
from ecommerce_intelligence.storage.postgres import connect_postgres, PROJECT_ROOT


@pytest.mark.skipif(
    os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
    reason="Requires local PostgreSQL and bronze event files",
)
def test_partial_failure_rolls_back_and_retry_skips(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = PROJECT_ROOT / "data" / "processed" / "bronze" / "events"
    source_path = next(
        (
            path
            for path in sorted(source_dir.glob("*.parquet"))
            if pq.read_metadata(path).num_rows >= 2
        ),
        None,
    )
    assert source_path is not None

    table = pq.read_table(source_path).slice(0, 2)
    rows = table.to_pylist()
    event_ids = (uuid4(), uuid4())
    fixture_name = f"rollback-test-{uuid4()}.parquet"

    for index, row in enumerate(rows):
        row["event_id"] = str(event_ids[index])
        row["source_dataset"] = "integration_test"
        row["source_file"] = fixture_name
        row["source_row_number"] = index

    fixture_dir = tmp_path / "bronze" / "events"
    fixture_dir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=table.schema),
        fixture_dir / fixture_name,
    )
    bronze_file = next(iter_bronze_files(tmp_path / "bronze", "events"))

    original_normalize = loader.normalize_value

    def fail_on_second_event_id(column: str, value: object) -> object:
        if column == "event_id" and str(value) == str(event_ids[1]):
            raise RuntimeError("injected after first row")
        return original_normalize(column, value)

    with connect_postgres() as connection:
        connection.autocommit = True

        def event_count() -> int:
            return connection.execute(
                "SELECT count(*) FROM landing.events "
                "WHERE event_id IN (%s, %s)",
                event_ids,
            ).fetchone()[0]

        def ledger_count() -> int:
            return connection.execute(
                "SELECT count(*) FROM landing.bronze_file_loads "
                "WHERE source_name = %s AND relative_path = %s",
                ("events", bronze_file.relative_path),
            ).fetchone()[0]

        with connection.transaction(force_rollback=True):
            assert event_count() == 0
            assert ledger_count() == 0

            monkeypatch.setattr(loader, "normalize_value", fail_on_second_event_id)

            with pytest.raises(RuntimeError, match="injected after first row"):
                loader.load_file(connection, bronze_file)

            assert event_count() == 0
            assert ledger_count() == 0

            monkeypatch.setattr(loader, "normalize_value", original_normalize)
            first = loader.load_file(connection, bronze_file)
            second = loader.load_file(connection, bronze_file)

            assert first.status == "loaded"
            assert first.inserted_count == 2
            assert second.status == "skipped"
            assert event_count() == 2
            assert ledger_count() == 1

        assert event_count() == 0
        assert ledger_count() == 0
