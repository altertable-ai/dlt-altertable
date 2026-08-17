from typing import Any

import pytest
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema

import dlt_altertable.destination
from dlt_altertable import CURSOR_HINT, altertable
from tests.conftest import FlightRecorder

sink = altertable.__wrapped__

CONNECTION = {
    "host": "flight.test",
    "catalog": "lakehouse",
    "schema": "raw",
    "username": "user",
    "password": "secret",
    "port": 15002,
    "tls": False,
}


def table_schema(name: str, write_disposition: str, **hints: Any) -> TTableSchema:
    return {
        "name": name,
        "write_disposition": write_disposition,
        "columns": {
            "id": {"name": "id", "data_type": "bigint"},
            "lastmodifieddate": {"name": "lastmodifieddate", "data_type": "bigint"},
        },
        **hints,
    }


def with_primary_key(table: TTableSchema, *columns: str) -> TTableSchema:
    for column in columns:
        table["columns"][column]["primary_key"] = True
    return table


@pytest.fixture
def replaced_tables(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stands in for the per-load-package state, which only exists inside a pipeline run."""
    tables: list[str] = []
    monkeypatch.setattr(
        dlt_altertable.destination, "tables_already_replaced", lambda: tables, raising=True
    )
    return tables


@pytest.fixture
def rows() -> list[dict[str, Any]]:
    return [{"id": 1, "lastmodifieddate": 10}, {"id": 2, "lastmodifieddate": 20}]


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize(
    ("write_disposition", "expected_mode"),
    [
        ("append", IngestTableMode.CREATE_APPEND),
        ("replace", IngestTableMode.REPLACE),
    ],
)
def test_write_disposition_selects_ingest_mode(
    recorder: FlightRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    write_disposition: str,
    expected_mode: IngestTableMode,
) -> None:
    sink(write_parquet(rows), table_schema("contacts", write_disposition), **CONNECTION)

    assert recorder.ingests[0].mode is expected_mode


def test_replace_only_replaces_the_first_file_of_a_load(
    recorder: FlightRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    replaced_tables: list[str],
) -> None:
    for part in range(3):
        sink(write_parquet(rows, f"part{part}"), table_schema("deals", "replace"), **CONNECTION)

    assert [ingest.mode for ingest in recorder.ingests] == [
        IngestTableMode.REPLACE,
        IngestTableMode.APPEND,
        IngestTableMode.APPEND,
    ]
    assert replaced_tables == ["deals"]


@pytest.mark.usefixtures("replaced_tables")
def test_replace_bookkeeping_is_per_table(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows, "a"), table_schema("contacts", "replace"), **CONNECTION)
    sink(write_parquet(rows, "b"), table_schema("deals", "replace"), **CONNECTION)

    assert [ingest.mode for ingest in recorder.ingests] == [
        IngestTableMode.REPLACE,
        IngestTableMode.REPLACE,
    ]


@pytest.mark.usefixtures("replaced_tables")
def test_merge_upserts_on_primary_key_with_cursor_hint(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(
        table_schema("contacts", "merge", **{CURSOR_HINT: "lastmodifieddate"}), "id"
    )

    sink(write_parquet(rows), table, **CONNECTION)

    assert recorder.ingests[0].mode is IngestTableMode.CREATE_APPEND
    assert recorder.ingests[0].incremental_options == IngestIncrementalOptions(
        primary_key=["id"], cursor_field=["lastmodifieddate"]
    )


@pytest.mark.usefixtures("replaced_tables")
def test_merge_without_cursor_hint_upserts_on_primary_key_alone(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id", "lastmodifieddate")

    sink(write_parquet(rows), table, **CONNECTION)

    assert recorder.ingests[0].incremental_options == IngestIncrementalOptions(
        primary_key=["id", "lastmodifieddate"], cursor_field=[]
    )


@pytest.mark.usefixtures("replaced_tables")
def test_merge_without_primary_key_is_a_terminal_failure(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table_schema("contacts", "merge"), **CONNECTION)

    assert "Table contacts: merge without a primary_key" in str(failure.value)
    assert recorder.ingests == []


@pytest.mark.usefixtures("replaced_tables")
def test_append_ignores_primary_key_and_cursor_hints(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(
        table_schema("contacts", "append", **{CURSOR_HINT: "lastmodifieddate"}), "id"
    )

    sink(write_parquet(rows), table, **CONNECTION)

    assert recorder.ingests[0].incremental_options is None


@pytest.mark.usefixtures("replaced_tables")
def test_parquet_file_is_streamed_into_a_committed_transaction(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    ingest = recorder.ingests[0]
    assert ingest.rows == rows
    assert ingest.schema.names == ["id", "lastmodifieddate"]
    assert ingest.catalog_name == "lakehouse"
    assert ingest.schema_name == "raw"
    assert recorder.calls == [
        "begin_transaction",
        "ingest",
        "close_writer",
        "commit",
        "close_client",
    ]


@pytest.mark.usefixtures("replaced_tables")
def test_empty_parquet_file_still_creates_the_table(recorder: FlightRecorder, tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), path)

    sink(str(path), table_schema("contacts", "replace"), **CONNECTION)

    ingest = recorder.ingests[0]
    assert ingest.batches == []
    assert ingest.schema.names == ["id"]
    assert ingest.mode is IngestTableMode.REPLACE


@pytest.mark.usefixtures("replaced_tables")
def test_connection_parameters_reach_the_client(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert recorder.connections == [
        {
            "username": "user",
            "password": "secret",
            "host": "flight.test",
            "port": 15002,
            "tls": False,
        }
    ]
