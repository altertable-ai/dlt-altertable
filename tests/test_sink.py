from typing import Any

import pytest
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from pyarrow.flight import FlightInternalError

import dlt_altertable.destination
from dlt_altertable import altertable
from tests.conftest import FlightRecorder

sink = altertable.__wrapped__

CONNECTION = {
    "host": "flight.test",
    "catalog": "lakehouse",
    "dataset_name": "raw",
    "username": "user",
    "password": "secret",
    "port": 15002,
    "tls": False,
}

SANDBOX_ENVIRONMENT = {
    "ALTERTABLE_HOST": "flight.sandbox",
    "ALTERTABLE_CATALOG": "lakehouse",
    "ALTERTABLE_SCHEMA": "crm",
    "ALTERTABLE_USERNAME": "sandbox",
    "ALTERTABLE_PASSWORD": "sandbox-secret",
    "ALTERTABLE_PORT": "15002",
    "ALTERTABLE_TLS": "false",
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


def with_dedup_sort(table: TTableSchema, column: str, order: str = "desc") -> TTableSchema:
    table["columns"][column]["dedup_sort"] = order
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
def test_merge_with_dedup_sort_becomes_the_server_cursor(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_dedup_sort(
        with_primary_key(table_schema("contacts", "merge"), "id"), "lastmodifieddate"
    )

    sink(write_parquet(rows), table, **CONNECTION)

    assert recorder.ingests[0].mode is IngestTableMode.CREATE_APPEND
    assert recorder.ingests[0].incremental_options == IngestIncrementalOptions(
        primary_key=["id"], cursor_field=["lastmodifieddate"]
    )


@pytest.mark.usefixtures("replaced_tables")
def test_merge_without_dedup_sort_upserts_on_primary_key_alone(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id")

    sink(write_parquet(rows), table, **CONNECTION)

    assert recorder.ingests[0].incremental_options == IngestIncrementalOptions(
        primary_key=["id"], cursor_field=[]
    )


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize(
    ("table", "unsupported"),
    [
        (table_schema("contacts", "merge"), "merge without a primary_key"),
        (
            with_primary_key(table_schema("links", "merge"), "id", "lastmodifieddate"),
            "merge with primary-key columns only",
        ),
        (
            with_dedup_sort(
                with_primary_key(table_schema("contacts", "merge"), "id"),
                "lastmodifieddate",
                order="asc",
            ),
            "dedup_sort 'asc'",
        ),
    ],
    ids=["no_primary_key", "primary_key_only_columns", "ascending_dedup_sort"],
)
def test_unsupported_merge_configurations_are_terminal(
    recorder: FlightRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    table: TTableSchema,
    unsupported: str,
) -> None:
    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, **CONNECTION)

    assert f"Table {table['name']}: {unsupported}" in str(failure.value)
    assert recorder.ingests == []


@pytest.mark.usefixtures("replaced_tables")
def test_merge_with_hard_delete_hint_is_terminal(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id")
    table["columns"]["deleted"] = {"name": "deleted", "data_type": "bool", "hard_delete": True}

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, **CONNECTION)

    assert "hard_delete" in str(failure.value)
    assert recorder.ingests == []


@pytest.mark.usefixtures("replaced_tables")
def test_append_ignores_primary_key_and_dedup_sort_hints(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_dedup_sort(
        with_primary_key(table_schema("contacts", "append"), "id"), "lastmodifieddate"
    )

    sink(write_parquet(rows), table, **CONNECTION)

    assert recorder.ingests[0].incremental_options is None


@pytest.mark.usefixtures("replaced_tables")
def test_schema_evolution_adds_new_columns_before_ingest(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    recorder.existing_columns = ["id"]

    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert recorder.statements == [
        'ALTER TABLE "lakehouse"."raw"."contacts" '
        'ADD COLUMN IF NOT EXISTS "lastmodifieddate" BIGINT'
    ]
    assert recorder.ingests[0].rows == rows


@pytest.mark.usefixtures("replaced_tables")
def test_new_tables_skip_schema_evolution(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert len(recorder.queries) == 1
    assert recorder.statements == []


@pytest.mark.usefixtures("replaced_tables")
def test_replace_skips_the_column_lookup(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "replace"), **CONNECTION)

    assert recorder.queries == []
    assert recorder.statements == []


@pytest.mark.usefixtures("replaced_tables")
def test_wrong_credentials_fail_terminally_naming_the_endpoint(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    recorder.unauthenticated = True

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert "flight.test:15002" in str(failure.value)
    assert "'user'" in str(failure.value)


@pytest.mark.usefixtures("replaced_tables")
def test_flight_errors_name_the_load_target(
    recorder: FlightRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_ingest(self: Any, **_: Any) -> None:
        raise FlightInternalError("Failed to create table")

    monkeypatch.setattr(dlt_altertable.destination.Client, "ingest", failing_ingest)

    with pytest.raises(RuntimeError) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert "lakehouse.raw.contacts" in str(failure.value)
    assert "flight.test:15002" in str(failure.value)


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


@pytest.mark.usefixtures("replaced_tables")
def test_connection_falls_back_to_the_sandbox_environment(
    recorder: FlightRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable, value in SANDBOX_ENVIRONMENT.items():
        monkeypatch.setenv(variable, value)

    sink(write_parquet(rows), table_schema("contacts", "append"))

    assert recorder.connections == [
        {
            "username": "sandbox",
            "password": "sandbox-secret",
            "host": "flight.sandbox",
            "port": 15002,
            "tls": False,
        }
    ]
    assert recorder.ingests[0].catalog_name == "lakehouse"
    assert recorder.ingests[0].schema_name == "crm"


@pytest.mark.usefixtures("replaced_tables")
def test_missing_configuration_is_terminal_and_names_every_surface(
    recorder: FlightRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"))

    assert "host is not configured" in str(failure.value)
    assert "destination.altertable.host" in str(failure.value)
    assert "ALTERTABLE_HOST" in str(failure.value)
    assert recorder.connections == []
