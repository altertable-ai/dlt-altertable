from pathlib import Path
from typing import Any

import pytest
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema

import dlt_altertable.destination
from dlt_altertable import altertable
from tests.conftest import HttpRecorder

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

BASE_URL = "http://flight.test:15002"

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
def evolved_tables(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    tables: list[str] = []
    monkeypatch.setattr(
        dlt_altertable.destination, "tables_already_evolved", lambda: tables, raising=True
    )
    return tables


@pytest.fixture
def replaced_tables(monkeypatch: pytest.MonkeyPatch, evolved_tables: list[str]) -> list[str]:
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
        ("append", "create_append"),
        ("replace", "overwrite"),
    ],
)
def test_write_disposition_selects_upload_mode(
    recorder: HttpRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    write_disposition: str,
    expected_mode: str,
) -> None:
    sink(write_parquet(rows), table_schema("contacts", write_disposition), **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.endpoint == "upload"
    assert upload.params["mode"] == expected_mode


def test_replace_only_replaces_the_first_file_of_a_load(
    recorder: HttpRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    replaced_tables: list[str],
) -> None:
    for part in range(3):
        sink(write_parquet(rows, f"part{part}"), table_schema("deals", "replace"), **CONNECTION)

    assert [upload.params["mode"] for upload in recorder.uploads] == [
        "overwrite",
        "append",
        "append",
    ]
    assert replaced_tables == ["deals"]


@pytest.mark.usefixtures("replaced_tables")
def test_replace_bookkeeping_is_per_table(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows, "a"), table_schema("contacts", "replace"), **CONNECTION)
    sink(write_parquet(rows, "b"), table_schema("deals", "replace"), **CONNECTION)

    assert [upload.params["mode"] for upload in recorder.uploads] == ["overwrite", "overwrite"]


@pytest.mark.usefixtures("replaced_tables")
def test_merge_with_dedup_sort_becomes_the_server_cursor(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_dedup_sort(
        with_primary_key(table_schema("contacts", "merge"), "id"), "lastmodifieddate"
    )

    sink(write_parquet(rows), table, **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.endpoint == "upsert"
    assert upload.params["primary_key"] == "id"
    assert upload.params["cursor_field"] == "lastmodifieddate"


@pytest.mark.usefixtures("replaced_tables")
def test_merge_without_dedup_sort_upserts_on_primary_key_alone(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id", "lastmodifieddate")

    sink(write_parquet(rows), table, **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.endpoint == "upsert"
    assert upload.params["primary_key"] == "id,lastmodifieddate"
    assert "cursor_field" not in upload.params


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize(
    ("table", "unsupported"),
    [
        (table_schema("contacts", "merge"), "merge without a primary_key"),
        (
            with_dedup_sort(
                with_primary_key(table_schema("contacts", "merge"), "id"),
                "lastmodifieddate",
                order="asc",
            ),
            "dedup_sort 'asc'",
        ),
    ],
    ids=["no_primary_key", "ascending_dedup_sort"],
)
def test_unsupported_merge_configurations_are_terminal(
    recorder: HttpRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    table: TTableSchema,
    unsupported: str,
) -> None:
    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, **CONNECTION)

    assert f"Table {table['name']}: {unsupported}" in str(failure.value)
    assert recorder.uploads == []
    assert recorder.statements == []


@pytest.mark.usefixtures("replaced_tables")
def test_merge_with_hard_delete_hint_is_terminal(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id")
    table["columns"]["deleted"] = {"name": "deleted", "data_type": "bool", "hard_delete": True}

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, **CONNECTION)

    assert "hard_delete" in str(failure.value)
    assert recorder.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_append_ignores_primary_key_and_dedup_sort_hints(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_dedup_sort(
        with_primary_key(table_schema("contacts", "append"), "id"), "lastmodifieddate"
    )

    sink(write_parquet(rows), table, **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.endpoint == "upload"
    assert "primary_key" not in upload.params


@pytest.mark.usefixtures("replaced_tables")
def test_schema_evolution_adds_new_columns_before_the_upload(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    recorder.existing_columns = ["id"]

    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert recorder.alters == [
        'ALTER TABLE "lakehouse"."raw"."contacts" '
        'ADD COLUMN IF NOT EXISTS "lastmodifieddate" BIGINT'
    ]
    assert recorder.uploads[0].rows == rows


@pytest.mark.usefixtures("replaced_tables")
def test_new_tables_skip_schema_evolution(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert len(recorder.schema_lookups) == 1
    assert recorder.alters == []


@pytest.mark.usefixtures("replaced_tables")
def test_replace_skips_the_column_lookup(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "replace"), **CONNECTION)

    assert recorder.statements == []


def test_schema_lookup_runs_once_per_table_per_load(
    recorder: HttpRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    evolved_tables: list[str],
    replaced_tables: list[str],
) -> None:
    sink(write_parquet(rows, "a"), table_schema("contacts", "append"), **CONNECTION)
    sink(write_parquet(rows, "b"), table_schema("contacts", "append"), **CONNECTION)

    assert len(recorder.schema_lookups) == 1
    assert evolved_tables == ["contacts"]


@pytest.mark.usefixtures("replaced_tables")
def test_wrong_credentials_fail_terminally(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    recorder.unauthenticated = True

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert "401" in str(failure.value)
    assert recorder.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_server_failures_are_transient_and_name_the_load_target(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    recorder.transient_upload_failures = 1

    with pytest.raises(RuntimeError) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert "lakehouse.raw.contacts" in str(failure.value)
    assert "503" in str(failure.value)


@pytest.mark.usefixtures("replaced_tables")
def test_query_stream_errors_are_transient(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    recorder.query_error = "worker lease expired"

    with pytest.raises(RuntimeError) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    assert "worker lease expired" in str(failure.value)
    assert recorder.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_the_parquet_file_is_posted_verbatim(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    path = write_parquet(rows)

    sink(path, table_schema("contacts", "append"), **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.url == f"{BASE_URL}/upload"
    assert upload.body == Path(path).read_bytes()
    assert upload.headers["Content-Type"] == "application/parquet"
    assert upload.params["catalog"] == "lakehouse"
    assert upload.params["schema"] == "raw"


@pytest.mark.usefixtures("replaced_tables")
def test_empty_parquet_file_is_still_uploaded(recorder: HttpRecorder, tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), path)

    sink(str(path), table_schema("contacts", "replace"), **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.rows == []
    assert upload.params["mode"] == "overwrite"


@pytest.mark.usefixtures("replaced_tables")
def test_connection_parameters_reach_the_request(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), **CONNECTION)

    upload = recorder.uploads[0]
    assert upload.url.startswith(BASE_URL)
    assert upload.auth == ("user", "secret")


@pytest.mark.usefixtures("replaced_tables")
def test_connection_falls_back_to_the_sandbox_environment(
    recorder: HttpRecorder,
    write_parquet,
    rows: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable, value in SANDBOX_ENVIRONMENT.items():
        monkeypatch.setenv(variable, value)

    sink(write_parquet(rows), table_schema("contacts", "append"))

    upload = recorder.uploads[0]
    assert upload.url == "http://flight.sandbox:15002/upload"
    assert upload.auth == ("sandbox", "sandbox-secret")
    assert upload.params["catalog"] == "lakehouse"
    assert upload.params["schema"] == "crm"


@pytest.mark.usefixtures("replaced_tables")
def test_missing_configuration_is_terminal_and_names_every_surface(
    recorder: HttpRecorder, write_parquet, rows: list[dict[str, Any]]
) -> None:
    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"))

    assert "host is not configured" in str(failure.value)
    assert "destination.altertable.host" in str(failure.value)
    assert "ALTERTABLE_HOST" in str(failure.value)
    assert recorder.uploads == []
