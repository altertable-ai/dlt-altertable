from pathlib import Path
from typing import Any

import pytest
from dlt.common.configuration import ConfigurationValueError
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TTableSchema

import dlt_altertable.destination
from dlt_altertable import altertable
from dlt_altertable.configuration import AltertableClientConfiguration
from tests.conftest import BASE_URL, FakeServer, make_config

sink = altertable.__wrapped__

ALTERTABLE_ENVIRONMENT = {
    "ALTERTABLE_HOST": "altertable.env",
    "ALTERTABLE_CATALOG": "lakehouse",
    "ALTERTABLE_SCHEMA": "crm",
    "ALTERTABLE_USERNAME": "env-user",
    "ALTERTABLE_PASSWORD": "env-secret",
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
    monkeypatch.setattr(dlt_altertable.destination, "evolved_tables", lambda: tables, raising=True)
    return tables


@pytest.fixture
def replaced_tables(monkeypatch: pytest.MonkeyPatch, evolved_tables: list[str]) -> list[str]:
    """Stands in for the per-load-package state, which only exists inside a pipeline run."""
    tables: list[str] = []
    monkeypatch.setattr(dlt_altertable.destination, "replaced_tables", lambda: tables, raising=True)
    return tables


@pytest.fixture
def rows() -> list[dict[str, Any]]:
    return [{"id": 1, "lastmodifieddate": 10}, {"id": 2, "lastmodifieddate": 20}]


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize(
    ("write_disposition", "expected_mode"),
    [
        ("append", "append"),
        ("replace", "overwrite"),
    ],
)
def test_write_disposition_selects_upload_mode(
    server: FakeServer,
    write_parquet,
    rows: list[dict[str, Any]],
    write_disposition: str,
    expected_mode: str,
) -> None:
    sink(write_parquet(rows), table_schema("contacts", write_disposition), config=make_config())

    upload = server.uploads[0]
    assert upload.endpoint == "upload"
    assert upload.params["mode"] == expected_mode


def test_replace_only_replaces_the_first_file_of_a_load(
    server: FakeServer,
    write_parquet,
    rows: list[dict[str, Any]],
    replaced_tables: list[str],
) -> None:
    for part in range(3):
        sink(
            write_parquet(rows, f"part{part}"),
            table_schema("deals", "replace"),
            config=make_config(),
        )

    assert [upload.params["mode"] for upload in server.uploads] == [
        "overwrite",
        "append",
        "append",
    ]
    assert replaced_tables == ["deals"]


@pytest.mark.usefixtures("replaced_tables")
def test_replace_bookkeeping_is_per_table(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows, "a"), table_schema("contacts", "replace"), config=make_config())
    sink(write_parquet(rows, "b"), table_schema("deals", "replace"), config=make_config())

    assert [upload.params["mode"] for upload in server.uploads] == ["overwrite", "overwrite"]


@pytest.mark.usefixtures("replaced_tables")
def test_merge_with_dedup_sort_becomes_the_server_cursor(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_dedup_sort(
        with_primary_key(table_schema("contacts", "merge"), "id"), "lastmodifieddate"
    )

    sink(write_parquet(rows), table, config=make_config())

    upload = server.uploads[0]
    assert upload.endpoint == "upsert"
    assert upload.params["primary_key"] == "id"
    assert upload.params["cursor_field"] == "lastmodifieddate"


@pytest.mark.usefixtures("replaced_tables")
def test_merge_without_dedup_sort_upserts_on_primary_key_alone(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id", "lastmodifieddate")

    sink(write_parquet(rows), table, config=make_config())

    upload = server.uploads[0]
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
    server: FakeServer,
    write_parquet,
    rows: list[dict[str, Any]],
    table: TTableSchema,
    unsupported: str,
) -> None:
    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, config=make_config())

    assert f"Table {table['name']}: {unsupported}" in str(failure.value)
    assert server.uploads == []
    assert server.statements == []


@pytest.mark.usefixtures("replaced_tables")
def test_merge_with_hard_delete_hint_is_terminal(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id")
    table["columns"]["deleted"] = {"name": "deleted", "data_type": "bool", "hard_delete": True}

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, config=make_config())

    assert "hard_delete" in str(failure.value)
    assert server.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_append_ignores_primary_key_and_dedup_sort_hints(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_dedup_sort(
        with_primary_key(table_schema("contacts", "append"), "id"), "lastmodifieddate"
    )

    sink(write_parquet(rows), table, config=make_config())

    upload = server.uploads[0]
    assert upload.endpoint == "upload"
    assert "primary_key" not in upload.params


@pytest.mark.usefixtures("replaced_tables")
def test_schema_evolution_adds_new_columns_before_the_upload(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    server.existing_columns = ["id"]

    sink(write_parquet(rows), table_schema("contacts", "append"), config=make_config())

    assert server.alters == [
        'ALTER TABLE "lakehouse"."raw"."contacts" '
        'ADD COLUMN IF NOT EXISTS "lastmodifieddate" BIGINT'
    ]
    assert server.uploads[0].rows == rows


@pytest.mark.usefixtures("replaced_tables")
def test_new_tables_skip_schema_evolution(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), config=make_config())

    assert len(server.schema_lookups) == 1
    assert server.alters == []


def append_table() -> TTableSchema:
    return table_schema("contacts", "append")


def merge_table() -> TTableSchema:
    return with_primary_key(table_schema("contacts", "merge"), "id")


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize("build_table", [append_table, merge_table], ids=["append", "merge"])
def test_a_missing_table_is_created_before_the_load(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]], build_table
) -> None:
    """Creating the table from dlt's typed schema keeps column types deliberate for every
    disposition, instead of leaving the table shape to inference on first contact."""
    sink(write_parquet(rows), build_table(), config=make_config())

    assert server.creates == [
        'CREATE SCHEMA IF NOT EXISTS "lakehouse"."raw"',
        'CREATE TABLE IF NOT EXISTS "lakehouse"."raw"."contacts" '
        '("id" BIGINT, "lastmodifieddate" BIGINT)',
    ]
    assert server.uploads[0].rows == rows


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize("build_table", [append_table, merge_table], ids=["append", "merge"])
def test_an_existing_table_is_not_recreated(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]], build_table
) -> None:
    server.existing_columns = ["id", "lastmodifieddate"]

    sink(write_parquet(rows), build_table(), config=make_config())

    assert server.creates == []
    assert server.alters == []


@pytest.mark.usefixtures("replaced_tables")
def test_replace_skips_the_column_lookup(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "replace"), config=make_config())

    assert server.statements == []


def test_schema_lookup_runs_once_per_existing_table_per_load(
    server: FakeServer,
    write_parquet,
    rows: list[dict[str, Any]],
    evolved_tables: list[str],
    replaced_tables: list[str],
) -> None:
    server.existing_columns = ["id", "lastmodifieddate"]

    sink(write_parquet(rows, "a"), table_schema("contacts", "append"), config=make_config())
    sink(write_parquet(rows, "b"), table_schema("contacts", "append"), config=make_config())

    assert len(server.schema_lookups) == 1
    assert evolved_tables == ["contacts"]


def test_a_created_table_is_not_cached_as_evolved(
    server: FakeServer,
    write_parquet,
    evolved_tables: list[str],
    replaced_tables: list[str],
) -> None:
    narrow_file = write_parquet([{"id": 1}], "narrow")
    sink(narrow_file, table_schema("contacts", "append"), config=make_config())

    assert any(statement.startswith("CREATE TABLE") for statement in server.statements)
    assert evolved_tables == []

    server.existing_columns = ["id"]
    wide_file = write_parquet([{"id": 2, "lastmodifieddate": 20}], "wide")
    sink(wide_file, table_schema("contacts", "append"), config=make_config())

    assert any("lastmodifieddate" in statement for statement in server.alters)
    assert evolved_tables == ["contacts"]


@pytest.mark.usefixtures("replaced_tables")
def test_narrower_files_are_padded_to_the_table_schema(server: FakeServer, write_parquet) -> None:
    narrow_file = write_parquet([{"id": 1}])

    sink(narrow_file, table_schema("contacts", "append"), config=make_config())

    upload = server.uploads[0]
    assert upload.schema.names == ["id", "lastmodifieddate"]
    assert upload.rows == [{"id": 1, "lastmodifieddate": None}]


@pytest.mark.usefixtures("replaced_tables")
def test_narrower_merge_files_are_posted_without_padding(server: FakeServer, write_parquet) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id")
    narrow_file = write_parquet([{"id": 1}])

    sink(narrow_file, table, config=make_config())

    upload = server.uploads[0]
    assert upload.schema.names == ["id"]
    assert upload.rows == [{"id": 1}]


@pytest.mark.usefixtures("replaced_tables")
def test_wei_columns_are_terminal(server: FakeServer, write_parquet) -> None:
    table = table_schema("transfers", "append")
    table["columns"]["value"] = {"name": "value", "data_type": "wei"}

    with pytest.raises(TerminalValueError) as failure:
        sink(write_parquet([{"id": 1}]), table, config=make_config())

    assert "wei" in str(failure.value)
    assert server.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_comma_in_key_columns_is_terminal(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    table = with_primary_key(table_schema("contacts", "merge"), "id")
    table["columns"]["external,id"] = {
        "name": "external,id",
        "data_type": "bigint",
        "primary_key": True,
    }

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table, config=make_config())

    assert "comma" in str(failure.value)
    assert server.uploads == []


def test_password_is_marked_as_a_secret() -> None:
    from dlt.common.configuration.specs.base_configuration import is_secret_hint

    fields = altertable().spec.get_resolvable_fields()
    assert is_secret_hint(fields["password"])


@pytest.mark.usefixtures("replaced_tables")
def test_wrong_credentials_fail_terminally(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    server.unauthenticated = True

    with pytest.raises(DestinationTerminalException) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), config=make_config())

    assert "401" in str(failure.value)
    assert server.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_server_failures_are_transient_and_name_the_load_target(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    server.transient_upload_failures = 1

    with pytest.raises(RuntimeError) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), config=make_config())

    assert "lakehouse.raw.contacts" in str(failure.value)
    assert "503" in str(failure.value)


@pytest.mark.usefixtures("replaced_tables")
def test_query_stream_errors_are_transient(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    server.query_error = "worker lease expired"

    with pytest.raises(RuntimeError) as failure:
        sink(write_parquet(rows), table_schema("contacts", "append"), config=make_config())

    assert "worker lease expired" in str(failure.value)
    assert server.uploads == []


@pytest.mark.usefixtures("replaced_tables")
def test_the_parquet_file_is_posted_verbatim(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    path = write_parquet(rows)

    sink(path, table_schema("contacts", "append"), config=make_config())

    upload = server.uploads[0]
    assert upload.url == f"{BASE_URL}/upload"
    assert upload.body == Path(path).read_bytes()
    assert upload.headers["Content-Type"] == "application/parquet"
    assert upload.params["catalog"] == "lakehouse"
    assert upload.params["schema"] == "raw"


@pytest.mark.usefixtures("replaced_tables")
def test_empty_parquet_file_is_still_uploaded(server: FakeServer, tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"id": pa.array([], type=pa.int64())}), path)

    sink(str(path), table_schema("contacts", "replace"), config=make_config())

    upload = server.uploads[0]
    assert upload.rows == []
    assert upload.params["mode"] == "overwrite"


@pytest.mark.usefixtures("replaced_tables")
def test_connection_parameters_reach_the_request(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows), table_schema("contacts", "append"), config=make_config())

    upload = server.uploads[0]
    assert upload.url.startswith(BASE_URL)
    assert upload.auth == ("user", "secret")


def test_configuration_falls_back_to_the_altertable_environment_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable, value in ALTERTABLE_ENVIRONMENT.items():
        monkeypatch.setenv(variable, value)

    config = AltertableClientConfiguration()
    config.on_resolved()

    assert config.base_url == "http://altertable.env:15002"
    assert config.basic_auth == ("env-user", "env-secret")
    assert config.catalog == "lakehouse"
    assert config.dataset_name == "crm"


def test_missing_configuration_is_terminal_and_names_every_surface() -> None:
    config = AltertableClientConfiguration()

    with pytest.raises(ConfigurationValueError) as failure:
        config.on_resolved()

    assert "host is not configured" in str(failure.value)
    assert "destination.altertable.host" in str(failure.value)
    assert "ALTERTABLE_HOST" in str(failure.value)
