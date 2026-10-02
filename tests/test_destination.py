from pathlib import Path
from typing import Any
from uuid import UUID

import dlt
import pytest
from dlt.common.configuration import ConfigurationValueError
from dlt.common.destination.exceptions import (
    DestinationIncompatibleLoaderFileFormatException,
    DestinationTerminalException,
)
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TTableSchema
from dlt.load.configuration import LoaderConfiguration
from dlt.load.utils import get_available_worker_slots

import dlt_altertable.destination
from dlt_altertable import altertable
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.destination import _upload as sink
from tests.conftest import BASE_URL, DESTINATION_OPTIONS, FakeServer, make_config

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
    assert server.swapped_tables == ["deals"]


@pytest.mark.usefixtures("replaced_tables")
def test_replace_bookkeeping_is_per_table(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    sink(write_parquet(rows, "a"), table_schema("contacts", "replace"), config=make_config())
    sink(write_parquet(rows, "b"), table_schema("deals", "replace"), config=make_config())

    assert [upload.params["mode"] for upload in server.uploads] == ["overwrite", "overwrite"]
    assert server.swapped_tables == ["contacts", "deals"]


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
def test_compute_size_reaches_every_query(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    assert make_config().compute_size == "XS"

    config = make_config(compute_size="M")

    sink(write_parquet(rows), table_schema("contacts", "append"), config=config)

    assert {payload["compute_size"] for payload in server.query_payloads} == {"M"}


@pytest.mark.usefixtures("replaced_tables")
def test_schema_evolution_adds_new_columns_before_the_upload(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    server.existing_columns = {"id": "BIGINT"}

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
    server.existing_columns = {"id": "BIGINT", "lastmodifieddate": "BIGINT"}

    sink(write_parquet(rows), build_table(), config=make_config())

    assert server.creates == []
    assert server.alters == []


@pytest.mark.usefixtures("replaced_tables")
def test_replace_swaps_staged_rows_in_one_statement(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]], monkeypatch
) -> None:
    monkeypatch.setattr(dlt_altertable.destination, "uuid4", lambda: UUID(int=1))
    sink(write_parquet(rows), table_schema("contacts", "replace"), config=make_config())

    assert len(server.schema_lookups) == 2
    assert server.uploads[0].params["mode"] == "overwrite"
    assert server.statements[-2:] == [
        'MERGE INTO "lakehouse"."raw"."contacts" AS stored '
        "USING (SELECT *, NULL::BIGINT AS _dlt_never_matching_hash_key "
        'FROM "lakehouse"."raw"."contacts__dlt_replace_00000000000000000000000000000001") '
        "AS staged ON stored.rowid = staged._dlt_never_matching_hash_key "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE "
        'WHEN NOT MATCHED THEN INSERT ("id", "lastmodifieddate") '
        'VALUES (staged."id", staged."lastmodifieddate")',
        "DROP TABLE IF EXISTS "
        '"lakehouse"."raw"."contacts__dlt_replace_00000000000000000000000000000001"',
    ]


@pytest.mark.usefixtures("replaced_tables")
def test_failed_replacement_upload_keeps_the_stored_rows(
    server: FakeServer, write_parquet, rows: list[dict[str, Any]]
) -> None:
    server.terminal_upload_failure = True

    with pytest.raises(DestinationTerminalException):
        sink(write_parquet(rows), table_schema("contacts", "replace"), config=make_config())

    assert server.swapped_tables == []
    assert not [statement for statement in server.statements if statement.startswith("DELETE")]


@pytest.mark.usefixtures("replaced_tables")
@pytest.mark.parametrize("stored_columns", [["lastmodifieddate", "id"], ["lastmodifieddate"]])
def test_replace_rejects_incompatible_column_order_before_swap(
    server: FakeServer, write_parquet, rows, stored_columns
) -> None:
    server.existing_columns = dict.fromkeys(stored_columns, "BIGINT")
    table = table_schema("contacts", "replace")

    with pytest.raises(DestinationTerminalException, match="column order"):
        sink(write_parquet(rows), table, config=make_config())

    assert server.swapped_tables == []
    assert server.alters == []
    assert list(table["columns"]) == ["id", "lastmodifieddate"]


@pytest.mark.usefixtures("replaced_tables")
def test_replace_adds_trailing_columns_before_swapping_rows(
    server: FakeServer, write_parquet, rows
):
    server.existing_columns = {"id": "BIGINT"}

    sink(write_parquet(rows), table_schema("contacts", "replace"), config=make_config())

    assert server.statements[-3] == (
        'ALTER TABLE "lakehouse"."raw"."contacts" ADD COLUMN "lastmodifieddate" BIGINT'
    )
    assert server.swapped_tables == ["contacts"]
    assert server.uploads[0].rows == rows


def test_schema_lookup_runs_once_per_existing_table_per_load(
    server: FakeServer,
    write_parquet,
    rows: list[dict[str, Any]],
    evolved_tables: list[str],
    replaced_tables: list[str],
) -> None:
    server.existing_columns = {"id": "BIGINT", "lastmodifieddate": "BIGINT"}

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

    server.existing_columns = {"id": "BIGINT"}
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
@pytest.mark.parametrize(
    ("table", "operation"),
    [
        (table_schema("contacts", "append"), "append"),
        (table_schema("contacts", "replace"), "overwrite"),
        (with_primary_key(table_schema("contacts", "merge"), "id"), "upsert"),
    ],
    ids=["append", "replace", "upsert"],
)
def test_server_failure_names_the_ingest_operation_and_target(
    server: FakeServer,
    write_parquet,
    rows: list[dict[str, Any]],
    table: TTableSchema,
    operation: str,
) -> None:
    server.transient_upload_failures = 1

    with pytest.raises(RuntimeError) as failure:
        sink(write_parquet(rows), table, config=make_config())

    assert f"{operation} lakehouse.raw.contacts" in str(failure.value)
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


@pytest.mark.parametrize("max_jobs", [None, 4])
def test_load_concurrency_defaults_to_one_and_can_be_overridden(max_jobs: int | None) -> None:
    options = {} if max_jobs is None else {"max_parallel_load_jobs": max_jobs}
    capabilities = altertable(**DESTINATION_OPTIONS, **options).capabilities()

    assert get_available_worker_slots(LoaderConfiguration(workers=20), capabilities, []) == (
        max_jobs or 1
    )


def test_file_size_default_can_be_overridden() -> None:
    assert altertable(**DESTINATION_OPTIONS).capabilities().recommended_file_size == 128 * 1024**2
    assert (
        altertable(**DESTINATION_OPTIONS, recommended_file_size=1024)
        .capabilities()
        .recommended_file_size
        == 1024
    )


def test_named_destination_configuration_keeps_precedence(
    server, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("DESTINATION__WAREHOUSE__HOST", "named.test")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__HOST", "wrong.test")
    options = {key: value for key, value in DESTINATION_OPTIONS.items() if key != "host"}
    pipeline = dlt.pipeline(
        pipeline_name="named",
        destination=altertable(destination_name="warehouse", **options),
        pipelines_dir=str(tmp_path),
    )

    pipeline.run([{"id": 1}], table_name="events")

    assert server.uploads_for("events")[0].url.startswith("http://named.test:")


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("batch_size", 1),
        ("loader_file_format", "typed-jsonl"),
        ("skip_dlt_columns_and_tables", True),
        ("destination_callable", lambda *args: None),
        ("spec", None),
        ("loader_parallelism_strategy", "parallel"),
        ("preferred_loader_file_format", "typed-jsonl"),
        ("supported_loader_file_formats", ["typed-jsonl", "parquet"]),
        ("loader_file_format_selector", lambda *args, **kwargs: ("typed-jsonl", ["typed-jsonl"])),
    ],
)
def test_unsafe_destination_overrides_fail_before_loading(option: str, value: Any) -> None:
    with pytest.raises(ConfigurationValueError, match=option):
        altertable(**DESTINATION_OPTIONS, **{option: value})


@pytest.mark.parametrize("override_formats", [False, True], ids=["default", "mutated_override"])
def test_jsonl_is_rejected_before_extraction(tmp_path: Path, override_formats: bool) -> None:
    formats = ["parquet"]
    options = {"supported_loader_file_formats": formats} if override_formats else {}
    destination = altertable(**DESTINATION_OPTIONS, **options)
    formats.append("typed-jsonl")
    pipeline = dlt.pipeline(
        pipeline_name="unsupported_format",
        destination=destination,
        pipelines_dir=str(tmp_path),
    )

    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        pipeline.extract([{"id": 1}], table_name="events", loader_file_format="typed-jsonl")

    assert pipeline.list_extracted_load_packages() == []


def test_environment_cannot_disable_state_or_parquet_uploads(
    server: FakeServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, value in {
        "BATCH_SIZE": "1",
        "LOADER_FILE_FORMAT": "typed-jsonl",
        "SKIP_DLT_COLUMNS_AND_TABLES": "true",
        "DESTINATION_CALLABLE": "missing_module.upload",
    }.items():
        monkeypatch.setenv(f"DESTINATION__ALTERTABLE__{name}", value)
    pipeline = dlt.pipeline(
        pipeline_name="fixed_upload_settings",
        destination=altertable(batch_size=0, **DESTINATION_OPTIONS),
        pipelines_dir=str(tmp_path),
    )

    pipeline.run([{"id": 1}], table_name="events")

    upload = server.uploads_for("events")[0]
    assert upload.rows[0]["id"] == 1
    assert "_dlt_load_id" in upload.schema.names
    assert len(server.uploads_for("_dlt_pipeline_state")) == 1
