from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import dlt
import pyarrow as pa
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_altertable import altertable
from tests.conftest import FakeServer, RecordedRequest

CONTACTS = [
    {"id": 1, "email": "ada@example.com", "lastmodifieddate": 10},
    {"id": 2, "email": "grace@example.com", "lastmodifieddate": 20},
]

DESTINATION_OPTIONS = {
    "host": "flight.test",
    "catalog": "lakehouse",
    "dataset_name": "raw",
    "username": "user",
    "password": "secret",
    "port": 15002,
    "tls": False,
}


def without_lineage(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {name: value for name, value in row.items() if not name.startswith("_dlt")}
        for row in rows
    ]


def data_uploads(server: FakeServer) -> list[RecordedRequest]:
    return [
        upload for upload in server.uploads if not upload.params["table"].startswith("_dlt")
    ]


@pytest.fixture
def run_pipeline(tmp_path: Path):
    def run(resource: Any, **destination_options: Any) -> None:
        pipeline = dlt.pipeline(
            pipeline_name="contract",
            destination=altertable(**{**DESTINATION_OPTIONS, **destination_options}),
            dataset_name="raw",
            pipelines_dir=str(tmp_path / "dlt"),
        )
        pipeline.run(resource)

    return run


@dlt.resource(name="contacts", write_disposition="merge", primary_key="id")
def merging_contacts() -> Iterator[list[dict[str, Any]]]:
    yield CONTACTS


@dlt.resource(name="deals", write_disposition="replace")
def replaced_deals() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": deal, "amount": deal * 100.0} for deal in range(1, 6)]


@dlt.resource(name="events", write_disposition="append")
def appended_events() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": 1, "kind": "page_view"}]


def test_merge_resource_becomes_a_server_side_upsert(
    server: FakeServer, run_pipeline
) -> None:
    resource = merging_contacts()
    resource.apply_hints(columns={"lastmodifieddate": {"dedup_sort": "desc"}})

    run_pipeline(resource)

    upload = server.uploads_for("contacts")[0]
    assert upload.endpoint == "upsert"
    assert upload.params["primary_key"] == "id"
    assert upload.params["cursor_field"] == "lastmodifieddate"
    assert without_lineage(upload.rows) == CONTACTS


def test_dlt_lineage_columns_are_loaded(server: FakeServer, run_pipeline) -> None:
    run_pipeline(appended_events())

    schema = server.uploads_for("events")[0].schema
    assert "_dlt_id" in schema.names
    assert "_dlt_load_id" in schema.names


def test_dlt_state_tables_are_loaded(server: FakeServer, run_pipeline) -> None:
    run_pipeline(appended_events())

    state_tables = [
        upload.params["table"]
        for upload in server.uploads
        if upload.params["table"].startswith("_dlt")
    ]
    assert "_dlt_pipeline_state" in state_tables


def test_each_file_becomes_one_post(server: FakeServer, run_pipeline) -> None:
    run_pipeline(appended_events())

    uploads = server.uploads_for("events")
    assert len(uploads) == 1
    assert uploads[0].params["mode"] == "append"


def test_replace_recreates_the_table_on_every_load(server: FakeServer, run_pipeline) -> None:
    run_pipeline(replaced_deals())
    run_pipeline(replaced_deals())

    assert [upload.params["mode"] for upload in server.uploads_for("deals")] == [
        "overwrite",
        "overwrite",
    ]


def test_replace_appends_the_remaining_files_of_one_load(
    server: FakeServer, run_pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "2")

    run_pipeline(replaced_deals())

    modes = [upload.params["mode"] for upload in server.uploads_for("deals")]
    assert len(modes) > 1, "expected the load to be split across several parquet files"
    assert modes[0] == "overwrite"
    assert set(modes[1:]) == {"append"}


def test_replace_is_reissued_when_the_first_attempt_fails(
    server: FakeServer, run_pipeline
) -> None:
    server.transient_upload_failures = 1

    run_pipeline(replaced_deals())

    assert server.attempts_for("deals") == 2, "expected dlt to retry the failed load job"
    assert [upload.params["mode"] for upload in server.uploads_for("deals")] == ["overwrite"]


def test_transient_failures_exhaust_after_five_attempts(
    server: FakeServer, run_pipeline
) -> None:
    server.transient_upload_failures = 99

    with pytest.raises(PipelineStepFailed):
        run_pipeline(appended_events())

    assert server.attempts_for("events") == 5


def test_terminal_failures_are_not_retried(server: FakeServer, run_pipeline) -> None:
    server.terminal_upload_failure = True

    with pytest.raises(PipelineStepFailed):
        run_pipeline(appended_events())

    assert server.attempts_for("events") == 1


def test_replace_resumes_as_append_after_a_failed_file(
    server: FakeServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "2")
    pipeline = dlt.pipeline(
        pipeline_name="contract_resume",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "dlt"),
    )
    server.successes_before_failures = 1
    server.transient_upload_failures = 99

    with pytest.raises(PipelineStepFailed):
        pipeline.run(replaced_deals())

    server.transient_upload_failures = 0
    pipeline.load()

    assert [upload.params["mode"] for upload in server.uploads_for("deals")] == [
        "overwrite",
        "append",
        "append",
    ], "a resumed load must not replace the table a second time"


@dlt.resource(name="by_merge_key", write_disposition="merge", merge_key="id")
def merged_on_merge_key() -> Iterator[list[dict[str, Any]]]:
    yield CONTACTS


@dlt.resource(
    name="by_scd2",
    write_disposition={"disposition": "merge", "strategy": "scd2"},
    primary_key="id",
)
def merged_with_scd2() -> Iterator[list[dict[str, Any]]]:
    yield CONTACTS


@pytest.mark.parametrize(
    ("resource", "unsupported"),
    [
        (merged_on_merge_key, "merge_key"),
        (merged_with_scd2, "merge strategy 'scd2'"),
    ],
    ids=["merge_key", "scd2"],
)
def test_unsupported_merge_configurations_fail_the_pipeline(
    server: FakeServer, run_pipeline, resource: Any, unsupported: str
) -> None:
    with pytest.raises(PipelineStepFailed) as failure:
        run_pipeline(resource())

    assert unsupported in str(failure.value)
    assert data_uploads(server) == []


@dlt.resource(name="events_nested", write_disposition="append")
def nested_events() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": 1, "payload": {"a": 1}, "tags": [1, 2]}]


def test_nested_data_stays_in_the_parent_table_as_json(
    server: FakeServer, run_pipeline
) -> None:
    run_pipeline(nested_events())

    assert [upload.params["table"] for upload in data_uploads(server)] == ["events_nested"]
    schema = server.uploads_for("events_nested")[0].schema
    assert schema.field("payload").type == pa.string()
    assert schema.field("tags").type == pa.string()


def test_multiple_resources_load_with_their_own_dispositions(
    server: FakeServer, run_pipeline
) -> None:
    run_pipeline([merging_contacts(), replaced_deals(), appended_events()])

    assert server.uploads_for("contacts")[0].endpoint == "upsert"
    assert server.uploads_for("deals")[0].params["mode"] == "overwrite"
    assert server.uploads_for("events")[0].params["mode"] == "append"
    assert without_lineage(server.uploads_for("contacts")[0].rows) == CONTACTS


@dlt.resource(name="UserEvents", write_disposition="append")
def camel_case_events() -> Iterator[list[dict[str, Any]]]:
    yield [{"CamelCase": 1, "with space": 2}]


def test_direct_naming_passes_identifiers_through_verbatim(
    server: FakeServer, run_pipeline
) -> None:
    run_pipeline(camel_case_events())

    upload = server.uploads_for("UserEvents")[0]
    assert upload.params["table"] == "UserEvents"
    assert upload.schema.names[:2] == ["CamelCase", "with space"]


@dlt.resource(name="typed_rows", write_disposition="append")
def typed_rows() -> Iterator[list[dict[str, Any]]]:
    yield [
        {
            "id": 1,
            "happened_at": datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
            "day": date(2026, 8, 17),
            "amount": Decimal("12.30"),
            "active": True,
            "payload": {"a": 1},
        }
    ]


def test_dlt_types_reach_the_wire_as_arrow_types(server: FakeServer, run_pipeline) -> None:
    run_pipeline(typed_rows())

    schema = server.uploads_for("typed_rows")[0].schema
    assert pa.types.is_timestamp(schema.field("happened_at").type)
    assert schema.field("happened_at").type.tz is not None
    assert pa.types.is_date(schema.field("day").type)
    assert pa.types.is_decimal(schema.field("amount").type)
    assert schema.field("active").type == pa.bool_()
    assert schema.field("payload").type == pa.string()


def test_explicit_arguments_beat_environment_variables(
    server: FakeServer, run_pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DESTINATION__ALTERTABLE__HOST", "flight.from-env")

    run_pipeline(appended_events())

    assert server.uploads_for("events")[0].url.startswith("http://flight.test:15002")


def test_destination_is_configurable_from_the_environment(
    server: FakeServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DESTINATION__ALTERTABLE__HOST", "flight.from-env")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__CATALOG", "env_catalog")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__DATASET_NAME", "env_schema")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__PORT", "15002")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__TLS", "false")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__USERNAME", "env_user")
    monkeypatch.setenv("DESTINATION__ALTERTABLE__PASSWORD", "env_secret")

    pipeline = dlt.pipeline(
        pipeline_name="contract_env",
        destination=altertable,
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "dlt"),
    )
    pipeline.run(appended_events())

    upload = server.uploads_for("events")[0]
    assert upload.url == "http://flight.from-env:15002/upload"
    assert upload.auth == ("env_user", "env_secret")
    assert upload.params["catalog"] == "env_catalog"
    assert upload.params["schema"] == "env_schema"
