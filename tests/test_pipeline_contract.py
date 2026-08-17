from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import pytest
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_altertable import altertable
from tests.conftest import FlightRecorder

CONTACTS = [
    {"id": 1, "email": "ada@example.com", "lastmodifieddate": 10},
    {"id": 2, "email": "grace@example.com", "lastmodifieddate": 20},
]


@pytest.fixture
def run_pipeline(tmp_path: Path):
    def run(resource: Any, **destination_options: Any) -> None:
        pipeline = dlt.pipeline(
            pipeline_name="contract",
            destination=altertable(
                host="flight.test",
                catalog="lakehouse",
                dataset_name="raw",
                username="user",
                password="secret",
                port=15002,
                tls=False,
                **destination_options,
            ),
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
    recorder: FlightRecorder, run_pipeline
) -> None:
    resource = merging_contacts()
    resource.apply_hints(columns={"lastmodifieddate": {"dedup_sort": "desc"}})

    run_pipeline(resource)

    ingest = recorder.ingests_for("contacts")[0]
    assert ingest.mode is IngestTableMode.CREATE_APPEND
    assert ingest.incremental_options == IngestIncrementalOptions(
        primary_key=["id"], cursor_field=["lastmodifieddate"]
    )
    assert ingest.rows == CONTACTS


def test_dlt_delivers_a_readable_parquet_file_path(recorder: FlightRecorder, run_pipeline) -> None:
    run_pipeline(appended_events())

    assert len(recorder.parquet_paths) == 1
    delivered = Path(recorder.parquet_paths[0])
    assert delivered.is_absolute()
    assert delivered.suffix == ".parquet"
    assert recorder.ingests[0].rows == [{"id": 1, "kind": "page_view"}]


def test_dlt_bookkeeping_columns_are_not_ingested(recorder: FlightRecorder, run_pipeline) -> None:
    run_pipeline(appended_events())

    assert recorder.ingests[0].schema.names == ["id", "kind"]


def test_each_file_is_ingested_in_its_own_transaction(
    recorder: FlightRecorder, run_pipeline
) -> None:
    run_pipeline(appended_events())

    assert recorder.calls == [
        "begin_transaction",
        "ingest",
        "close_writer",
        "commit",
        "close_client",
    ]


def test_replace_recreates_the_table_on_every_load(recorder: FlightRecorder, run_pipeline) -> None:
    run_pipeline(replaced_deals())
    run_pipeline(replaced_deals())

    assert [ingest.mode for ingest in recorder.ingests_for("deals")] == [
        IngestTableMode.REPLACE,
        IngestTableMode.REPLACE,
    ]


def test_replace_appends_the_remaining_files_of_one_load(
    recorder: FlightRecorder, run_pipeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "2")

    run_pipeline(replaced_deals())

    modes = [ingest.mode for ingest in recorder.ingests_for("deals")]
    assert len(modes) > 1, "expected the load to be split across several parquet files"
    assert modes[0] is IngestTableMode.REPLACE
    assert set(modes[1:]) == {IngestTableMode.APPEND}


def test_replace_is_reissued_when_the_first_attempt_fails(
    recorder: FlightRecorder, run_pipeline
) -> None:
    recorder.transient_ingest_failures = 1

    run_pipeline(replaced_deals())

    assert recorder.calls.count("ingest") == 2, "expected dlt to retry the failed load job"
    assert [ingest.mode for ingest in recorder.ingests_for("deals")] == [IngestTableMode.REPLACE]


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
    recorder: FlightRecorder, run_pipeline, resource: Any, unsupported: str
) -> None:
    with pytest.raises(PipelineStepFailed) as failure:
        run_pipeline(resource())

    assert unsupported in str(failure.value)
    assert recorder.ingests == []


def test_destination_is_configurable_from_the_environment(
    recorder: FlightRecorder, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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

    assert recorder.connections == [
        {
            "username": "env_user",
            "password": "env_secret",
            "host": "flight.from-env",
            "port": 15002,
            "tls": False,
        }
    ]
    assert recorder.ingests[0].catalog_name == "env_catalog"
    assert recorder.ingests[0].schema_name == "env_schema"
