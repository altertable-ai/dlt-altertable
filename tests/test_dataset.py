from pathlib import Path

import dlt
import pytest
from dlt.common.configuration import ConfigurationValueError
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_altertable import altertable, verify_load
from dlt_altertable.verify import load_context
from tests.conftest import DESTINATION_OPTIONS, FakeServer


@dlt.source
def first():
    yield dlt.resource([{"id": 1}], name="events")


@dlt.source
def second():
    yield dlt.resource([{"id": 2}], name="events")


@pytest.mark.parametrize("pipeline_dataset, expected", [("selected", "selected"), (None, "raw")])
def test_the_pipeline_dataset_overrides_altertable_schema(
    server: FakeServer, tmp_path: Path, monkeypatch, pipeline_dataset, expected
) -> None:
    monkeypatch.setenv("ALTERTABLE_SCHEMA", "raw")
    pipeline = dlt.pipeline(
        pipeline_name="dataset_selection",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name=pipeline_dataset,
        pipelines_dir=str(tmp_path),
    )

    pipeline.run([{"id": 1}], table_name="events")

    assert {upload.params["schema"] for upload in server.uploads} == {expected}
    assert pipeline.sql_client().dataset_name == expected
    assert pipeline.dataset().sql_client.dataset_name == expected
    assert load_context(pipeline)[2].dataset_name == expected


def test_the_destination_does_not_take_a_dataset() -> None:
    with pytest.raises(ConfigurationValueError, match=r"dlt\.pipeline"):
        altertable(**DESTINATION_OPTIONS, dataset_name="raw")


def test_a_missing_dataset_fails_before_any_request(
    server: FakeServer, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("ALTERTABLE_SCHEMA")
    pipeline = dlt.pipeline(
        pipeline_name="missing_dataset",
        destination=altertable(**DESTINATION_OPTIONS),
        pipelines_dir=str(tmp_path),
    )

    with pytest.raises(PipelineStepFailed, match=r"dataset_name is not configured.*dlt\.pipeline"):
        pipeline.run([{"id": 1}], table_name="events")

    assert server.uploads == []
    assert server.statements == []


def test_each_source_schema_derives_its_dataset_from_the_pipeline_binding(
    server: FakeServer, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("USE_SINGLE_DATASET", "false")
    destination = altertable(**DESTINATION_OPTIONS, naming_convention="snake_case")
    pipeline = dlt.pipeline(
        pipeline_name="multiple_schemas",
        destination=destination,
        dataset_name="Selected Data_",
        pipelines_dir=str(tmp_path),
    )

    pipeline.run([first(), second()])

    assert {upload.params["schema"] for upload in server.uploads_for("events")} == {
        "selected_datax",
        "selected_data_second",
    }
    assert pipeline.dataset().sql_client.dataset_name == "selected_datax"
    assert pipeline.destination_client("second").sql_client.dataset_name == "selected_data_second"
    with pytest.raises(ValueError, match="multiple datasets"):
        verify_load(pipeline)

    pipeline.run(second())

    assert load_context(pipeline)[2].dataset_name == "selected_data_second"
