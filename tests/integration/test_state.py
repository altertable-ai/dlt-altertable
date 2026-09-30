from typing import Any

import dlt
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

import dlt_altertable.api
from dlt_altertable.api import execute_sql
from dlt_altertable.table_schema import qualified_table_name
from tests.conftest import FakeResponse


@pytest.mark.integration
def test_fresh_runner_restores_cursor_and_schema(mock_config, pipeline_factory):
    starts = []
    incremental = dlt.sources.incremental("id", initial_value=0)
    records = [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}]

    @dlt.resource
    def events(cursor=incremental):
        starts.append(cursor.last_value)
        yield records

    pipeline_factory("original").run(events())
    records.append({"id": 3})
    restored = pipeline_factory("fresh")

    restored.run(events())

    assert starts == [0, 2]
    assert "label" in restored.default_schema.tables["events"]["columns"]
    table = qualified_table_name(mock_config, "events")
    assert execute_sql(mock_config, f"SELECT id, label FROM {table} ORDER BY id") == [
        [1, "one"],
        [2, "two"],
        [3, None],
    ]
    versions = qualified_table_name(mock_config, "_dlt_version")
    assert execute_sql(
        mock_config, f"SELECT count(*), count(DISTINCT version_hash) FROM {versions}"
    ) == [[1, 1]]


@pytest.mark.integration
@pytest.mark.parametrize("has_checkpoint", [False, True])
def test_failed_completion_restores_only_committed_state(
    mock_config, pipeline_factory, monkeypatch, has_checkpoint
):
    starts = []
    incremental = dlt.sources.incremental("id", initial_value=0)
    records = [{"id": 1}]

    @dlt.resource
    def events(cursor=incremental):
        starts.append(cursor.last_value)
        yield records

    original = pipeline_factory("original")
    if has_checkpoint:
        original.run(events())
        records.append({"id": 2})

    post = dlt_altertable.api.session.post
    loads_table = qualified_table_name(mock_config, "_dlt_loads")

    def fail_completion(url: str, **kwargs: Any):
        statement = kwargs.get("json", {}).get("statement", "")
        if statement.startswith(f"INSERT INTO {loads_table} "):
            return FakeResponse(503, "completion unavailable")
        return post(url, **kwargs)

    with monkeypatch.context() as failure:
        failure.setattr(dlt_altertable.api.session, "post", fail_completion)
        with pytest.raises(PipelineStepFailed, match="completion unavailable"):
            original.run(events())

    state_table = qualified_table_name(mock_config, "_dlt_pipeline_state")
    assert execute_sql(mock_config, f"SELECT count(*) FROM {state_table}") == [
        [1 + int(has_checkpoint)]
    ]

    restored = pipeline_factory("before_completion")
    restored.sync_destination()
    restored.extract(events())

    assert starts[-1] == int(has_checkpoint)

    original.load()
    pipeline_factory("after_completion").run(events())

    assert starts[-1] == len(records)
    table = qualified_table_name(mock_config, "events")
    assert execute_sql(mock_config, f"SELECT count(*) FROM {table}") == [[len(records)]]


@pytest.mark.integration
def test_drop_storage_dry_run_preserves_data(mock_config, pipeline_factory):
    pipeline = pipeline_factory("original")
    pipeline.run([{"id": 1}, {"id": 2}], table_name="events")
    table = qualified_table_name(mock_config, "events")

    with pipeline.destination_client() as client:
        with pytest.warns(UserWarning, match="not executed"):
            client.drop_storage()

    assert execute_sql(mock_config, f"SELECT id FROM {table} ORDER BY id") == [[1], [2]]


@pytest.mark.integration
def test_dropped_storage_resets_the_local_cursor(mock_config, pipeline_factory):
    starts = []
    incremental = dlt.sources.incremental("id", initial_value=0)

    @dlt.resource
    def events(cursor=incremental):
        starts.append(cursor.last_value)
        yield [{"id": 1}, {"id": 2}]

    original = pipeline_factory("original")
    original.run(events())

    with original.destination_client() as client:
        client.drop_storage(dry_run=False)
    pipeline_factory("original").run(events())

    assert starts == [0, 0]
    table = qualified_table_name(mock_config, "events")
    assert execute_sql(mock_config, f"SELECT id FROM {table} ORDER BY id") == [[1], [2]]


@pytest.mark.integration
def test_pipeline_names_isolate_checkpoints(pipeline_factory):
    starts = []
    incremental = dlt.sources.incremental("id", initial_value=0)

    @dlt.resource
    def events(cursor=incremental):
        starts.append(cursor.last_value)
        yield [{"id": 1}]

    pipeline_factory("first", pipeline_name="first").run(events())
    pipeline_factory("second", pipeline_name="second").run(events())

    pipeline_factory("restored", pipeline_name="first").run(events())

    assert starts == [0, 0, 1]
