import dlt
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.pipeline.exceptions import PipelineStepFailed

import dlt_altertable.api
from dlt_altertable import altertable
from tests.conftest import DESTINATION_OPTIONS, FakeResponse


@pytest.fixture
def pipeline(tmp_path):
    return dlt.pipeline(
        pipeline_name="state_restore",
        destination=altertable(**DESTINATION_OPTIONS),
        pipelines_dir=str(tmp_path),
    )


@pytest.mark.parametrize(
    ("hints", "message"),
    [
        ({"primary_key": None}, "merge without a primary_key"),
        (
            {"write_disposition": {"disposition": "merge", "strategy": "scd2"}},
            "`scd2` merge strategy not supported",
        ),
        ({"merge_key": "value"}, "merge_key"),
        (
            {"columns": {"deleted": {"hard_delete": True}, "value": {"hard_delete": True}}},
            'multiple "hard_delete"',
        ),
        ({"columns": {"value": {"dedup_sort": "asc"}}}, "dedup_sort 'asc'"),
        ({"columns": {"id": {"nullable": True}}}, "primary_key columns must be non-nullable"),
        ({"primary_key": "id,part"}, "contain a comma"),
        ({"columns": {"id,part": {"dedup_sort": "desc"}}}, "contain a comma"),
        (
            {"write_disposition": "append", "columns": {"value": {"data_type": "wei"}}},
            "has type wei",
        ),
        (
            {"write_disposition": "replace", "columns": {"value": {"data_type": "wei"}}},
            "has type wei",
        ),
        (
            {"columns": {"value": {"data_type": "decimal", "precision": 39, "scale": 0}}},
            "invalid decimal precision or scale",
        ),
    ],
    ids=[
        "missing_primary_key",
        "scd2",
        "merge_key",
        "multiple_hard_delete_hints",
        "ascending_dedup_sort",
        "nullable_primary_key",
        "comma_primary_key",
        "comma_cursor",
        "wei_append",
        "wei_replace",
        "decimal_precision",
    ],
)
def test_invalid_table_stops_the_load_before_any_table_is_written(server, pipeline, hints, message):
    options = {"write_disposition": "merge", "primary_key": "id"} | hints
    invalid = dlt.resource(
        [{"id": 1, "value": 1, "deleted": False, "id,part": 1}],
        name="invalid",
        **options,
    )
    valid = dlt.resource([{"id": 1}], name="valid")

    with pytest.raises(PipelineStepFailed, match=message):
        pipeline.run([valid, invalid])

    assert server.uploads == []
    assert all(statement.startswith("SELECT") for statement in server.statements)


@pytest.mark.parametrize(
    "options", [{}, {"dry_run": True}, {"dry_run": "false"}, {"dry_run": None}, {"dry_run": 0}]
)
def test_drop_storage_previews_sql_without_sending_requests(server, pipeline, options):
    with pipeline.destination_client() as client:
        with pytest.warns(UserWarning, match="not executed") as warnings:
            client.drop_storage(**options)

    message = str(warnings[0].message)
    assert 'DROP SCHEMA IF EXISTS "lakehouse"."raw" CASCADE' in message
    assert "dry_run=False" in message
    assert server.statements == []


def test_drop_storage_executes_only_when_explicitly_requested(server, pipeline):
    with pipeline.destination_client() as client:
        client.config.catalog = 'lake"house'
        client.config.dataset_name = "raw.schema"

        client.drop_storage(dry_run=False)

    assert server.statements == ['DROP SCHEMA IF EXISTS "lake""house"."raw.schema" CASCADE']


@pytest.mark.parametrize("delete_schema", [True, False])
def test_drop_tables_requires_explicit_opt_in_before_any_query(server, pipeline, delete_schema):
    with pipeline.destination_client() as client:
        with pytest.raises(DestinationTerminalException, match="allow_destructive_refresh=True"):
            client.drop_tables("events", delete_schema=delete_schema)

    assert server.statements == []


def test_drop_tables_accepts_the_destination_opt_in(server, tmp_path):
    pipeline = dlt.pipeline(
        pipeline_name="refresh",
        destination=altertable(**DESTINATION_OPTIONS, allow_destructive_refresh=True),
        pipelines_dir=str(tmp_path),
    )

    with pipeline.destination_client() as client:
        client.drop_tables('event"names', delete_schema=False)

    assert server.statements == ['DROP TABLE IF EXISTS "lakehouse"."raw"."event""names"']


@pytest.mark.parametrize("unauthenticated", [False, True], ids=["stream-error", "authentication"])
def test_state_lookup_errors_preserve_local_state(server, pipeline, unauthenticated):
    pipeline.run([{"id": 1}], table_name="events")
    state = pipeline.state
    server.unauthenticated = unauthenticated
    server.query_error = "query failed"
    message = "Invalid credentials" if unauthenticated else "query failed"

    with pytest.raises(PipelineStepFailed) as failure:
        pipeline.sync_destination()

    assert message in str(failure.value)
    assert pipeline.state == state


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param("", id="empty-response"),
        pytest.param("{}\n[1]", id="invalid-column-headers"),
        pytest.param('{}\n[]\n{"error": {}}', id="invalid-error-frame"),
        pytest.param('{"error": "query failed"}\n[]', id="missing-metadata"),
        pytest.param('{}\n[]\n{"error": "query failed"}\n[1]', id="rows-after-error"),
    ],
)
def test_malformed_state_response_is_not_empty_storage(server, monkeypatch, pipeline, payload):
    pipeline.run([{"id": 1}], table_name="events")
    state = pipeline.state
    monkeypatch.setattr(
        dlt_altertable.api.session, "post", lambda *args, **kwargs: FakeResponse(200, payload)
    )

    with pytest.raises(PipelineStepFailed, match="Malformed query response"):
        pipeline.sync_destination()
    assert pipeline.state == state
