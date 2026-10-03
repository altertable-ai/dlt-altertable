from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)
from dlt.common.exceptions import TerminalValueError
from dlt.pipeline.exceptions import PipelineStepFailed

import dlt_altertable.api
from dlt_altertable import altertable, altertable_adapter, verify_load
from dlt_altertable.merge import stage_file
from tests.conftest import DESTINATION_OPTIONS, make_config


def test_staging_requires_a_file_path() -> None:
    with pytest.raises(TerminalValueError, match="Parquet file path"):
        stage_file(make_config(), [{"id": 1}], {"name": "deals", "columns": {}})


def deals(rows: list[dict[str, Any]], **hints: Any):
    return dlt.resource(
        rows,
        name="deals",
        write_disposition="merge",
        primary_key="id",
        max_table_nesting=3,
        **hints,
    )


@pytest.mark.ducklake
def test_nested_merge_moves_between_partitions_and_deletes_children(
    local_pipeline, connection, tmp_path
):
    def load(rows):
        return local_pipeline.run(
            altertable_adapter(
                deals(rows, columns={"deleted": {"hard_delete": True}}), partition="region"
            )
        )

    load(
        [
            {"id": 1, "region": "west", "deleted": False, "items": ["old"]},
            {"id": 2, "region": "north", "deleted": False, "items": ["keep"]},
        ]
    )
    load([{"id": 1, "region": "east", "deleted": False, "items": ["new"]}])

    assert connection.execute(
        "SELECT id, region FROM lakehouse.raw.deals ORDER BY id"
    ).fetchall() == [
        (1, "east"),
        (2, "north"),
    ]
    assert list((tmp_path / "data").rglob("region=east/*.parquet"))
    assert connection.execute(
        "SELECT value FROM lakehouse.raw.deals__items ORDER BY value"
    ).fetchall() == [("keep",), ("new",)]

    load([{"id": 1, "deleted": True}])

    assert connection.execute("SELECT id FROM lakehouse.raw.deals").fetchall() == [(2,)]
    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("keep",)
    ]


@pytest.mark.ducklake
def test_nested_merge_upserts_flattened_properties_and_scalar_items(local_pipeline, connection):
    def resource(rows):
        return dlt.resource(
            rows,
            name="mrg_deals",
            primary_key="id",
            write_disposition="merge",
            max_table_nesting=1,
        )

    rows = [{"id": 1, "properties": {"dealname": "d1"}, "items": ["A", "B"]}]
    local_pipeline.run(resource(rows))

    assert connection.execute(
        "SELECT id, properties__dealname FROM lakehouse.raw.mrg_deals"
    ).fetchall() == [(1, "d1")]
    children_query = (
        "SELECT p.id, c.value FROM lakehouse.raw.mrg_deals p "
        "JOIN lakehouse.raw.mrg_deals__items c ON c._dlt_parent_id = p._dlt_id "
        "ORDER BY c._dlt_list_idx"
    )
    assert connection.execute(children_query).fetchall() == [(1, "A"), (1, "B")]

    local_pipeline.run(resource([{"id": 1, "properties": {"dealname": "d2"}, "items": ["C"]}]))

    assert connection.execute(
        "SELECT id, properties__dealname FROM lakehouse.raw.mrg_deals"
    ).fetchall() == [(1, "d2")]
    assert connection.execute(children_query).fetchall() == [(1, "C")]

    local_pipeline.run(resource([{"id": 1, "properties": {"dealname": "d2"}, "items": []}]))

    assert connection.execute("SELECT count(*) FROM lakehouse.raw.mrg_deals").fetchone() == (1,)
    assert connection.execute("SELECT count(*) FROM lakehouse.raw.mrg_deals__items").fetchone() == (
        0,
    )


@pytest.mark.ducklake
def test_nested_merge_removes_missing_children_and_preserves_other_parents(
    local_pipeline, connection
):
    local_pipeline.run(
        deals(
            [
                {
                    "id": 1,
                    "properties": {"dealname": "Initial"},
                    "items": [{"sku": "A", "tags": ["x", "y"]}, {"sku": "B", "tags": ["z"]}],
                },
                {
                    "id": 2,
                    "properties": {"dealname": "Other"},
                    "items": [{"sku": "C", "tags": ["keep"]}],
                },
            ]
        )
    )

    local_pipeline.run(
        deals(
            [
                {
                    "id": 1,
                    "properties": {"dealname": "Updated"},
                    "items": [{"sku": "D", "tags": ["new"]}],
                }
            ]
        )
    )

    assert connection.execute(
        "SELECT id, properties__dealname FROM lakehouse.raw.deals ORDER BY id"
    ).fetchall() == [(1, "Updated"), (2, "Other")]
    assert connection.execute(
        "SELECT sku FROM lakehouse.raw.deals__items ORDER BY sku"
    ).fetchall() == [("C",), ("D",)]
    assert connection.execute(
        "SELECT value FROM lakehouse.raw.deals__items__tags ORDER BY value"
    ).fetchall() == [("keep",), ("new",)]
    assert connection.execute(
        "SELECT count(*) FROM lakehouse.raw.deals p JOIN lakehouse.raw.deals__items c "
        "ON c._dlt_parent_id = p._dlt_id"
    ).fetchone() == (2,)

    local_pipeline.run(deals([{"id": 1, "items": []}]))

    assert connection.execute(
        "SELECT properties__dealname FROM lakehouse.raw.deals WHERE id = 1"
    ).fetchone() == (None,)
    assert connection.execute("SELECT sku FROM lakehouse.raw.deals__items").fetchall() == [("C",)]
    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items__tags").fetchall() == [
        ("keep",)
    ]


@pytest.mark.ducklake
@pytest.mark.parametrize(("live", "deleted"), [(False, True), (None, "2026-10-02")])
def test_hard_delete_removes_parent_and_children_without_inserting_unknown_tombstones(
    local_pipeline, connection, live, deleted
):
    columns = {"deleted": {"hard_delete": True}}
    local_pipeline.run(
        deals(
            [
                {"id": 1, "deleted": live, "items": ["A"]},
                {"id": 2, "deleted": live, "items": ["B"]},
            ],
            columns=columns,
        )
    )

    local_pipeline.run(
        deals([{"id": 1, "deleted": deleted}, {"id": 99, "deleted": deleted}], columns=columns)
    )

    assert connection.execute("SELECT id FROM lakehouse.raw.deals").fetchall() == [(2,)]
    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [("B",)]


@pytest.mark.ducklake
def test_hard_delete_uses_the_latest_cursor_across_files_and_existing_rows(
    local_pipeline, connection, monkeypatch
):
    columns = {"deleted": {"hard_delete": True}, "version": {"dedup_sort": "desc"}}
    monkeypatch.setenv("DATA_WRITER__FILE_MAX_ITEMS", "1")
    local_pipeline.run(deals([{"id": 1, "version": 10, "deleted": False}], columns=columns))

    local_pipeline.run(deals([{"id": 1, "version": 9, "deleted": True}], columns=columns))

    assert connection.execute("SELECT version FROM lakehouse.raw.deals").fetchall() == [(10,)]

    local_pipeline.run(
        deals(
            [{"id": 1, "version": 12, "deleted": True}, {"id": 1, "version": 11, "deleted": False}],
            columns=columns,
        )
    )

    assert connection.execute("SELECT count(*) FROM lakehouse.raw.deals").fetchone() == (0,)


@pytest.mark.ducklake
def test_nested_merge_does_not_apply_children_of_a_stale_parent(local_pipeline, connection):
    columns = {"version": {"dedup_sort": "desc"}}
    local_pipeline.run(deals([{"id": 1, "version": 10, "items": ["new"]}], columns=columns))

    local_pipeline.run(deals([{"id": 1, "version": 9, "items": ["old", "extra"]}], columns=columns))

    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("new",)
    ]


@pytest.mark.ducklake
@pytest.mark.parametrize("after_commit", [False, True])
def test_staged_merge_resumes_after_a_failed_commit_request(
    local_pipeline, connection, monkeypatch, after_commit
):
    local_pipeline.run(deals([{"id": 1, "items": ["old"]}]))
    execute = dlt_altertable.api.execute_sql

    def fail_merge(config, statement):
        if statement.startswith("BEGIN"):
            if after_commit:
                execute(config, statement)
            else:
                statement = statement.replace("COMMIT;", "SELECT error('injected'); COMMIT;")
                execute(config, statement)
            raise RuntimeError("lost commit response")
        return execute(config, statement)

    monkeypatch.setattr(dlt_altertable.api, "execute_sql", fail_merge)

    with pytest.raises(PipelineStepFailed):
        local_pipeline.run(deals([{"id": 1, "items": ["new"]}]))

    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("new" if after_commit else "old",)
    ]

    monkeypatch.setattr(dlt_altertable.api, "execute_sql", execute)
    local_pipeline.load()

    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("new",)
    ]
    assert connection.execute("SELECT count(*) FROM lakehouse.raw._dlt_loads").fetchone() == (2,)
    assert (
        connection.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE '%_dlt_staging_%'"
        ).fetchall()
        == []
    )


@pytest.mark.ducklake
def test_fresh_pipeline_resumes_after_a_lost_upload_response(
    local_pipeline, connection, monkeypatch, tmp_path: Path
):
    local_pipeline.run(deals([{"id": 1, "items": ["old"]}]))
    upload = dlt_altertable.api.post_parquet
    child_uploads: list[str] = []

    def lose_upload_response(config, endpoint, params, path, action):
        upload(config, endpoint, params, path, action)
        if action == "stage deals__items":
            child_uploads.append(params["table"])
            if len(child_uploads) == 1:
                raise DestinationTransientException("lost upload response")

    monkeypatch.setattr(dlt_altertable.api, "post_parquet", lose_upload_response)

    with pytest.raises(PipelineStepFailed):
        local_pipeline.run(deals([{"id": 1, "items": ["new", "second"]}]))

    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("old",)
    ]

    resumed_pipeline = dlt.pipeline(
        pipeline_name=local_pipeline.pipeline_name,
        destination=altertable(**DESTINATION_OPTIONS),
        pipelines_dir=str(tmp_path),
    )
    resumed_pipeline.load()

    assert len(child_uploads) == 2
    assert child_uploads[0] == child_uploads[1]
    assert connection.execute("SELECT id FROM lakehouse.raw.deals").fetchall() == [(1,)]
    assert connection.execute(
        "SELECT value FROM lakehouse.raw.deals__items ORDER BY value"
    ).fetchall() == [("new",), ("second",)]
    assert connection.execute("SELECT count(*) FROM lakehouse.raw._dlt_loads").fetchone() == (2,)
    assert (
        connection.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE '%_dlt_staging_%'"
        ).fetchall()
        == []
    )


@pytest.mark.ducklake
def test_failed_staging_upload_does_not_publish_a_partial_parent_chain(
    local_pipeline, connection, monkeypatch
):
    local_pipeline.run(deals([{"id": 1, "items": ["old"]}]))
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    upload = dlt_altertable.api.post_parquet

    def fail_children(config, endpoint, params, path, action):
        if action == "stage deals__items":
            raise DestinationTerminalException("injected child upload failure")
        return upload(config, endpoint, params, path, action)

    monkeypatch.setattr(dlt_altertable.api, "post_parquet", fail_children)

    with pytest.raises(PipelineStepFailed, match="incomplete staging"):
        local_pipeline.run(deals([{"id": 1, "items": ["new"]}]))

    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("old",)
    ]
    assert connection.execute("SELECT count(*) FROM lakehouse.raw._dlt_loads").fetchone() == (1,)


@pytest.mark.ducklake
def test_nested_merge_rejects_ambiguous_duplicate_parent_snapshots(local_pipeline, connection):
    local_pipeline.run(deals([{"id": 1, "items": ["old"]}]))

    with pytest.raises(PipelineStepFailed, match="one row per primary key"):
        local_pipeline.run(deals([{"id": 1, "items": ["first"]}, {"id": 1, "items": ["second"]}]))

    assert connection.execute("SELECT value FROM lakehouse.raw.deals__items").fetchall() == [
        ("old",)
    ]


@pytest.mark.ducklake
def test_verify_load_handles_nested_rows_and_a_delete_only_load(local_pipeline):
    columns = {"deleted": {"hard_delete": True}}
    local_pipeline.run(
        deals([{"id": 1, "deleted": False, "items": [{"tags": ["A"]}]}], columns=columns)
    )

    assert verify_load(local_pipeline) == []

    local_pipeline.run(deals([{"id": 1, "deleted": True}], columns=columns))

    assert verify_load(local_pipeline) == []
