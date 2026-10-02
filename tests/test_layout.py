import json
from datetime import UTC, datetime

import dlt
import pytest
from dlt.common.exceptions import TerminalValueError

from dlt_altertable import altertable_partition
from dlt_altertable.table_schema import create_or_evolve_table
from tests.conftest import (
    DESTINATION_OPTIONS,
    FakeServer,
    make_config,
)


def events_table():
    return {
        "name": "events",
        "columns": {
            "event time": {"name": "event time", "data_type": "timestamp"},
            "category": {"name": "category", "data_type": "text"},
            "score": {"name": "score", "data_type": "bigint"},
        },
    }


@pytest.mark.altertable
def test_unchanged_partition_hints_do_not_create_snapshots(connection, monkeypatch):
    monkeypatch.setattr(
        "dlt_altertable.table_schema.execute_sql",
        lambda config, statement: connection.execute(statement).fetchall(),
    )
    table = events_table()
    table["columns"]["category"]["partition"] = True
    config = make_config()
    create_or_evolve_table(config, table)
    snapshot_count = connection.execute("SELECT count(*) FROM lakehouse.snapshots()").fetchone()

    for _ in range(3):
        create_or_evolve_table(config, table)

    assert (
        connection.execute("SELECT count(*) FROM lakehouse.snapshots()").fetchone()
        == snapshot_count
    )
    table["x-altertable-partition"] = []
    create_or_evolve_table(config, table)
    cleared_count = connection.execute("SELECT count(*) FROM lakehouse.snapshots()").fetchone()
    create_or_evolve_table(config, table)
    assert (
        connection.execute("SELECT count(*) FROM lakehouse.snapshots()").fetchone() == cleared_count
    )


@pytest.mark.parametrize("sort", ["score", [], {"column": "score", "direction": "desc"}])
def test_adapter_rejects_sort_without_modifying_resource(sort):
    from dlt_altertable import altertable_adapter

    resource = dlt.resource([], name="events")
    original = resource.compute_table_schema()

    with pytest.raises(TerminalValueError, match="Sort hints are not supported yet"):
        altertable_adapter(resource, partition="category", sort=sort)

    assert resource.compute_table_schema() == original


def test_standard_hints_apply_before_upload_and_change_on_existing_tables(server: FakeServer):
    table = events_table()
    table["columns"]["category"]["partition"] = True
    create_or_evolve_table(make_config(), table)
    assert server.alters == [
        'ALTER TABLE "lakehouse"."raw"."events" SET PARTITIONED BY ("category")',
    ]
    server.existing_columns = list(table["columns"])
    table["columns"]["category"]["partition"] = False
    create_or_evolve_table(make_config(), table)
    assert server.alters[-1:] == [
        'ALTER TABLE "lakehouse"."raw"."events" RESET PARTITIONED BY',
    ]


def test_adapter_preserves_order_and_serializable_hints():
    from dlt_altertable import altertable_adapter
    from dlt_altertable.layout import partition_expressions

    resource = altertable_adapter(
        dlt.resource([], name="events", columns=events_table()["columns"]),
        partition=[
            altertable_partition.year("event time"),
            altertable_partition.month("event time"),
            altertable_partition.bucket(8, "category"),
        ],
    )
    table = json.loads(json.dumps(resource.compute_table_schema()))
    assert partition_expressions(table) == [
        'year("event time")',
        'month("event time")',
        'bucket(8, "category")',
    ]


@pytest.mark.parametrize(
    "partition",
    [
        altertable_partition.truncate(4, "category"),
        altertable_partition.bucket(0, "category"),
        altertable_partition.year("event time", partition_field_name="year"),
        {"column": "category"},
    ],
)
def test_adapter_rejects_unsupported_partition_specs_as_value_errors(partition):
    from dlt_altertable import altertable_adapter

    with pytest.raises(ValueError):
        altertable_adapter([], partition=partition)


@pytest.mark.parametrize(
    ("hint", "keys"),
    [
        ("x-altertable-partition", ["missing"]),
        ("x-altertable-partition", [""]),
        ("x-altertable-partition", ["category\x00"]),
        ("x-altertable-partition", [{"column": "category", "transform": "year"}]),
        ("x-altertable-partition", [{"column": "score", "transform": "hour"}]),
        ("x-altertable-partition", [{"column": "score", "transform": "evil();"}]),
        ("x-altertable-partition", [{"column": "score", "transform": "bucket", "buckets": 0}]),
        ("x-altertable-partition", [{"column": "score", "transform": "bucket", "buckets": True}]),
        ("x-altertable-partition", [{"column": "score", "buckets": 4}]),
        ("x-altertable-partition", ["score", "score"]),
        ("x-altertable-sort", [{"column": "score", "direction": "desc; DROP TABLE events"}]),
        ("x-altertable-sort", [{"column": "score", "nulls": "first"}]),
        ("x-altertable-sort", [42]),
        ("x-altertable-sort", "score"),
    ],
)
def test_invalid_layout_fails_before_any_query(server: FakeServer, hint, keys):
    table = events_table()
    table[hint] = keys
    with pytest.raises(TerminalValueError):
        create_or_evolve_table(make_config(), table)
    assert server.statements == []


def test_unhinted_layout_is_preserved_and_explicit_empty_adapter_resets():
    from dlt_altertable import altertable_adapter
    from dlt_altertable.layout import partition_expressions

    assert partition_expressions(events_table()) is None
    resource = altertable_adapter([], partition=[])
    assert partition_expressions(resource.compute_table_schema()) == []


def test_identifiers_are_quoted_as_identifiers():
    from dlt_altertable.layout import partition_expressions

    table = events_table()
    name = 'score"); DELETE FROM events; --'
    table["columns"][name] = {"name": name, "data_type": "bigint", "partition": True}
    assert partition_expressions(table) == ['"score""); DELETE FROM events; --"']


@pytest.mark.altertable
@pytest.mark.parametrize(
    ("naming_convention", "column_name"),
    [("direct", 'event"time'), ("snake_case", "eventTime")],
)
def test_native_layout_uses_normalized_adapter_references(
    tmp_path, monkeypatch, naming_convention, column_name, connection
):
    from dlt_altertable import altertable, altertable_adapter
    from dlt_altertable.table_schema import qualified_table_name

    monkeypatch.setenv("LOAD__WORKERS", "1")
    monkeypatch.setenv("SCHEMA__NAMING", naming_convention)

    def execute_local(config, statement):
        return connection.execute(statement).fetchall()

    monkeypatch.setattr("dlt_altertable.table_schema.execute_sql", execute_local)
    monkeypatch.setattr("dlt_altertable.job_client.execute_sql", execute_local)

    def upload_local(config, endpoint, params, path, action):
        connection.execute(
            f"INSERT INTO {qualified_table_name(config, params['table'])} "
            "SELECT * FROM read_parquet(?)",
            [path],
        )

    monkeypatch.setattr("dlt_altertable.destination.post_parquet", upload_local)
    pipeline = dlt.pipeline(
        pipeline_name="normalized_layout",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    resource = altertable_adapter(
        dlt.resource(
            [{column_name: datetime(2026, 1, day, tzinfo=UTC)} for day in (1, 3)], name="events"
        ),
        partition=altertable_partition.year(column_name),
    )

    pipeline.run(resource)

    partition_file = next((tmp_path / "data").rglob("*year=2026/*.parquet"))
    assert connection.execute(
        'SELECT day("event_time") FROM read_parquet(?) ORDER BY 1', [str(partition_file)]
    ).fetchall() == [(1,), (3,)]
