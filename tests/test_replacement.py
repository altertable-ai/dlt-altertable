from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException

from dlt_altertable.table_schema import replace_rows
from tests.conftest import events_table, make_config, worker_ingest_into


@pytest.mark.ducklake
def test_replace_preserves_a_table_with_the_old_staging_name(
    connection, native_upload, write_parquet
):
    connection.execute("CREATE SCHEMA lakehouse.raw")
    connection.execute("CREATE TABLE lakehouse.raw.events__dlt_replace AS SELECT 42 AS preserved")
    table = {
        "name": "events",
        "write_disposition": "replace",
        "columns": {"value": {"name": "value", "data_type": "bigint"}},
    }

    native_upload(write_parquet([{"value": 1}]), table, config=make_config())

    assert connection.execute("SELECT * FROM lakehouse.raw.events__dlt_replace").fetchall() == [
        (42,)
    ]
    assert connection.execute("SELECT * FROM lakehouse.raw.events").fetchall() == [(1,)]


@pytest.mark.ducklake
@pytest.mark.parametrize("stored_type", ["BIGINT", "SMALLINT"])
def test_failed_swap_drops_its_staging_table(
    connection, native_upload, write_parquet, monkeypatch, stored_type
):
    connection.execute("CREATE SCHEMA lakehouse.raw")
    connection.execute(f"CREATE TABLE lakehouse.raw.events AS SELECT 1::{stored_type} AS value")

    def failing_swap(config, statement):
        if statement.startswith("MERGE"):
            raise DestinationTerminalException("injected swap failure")
        return connection.execute(statement).fetchall()

    monkeypatch.setattr("dlt_altertable.table_schema.execute_sql", failing_swap)
    table = {
        "name": "events",
        "write_disposition": "replace",
        "columns": {"value": {"name": "value", "data_type": "bigint"}},
    }

    with pytest.raises(DestinationTerminalException, match="injected swap failure"):
        native_upload(write_parquet([{"value": 2}]), table, config=make_config())

    assert connection.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall() == [("events",)]
    assert connection.execute("SELECT * FROM lakehouse.raw.events").fetchall() == [(1,)]


@pytest.mark.ducklake
@pytest.mark.parametrize(
    ("arrow_type", "stored_type", "value"),
    [
        ("float64", "FLOAT", 16777217.0),
        ("uint64", None, 2**64 - 1),
        ("uint64", "DECIMAL(20,0)", 2**64 - 1),
        ("int64", "DECIMAL(20,0)", -(2**63)),
        ("int16", None, 32767),
    ],
)
def test_replace_preserves_numeric_values(
    connection, native_upload, tmp_path, arrow_type, stored_type, value
):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns

    source = pa.table({"value": pa.array([value], type=getattr(pa, arrow_type)())})
    path = str(tmp_path / "numbers.parquet")
    pq.write_table(source, path)
    table = {
        "name": "numbers",
        "write_disposition": "replace",
        "columns": py_arrow_to_table_schema_columns(source.schema),
    }
    if stored_type:
        connection.execute("CREATE SCHEMA lakehouse.raw")
        connection.execute(f"CREATE TABLE lakehouse.raw.numbers (value {stored_type})")
        connection.execute("INSERT INTO lakehouse.raw.numbers VALUES (1)")

    native_upload(path, table, config=make_config())

    assert connection.execute("SELECT value FROM lakehouse.raw.numbers").fetchall() == [(value,)]


@pytest.mark.ducklake
@pytest.mark.parametrize(
    ("stored_type", "arrow_type", "value"),
    [
        ("UINTEGER", pa.int32(), -1),
        ("DECIMAL(10,2)", pa.decimal128(10, 3), Decimal("1.234")),
    ],
)
def test_replace_rejects_lossy_type_changes(
    connection, native_upload, tmp_path, stored_type, arrow_type, value
):
    import pyarrow.parquet as pq
    from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns

    connection.execute("CREATE SCHEMA lakehouse.raw")
    connection.execute(f"CREATE TABLE lakehouse.raw.numbers AS SELECT 42::{stored_type} AS value")
    path = str(tmp_path / "negative.parquet")
    source = pa.table({"value": pa.array([value], arrow_type)})
    pq.write_table(source, path)
    table = {
        "name": "numbers",
        "write_disposition": "replace",
        "columns": py_arrow_to_table_schema_columns(source.schema),
    }

    with pytest.raises(DestinationTerminalException, match="stored column has type"):
        native_upload(path, table, config=make_config())

    assert connection.execute("SELECT value FROM lakehouse.raw.numbers").fetchall() == [(42,)]


@pytest.mark.ducklake
def test_replace_preserves_nanosecond_timestamps(connection, native_upload, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns

    source = pa.table({"value": pa.array([1_000_000_001], pa.timestamp("ns"))})
    path = str(tmp_path / "timestamps.parquet")
    pq.write_table(source, path)
    table = {
        "name": "events",
        "write_disposition": "replace",
        "columns": py_arrow_to_table_schema_columns(source.schema),
    }

    native_upload(path, table, config=make_config())

    assert connection.execute("SELECT epoch_ns(value) FROM lakehouse.raw.events").fetchall() == [
        (1_000_000_001,)
    ]


@pytest.mark.parametrize(
    "stored_types",
    [
        {"score": "VARCHAR"},
        {"unexpected_column": "BIGINT"},
    ],
)
def test_replacement_rejects_incompatible_storage_before_swap(monkeypatch, stored_types):
    table = events_table()
    table["write_disposition"] = "replace"
    statements = []
    columns = {
        "event time": "TIMESTAMP WITH TIME ZONE",
        "category": "VARCHAR",
        "score": "BIGINT",
        **stored_types,
    }

    def execute_local(config, statement):
        statements.append(statement)
        incoming = [
            ("event time", "TIMESTAMP WITH TIME ZONE"),
            ("category", "VARCHAR"),
            ("score", "BIGINT"),
        ]
        return incoming if "staging" in statement else list(columns.items())

    monkeypatch.setattr("dlt_altertable.table_schema.execute_sql", execute_local)
    with pytest.raises(DestinationTerminalException, match="replacement"):
        replace_rows(make_config(), table, "staging")
    assert len(statements) == 2
    assert statements[0].startswith("SELECT")


@pytest.mark.ducklake
def test_native_replace_keeps_tables_from_overwrite_uploads_and_rows_on_failure(
    tmp_path: Path, monkeypatch, connection
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from dlt_altertable.destination import _upload as sink

    def narrow_file(name: str, values: list[int]) -> str:
        path = tmp_path / f"{name}.parquet"
        columns = {
            "small": pa.array(values, pa.int16()),
            "unsigned": pa.array(values, pa.uint32()),
            "ratio": pa.array([value / 2 for value in values], pa.float32()),
        }
        pq.write_table(pa.table(columns), path)
        return str(path)

    upload = worker_ingest_into(connection)
    monkeypatch.setattr(
        "dlt_altertable.table_schema.execute_sql",
        lambda config, statement: connection.execute(statement).fetchall(),
    )
    monkeypatch.setattr("dlt_altertable.destination.replaced_tables", list)
    monkeypatch.setattr("dlt_altertable.destination.evolved_tables", list)
    monkeypatch.setattr("dlt_altertable.destination.post_parquet", upload)
    connection.execute("CREATE SCHEMA lakehouse.raw")
    upload(None, "upload", {"table": "numbers", "mode": "overwrite"}, narrow_file("old", [1]), "")
    connection.execute("ALTER TABLE lakehouse.raw.numbers SET PARTITIONED BY (small)")
    table = {
        "name": "numbers",
        "write_disposition": "replace",
        "columns": {
            "small": {"name": "small", "data_type": "bigint", "precision": 16},
            "unsigned": {"name": "unsigned", "data_type": "bigint", "precision": 32},
            "ratio": {"name": "ratio", "data_type": "double"},
        },
    }

    sink(narrow_file("new", [7]), table, config=make_config())

    assert connection.execute("SELECT * FROM lakehouse.raw.numbers").fetchall() == [(7, 7, 3.5)]
    assert list((tmp_path / "data" / "raw" / "numbers").rglob("small=7/*.parquet"))
    assert connection.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'numbers' ORDER BY ordinal_position"
    ).fetchall() == [("SMALLINT",), ("UINTEGER",), ("FLOAT",)]

    def failing_upload(config, endpoint, params, path, action):
        raise DestinationTerminalException("injected upload failure")

    monkeypatch.setattr("dlt_altertable.destination.post_parquet", failing_upload)
    with pytest.raises(DestinationTerminalException, match="injected"):
        sink(narrow_file("failed", [9]), table, config=make_config())
    assert connection.execute("SELECT small FROM lakehouse.raw.numbers").fetchall() == [(7,)]


@pytest.mark.ducklake
def test_pipeline_append_then_replace_preserves_integer_boundaries(
    tmp_path, monkeypatch, connection
):
    import dlt

    from dlt_altertable import altertable
    from tests.conftest import DESTINATION_OPTIONS

    monkeypatch.setenv("LOAD__WORKERS", "1")
    for module in ("table_schema", "job_client"):
        monkeypatch.setattr(
            f"dlt_altertable.{module}.execute_sql",
            lambda config, statement: connection.execute(statement).fetchall(),
        )
    monkeypatch.setattr("dlt_altertable.destination.post_parquet", worker_ingest_into(connection))
    pipeline = dlt.pipeline(
        pipeline_name="replacement_integer_boundaries",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )

    for mode, arrow_type, value in (
        ("append", pa.uint64(), 2**64 - 1),
        ("replace", pa.int64(), -(2**63)),
        ("replace", pa.uint64(), 2**64 - 1),
    ):
        source = pa.table({"value": pa.array([value], arrow_type)})
        pipeline.run(source, table_name="numbers", write_disposition=mode)
        assert connection.execute("SELECT value FROM lakehouse.raw.numbers").fetchall() == [
            (value,)
        ]
        assert connection.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_catalog = 'lakehouse' AND table_schema = 'raw' "
            "AND table_name = 'numbers' AND column_name = 'value'"
        ).fetchone() == ("DECIMAL(20,0)",)


@pytest.mark.ducklake
@pytest.mark.parametrize("change", ["signed", "unsigned", "signedness", "decimal"])
def test_pipeline_replace_reconciles_schema_changes(tmp_path, monkeypatch, connection, change):
    import dlt
    import pyarrow as pa

    from dlt_altertable import altertable
    from tests.conftest import DESTINATION_OPTIONS

    monkeypatch.setenv("LOAD__WORKERS", "1")
    for module in ("table_schema", "job_client"):
        monkeypatch.setattr(
            f"dlt_altertable.{module}.execute_sql",
            lambda config, statement: connection.execute(statement).fetchall(),
        )
    monkeypatch.setattr("dlt_altertable.destination.post_parquet", worker_ingest_into(connection))
    pipeline = dlt.pipeline(
        pipeline_name="replacement_evolution",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    old, new = {
        "signed": (
            pa.table({"value": pa.array([1], pa.int16())}),
            pa.table({"value": pa.array([40000], pa.int32())}),
        ),
        "unsigned": (
            pa.table({"value": pa.array([1], pa.uint8())}),
            pa.table({"value": pa.array([300], pa.uint16())}),
        ),
        "signedness": (
            pa.table({"value": pa.array([1], pa.uint32())}),
            pa.table({"value": pa.array([-1], pa.int64())}),
        ),
        "decimal": (
            pa.table({"value": pa.array([Decimal("1.00")], pa.decimal128(10, 2))}),
            pa.table({"value": pa.array([Decimal("123456789.01")], pa.decimal128(12, 2))}),
        ),
    }[change]
    pipeline.run(old, table_name="events", write_disposition="replace")
    connection.execute("ALTER TABLE lakehouse.raw.events SET PARTITIONED BY (value)")
    connection.execute("ALTER TABLE lakehouse.raw.events SET SORTED BY (value DESC)")
    table_id_query = (
        "SELECT table_id FROM __ducklake_metadata_lakehouse.main.ducklake_table "
        "WHERE table_name = 'events' AND end_snapshot IS NULL"
    )
    old_table_id = connection.execute(table_id_query).fetchone()
    pipeline.run(new, table_name="events", write_disposition="replace")
    assert connection.execute(table_id_query).fetchone() == old_table_id
    assert connection.execute(
        "SELECT expression, sort_direction "
        "FROM __ducklake_metadata_lakehouse.main.ducklake_sort_expression"
    ).fetchall() == [('"value"', "DESC")]
    assert list((tmp_path / "data" / "raw" / "events").rglob("value=*/*.parquet"))
    pipeline.run(new, table_name="events", write_disposition="append")

    result = connection.execute("SELECT * FROM lakehouse.raw.events").to_arrow_table()
    assert result.column_names == new.column_names
    assert result.to_pylist() == new.to_pylist() * 2
