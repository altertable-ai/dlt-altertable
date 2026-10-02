import pytest

from tests.conftest import DESTINATION_OPTIONS, worker_ingest_into


@pytest.mark.ducklake
@pytest.mark.parametrize(
    ("arrow_types", "values", "stored_type"),
    [
        pytest.param(
            ["uint64", "uint64"], [2**64 - 1, 0], "DECIMAL(20,0)", id="unsigned_64_bit_range"
        ),
        pytest.param(
            ["uint64", "int64", "uint64", "int64", "uint64"],
            [1, -1, 2**64 - 1, -(2**63), 0],
            "DECIMAL(20,0)",
            id="unsigned_to_signed_64_bit_range",
        ),
        pytest.param(
            ["int64", "uint64", "int64"],
            [-(2**63), 2**64 - 1, -1],
            "DECIMAL(20,0)",
            id="signed_to_unsigned_64_bit_range",
        ),
        (["int16", "int64"], [32767, 40000], "BIGINT"),
        (["uint16", "uint32", "int64"], [32767, 70000, -1], "BIGINT"),
    ],
)
def test_pipeline_preserves_integer_ranges_across_loads(
    connection, tmp_path, monkeypatch, arrow_types, values, stored_type
):
    import dlt
    import pyarrow as pa

    from dlt_altertable import altertable

    monkeypatch.setenv("LOAD__WORKERS", "1")
    monkeypatch.setenv("LOAD__RAISE_ON_MAX_RETRIES", "1")

    def execute_local(config, statement):
        return connection.execute(statement).fetchall()

    monkeypatch.setattr("dlt_altertable.table_schema.execute_sql", execute_local)
    monkeypatch.setattr("dlt_altertable.job_client.execute_sql", execute_local)
    monkeypatch.setattr("dlt_altertable.destination.post_parquet", worker_ingest_into(connection))
    pipeline = dlt.pipeline(
        pipeline_name="integer_types",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    for arrow_type, value in zip(arrow_types, values, strict=True):
        source = pa.table({"value": pa.array([value], getattr(pa, arrow_type)())})
        pipeline.run(dlt.resource(source, name="numbers", write_disposition="append"))

    assert connection.execute(
        "SELECT value FROM lakehouse.raw.numbers ORDER BY value"
    ).fetchall() == [(value,) for value in sorted(values)]
    assert connection.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'numbers' AND column_name = 'value'"
    ).fetchall() == [(stored_type,)]


@pytest.mark.ducklake
def test_pipeline_preserves_nanosecond_timestamps(connection, tmp_path, monkeypatch):
    import dlt
    import pyarrow as pa

    from dlt_altertable import altertable

    monkeypatch.setenv("LOAD__WORKERS", "1")
    monkeypatch.delenv("DATA_WRITER__VERSION", raising=False)

    def execute_local(config, statement):
        return connection.execute(statement).fetchall()

    monkeypatch.setattr("dlt_altertable.table_schema.execute_sql", execute_local)
    monkeypatch.setattr("dlt_altertable.job_client.execute_sql", execute_local)
    monkeypatch.setattr("dlt_altertable.destination.post_parquet", worker_ingest_into(connection))
    pipeline = dlt.pipeline(
        pipeline_name="timestamp_precision",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )

    source = pa.table({"value": pa.array([1_000_000_001], pa.timestamp("ns"))})
    pipeline.run(
        dlt.resource(
            source,
            name="events",
            columns={"value": {"timezone": False}},
        )
    )

    assert connection.execute("SELECT epoch_ns(value) FROM lakehouse.raw.events").fetchall() == [
        (1_000_000_001,)
    ]


@pytest.mark.ducklake
def test_uint64_file_widens_a_column_after_the_schema_lookup_is_cached(
    connection, tmp_path, monkeypatch
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from dlt_altertable.destination import _upload
    from tests.conftest import make_config

    connection.execute("CREATE SCHEMA lakehouse.raw")
    connection.execute("CREATE TABLE lakehouse.raw.numbers (value BIGINT)")
    evolved_tables = []
    monkeypatch.setattr("dlt_altertable.destination.evolved_tables", lambda: evolved_tables)
    monkeypatch.setattr("dlt_altertable.destination.replaced_tables", lambda: [])
    monkeypatch.setattr(
        "dlt_altertable.table_schema.execute_sql",
        lambda config, statement: connection.execute(statement).fetchall(),
    )
    monkeypatch.setattr("dlt_altertable.destination.post_parquet", worker_ingest_into(connection))
    table = {"name": "numbers", "columns": {"value": {"name": "value", "data_type": "bigint"}}}

    for index, (arrow_type, value) in enumerate([(pa.int64(), -1), (pa.uint64(), 2**64 - 1)]):
        path = tmp_path / f"part{index}.parquet"
        pq.write_table(pa.table({"value": pa.array([value], arrow_type)}), path)
        _upload(str(path), table, config=make_config())

    assert connection.execute(
        "SELECT value FROM lakehouse.raw.numbers ORDER BY value"
    ).fetchall() == [(-1,), (2**64 - 1,)]
