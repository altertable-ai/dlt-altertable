from pathlib import Path
from typing import TYPE_CHECKING

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.schema import TTableSchema

from dlt_altertable.destination import _upload
from tests.conftest import make_config

if TYPE_CHECKING:
    import duckdb


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
    connection: "duckdb.DuckDBPyConnection",
    local_pipeline: dlt.Pipeline,
    arrow_types: list[str],
    values: list[int],
    stored_type: str,
) -> None:
    for arrow_type, value in zip(arrow_types, values, strict=True):
        source = pa.table({"value": pa.array([value], getattr(pa, arrow_type)())})
        local_pipeline.run(dlt.resource(source, name="numbers", write_disposition="append"))

    assert connection.execute(
        "SELECT value FROM lakehouse.raw.numbers ORDER BY value"
    ).fetchall() == [(value,) for value in sorted(values)]
    assert connection.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'numbers' AND column_name = 'value'"
    ).fetchall() == [(stored_type,)]


@pytest.mark.ducklake
def test_pipeline_preserves_nanosecond_timestamps(
    connection: "duckdb.DuckDBPyConnection",
    local_pipeline: dlt.Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATA_WRITER__VERSION", raising=False)

    source = pa.table({"value": pa.array([1_000_000_001], pa.timestamp("ns"))})
    local_pipeline.run(
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
@pytest.mark.usefixtures("local_api", "load_package_state")
def test_uint64_file_widens_a_column_after_the_schema_lookup_is_cached(
    connection: "duckdb.DuckDBPyConnection", tmp_path: Path
) -> None:
    connection.execute("CREATE SCHEMA lakehouse.raw")
    connection.execute("CREATE TABLE lakehouse.raw.numbers (value BIGINT)")
    table: TTableSchema = {
        "name": "numbers",
        "columns": {"value": {"name": "value", "data_type": "bigint"}},
    }

    for index, (arrow_type, value) in enumerate([(pa.int64(), -1), (pa.uint64(), 2**64 - 1)]):
        path = tmp_path / f"part{index}.parquet"
        pq.write_table(pa.table({"value": pa.array([value], arrow_type)}), path)
        _upload(str(path), table, config=make_config())

    assert connection.execute(
        "SELECT value FROM lakehouse.raw.numbers ORDER BY value"
    ).fetchall() == [(-1,), (2**64 - 1,)]
