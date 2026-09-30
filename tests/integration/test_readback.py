from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException

pytestmark = pytest.mark.integration


def test_dataset_reads_from_the_configured_schema(mock_config, pipeline_factory) -> None:
    pipeline = pipeline_factory("original")
    pipeline.run([{"id": i, "label": str(i)} for i in range(1, 6)], table_name="events")
    dataset = pipeline.dataset()
    assert pipeline.dataset_name != mock_config.dataset_name
    assert dataset.events.select("id").order_by("id").fetchall() == [(1,), (2,), (3,), (4,), (5,)]
    assert dataset("SELECT id FROM events ORDER BY id", _execute_raw_query=True).fetchall() == [
        (1,),
        (2,),
        (3,),
        (4,),
        (5,),
    ]
    selected = dataset.events.select("id").where("id", "gte", 3).order_by("id").limit(2)
    assert selected.arrow().to_pylist() == [{"id": 3}, {"id": 4}]
    assert selected.df()["id"].tolist() == [3, 4]
    chunks = list(dataset.events.select("id").order_by("id").iter_arrow(chunk_size=2))
    assert [chunk.num_rows for chunk in chunks] == [2, 2, 1]
    assert pa.concat_tables(chunks).column("id").to_pylist() == [1, 2, 3, 4, 5]
    with pipeline.sql_client() as sql_client:
        assert sql_client.execute_sql("SELECT id FROM events WHERE id = 1") == [[1]]


def test_dataset_preserves_result_types(pipeline_factory) -> None:
    pipeline = pipeline_factory("original")
    pipeline.run([{"id": 1}], table_name="events")
    dataset = pipeline.dataset()
    query = "SELECT NULL::BIGINT AS id, NULL::DECIMAL(12,3) AS amount, NULL::TIMESTAMPTZ AS instant"
    for suffix, expected_rows in [("", 1), (" WHERE FALSE", 0)]:
        result = dataset(query + suffix, _execute_raw_query=True).arrow()
        assert result.num_rows == expected_rows
        assert result.schema.names == ["id", "amount", "instant"]
        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("amount").type == pa.decimal128(12, 3)
        timestamp_type = result.schema.field("instant").type
        assert pa.types.is_timestamp(timestamp_type)
        assert timestamp_type.unit == "us"
        assert timestamp_type.tz in {"UTC", "Etc/UTC"}
    result = dataset(
        "SELECT 12.345::DECIMAL(12,3) AS amount, TIMESTAMPTZ '2026-09-30 12:00:00+00' AS instant",
        _execute_raw_query=True,
    ).fetchone()
    assert result == (Decimal("12.345"), datetime(2026, 9, 30, 12, tzinfo=UTC))
    empty_uuid = dataset(
        "SELECT NULL::UUID AS identifier WHERE FALSE", _execute_raw_query=True
    ).arrow()
    assert empty_uuid.num_rows == 0
    assert empty_uuid.schema == pa.schema([("identifier", pa.string())])


def test_query_failures_are_not_empty_results(pipeline_factory) -> None:
    pipeline = pipeline_factory("valid")
    pipeline.run([{"id": 1}], table_name="events")
    with pytest.raises((RuntimeError, DestinationTerminalException), match="missing_table"):
        pipeline.dataset()("SELECT * FROM missing_table", _execute_raw_query=True).fetchall()
    invalid_pipeline = pipeline_factory("invalid")
    invalid_pipeline.destination.config_params["password"] = "wrong-password"
    with pytest.raises(DestinationTerminalException, match="401"):
        invalid_pipeline.dataset(schema=pipeline.default_schema)(
            "SELECT 1", _execute_raw_query=True
        ).fetchall()
