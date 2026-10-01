from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException

pytestmark = pytest.mark.integration


def test_loaded_arrow_types_round_trip(pipeline_factory) -> None:
    pipeline = pipeline_factory("original")
    source = pa.table(
        {
            "id": pa.array([2**53 + 1, None], type=pa.int64()),
            "amount": pa.array([Decimal("123456789.123"), None], type=pa.decimal128(12, 3)),
            "instant": pa.array(
                [datetime(2026, 9, 30, 12, tzinfo=UTC), None], type=pa.timestamp("us", tz="UTC")
            ),
        }
    )
    pipeline.run(source, table_name="events")
    result = pipeline.dataset().events.select(*source.column_names).order_by("id").arrow()
    assert result.schema.field("id").type == pa.int64()
    assert result.schema.field("amount").type == pa.decimal128(12, 3)
    assert result.cast(source.schema).equals(source)


def test_dataset_reads_from_the_configured_schema(mock_config, pipeline_factory) -> None:
    pipeline = pipeline_factory("original")
    pipeline.run([{"id": i, "label": str(i)} for i in range(1, 6)], table_name="events")
    dataset = pipeline.dataset()
    events_query = "SELECT id FROM events ORDER BY id"
    expected_rows = [(1,), (2,), (3,), (4,), (5,)]
    assert pipeline.dataset_name != mock_config.dataset_name
    assert dataset.events.select("id").order_by("id").fetchall() == expected_rows
    assert dataset(events_query, _execute_raw_query=True).fetchall() == expected_rows
    selected = dataset.events.select("id").where("id", "gte", 3).order_by("id").limit(2)
    assert selected.arrow().to_pylist() == [{"id": 3}, {"id": 4}]
    assert selected.df()["id"].tolist() == [3, 4]
    chunks = list(dataset.events.select("id").order_by("id").iter_arrow(chunk_size=2))
    assert [chunk.num_rows for chunk in chunks] == [2, 2, 1]
    assert pa.concat_tables(chunks).column("id").to_pylist() == [1, 2, 3, 4, 5]
    with pipeline.sql_client() as sql_client:
        assert sql_client.execute_sql("SELECT id FROM events WHERE id = 1") == [(1,)]


def test_dataset_preserves_result_types(pipeline_factory) -> None:
    pipeline = pipeline_factory("original")
    pipeline.run([{"id": 1}], table_name="events")
    dataset = pipeline.dataset()
    nullable_types_query = (
        "SELECT NULL::BIGINT AS id, NULL::DECIMAL(12,3) AS amount, NULL::TIMESTAMPTZ AS instant"
    )
    for filter_clause, expected_row_count in [("", 1), (" WHERE FALSE", 0)]:
        result = dataset(nullable_types_query + filter_clause, _execute_raw_query=True).arrow()
        assert result.num_rows == expected_row_count
        assert result.schema.names == ["id", "amount", "instant"]
        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("amount").type == pa.decimal128(12, 3)
        timestamp_type = result.schema.field("instant").type
        assert pa.types.is_timestamp(timestamp_type)
        assert timestamp_type.unit == "us"
        assert timestamp_type.tz in {"UTC", "Etc/UTC"}
    typed_values_query = (
        "SELECT 12.345::DECIMAL(12,3) AS amount, TIMESTAMPTZ '2026-09-30 12:00:00+00' AS instant, "
        "DATE '2026-01-02' AS day"
    )
    expected_typed_row = (
        Decimal("12.345"),
        datetime(2026, 9, 30, 12, tzinfo=UTC),
        date(2026, 1, 2),
    )
    assert dataset(typed_values_query, _execute_raw_query=True).fetchone() == expected_typed_row
    with pipeline.sql_client() as sql_client:
        assert sql_client.execute_sql(typed_values_query) == [expected_typed_row]
    empty_uuid = dataset(
        "SELECT NULL::UUID AS identifier WHERE FALSE", _execute_raw_query=True
    ).arrow()
    assert empty_uuid.num_rows == 0
    assert empty_uuid.schema == pa.schema([("identifier", pa.string())])


def test_has_dataset_reports_whether_the_schema_exists(pipeline_factory) -> None:
    pipeline = pipeline_factory("original")
    with pipeline.sql_client() as sql_client:
        assert sql_client.has_dataset() is False
    pipeline.run([{"id": 1}], table_name="events")
    with pipeline.sql_client() as sql_client:
        assert sql_client.has_dataset() is True


def test_query_failures_are_not_empty_results(pipeline_factory) -> None:
    loaded_pipeline = pipeline_factory("valid")
    loaded_pipeline.run([{"id": 1}], table_name="events")
    missing_table_query = "SELECT * FROM missing_table"
    with pytest.raises(RuntimeError, match="HTTP 500.*missing_table"):
        loaded_pipeline.dataset()(missing_table_query, _execute_raw_query=True).fetchall()
    with loaded_pipeline.sql_client() as sql_client:
        with pytest.raises(RuntimeError, match="HTTP 500.*missing_table"):
            sql_client.execute_sql(missing_table_query)
    unauthenticated_pipeline = pipeline_factory("invalid")
    unauthenticated_pipeline.destination.config_params["password"] = "wrong-password"
    with pytest.raises(DestinationTerminalException, match="401"):
        unauthenticated_pipeline.dataset(schema=loaded_pipeline.default_schema)(
            "SELECT 1", _execute_raw_query=True
        ).fetchall()
