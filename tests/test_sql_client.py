import io
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from dlt.common.destination.exceptions import DestinationTerminalException

from dlt_altertable import altertable
from dlt_altertable.api import execute_sql
from dlt_altertable.sql_client import AltertableSqlClient
from tests.conftest import make_config, query_response


def parquet_response(table: pa.Table) -> requests.Response:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    response = requests.Response()
    response.status_code = 200
    response._content = buffer.getvalue()
    return response


@pytest.fixture
def sql_client() -> AltertableSqlClient:
    return AltertableSqlClient(make_config(), altertable().capabilities())


def test_cursor_reads_share_position(sql_client, monkeypatch) -> None:
    table = pa.table({"id": [1, 2, 3, 4, 5]})
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: parquet_response(table))
    with sql_client, sql_client.execute_query("SELECT id FROM events") as cursor:
        assert cursor.description[0][:2] == ("id", pa.int64())
        assert cursor.columns_schema["id"]["data_type"] == "bigint"
        assert cursor.fetchone() == (1,)
        assert cursor.fetchmany(chunk_size=2) == [(2,), (3,)]
        assert cursor.arrow().to_pylist() == [{"id": 4}, {"id": 5}]
        assert cursor.fetchone() is None
    with sql_client, sql_client.execute_query("SELECT id FROM events") as cursor:
        assert list(cursor.iter_fetch(2)) == [[(1,), (2,)], [(3,), (4,)], [(5,)]]
    with sql_client, sql_client.execute_query("SELECT id FROM events") as cursor:
        assert [chunk.num_rows for chunk in cursor.iter_arrow(2)] == [2, 2, 1]
    with pytest.raises(RuntimeError, match="closed"):
        cursor.fetchall()


def test_arrow_preserves_types_and_duplicate_names(sql_client, monkeypatch) -> None:
    table = pa.Table.from_arrays(
        [
            pa.array([None], type=pa.int64()),
            pa.array([Decimal("123456789.123")], type=pa.decimal128(12, 3)),
            pa.array([None], type=pa.timestamp("ns")),
        ],
        names=["same", "same", "instant"],
    )
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: parquet_response(table))
    with sql_client, sql_client.execute_query("SELECT values") as cursor:
        assert cursor.arrow().schema == table.schema
    with sql_client, sql_client.execute_query("SELECT values") as cursor:
        assert cursor.fetchall() == [(None, Decimal("123456789.123"), None)]


@pytest.mark.parametrize("column_type", [pa.int64(), pa.string()])
def test_empty_result_preserves_column_type(sql_client, monkeypatch, column_type) -> None:
    table = pa.table({"id": pa.array([], type=column_type)})
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: parquet_response(table))
    with sql_client, sql_client.execute_query("SELECT id FROM events WHERE FALSE") as cursor:
        result = cursor.arrow()
        assert result.num_rows == 0
        assert result.schema == table.schema


def test_rejects_binding_and_transactions_before_http(sql_client) -> None:
    with sql_client:
        with pytest.raises(NotImplementedError, match="parameter"):
            sql_client.execute_sql("SELECT %s", 1)
        with pytest.raises(NotImplementedError, match="parameter"):
            with sql_client.execute_query("SELECT :id", id=1):
                pass
        for method in (
            sql_client.begin_transaction,
            sql_client.commit_transaction,
            sql_client.rollback_transaction,
        ):
            with pytest.raises(NotImplementedError, match="transaction"):
                method()


def test_http_and_stream_failures_propagate(sql_client, monkeypatch) -> None:
    response = requests.Response()
    response.status_code = 401
    response._content = b"invalid credentials"
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: response)
    with sql_client, pytest.raises(DestinationTerminalException, match="401"):
        with sql_client.execute_query("SELECT 1"):
            pass
    monkeypatch.setattr(
        "dlt_altertable.api.session.post",
        lambda *a, **kw: query_response([{"error": "worker failed"}]),
    )
    with pytest.raises(RuntimeError, match="worker failed"):
        execute_sql(make_config(), "SELECT 1")
    response.status_code = 200
    response._content = b"partial parquet stream"
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: response)
    with sql_client, pytest.raises(pa.ArrowInvalid):
        with sql_client.execute_query("SELECT 1"):
            pass


@pytest.mark.parametrize("body", ["", "{}", "{}\n{}", "{}\n[]\n{}"])
def test_truncated_or_malformed_stream_is_not_an_empty_result(monkeypatch, body) -> None:
    response = requests.Response()
    response.status_code = 200
    response._content = body.encode()
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: response)
    with pytest.raises(RuntimeError, match="Malformed query response"):
        execute_sql(make_config(), "SELECT 1")
