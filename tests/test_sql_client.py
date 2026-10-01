import io
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt_altertable import altertable
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


@pytest.mark.parametrize("chunk_size", [None, 0], ids=["default", "zero"])
def test_fetch_methods_advance_the_same_cursor(sql_client, monkeypatch, chunk_size) -> None:
    table = pa.table({"id": [1, 2, 3, 4, 5]})
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: parquet_response(table))
    with sql_client, sql_client.execute_query("SELECT id FROM events") as cursor:
        assert cursor.description[0][:2] == ("id", pa.int64())
        assert cursor.columns_schema["id"]["data_type"] == "bigint"
        assert cursor.fetchone() == (1,)
        assert cursor.fetchmany(chunk_size=2) == [(2,), (3,)]
        assert cursor.arrow(chunk_size=chunk_size).to_pylist() == [{"id": 4}, {"id": 5}]
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


@pytest.mark.parametrize("row_count", [0, 1], ids=["empty", "null-only"])
def test_empty_and_null_results_preserve_arrow_types(sql_client, monkeypatch, row_count) -> None:
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("label", pa.string()),
            ("amount", pa.decimal128(12, 3)),
            ("instant", pa.timestamp("ns", tz="UTC")),
        ]
    )
    table = pa.Table.from_pylist([{}] * row_count, schema=schema)
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *a, **kw: parquet_response(table))
    with sql_client, sql_client.execute_query("SELECT * FROM events") as cursor:
        result = cursor.arrow()
        assert result.num_rows == row_count
        assert result.schema == schema


def test_rejects_binding_and_transactions_before_http(sql_client, monkeypatch) -> None:
    monkeypatch.setattr(
        "dlt_altertable.api.session.post",
        lambda *args, **kwargs: pytest.fail("Unsupported operations must not send HTTP requests."),
    )
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


@pytest.mark.parametrize("query_method", ["execute_sql", "execute_query"])
def test_queries_use_the_configured_catalog_and_current_dataset(
    sql_client, monkeypatch, query_method
) -> None:
    def post_query(url, *, json, **kwargs):
        assert json["catalog"] == "lakehouse"
        assert json["schema"] == "other_schema"
        assert json["compute_size"] == "XS"
        return (
            parquet_response(pa.table({"id": [1]}))
            if json.get("format") == "parquet"
            else query_response([[1]])
        )

    monkeypatch.setattr("dlt_altertable.api.session.post", post_query)

    with sql_client, sql_client.with_alternative_dataset_name("other_schema"):
        if query_method == "execute_sql":
            assert sql_client.execute_sql("SELECT id FROM events") == [[1]]
        else:
            with sql_client.execute_query("SELECT id FROM events") as cursor:
                assert cursor.fetchall() == [(1,)]


@pytest.mark.parametrize(("rows", "exists"), [([[1]], True), ([], False)], ids=["found", "missing"])
def test_has_dataset_looks_up_the_current_dataset_without_scoping_to_it(
    sql_client, monkeypatch, rows, exists
) -> None:
    def post_query(url, *, json, **kwargs):
        assert "schema" not in json
        assert "catalog_name = E'lakehouse' AND schema_name = E'other_schema'" in json["statement"]
        return query_response(rows)

    monkeypatch.setattr("dlt_altertable.api.session.post", post_query)

    with sql_client, sql_client.with_alternative_dataset_name("other_schema"):
        assert sql_client.has_dataset() is exists


@pytest.mark.parametrize(
    ("query_method", "status_code"),
    [
        pytest.param("execute_sql", 200, id="ndjson-error"),
        pytest.param("execute_sql", 400, id="sql-http-error"),
        pytest.param("execute_query", 400, id="terminal-parquet-error"),
        pytest.param("execute_query", 500, id="transient-parquet-error"),
    ],
)
def test_missing_tables_raise_the_error_dlt_handles(
    sql_client, monkeypatch, query_method, status_code
) -> None:
    error = "Catalog Error: Table with name missing_table does not exist!"
    response = requests.Response()
    response.status_code = status_code
    response._content = (
        query_response([{"error": error}]).text.encode() if status_code == 200 else error.encode()
    )
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *args, **kwargs: response)

    with sql_client, pytest.raises(DatabaseUndefinedRelation, match="missing_table") as failure:
        if query_method == "execute_sql":
            sql_client.execute_sql("SELECT * FROM missing_table")
        else:
            with sql_client.execute_query("SELECT * FROM missing_table"):
                pass

    assert failure.value.__cause__ is failure.value.dbapi_exception


@pytest.mark.parametrize(
    ("status_code", "body", "expected_error"),
    [
        pytest.param(
            401, b"invalid credentials", DestinationTerminalException, id="authentication"
        ),
        pytest.param(400, b"Parser Error: invalid SQL", DestinationTerminalException, id="syntax"),
        pytest.param(
            400,
            b'Binder Error: Referenced column "missing_column" not found in FROM clause!',
            DestinationTerminalException,
            id="missing-column",
        ),
        pytest.param(500, b"worker failed", RuntimeError, id="worker"),
        pytest.param(200, b"partial parquet stream", pa.ArrowInvalid, id="truncated-parquet"),
    ],
)
def test_other_query_errors_keep_their_original_type(
    sql_client, monkeypatch, status_code, body, expected_error
) -> None:
    response = requests.Response()
    response.status_code = status_code
    response._content = body
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *args, **kwargs: response)

    with sql_client, pytest.raises(expected_error) as failure:
        with sql_client.execute_query(
            "SELECT 'Catalog Error: Table with name missing_table does not exist!'"
        ):
            pass

    assert type(failure.value) is expected_error
