import io
import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
)

from dlt_altertable import altertable, api
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
    with sql_client:
        assert sql_client.execute_sql("SELECT values") == [(None, Decimal("123456789.123"), None)]


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
        assert json["format"] == "parquet"
        return parquet_response(pa.table({"id": [1]}))

    monkeypatch.setattr("dlt_altertable.api.session.post", post_query)

    with sql_client, sql_client.with_alternative_dataset_name("other_schema"):
        if query_method == "execute_sql":
            assert sql_client.execute_sql("SELECT id FROM events") == [(1,)]
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
    ("status_code", "expected_error"),
    [
        pytest.param(400, DatabaseTerminalException, id="bad-request"),
        pytest.param(401, DatabaseTerminalException, id="authentication"),
        pytest.param(429, DatabaseTransientException, id="rate-limit"),
        pytest.param(500, DatabaseTransientException, id="server-error"),
    ],
)
def test_query_errors_follow_http_status_and_preserve_diagnostics(
    sql_client, monkeypatch, status_code, expected_error
) -> None:
    message = "Catalog Error: Table with name events does not exist!"
    response = requests.Response()
    response.status_code = status_code
    response._content = message.encode()
    statement = "SELECT 'HTTP 400: Permission Error: denied'"
    monkeypatch.setattr(api.session, "post", lambda *args, **kwargs: response)

    with sql_client, pytest.raises(expected_error) as failure:
        sql_client.execute_sql(statement)

    cause = failure.value.__cause__
    assert type(failure.value) is expected_error
    assert isinstance(cause, requests.HTTPError)
    assert cause.response is response
    assert failure.value.dbapi_exception is cause
    assert message in str(failure.value)
    assert repr(statement) in str(failure.value)


def test_truncated_parquet_is_not_a_successful_query(sql_client, monkeypatch) -> None:
    response = requests.Response()
    response.status_code = 200
    response._content = b"partial parquet stream"
    monkeypatch.setattr(api.session, "post", lambda *args, **kwargs: response)

    with sql_client, pytest.raises(pa.ArrowInvalid):
        sql_client.execute_sql("SELECT id FROM events")


@pytest.mark.parametrize("line_ending", ["\n", "\r\n"], ids=["lf", "crlf"])
def test_ndjson_preserves_unicode_separators(monkeypatch, line_ending) -> None:
    value = "first\u2028second\u2029third\u0085last"
    entries = [{}, [{"name": "value", "type": "VARCHAR"}], [value]]
    response = requests.Response()
    response.status_code = 200
    response.encoding = "utf-8"
    response._content = (
        line_ending.join(json.dumps(entry, ensure_ascii=False) for entry in entries)
        + line_ending * 2
    ).encode()
    monkeypatch.setattr(api.session, "post", lambda *a, **kw: response)

    rows = api.execute_sql(make_config(), "SELECT value")

    assert rows == [[value]]


@pytest.mark.parametrize("after_rows", [False, True], ids=["before-rows", "after-rows"])
def test_stream_failures_remain_retryable_and_preserve_diagnostics(monkeypatch, after_rows):
    statement = "SELECT id FROM events"
    message = 'query failed\nwith "quotes" and Unicode \u2028 separators'
    entries = [{}, [{"name": "id", "type": "INTEGER"}], [1]] if after_rows else [{}]
    entries.append({"error": message})
    response = requests.Response()
    response.status_code = 200
    response._content = "\n".join(json.dumps(entry) for entry in entries).encode()
    monkeypatch.setattr(api.session, "post", lambda *a, **kw: response)

    with pytest.raises(DatabaseTransientException) as failure:
        api.execute_sql(make_config(), statement)

    assert type(failure.value) is DatabaseTransientException
    assert message in str(failure.value)
    assert repr(statement) in str(failure.value)
    assert failure.value.dbapi_exception is failure.value.__cause__


@pytest.mark.parametrize(
    "error",
    [
        requests.Timeout("timed out"),
        requests.ConnectionError("connection closed"),
        requests.exceptions.ChunkedEncodingError("query stream failed"),
    ],
)
@pytest.mark.parametrize("operation", ["query", "upload"])
def test_transport_failures_are_transient(monkeypatch, write_parquet, error, operation):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(api.session, "post", fail)

    with pytest.raises(DatabaseTransientException) as failure:
        if operation == "query":
            api.execute_sql(make_config(), "SELECT id FROM events")
        else:
            api.post_parquet(make_config(), "upload", {}, write_parquet([{"id": 1}]), "load")

    assert failure.value.dbapi_exception is error
    assert failure.value.__cause__ is error


@pytest.mark.parametrize(
    "error",
    [
        ValueError("invalid local value"),
        requests.exceptions.InvalidURL("invalid host"),
        DatabaseTerminalException(ValueError("already classified")),
    ],
)
def test_database_exception_mapping_preserves_unrelated_and_classified_errors(sql_client, error):
    assert sql_client._make_database_exception(error) is error
