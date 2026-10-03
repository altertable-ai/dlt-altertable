import io
import json
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
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
    ("status_code", "body", "expected_error"),
    [
        pytest.param(401, b"invalid credentials", DatabaseTerminalException, id="authentication"),
        pytest.param(400, b"Parser Error: invalid SQL", DatabaseTerminalException, id="syntax"),
        pytest.param(
            400,
            b'Binder Error: Referenced column "missing_column" not found in FROM clause!',
            DatabaseTerminalException,
            id="missing-column",
        ),
        pytest.param(
            400,
            b"Catalog Error: Table with name missing_table does not exist!",
            DatabaseUndefinedRelation,
            id="missing-table",
        ),
        pytest.param(
            500,
            b"Catalog Error: Table with name missing_table does not exist!",
            DatabaseTransientException,
            id="missing-table-server-error",
        ),
        pytest.param(
            400,
            b'No catalog + schema named "missing" found.',
            DatabaseTerminalException,
            id="missing-schema",
        ),
        pytest.param(404, b"not found", DatabaseTerminalException, id="missing-endpoint"),
        pytest.param(
            403,
            b"Catalog Error: Table with name missing_table does not exist!",
            DatabaseTerminalException,
            id="permission-status-takes-precedence",
        ),
        pytest.param(
            404,
            b"Catalog Error: Table with name missing_table does not exist!",
            DatabaseTerminalException,
            id="not-found-status-takes-precedence",
        ),
        pytest.param(429, b"rate limited", DatabaseTransientException, id="rate-limit"),
        pytest.param(500, b"worker failed", DatabaseTransientException, id="worker"),
        pytest.param(200, b"partial parquet stream", pa.ArrowInvalid, id="truncated-parquet"),
    ],
)
def test_query_errors_use_server_status_and_message_without_inspecting_the_query(
    sql_client, monkeypatch, status_code, body, expected_error
) -> None:
    response = requests.Response()
    response.status_code = status_code
    response._content = body
    query_with_error_text = (
        "SELECT 'worker failed with HTTP 400: Catalog Error: Table with name fake does not exist!'"
    )
    monkeypatch.setattr("dlt_altertable.api.session.post", lambda *args, **kwargs: response)

    with sql_client, pytest.raises(expected_error) as failure:
        with sql_client.execute_query(query_with_error_text):
            pass

    assert type(failure.value) is expected_error
    if status_code != 200:
        cause = failure.value.__cause__
        assert isinstance(cause, requests.HTTPError)
        assert cause.response is response
        assert failure.value.dbapi_exception is cause
        assert repr(query_with_error_text) in str(failure.value)


@pytest.mark.parametrize("separator", ["\u2028", "\u2029", "\u0085"])
@pytest.mark.parametrize("line_ending", ["\n", "\r\n"], ids=["lf", "crlf"])
def test_ndjson_preserves_unicode_separators(monkeypatch, separator, line_ending) -> None:
    value = f"before{separator}after"
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


@pytest.mark.parametrize(
    ("statement", "expected_error"),
    [
        ("SELECT * FROM missing_table", DatabaseUndefinedRelation),
        ("CREATE TABLE missing_schema.example(id INT)", DatabaseUndefinedRelation),
        ("SELECT * FROM memory.missing_schema.missing_table", DatabaseUndefinedRelation),
        ("SELECT missing_column FROM (SELECT 1)", DatabaseTerminalException),
        ("SELECT no_such_function()", DatabaseTerminalException),
        ("SELECT CAST('no' AS INTEGER)", DatabaseTerminalException),
        (
            "CREATE TABLE example(id INT NOT NULL); INSERT INTO example VALUES (NULL)",
            DatabaseTerminalException,
        ),
        (
            "CREATE TABLE example(id INT PRIMARY KEY); INSERT INTO example VALUES (1), (1)",
            DatabaseTerminalException,
        ),
        ("CREATE TABLE example(id INT); CREATE TABLE example(id INT)", DatabaseTerminalException),
        (
            'CREATE TABLE "foo does not exist!\nbar"(id INT); '
            'CREATE TABLE "foo does not exist!\nbar"(id INT)',
            DatabaseTerminalException,
        ),
        ("SELECT 9223372036854775807::BIGINT + 1", DatabaseTerminalException),
        ("SELEC 1", DatabaseTransientException),
    ],
)
@pytest.mark.parametrize("after_rows", [False, True])
def test_stream_errors_classify_real_duckdb_messages(
    monkeypatch, statement, expected_error, after_rows
):
    with duckdb.connect() as connection, pytest.raises(duckdb.Error) as database_failure:
        connection.execute(statement)
    message = str(database_failure.value)
    entries = [{}, [], [1]] if after_rows else [{}]
    entries.append({"error": message})
    response = requests.Response()
    response.status_code = 200
    response._content = "\n".join(json.dumps(entry) for entry in entries).encode()
    monkeypatch.setattr(api.session, "post", lambda *a, **kw: response)

    with pytest.raises(expected_error) as failure:
        api.execute_sql(make_config(), statement)

    assert type(failure.value) is expected_error
    assert message in str(failure.value)
    assert repr(statement) in str(failure.value)
    assert failure.value.dbapi_exception is failure.value.__cause__


@pytest.mark.parametrize(
    "message",
    [
        "not found",
        "worker lease expired",
        "Internal error",
        "Permission denied\nCatalog Error: Table with name fake does not exist!",
        "Parser Error: syntax error\n"
        "LINE 1: SELECT 'Catalog Error: Table with name fake does not exist!'",
        "Invalid Input Error: Catalog Error: Table with name fake does not exist!",
        "Worker Error: Catalog Error: Table with name fake does not exist!",
    ],
)
def test_unrecognized_stream_errors_do_not_become_missing_relations(monkeypatch, message):
    monkeypatch.setattr(api.session, "post", lambda *a, **kw: query_response([{"error": message}]))

    with pytest.raises(DatabaseTransientException):
        api.execute_sql(
            make_config(), "SELECT 'Catalog Error: Table with name fake does not exist!'"
        )


@pytest.mark.ducklake
def test_missing_ducklake_schema_is_an_undefined_relation(connection, monkeypatch):
    statement = "CREATE TABLE lakehouse.missing_schema.example(id INT)"
    with pytest.raises(duckdb.BinderException) as database_failure:
        connection.execute(statement)
    message = str(database_failure.value)
    monkeypatch.setattr(api.session, "post", lambda *a, **kw: query_response([{"error": message}]))

    with pytest.raises(DatabaseUndefinedRelation):
        api.execute_sql(make_config(), statement)


@pytest.mark.ducklake
def test_http_transaction_conflicts_are_transient(connection, monkeypatch, write_parquet):
    connection.execute("CREATE TABLE lakehouse.example AS SELECT 1 AS id")
    with connection.cursor() as first, connection.cursor() as second:
        first.execute("BEGIN")
        second.execute("BEGIN")
        first.execute("UPDATE lakehouse.example SET id = 2")
        second.execute("UPDATE lakehouse.example SET id = 3")
        first.execute("COMMIT")
        with pytest.raises(duckdb.TransactionException) as database_failure:
            second.execute("COMMIT")
    response = requests.Response()
    response.status_code = 400
    response._content = str(database_failure.value).encode()
    monkeypatch.setattr(api.session, "post", lambda *a, **kw: response)

    with pytest.raises(DatabaseTransientException):
        api.post_parquet(make_config(), "upsert", {}, write_parquet([{"id": 1}]), "upsert")


@pytest.mark.parametrize(
    "error",
    [
        requests.Timeout("timed out"),
        requests.ConnectionError("connection closed"),
        requests.exceptions.ChunkedEncodingError("query stream failed"),
    ],
)
@pytest.mark.parametrize("operation", ["sql", "parquet", "upload", "upsert"])
def test_transport_failures_are_transient(sql_client, monkeypatch, write_parquet, error, operation):
    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(api.session, "post", fail)

    with sql_client, pytest.raises(DatabaseTransientException) as failure:
        if operation == "parquet":
            sql_client.execute_sql("SELECT * FROM missing_table")
        elif operation == "sql":
            api.execute_sql(make_config(), "SELECT * FROM missing_table")
        else:
            api.post_parquet(make_config(), operation, {}, write_parquet([{"id": 1}]), "load")

    assert failure.value.dbapi_exception is error
    assert failure.value.__cause__ is error


@pytest.mark.parametrize(
    "error",
    [
        ValueError("invalid local value"),
        NotImplementedError("binding"),
        pa.ArrowInvalid("invalid parquet"),
        json.JSONDecodeError("invalid JSON", "", 0),
        requests.exceptions.InvalidURL("invalid host"),
        DatabaseTerminalException(ValueError("already classified")),
    ],
)
def test_database_exception_mapping_preserves_unrelated_and_classified_errors(sql_client, error):
    assert sql_client._make_database_exception(error) is error
