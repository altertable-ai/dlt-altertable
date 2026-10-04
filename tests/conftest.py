import io
import json as jsonlib
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dlt.common.configuration.container import Container
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import (
    LoadPackageStateInjectableContext,
    PackageStorage,
    destination_state,
)

import dlt_altertable.api
from dlt_altertable import altertable
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.table_schema import qualified_table_name

if TYPE_CHECKING:
    import duckdb

DESTINATION_OPTIONS = AltertableClientConfiguration(
    host="altertable.test",
    catalog="lakehouse",
    dataset_name="raw",
    username="user",
    password="secret",
    port=15002,
    tls=False,
).as_dict_nondefault()

BASE_URL = "http://altertable.test:15002"


def make_config(**overrides: Any) -> AltertableClientConfiguration:
    config = AltertableClientConfiguration(**{**DESTINATION_OPTIONS, **overrides})
    config.on_resolved()
    return config


@dataclass
class RecordedRequest:
    url: str
    endpoint: str
    params: dict[str, Any]
    headers: dict[str, str]
    auth: tuple[str, str]
    body: bytes

    @property
    def rows(self) -> list[dict[str, Any]]:
        return pq.read_table(io.BytesIO(self.body)).to_pylist()

    @property
    def schema(self) -> pa.Schema:
        return pq.read_table(io.BytesIO(self.body)).schema


@dataclass
class FakeResponse:
    status_code: int
    text: str = ""


def query_response(rows: list[Any]) -> FakeResponse:
    return FakeResponse(200, "\n".join(jsonlib.dumps(line) for line in [{}, [], *rows]))


@dataclass
class FakeServer:
    uploads: list[RecordedRequest] = field(default_factory=list)
    attempted_tables: list[str] = field(default_factory=list)
    statements: list[str] = field(default_factory=list)
    query_payloads: list[dict[str, Any]] = field(default_factory=list)
    existing_columns: list[str] = field(default_factory=list)
    catalogs: dict[str, bool] = field(default_factory=lambda: {"lakehouse": False})
    successes_before_failures: int = 0
    transient_upload_failures: int = 0
    terminal_upload_failure: bool = False
    unauthenticated: bool = False
    query_error: str | None = None

    def uploads_for(self, table_name: str) -> list[RecordedRequest]:
        return [upload for upload in self.uploads if upload.params["table"] == table_name]

    def attempts_for(self, table_name: str) -> int:
        return self.attempted_tables.count(table_name)

    @property
    def schema_lookups(self) -> list[str]:
        return [s for s in self.statements if "information_schema.columns" in s]

    @property
    def alters(self) -> list[str]:
        return [statement for statement in self.statements if statement.startswith("ALTER")]

    @property
    def creates(self) -> list[str]:
        return [statement for statement in self.statements if statement.startswith("CREATE")]

    def post_query(self, payload: dict[str, Any]) -> FakeResponse:
        statement = payload["statement"]
        self.statements.append(statement)
        self.query_payloads.append(dict(payload))
        if self.query_error is not None:
            return query_response([{"error": self.query_error}])
        if statement.startswith(("ALTER", "CREATE")):
            return query_response([])
        if "information_schema.tables" in statement:
            landed = {upload.params["table"] for upload in self.uploads}
            return query_response([[t] for t in sorted(landed)])
        if "duckdb_databases" in statement:
            excluded = {"memory"} if "database_name <> 'memory'" in statement else set()
            return query_response(
                [
                    [name, readonly]
                    for name, readonly in self.catalogs.items()
                    if name not in excluded
                ]
            )
        if statement.startswith("SELECT column_name, data_type"):
            return query_response([[column, "BIGINT"] for column in self.existing_columns])
        return query_response([[column] for column in self.existing_columns])

    def post(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        data: Any = None,
        json: dict[str, Any] | None = None,
        auth: tuple[str, str] | None = None,
        headers: dict[str, str] | None = None,
        timeout: Any = None,
    ) -> FakeResponse:
        if self.unauthenticated:
            return FakeResponse(401, "Invalid credentials")

        if url.endswith("/query"):
            assert json is not None
            return self.post_query(json)

        assert params is not None
        assert auth is not None
        table_name = params["table"]
        if not table_name.startswith("_dlt"):
            self.attempted_tables.append(table_name)
            if self.successes_before_failures > 0:
                self.successes_before_failures -= 1
            elif self.terminal_upload_failure:
                return FakeResponse(400, "injected terminal failure")
            elif self.transient_upload_failures:
                self.transient_upload_failures -= 1
                return FakeResponse(503, "no compute capacity")

        body = data.read() if hasattr(data, "read") else data
        self.uploads.append(
            RecordedRequest(
                url=url,
                endpoint=url.rsplit("/", 1)[1],
                params=dict(params),
                headers=dict(headers or {}),
                auth=auth,
                body=body,
            )
        )
        return FakeResponse(200)


@pytest.fixture(autouse=True)
def isolated_altertable_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for suffix in ("HOST", "PORT", "TLS", "USERNAME", "PASSWORD", "CATALOG", "SCHEMA"):
        monkeypatch.delenv(f"ALTERTABLE_{suffix}", raising=False)


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> FakeServer:
    fake_server = FakeServer()
    monkeypatch.setattr(dlt_altertable.api, "session", fake_server, raising=True)
    return fake_server


@pytest.fixture
def load_package_state(tmp_path: Path) -> Iterator[dict[str, Any]]:
    storage = PackageStorage(FileStorage(str(tmp_path)), initial_state="normalized")
    storage.create_package("1")
    context = LoadPackageStateInjectableContext(storage=storage, load_id="1")
    with Container().injectable_context(context):
        yield destination_state()


@pytest.fixture
def write_parquet(tmp_path: Path) -> Callable[..., str]:
    def write(rows: list[dict[str, Any]], name: str = "data") -> str:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        return str(path)

    return write


@pytest.fixture
def connection(
    tmp_path: Path, request: pytest.FixtureRequest
) -> Iterator["duckdb.DuckDBPyConnection"]:
    import duckdb

    with duckdb.connect() as connection:
        try:
            connection.execute("INSTALL ducklake")
            connection.execute("LOAD ducklake")
        except duckdb.Error as error:
            if os.environ.get("CI"):
                raise
            # pytest 8 callable typing: https://github.com/astral-sh/ty/issues/2797
            reason = f"DuckDB cannot load its ducklake extension: {error}"
            pytest.skip(reason)  # ty: ignore[too-many-positional-arguments]
        connection.execute(
            f"ATTACH 'ducklake:{tmp_path}/metadata.duckdb' AS lakehouse "
            f"(DATA_PATH '{tmp_path}/data/', METADATA_SCHEMA '{getattr(request, 'param', 'main')}')"
        )
        connection.execute("CALL lakehouse.set_option('data_inlining_row_limit', 0)")
        yield connection


@pytest.fixture
def local_api(connection: "duckdb.DuckDBPyConnection", monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOAD__WORKERS", "1")
    monkeypatch.setenv("LOAD__RAISE_ON_MAX_RETRIES", "1")

    def execute(config: AltertableClientConfiguration, statement: str) -> list[tuple[Any, ...]]:
        with connection.cursor() as session:
            return session.execute(statement).fetchall()

    def upload(
        config: AltertableClientConfiguration,
        endpoint: str,
        params: dict[str, str],
        path: str,
        action: str,
    ) -> None:
        assert endpoint == "upload"
        target = qualified_table_name(config, params["table"])
        if params["mode"] == "overwrite":
            connection.execute(
                f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM read_parquet(?)", [path]
            )
        else:
            connection.execute(f"INSERT INTO {target} SELECT * FROM read_parquet(?)", [path])

    monkeypatch.setattr(dlt_altertable.api, "execute_sql", execute)
    monkeypatch.setattr(dlt_altertable.api, "post_parquet", upload)


@pytest.fixture
def local_pipeline(
    local_api: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dlt.Pipeline:
    monkeypatch.setenv("SCHEMA__NAMING", "snake_case")
    return dlt.pipeline(
        pipeline_name="merge_contract",
        destination=altertable(**DESTINATION_OPTIONS),
        pipelines_dir=str(tmp_path),
    )
