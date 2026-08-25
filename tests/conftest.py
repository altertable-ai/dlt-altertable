import io
import json as jsonlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import dlt_altertable.api
from dlt_altertable.configuration import AltertableClientConfiguration

DESTINATION_OPTIONS = {
    "host": "altertable.test",
    "catalog": "lakehouse",
    "dataset_name": "raw",
    "username": "user",
    "password": "secret",
    "port": 15002,
    "tls": False,
}

BASE_URL = "http://altertable.test:15002"

QUALIFIED_TABLE = re.compile(r'"[^"]+"\."[^"]+"\."([^"]+)"')


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
    counts: dict[str, list[int]] = field(default_factory=dict)
    counts_asked: dict[str, str] = field(default_factory=dict)
    missing_tables: list[str] = field(default_factory=list)
    successes_before_failures: int = 0
    transient_upload_failures: int = 0
    terminal_upload_failure: bool = False
    unauthenticated: bool = False
    query_error: str | None = None

    def uploads_for(self, table_name: str) -> list[RecordedRequest]:
        return [upload for upload in self.uploads if upload.params["table"] == table_name]

    def attempts_for(self, table_name: str) -> int:
        return self.attempted_tables.count(table_name)

    def count_query_response(self, statement: str) -> FakeResponse:
        qualified_table = QUALIFIED_TABLE.search(statement)
        assert qualified_table, f"count query names no qualified table: {statement}"
        table_name = qualified_table.group(1)
        self.counts_asked[table_name] = statement
        if table_name in self.counts:
            return query_response([self.counts[table_name]])
        uploaded_row_count = self.uploaded_row_count(table_name, statement)
        selected_count_columns = statement.count("count(")
        return query_response([[uploaded_row_count] * selected_count_columns])

    def uploaded_row_count(self, table_name: str, count_query: str) -> int:
        """A replace count query spans the whole replacement table, so it names no load id,
        while an append or merge one counts only the rows whose load id it names."""
        rows = [row for upload in self.uploads_for(table_name) for row in upload.rows]
        if "_dlt_load_id" not in count_query:
            return len(rows)
        return sum(1 for row in rows if f"'{row['_dlt_load_id']}'" in count_query)

    @property
    def schema_lookups(self) -> list[str]:
        return [s for s in self.statements if "information_schema.columns" in s]

    @property
    def alters(self) -> list[str]:
        return [statement for statement in self.statements if statement.startswith("ALTER")]

    @property
    def creates(self) -> list[str]:
        return [statement for statement in self.statements if statement.startswith("CREATE")]

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
            statement = json["statement"]
            self.statements.append(statement)
            self.query_payloads.append(dict(json))
            if self.query_error is not None:
                return query_response([{"error": self.query_error}])
            if statement.startswith(("ALTER", "CREATE")):
                return query_response([])
            if "information_schema.tables" in statement:
                landed = {upload.params["table"] for upload in self.uploads}
                return query_response([[t] for t in sorted(landed - set(self.missing_tables))])
            if "duckdb_databases" in statement:
                excluded = {"memory"} if "database_name <> 'memory'" in statement else set()
                return query_response(
                    [
                        [name, readonly]
                        for name, readonly in self.catalogs.items()
                        if name not in excluded
                    ]
                )
            if statement.startswith("SELECT count"):
                return self.count_query_response(statement)
            return query_response([[column] for column in self.existing_columns])

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
def write_parquet(tmp_path: Path):
    def write(rows: list[dict[str, Any]], name: str = "data") -> str:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        return str(path)

    return write
