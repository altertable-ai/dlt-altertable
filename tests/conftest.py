import io
import json as jsonlib
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import dlt_altertable.api


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


@dataclass
class FakeServer:
    uploads: list[RecordedRequest] = field(default_factory=list)
    attempted_tables: list[str] = field(default_factory=list)
    statements: list[str] = field(default_factory=list)
    existing_columns: list[str] = field(default_factory=list)
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
        return [statement for statement in self.statements if "information_schema" in statement]

    @property
    def alters(self) -> list[str]:
        return [statement for statement in self.statements if statement.startswith("ALTER")]

    @property
    def creates(self) -> list[str]:
        return [statement for statement in self.statements if statement.startswith("CREATE")]


def fake_post(server: FakeServer):
    def post(
        url: str,
        params: dict[str, Any] | None = None,
        data: Any = None,
        json: dict[str, Any] | None = None,
        auth: tuple[str, str] | None = None,
        headers: dict[str, str] | None = None,
        timeout: Any = None,
    ) -> FakeResponse:
        if server.unauthenticated:
            return FakeResponse(401, "Invalid credentials")

        if url.endswith("/query"):
            statement = json["statement"]
            server.statements.append(statement)
            if server.query_error is not None:
                lines: list[Any] = [{}, [], {"error": server.query_error}]
            elif statement.startswith("ALTER"):
                lines = [{}, []]
            else:
                lines = [
                    {},
                    [{"name": "column_name", "type": "VARCHAR"}],
                    *[[column] for column in server.existing_columns],
                ]
            return FakeResponse(200, "\n".join(jsonlib.dumps(line) for line in lines))

        table_name = params["table"]
        if not table_name.startswith("_dlt"):
            server.attempted_tables.append(table_name)
            if server.successes_before_failures > 0:
                server.successes_before_failures -= 1
            elif server.terminal_upload_failure:
                return FakeResponse(400, "injected terminal failure")
            elif server.transient_upload_failures:
                server.transient_upload_failures -= 1
                return FakeResponse(503, "no compute capacity")

        if hasattr(data, "read"):
            chunks = []
            while chunk := data.read():
                chunks.append(chunk)
            body = b"".join(chunks)
        else:
            body = data
        server.uploads.append(
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

    return post


@pytest.fixture(autouse=True)
def isolated_altertable_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for suffix in ("HOST", "PORT", "TLS", "USERNAME", "PASSWORD", "CATALOG", "SCHEMA"):
        monkeypatch.delenv(f"ALTERTABLE_{suffix}", raising=False)


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> FakeServer:
    server = FakeServer()
    monkeypatch.setattr(
        dlt_altertable.api,
        "session",
        SimpleNamespace(post=fake_post(server)),
        raising=True,
    )
    return server


@pytest.fixture
def write_parquet(tmp_path: Path):
    def write(rows: list[dict[str, Any]], name: str = "data") -> str:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        return str(path)

    return write
