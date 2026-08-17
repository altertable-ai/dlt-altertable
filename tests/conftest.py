from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode

import dlt_altertable.destination


@dataclass
class RecordedIngest:
    table_name: str
    schema: pa.Schema
    schema_name: str
    catalog_name: str
    mode: IngestTableMode
    incremental_options: IngestIncrementalOptions | None
    batches: list[pa.RecordBatch] = field(default_factory=list)

    @property
    def rows(self) -> list[dict[str, Any]]:
        return pa.Table.from_batches(self.batches, schema=self.schema).to_pylist()


@dataclass
class FlightRecorder:
    connections: list[dict[str, Any]] = field(default_factory=list)
    ingests: list[RecordedIngest] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)
    parquet_paths: list[str] = field(default_factory=list)

    def ingests_for(self, table_name: str) -> list[RecordedIngest]:
        return [ingest for ingest in self.ingests if ingest.table_name == table_name]


class RecordingWriter:
    def __init__(self, recorder: FlightRecorder, ingest: RecordedIngest) -> None:
        self._recorder = recorder
        self._ingest = ingest

    def write(self, batch: pa.RecordBatch) -> None:
        self._ingest.batches.append(batch)

    def close(self) -> None:
        self._recorder.calls.append("close_writer")

    def __enter__(self) -> "RecordingWriter":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class RecordingTransaction:
    def __init__(self, recorder: FlightRecorder) -> None:
        self._recorder = recorder

    def __enter__(self) -> "RecordingTransaction":
        return self

    def __exit__(self, exc_type: object, *exc_info: object) -> None:
        self._recorder.calls.append("rollback" if exc_type else "commit")


def recording_client_class(recorder: FlightRecorder) -> type:
    class RecordingClient:
        def __init__(self, username: str, password: str, **connection: Any) -> None:
            recorder.connections.append({"username": username, "password": password, **connection})

        def begin_transaction(self) -> RecordingTransaction:
            recorder.calls.append("begin_transaction")
            return RecordingTransaction(recorder)

        def ingest(
            self,
            *,
            table_name: str,
            schema: pa.Schema,
            schema_name: str,
            catalog_name: str,
            mode: IngestTableMode,
            incremental_options: IngestIncrementalOptions | None,
            transaction: RecordingTransaction,
        ) -> RecordingWriter:
            recorder.calls.append("ingest")
            ingest = RecordedIngest(
                table_name=table_name,
                schema=schema,
                schema_name=schema_name,
                catalog_name=catalog_name,
                mode=mode,
                incremental_options=incremental_options,
            )
            recorder.ingests.append(ingest)
            return RecordingWriter(recorder, ingest)

        def close(self) -> None:
            recorder.calls.append("close_client")

        def __enter__(self) -> "RecordingClient":
            return self

        def __exit__(self, *exc_info: object) -> None:
            self.close()

    return RecordingClient


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> FlightRecorder:
    recorder = FlightRecorder()

    def open_and_record(path: str) -> pq.ParquetFile:
        recorder.parquet_paths.append(path)
        return pq.ParquetFile(path)

    monkeypatch.setattr(
        dlt_altertable.destination, "Client", recording_client_class(recorder), raising=True
    )
    monkeypatch.setattr(
        dlt_altertable.destination, "pq", SimpleNamespace(ParquetFile=open_and_record), raising=True
    )
    return recorder


@pytest.fixture
def write_parquet(tmp_path: Path):
    def write(rows: list[dict[str, Any]], name: str = "data") -> str:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        return str(path)

    return write
