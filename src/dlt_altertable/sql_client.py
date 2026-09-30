from collections.abc import Generator, Iterator
from contextlib import closing, contextmanager, suppress
from typing import Any, AnyStr, cast

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.dataset import DBApiCursor
from dlt.common.libs.pyarrow import UnsupportedArrowTypeException, get_column_type_from_py_arrow
from dlt.common.schema.typing import TColumnSchema
from dlt.destinations.sql_client import DBApiCursorImpl, SqlClientBase

from dlt_altertable import api
from dlt_altertable.configuration import AltertableClientConfiguration


class ArrowTableCursor:
    def __init__(self, table: pa.Table) -> None:
        self.table: pa.Table | None = table
        self.row_offset = 0
        self.description = tuple(
            (field.name, field.type, None, None, None, None, field.nullable)
            for field in table.schema
        )

    def fetch_arrow(self, chunk_size: int | None = None) -> pa.Table:
        if self.table is None:
            raise RuntimeError("The query cursor is closed.")
        if chunk_size is not None and chunk_size <= 0:
            raise ValueError("chunk size must be greater than zero")
        result = self.table.slice(self.row_offset, chunk_size)
        self.row_offset += result.num_rows
        return result

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.fetchmany()

    def fetchmany(self, chunk_size: int | None = None) -> list[tuple[Any, ...]]:
        table = self.fetch_arrow(chunk_size)
        return list(zip(*(column.to_pylist() for column in table.columns), strict=True))

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Run queries through sql_client.execute_query().")

    def fetchone(self) -> tuple[Any, ...] | None:
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def close(self) -> None:
        self.table = None


class DltArrowCursor(DBApiCursorImpl):
    native_cursor: ArrowTableCursor

    def _set_default_schema_columns(self) -> None:
        super()._set_default_schema_columns()
        for name, arrow_type, *_, nullable in self.native_cursor.description:
            self.columns_schema[name]["nullable"] = nullable
            with suppress(UnsupportedArrowTypeException):
                self.columns_schema[name].update(
                    cast(TColumnSchema, get_column_type_from_py_arrow(arrow_type))
                )

    def iter_arrow(self, chunk_size: int | None) -> Generator[pa.Table, None, None]:
        if chunk_size is None:
            yield self.native_cursor.fetch_arrow()
            return
        while (table := self.native_cursor.fetch_arrow(chunk_size)).num_rows:
            yield table


class AltertableSqlClient(SqlClientBase[requests.Session | None]):
    def __init__(
        self,
        config: AltertableClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        self.config = config
        self._connection: requests.Session | None = None
        super().__init__(
            cast(str, config.catalog),
            cast(str, config.dataset_name),
            f"{config.dataset_name}_staging",
            capabilities,
        )

    def open_connection(self) -> requests.Session:
        self._connection = api.session
        return self._connection

    def close_connection(self) -> None:
        self._connection = None

    @property
    def native_connection(self) -> requests.Session | None:
        return self._connection

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> str:
        catalog = cast(str, self.database_name)
        if casefold:
            catalog = self.capabilities.casefold_identifier(catalog)
        return self.capabilities.escape_identifier(catalog) if quote else catalog

    def begin_transaction(self) -> Any:
        raise NotImplementedError("Altertable HTTP queries do not support transactions.")

    def commit_transaction(self) -> None:
        raise NotImplementedError("Altertable HTTP queries do not support transactions.")

    def rollback_transaction(self) -> None:
        raise NotImplementedError("Altertable HTTP queries do not support transactions.")

    @staticmethod
    def _make_database_exception(ex: Exception) -> Exception:
        return ex

    @staticmethod
    def _query_text(query: AnyStr, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        if args or kwargs:
            raise NotImplementedError("Altertable HTTP queries do not support parameter binding.")
        return query.decode("utf-8") if isinstance(query, bytes) else query

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> list[list]:
        statement = self._query_text(sql, args, kwargs)
        self._ensure_native_conn()
        return api.execute_sql(self.config, statement, dataset_name=self.dataset_name)

    @contextmanager
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        statement = self._query_text(query, args, kwargs)
        self._ensure_native_conn()
        response = api.post_query(
            self.config, statement, output_format="parquet", dataset_name=self.dataset_name
        )
        # ponytail: responses are buffered; use streaming Parquet files if result memory matters.
        table = pq.ParquetFile(pa.BufferReader(response.content)).read()
        # dlt's native cursor annotation includes conversions provided by this wrapper.
        with closing(DltArrowCursor(cast(DBApiCursor, ArrowTableCursor(table)))) as cursor:
            yield cursor
