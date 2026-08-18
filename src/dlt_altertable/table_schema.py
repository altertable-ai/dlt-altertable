import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager

import pyarrow.parquet as pq
from dlt.common import logger
from dlt.common.configuration.container import Container
from dlt.common.data_writers.escape import escape_duckdb_literal, escape_postgres_identifier
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.libs.pyarrow import normalize_py_arrow_item
from dlt.common.normalizers.naming.direct import NamingConvention
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TColumnSchema

from dlt_altertable.api import execute_sql
from dlt_altertable.configuration import AltertableClientConfiguration

SQL_TYPES = {
    "text": "VARCHAR",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP WITH TIME ZONE",
    "date": "DATE",
    "time": "TIME",
    "json": "VARCHAR",
    "binary": "BLOB",
    "decimal": "DECIMAL(38,9)",
}


def sql_type(column: TColumnSchema) -> str:
    if column["data_type"] == "wei":
        raise DestinationTerminalException(
            f"Column {column['name']} has type wei, which needs 76 digits while DuckDB "
            "decimals stop at 38."
        )
    if column["data_type"] == "decimal" and (precision := column.get("precision")) is not None:
        return f"DECIMAL({precision},{column.get('scale', 0)})"
    if column["data_type"] == "timestamp" and column.get("timezone") is False:
        return "TIMESTAMP"
    return SQL_TYPES[column["data_type"]]


def destination_capabilities() -> DestinationCapabilitiesContext:
    try:
        return Container()[DestinationCapabilitiesContext]
    except Exception:
        return DestinationCapabilitiesContext.generic_capabilities()


def qualified_table_name(config: AltertableClientConfiguration, table_name: str) -> str:
    return ".".join(
        escape_postgres_identifier(part)
        for part in (config.catalog, config.dataset_name, table_name)
    )


def create_table(config: AltertableClientConfiguration, table: TTableSchema) -> None:
    columns = ", ".join(
        f"{escape_postgres_identifier(name)} {sql_type(column)}"
        for name, column in table["columns"].items()
    )
    execute_sql(
        config,
        "CREATE SCHEMA IF NOT EXISTS "
        f"{escape_postgres_identifier(config.catalog)}"
        f".{escape_postgres_identifier(config.dataset_name)}",
    )
    execute_sql(
        config,
        f"CREATE TABLE IF NOT EXISTS {qualified_table_name(config, table['name'])} ({columns})",
    )
    logger.info(
        f"Created table {config.catalog}.{config.dataset_name}.{table['name']} "
        f"with {len(table['columns'])} columns"
    )


def create_or_evolve_table(config: AltertableClientConfiguration, table: TTableSchema) -> bool:
    """Neither append nor upsert creates its target, so the destination owns creation. A table
    left by an earlier load must also gain the columns that dlt's schema evolution added since.

    Returns whether the table already existed: a table just created from this file's schema must
    not be cached as evolved, because a later file of the same load may carry new columns."""
    rows = execute_sql(
        config,
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog = {escape_duckdb_literal(config.catalog)} "
        f"AND table_schema = {escape_duckdb_literal(config.dataset_name)} "
        f"AND table_name = {escape_duckdb_literal(table['name'])}",
    )
    existing = {row[0] for row in rows}
    if not existing:
        create_table(config, table)
        return False
    for name, column in table["columns"].items():
        if name not in existing:
            execute_sql(
                config,
                f"ALTER TABLE {qualified_table_name(config, table['name'])} "
                f"ADD COLUMN IF NOT EXISTS {escape_postgres_identifier(name)} {sql_type(column)}",
            )
            logger.info(
                f"Added column {name} to {config.catalog}.{config.dataset_name}.{table['name']}"
            )
    return True


@contextmanager
def aligned_parquet(parquet_file_path: str, table: TTableSchema) -> Iterator[str]:
    """The server appends by exact column match, and dlt can evolve the schema in the middle of
    a load, so an earlier file may carry fewer columns than the table the load builds. Yields
    the file itself when it already matches, otherwise a temporary copy padded with typed NULL
    columns. Deletable once the server appends by column name."""
    if pq.read_schema(parquet_file_path).names == list(table["columns"]):
        yield parquet_file_path
        return
    aligned = normalize_py_arrow_item(
        pq.read_table(parquet_file_path),
        table["columns"],
        NamingConvention(),
        destination_capabilities(),
    )
    handle, aligned_path = tempfile.mkstemp(suffix=".parquet")
    os.close(handle)
    pq.write_table(aligned, aligned_path)
    try:
        yield aligned_path
    finally:
        os.unlink(aligned_path)
