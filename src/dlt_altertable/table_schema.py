import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common import logger
from dlt.common.data_writers.escape import escape_duckdb_literal, escape_postgres_identifier
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.client import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.libs.pyarrow import normalize_py_arrow_item
from dlt.common.normalizers.naming.direct import NamingConvention
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TColumnSchema

from dlt_altertable import api
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.layout import partition_expressions

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
        raise TerminalValueError(
            f"Column {column['name']} has type wei, which needs 78 digits while DuckDB "
            "decimals stop at 38."
        )
    if column["data_type"] == "decimal" and (precision := column.get("precision")) is not None:
        scale = column.get("scale") or 0
        if not (1 <= precision <= 38 and 0 <= scale <= precision):
            raise TerminalValueError(
                f"Column {column['name']} has invalid decimal precision or scale: "
                f"DECIMAL({precision},{scale}). DuckDB requires precision between 1 and 38 "
                "and scale between 0 and precision."
            )
        return f"DECIMAL({precision},{scale})"
    if column["data_type"] == "timestamp" and column.get("timezone") is False:
        return "TIMESTAMP_NS" if (column.get("precision") or 6) > 6 else "TIMESTAMP"
    return SQL_TYPES[cast(str, column["data_type"])]


def qualified_schema_name(config: AltertableClientConfiguration) -> str:
    return ".".join(
        escape_postgres_identifier(part)
        for part in cast(tuple[str, str], (config.catalog, config.dataset_name))
    )


def qualified_table_name(config: AltertableClientConfiguration, table_name: str) -> str:
    return f"{qualified_schema_name(config)}.{escape_postgres_identifier(table_name)}"


def apply_partitioning(
    config: AltertableClientConfiguration, table: TTableSchema | PreparedTableSchema
) -> None:
    expressions = partition_expressions(table)
    if expressions is None:
        return
    clause = (
        f"SET PARTITIONED BY ({', '.join(expressions)})" if expressions else "RESET PARTITIONED BY"
    )
    api.execute_sql(
        config, f"ALTER TABLE {qualified_table_name(config, cast(str, table['name']))} {clause}"
    )


def create_table(
    config: AltertableClientConfiguration,
    table: TTableSchema | PreparedTableSchema,
    column_types: dict[str, str],
) -> None:
    table_name = cast(str, table["name"])
    columns = ", ".join(
        f"{escape_postgres_identifier(name)} {data_type}"
        for name, data_type in column_types.items()
    )
    api.execute_sql(
        config,
        f"CREATE SCHEMA IF NOT EXISTS {qualified_schema_name(config)}",
    )
    api.execute_sql(
        config,
        f"CREATE TABLE IF NOT EXISTS {qualified_table_name(config, table_name)} ({columns})",
    )
    apply_partitioning(config, table)
    logger.info(
        f"Created table {config.catalog}.{config.dataset_name}.{table_name} "
        f"with {len(table['columns'])} columns"
    )


def create_or_evolve_table(
    config: AltertableClientConfiguration,
    table: TTableSchema | PreparedTableSchema,
    parquet_schema: pa.Schema | None = None,
) -> bool:
    """Creating the table from dlt's typed schema keeps column types and later evolution
    deliberate instead of whatever the server would infer from the first file. A table left by
    an earlier load must also gain the columns that dlt's schema evolution added since.

    Returns whether the lookup found an existing table, the only answer worth caching for the
    rest of the load: a CREATE is never read back to confirm what the table now holds."""
    column_types = {name: sql_type(column) for name, column in table["columns"].items()}
    if parquet_schema is not None:
        for field in parquet_schema:
            if field.name in column_types and pa.types.is_uint64(field.type):
                column_types[field.name] = "DECIMAL(20,0)"
    partition_expressions(table)
    table_name = cast(str, table["name"])
    rows = api.execute_sql(
        config,
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_catalog = {escape_duckdb_literal(config.catalog)} "
        f"AND table_schema = {escape_duckdb_literal(config.dataset_name)} "
        f"AND table_name = {escape_duckdb_literal(table_name)}",
    )
    existing = dict(rows)
    if not existing:
        create_table(config, table, column_types)
        return False
    for name, data_type in column_types.items():
        if name not in existing:
            api.execute_sql(
                config,
                f"ALTER TABLE {qualified_table_name(config, table_name)} "
                f"ADD COLUMN IF NOT EXISTS {escape_postgres_identifier(name)} {data_type}",
            )
            logger.info(
                f"Added column {name} to {config.catalog}.{config.dataset_name}.{table_name}"
            )
        elif existing[name] == "BIGINT" and data_type == "DECIMAL(20,0)":
            api.execute_sql(
                config,
                f"ALTER TABLE {qualified_table_name(config, table_name)} "
                f"ALTER COLUMN {escape_postgres_identifier(name)} TYPE DECIMAL(20,0)",
            )
    apply_partitioning(config, table)
    return True


@contextmanager
def aligned_parquet(parquet_file_path: str, table: TTableSchema) -> Iterator[str]:
    """Pad Parquet files with typed NULL columns to match the current table schema."""
    if pq.read_schema(parquet_file_path).names == list(table["columns"]):
        yield parquet_file_path
        return
    aligned = normalize_py_arrow_item(
        pq.read_table(parquet_file_path),
        table["columns"],
        NamingConvention(),
        DestinationCapabilitiesContext.generic_capabilities(),
    )
    with tempfile.TemporaryDirectory() as aligned_dir:
        aligned_path = os.path.join(aligned_dir, "aligned.parquet")
        pq.write_table(aligned, aligned_path)
        yield aligned_path
