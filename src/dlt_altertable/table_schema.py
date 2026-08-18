import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TColumnSchema

from dlt_altertable.api import execute_sql

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
    "wei": "DECIMAL(38,0)",
}

ARROW_TYPES = {
    "text": pa.string(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "json": pa.string(),
    "binary": pa.binary(),
}


def sql_type(column: TColumnSchema) -> str:
    if column["data_type"] == "decimal" and (precision := column.get("precision")) is not None:
        return f"DECIMAL({precision},{column.get('scale', 0)})"
    if column["data_type"] == "timestamp" and column.get("timezone") is False:
        return "TIMESTAMP"
    return SQL_TYPES[column["data_type"]]


def arrow_type(column: TColumnSchema) -> pa.DataType:
    if column["data_type"] == "timestamp":
        timezone = None if column.get("timezone") is False else "UTC"
        return pa.timestamp("us", tz=timezone)
    if column["data_type"] in ("decimal", "wei"):
        default_scale = 9 if column["data_type"] == "decimal" else 0
        return pa.decimal128(column.get("precision", 38), column.get("scale", default_scale))
    return ARROW_TYPES[column["data_type"]]


def create_table(
    base_url: str, auth: tuple[str, str], catalog: str, dataset_name: str, table: TTableSchema
) -> None:
    columns = ", ".join(f'"{name}" {sql_type(column)}' for name, column in table["columns"].items())
    execute_sql(base_url, auth, f'CREATE SCHEMA IF NOT EXISTS "{catalog}"."{dataset_name}"')
    execute_sql(
        base_url,
        auth,
        f'CREATE TABLE IF NOT EXISTS "{catalog}"."{dataset_name}"."{table["name"]}" ({columns})',
    )


def create_or_evolve_table(
    base_url: str, auth: tuple[str, str], catalog: str, dataset_name: str, table: TTableSchema
) -> bool:
    """Neither append nor upsert creates its target: the server fails an append on a missing table
    and runs an upsert as a MERGE, so the destination owns creation. A table left by an earlier
    load must also gain the columns that dlt's schema evolution added since.

    Returns whether the table already existed: a table just created from this file's schema must
    not be cached as evolved, because a later file of the same load may carry new columns."""
    rows = execute_sql(
        base_url,
        auth,
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog = '{catalog}' AND table_schema = '{dataset_name}' "
        f"AND table_name = '{table['name']}'",
    )
    existing = {row[0] for row in rows}
    if not existing:
        create_table(base_url, auth, catalog, dataset_name, table)
        return False
    for name, column in table["columns"].items():
        if name not in existing:
            execute_sql(
                base_url,
                auth,
                f'ALTER TABLE "{catalog}"."{dataset_name}"."{table["name"]}" '
                f'ADD COLUMN IF NOT EXISTS "{name}" {sql_type(column)}',
            )
    return True


def align_to_table_schema(parquet_file_path: str, table: TTableSchema) -> str | None:
    """The server appends by exact column match, but a file written before dlt evolved the
    load's schema can be narrower than the table, which is built from the load-level union.
    Returns the path of a padded copy, or None when the file already matches and can be
    posted verbatim."""
    if pq.read_schema(parquet_file_path).names == list(table["columns"]):
        return None
    data = pq.read_table(parquet_file_path)
    aligned = pa.table(
        {
            name: data.column(name)
            if name in data.column_names
            else pa.nulls(data.num_rows, type=arrow_type(column))
            for name, column in table["columns"].items()
        }
    )
    handle, aligned_path = tempfile.mkstemp(suffix=".parquet")
    os.close(handle)
    pq.write_table(aligned, aligned_path)
    return aligned_path
