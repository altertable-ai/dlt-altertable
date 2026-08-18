import os

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from altertable_flightsql import Client
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    has_column_with_prop,
)
from pyarrow.flight import FlightError, FlightUnauthenticatedError

MERGE_STRATEGY_HINT = "x-merge-strategy"

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


def configured(value: str | None, parameter: str, env_var: str) -> str:
    if resolved := value or os.environ.get(env_var):
        return resolved
    raise DestinationTerminalException(
        f"{parameter} is not configured: pass {parameter}= to altertable(), set "
        f"destination.altertable.{parameter} in .dlt/secrets.toml, or export {env_var}."
    )


def primary_key_columns(table: TTableSchema) -> list[str]:
    return get_columns_names_with_prop(table, "primary_key")


def cursor_columns(table: TTableSchema) -> list[str]:
    dedup_sort = get_dedup_sort_tuple(table)
    return [dedup_sort[0]] if dedup_sort else []


def unsupported_merge_configuration(table: TTableSchema) -> str | None:
    strategy = table.get(MERGE_STRATEGY_HINT)
    if strategy not in (None, "upsert"):
        return f"merge strategy {strategy!r}"
    if has_column_with_prop(table, "merge_key"):
        return "merge_key"
    if has_column_with_prop(table, "hard_delete"):
        return "the hard_delete column hint"
    dedup_sort = get_dedup_sort_tuple(table)
    if dedup_sort and dedup_sort[1] != "desc":
        return f"dedup_sort {dedup_sort[1]!r} (the server keeps the highest value, use 'desc')"
    primary_key = primary_key_columns(table)
    if not primary_key:
        return "merge without a primary_key"
    if all(name in primary_key for name in table["columns"]):
        return "merge with primary-key columns only"
    return None


def incremental_options(table: TTableSchema) -> IngestIncrementalOptions | None:
    if table.get("write_disposition") != "merge":
        return None
    if unsupported := unsupported_merge_configuration(table):
        raise DestinationTerminalException(
            f"Table {table['name']}: {unsupported} is not supported by the Altertable "
            "destination, which runs merge as a server-side upsert on the primary_key."
        )
    return IngestIncrementalOptions(
        primary_key=primary_key_columns(table), cursor_field=cursor_columns(table)
    )


def ingest_mode(table: TTableSchema, table_already_replaced: bool) -> IngestTableMode:
    if table.get("write_disposition") != "replace":
        return IngestTableMode.CREATE_APPEND
    return IngestTableMode.APPEND if table_already_replaced else IngestTableMode.REPLACE


def tables_already_replaced() -> list[str]:
    return dlt.current.destination_state().setdefault("replaced_tables", [])


def tables_already_evolved() -> list[str]:
    return dlt.current.destination_state().setdefault("evolved_tables", [])


def declared_columns(table: TTableSchema, parquet_schema: pa.Schema) -> list[str]:
    """dlt writes _dlt_id and _dlt_load_id into the parquet file even when it hides them from the
    table schema, so the file is the wrong source of truth for what belongs in the lakehouse."""
    return [name for name in parquet_schema.names if name in table["columns"]]


def sql_type(column: TColumnSchema) -> str:
    if column["data_type"] == "decimal" and (precision := column.get("precision")) is not None:
        return f"DECIMAL({precision},{column.get('scale', 0)})"
    if column["data_type"] == "timestamp" and column.get("timezone") is False:
        return "TIMESTAMP"
    return SQL_TYPES[column["data_type"]]


def existing_column_names(
    client: Client, catalog: str, dataset_name: str, table_name: str
) -> set[str]:
    result = client.query(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog = '{catalog}' AND table_schema = '{dataset_name}' "
        f"AND table_name = '{table_name}'"
    ).read_all()
    return {column.as_py() for column in result.column("column_name")}


def add_new_columns(
    client: Client, catalog: str, dataset_name: str, table: TTableSchema, column_names: list[str]
) -> None:
    """The server appends by exact column match, so a table created by an earlier load must first
    gain the columns that dlt's schema evolution added since."""
    existing = existing_column_names(client, catalog, dataset_name, table["name"])
    if not existing:
        return
    for name in column_names:
        if name not in existing:
            client.execute(
                f'ALTER TABLE "{catalog}"."{dataset_name}"."{table["name"]}" '
                f'ADD COLUMN IF NOT EXISTS "{name}" {sql_type(table["columns"][name])}'
            )


@dlt.destination(
    name="altertable",
    naming_convention="direct",
    loader_file_format="parquet",
    batch_size=0,
    skip_dlt_columns_and_tables=True,
    max_table_nesting=0,
    loader_parallelism_strategy="table-sequential",
)
def altertable(
    parquet_file_path: str,
    table: TTableSchema,
    host: str | None = None,
    catalog: str | None = None,
    dataset_name: str | None = None,
    username: str | None = None,
    password: str | None = None,
    port: int | None = None,
    tls: bool | None = None,
) -> None:
    host = configured(host, "host", "ALTERTABLE_HOST")
    catalog = configured(catalog, "catalog", "ALTERTABLE_CATALOG")
    dataset_name = configured(dataset_name, "dataset_name", "ALTERTABLE_SCHEMA")
    username = configured(username, "username", "ALTERTABLE_USERNAME")
    password = configured(password, "password", "ALTERTABLE_PASSWORD")
    port = int(port) if port is not None else int(os.environ.get("ALTERTABLE_PORT", "443"))
    tls = tls if tls is not None else os.environ.get("ALTERTABLE_TLS", "true").lower() != "false"

    options = incremental_options(table)
    replaced_tables = tables_already_replaced()
    evolved_tables = tables_already_evolved()
    mode = ingest_mode(table, table["name"] in replaced_tables)

    try:
        with (
            pq.ParquetFile(parquet_file_path) as parquet_file,
            Client(username, password, host=host, port=port, tls=tls) as client,
        ):
            columns = declared_columns(table, parquet_file.schema_arrow)
            arrow_schema = pa.schema([parquet_file.schema_arrow.field(name) for name in columns])
            if mode is IngestTableMode.CREATE_APPEND and table["name"] not in evolved_tables:
                add_new_columns(client, catalog, dataset_name, table, columns)
                evolved_tables.append(table["name"])
            with client.ingest(
                table_name=table["name"],
                schema=arrow_schema,
                schema_name=dataset_name,
                catalog_name=catalog,
                mode=mode,
                incremental_options=options,
            ) as writer:
                for batch in parquet_file.iter_batches(columns=columns):
                    writer.write(batch)
    except FlightUnauthenticatedError as error:
        raise DestinationTerminalException(
            f"Authentication to {host}:{port} failed for user {username!r}: {error}"
        ) from error
    except FlightError as error:
        raise RuntimeError(
            f"Loading {catalog}.{dataset_name}.{table['name']} through {host}:{port} "
            f"failed: {error}"
        ) from error

    if mode is IngestTableMode.REPLACE:
        replaced_tables.append(table["name"])
