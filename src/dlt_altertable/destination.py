import json
import os
import tempfile

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    has_column_with_prop,
)
from dlt.common.typing import TSecretStrValue

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

UPLOAD_TIMEOUT = (30, 3600)
QUERY_TIMEOUT = (10, 300)
TERMINAL_STATUSES = {400, 401, 402, 403, 404, 405}


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
    if not primary_key_columns(table):
        return "merge without a primary_key"
    return None


def merge_params(table: TTableSchema) -> dict[str, str] | None:
    if table.get("write_disposition") != "merge":
        return None
    if unsupported := unsupported_merge_configuration(table):
        raise DestinationTerminalException(
            f"Table {table['name']}: {unsupported} is not supported by the Altertable "
            "destination, which runs merge as a server-side upsert on the primary_key."
        )
    key_columns = [*primary_key_columns(table), *cursor_columns(table)]
    if invalid := [column for column in key_columns if "," in column]:
        raise DestinationTerminalException(
            f"Table {table['name']}: column names {invalid} contain a comma, which the "
            "comma-separated primary_key and cursor_field parameters cannot express."
        )
    params = {"primary_key": ",".join(primary_key_columns(table))}
    if cursor := cursor_columns(table):
        params["cursor_field"] = ",".join(cursor)
    return params


def upload_mode(table: TTableSchema, table_already_replaced: bool) -> str:
    if table.get("write_disposition") == "replace" and not table_already_replaced:
        return "overwrite"
    return "append"


def tables_already_replaced() -> list[str]:
    return dlt.current.destination_state().setdefault("replaced_tables", [])


def tables_already_evolved() -> list[str]:
    return dlt.current.destination_state().setdefault("evolved_tables", [])


class ChunkedFileReader:
    """Streams a file to requests in 1MiB reads, bypassing urllib3's 16KiB send loop (measured
    31% faster on loopback). Exposing `len` keeps the upload Content-Length framed."""

    def __init__(self, file) -> None:
        self.file = file
        self.len = os.fstat(file.fileno()).st_size

    def read(self, size: int = -1) -> bytes:
        return self.file.read(1 << 20)


def raise_for_failure(response: requests.Response, action: str) -> None:
    if response.status_code == 200:
        return
    detail = f"{action} failed with HTTP {response.status_code}: {response.text.strip()}"
    if response.status_code in TERMINAL_STATUSES:
        raise DestinationTerminalException(detail)
    raise RuntimeError(detail)


def execute_sql(base_url: str, auth: tuple[str, str], statement: str) -> list[list]:
    response = requests.post(
        f"{base_url}/query",
        json={"statement": statement, "ephemeral": True, "compute_size": "XS"},
        auth=auth,
        timeout=QUERY_TIMEOUT,
    )
    raise_for_failure(response, f"query {statement!r}")
    payload = [json.loads(line) for line in response.text.splitlines() if line.strip()]
    for entry in payload:
        if isinstance(entry, dict) and "error" in entry:
            raise RuntimeError(f"query {statement!r} failed mid-stream: {entry['error']}")
    return payload[2:]


def sql_type(column: TColumnSchema) -> str:
    if column["data_type"] == "decimal" and (precision := column.get("precision")) is not None:
        return f"DECIMAL({precision},{column.get('scale', 0)})"
    if column["data_type"] == "timestamp" and column.get("timezone") is False:
        return "TIMESTAMP"
    return SQL_TYPES[column["data_type"]]


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


def arrow_type(column: TColumnSchema) -> pa.DataType:
    if column["data_type"] == "timestamp":
        timezone = None if column.get("timezone") is False else "UTC"
        return pa.timestamp("us", tz=timezone)
    if column["data_type"] in ("decimal", "wei"):
        default_scale = 9 if column["data_type"] == "decimal" else 0
        return pa.decimal128(column.get("precision", 38), column.get("scale", default_scale))
    return ARROW_TYPES[column["data_type"]]


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


def sync_table_schema(
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


@dlt.destination(
    name="altertable",
    naming_convention="direct",
    loader_file_format="parquet",
    batch_size=0,
    skip_dlt_columns_and_tables=False,
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
    password: TSecretStrValue | None = None,
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

    base_url = f"{'https' if tls else 'http'}://{host}:{port}"
    auth = (username, password)
    upsert_params = merge_params(table)

    replaced_tables = tables_already_replaced()
    evolved_tables = tables_already_evolved()
    mode = upload_mode(table, table["name"] in replaced_tables)

    if mode != "overwrite" and table["name"] not in evolved_tables:
        if sync_table_schema(base_url, auth, catalog, dataset_name, table):
            evolved_tables.append(table["name"])

    params = {"catalog": catalog, "schema": dataset_name, "table": table["name"]}
    if upsert_params is not None:
        endpoint = "upsert"
        params |= upsert_params
    else:
        endpoint = "upload"
        params["mode"] = mode

    aligned_path = align_to_table_schema(parquet_file_path, table)
    try:
        with open(aligned_path or parquet_file_path, "rb") as parquet_file:
            response = requests.post(
                f"{base_url}/{endpoint}",
                params=params,
                data=ChunkedFileReader(parquet_file),
                auth=auth,
                headers={"Content-Type": "application/parquet"},
                timeout=UPLOAD_TIMEOUT,
            )
    finally:
        if aligned_path:
            os.unlink(aligned_path)
    raise_for_failure(response, f"loading {catalog}.{dataset_name}.{table['name']}")

    if mode == "overwrite":
        replaced_tables.append(table["name"])
