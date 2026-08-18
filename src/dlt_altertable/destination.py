import os

import dlt
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    has_column_with_prop,
)
from dlt.common.typing import TSecretStrValue

from dlt_altertable.api import post_parquet
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.table_schema import align_to_table_schema, create_or_evolve_table


def primary_key_columns(table: TTableSchema) -> list[str]:
    return get_columns_names_with_prop(table, "primary_key")


def dedup_sort_column(table: TTableSchema) -> str | None:
    dedup_sort = get_dedup_sort_tuple(table)
    return dedup_sort[0] if dedup_sort else None


def unsupported_merge_configuration(table: TTableSchema) -> str | None:
    strategy = table.get("x-merge-strategy")
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


def upsert_params(table: TTableSchema) -> dict[str, str] | None:
    if table.get("write_disposition") != "merge":
        return None
    if unsupported := unsupported_merge_configuration(table):
        raise DestinationTerminalException(
            f"Table {table['name']}: {unsupported} is not supported by the Altertable "
            "destination, which runs merge as a server-side upsert on the primary_key."
        )
    cursor = dedup_sort_column(table)
    key_columns = [*primary_key_columns(table), *([cursor] if cursor else [])]
    if invalid := [column for column in key_columns if "," in column]:
        raise DestinationTerminalException(
            f"Table {table['name']}: column names {invalid} contain a comma, which the "
            "comma-separated primary_key and cursor_field parameters cannot express."
        )
    params = {"primary_key": ",".join(primary_key_columns(table))}
    if cursor:
        params["cursor_field"] = cursor
    return params


def upload_mode(table: TTableSchema, table_already_replaced: bool) -> str:
    if table.get("write_disposition") == "replace" and not table_already_replaced:
        return "overwrite"
    return "append"


def replaced_tables() -> list[str]:
    return dlt.current.destination_state().setdefault("replaced_tables", [])


def evolved_tables() -> list[str]:
    return dlt.current.destination_state().setdefault("evolved_tables", [])


@dlt.destination(
    name="altertable",
    naming_convention="direct",
    loader_file_format="parquet",
    batch_size=0,
    skip_dlt_columns_and_tables=False,
    max_table_nesting=0,
    loader_parallelism_strategy="table-sequential",
    spec=AltertableClientConfiguration,
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
    config = AltertableClientConfiguration(
        host=host,
        catalog=catalog,
        dataset_name=dataset_name,
        username=username,
        password=password,
        port=port,
        tls=tls,
    )
    config.on_resolved()
    catalog = config.catalog
    dataset_name = config.dataset_name
    base_url = config.base_url
    auth = config.basic_auth
    upsert = upsert_params(table)

    already_replaced = replaced_tables()
    already_evolved = evolved_tables()
    mode = upload_mode(table, table["name"] in already_replaced)

    if mode != "overwrite" and table["name"] not in already_evolved:
        if create_or_evolve_table(base_url, auth, catalog, dataset_name, table):
            already_evolved.append(table["name"])

    params = {"catalog": catalog, "schema": dataset_name, "table": table["name"]}
    if upsert is not None:
        endpoint = "upsert"
        params |= upsert
    else:
        endpoint = "upload"
        params["mode"] = mode

    aligned_path = align_to_table_schema(parquet_file_path, table)
    try:
        post_parquet(
            base_url,
            auth,
            endpoint,
            params,
            aligned_path or parquet_file_path,
            f"loading {catalog}.{dataset_name}.{table['name']}",
        )
    finally:
        if aligned_path:
            os.unlink(aligned_path)

    if mode == "overwrite":
        already_replaced.append(table["name"])
