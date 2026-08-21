from typing import Literal, cast

import dlt
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    has_column_with_prop,
)

from dlt_altertable.api import post_parquet
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.table_schema import aligned_parquet, create_or_evolve_table

type IngestMode = Literal["append", "overwrite", "upsert"]


def primary_key_columns(table: TTableSchema) -> list[str]:
    return get_columns_names_with_prop(table, "primary_key")


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
    primary_keys = primary_key_columns(table)
    if nullable_primary_key_columns := [
        name for name in primary_keys if table["columns"][name].get("nullable")
    ]:
        raise DestinationTerminalException(
            f"Table {table['name']}: primary_key columns must be non-nullable: "
            f"{', '.join(nullable_primary_key_columns)}."
        )
    dedup_sort = get_dedup_sort_tuple(table)
    cursor = dedup_sort[0] if dedup_sort else None
    key_columns = [*primary_keys, *([cursor] if cursor else [])]
    if invalid := [column for column in key_columns if "," in column]:
        raise DestinationTerminalException(
            f"Table {table['name']}: column names {invalid} contain a comma, which the "
            "comma-separated primary_key and cursor_field parameters cannot express."
        )
    params = {"primary_key": ",".join(primary_keys)}
    if cursor:
        params["cursor_field"] = cursor
    return params


def upload_mode(table: TTableSchema, table_already_replaced: bool) -> IngestMode:
    if table.get("write_disposition") == "replace" and not table_already_replaced:
        return "overwrite"
    return "append"


def replaced_tables() -> list[str]:
    return dlt.current.destination_state().setdefault("replaced_tables", [])


def evolved_tables() -> list[str]:
    return dlt.current.destination_state().setdefault("evolved_tables", [])


# batch_size=0 hands each load job a file path, the one branch of dlt's `TDataItems | str` argument.
@dlt.destination(  # ty: ignore[invalid-argument-type]
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
    config: AltertableClientConfiguration = dlt.config.value,
) -> None:
    upsert = upsert_params(table)
    table_name = cast(str, table["name"])

    already_replaced = replaced_tables()
    already_evolved = evolved_tables()
    ingest_mode: IngestMode = (
        "upsert" if upsert is not None else upload_mode(table, table_name in already_replaced)
    )

    if ingest_mode != "overwrite" and table_name not in already_evolved:
        if create_or_evolve_table(config, table):
            already_evolved.append(table_name)

    params = {
        "catalog": cast(str, config.catalog),
        "schema": cast(str, config.dataset_name),
        "table": table_name,
    }
    action = f"{ingest_mode} {config.catalog}.{config.dataset_name}.{table_name}"
    if upsert is not None:
        post_parquet(config, "upsert", params | upsert, parquet_file_path, action)
    else:
        params["mode"] = ingest_mode
        with aligned_parquet(parquet_file_path, table) as upload_path:
            post_parquet(config, "upload", params, upload_path, action)

    if ingest_mode == "overwrite":
        already_replaced.append(table_name)
