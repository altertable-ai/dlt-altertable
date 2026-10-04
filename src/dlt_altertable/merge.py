from collections.abc import Mapping, Sequence
from copy import copy
from hashlib import sha256
from typing import cast, override

import pyarrow.parquet as pq
from dlt.common.data_writers.escape import escape_duckdb_literal, escape_postgres_identifier
from dlt.common.destination.client import PreparedTableSchema
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop, get_dedup_sort_tuple
from dlt.common.storages.load_package import (
    commit_load_package_state,
    destination_state,
    load_package_state,
)
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.destinations.sql_jobs import SqlMergeFollowupJob

from dlt_altertable import api as altertable_api
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.sql_client import AltertableSqlClient
from dlt_altertable.table_schema import (
    aligned_parquet,
    create_or_evolve_table,
    qualified_schema_name,
)


class MergeThenDeleteJob(SqlMergeFollowupJob):
    @classmethod
    @override
    def gen_upsert_merge_sql(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        primary_keys: Sequence[str],
        root_table_column_names: Sequence[str],
        hard_delete_col: str | None,
        deleted_cond: str | None,
        insert_only: bool = False,
        not_deleted_cond: str | None = None,
    ) -> list[str]:
        statements = super().gen_upsert_merge_sql(
            root_table_name,
            staging_root_table_name,
            primary_keys,
            root_table_column_names,
            hard_delete_col=None,
            deleted_cond=None,
        )
        if hard_delete_col:
            keys = ", ".join(primary_keys)
            statements.append(
                f"DELETE FROM {root_table_name} WHERE ({keys}) IN "
                f"(SELECT {keys} FROM {staging_root_table_name} WHERE {deleted_cond});"
            )
        return statements


def staging_config(
    config: AltertableClientConfiguration, load_id: str
) -> AltertableClientConfiguration:
    staged = copy(config)
    suffix = sha256(load_id.encode()).hexdigest()[:16]
    staged.dataset_name = f"{config.dataset_name}_dlt_staging_{suffix}"
    return staged


def staged_uploads() -> dict[str, dict[str, bool]]:
    return destination_state().setdefault("staged_files", {})


def completed_staging_tables() -> dict[str, list[str]]:
    uploads = staged_uploads()
    if not all(
        uploaded for table_uploads in uploads.values() for uploaded in table_uploads.values()
    ):
        raise DestinationTerminalException("Cannot merge a load with incomplete staging uploads.")
    return {table_name: list(table_uploads) for table_name, table_uploads in uploads.items()}


def stage_file(config: AltertableClientConfiguration, path: object, table: TTableSchema) -> None:
    if not isinstance(path, str):
        raise TerminalValueError("Staged merges require a Parquet file path.")
    staged = staging_config(config, load_package_state()["load_id"])
    file_id = ParsedLoadJobFileName.parse(path).job_id()
    staging_table_name = "_dlt_file_" + sha256(file_id.encode()).hexdigest()[:32]
    table_uploads = staged_uploads().setdefault(cast(str, table["name"]), {})
    table_uploads[staging_table_name] = False
    commit_load_package_state()
    create_or_evolve_table(config, table, pq.read_schema(path))
    altertable_api.execute_sql(
        config, f"CREATE SCHEMA IF NOT EXISTS {qualified_schema_name(staged)}"
    )
    with aligned_parquet(path, table) as aligned_path:
        altertable_api.post_parquet(
            staged,
            "upload",
            {
                "catalog": cast(str, staged.catalog),
                "schema": cast(str, staged.dataset_name),
                "table": staging_table_name,
                "mode": "overwrite",
            },
            aligned_path,
            f"stage {table['name']}",
        )
    table_uploads[staging_table_name] = True


def _staging_source_sql(
    table_name: str,
    sql_client: AltertableSqlClient,
    staging_tables: Mapping[str, Sequence[str]],
) -> str:
    target_table_name, _ = sql_client.get_qualified_table_names(table_name)
    return (
        " UNION ALL BY NAME ".join(
            f"SELECT * FROM {sql_client.get_qualified_table_names(name)[1]}"
            for name in staging_tables.get(table_name, ())
        )
        or f"SELECT * FROM {target_table_name} WHERE FALSE"
    )


def _root_staging_statements(
    table_chain: Sequence[PreparedTableSchema],
    sql_client: AltertableSqlClient,
    staging_tables: Mapping[str, Sequence[str]],
) -> list[str]:
    root_table = table_chain[0]
    root_table_name = cast(str, root_table["name"])
    target_table_name, staging_table_name = sql_client.get_qualified_table_names(root_table_name)
    column_hints: TTableSchema = {"columns": root_table["columns"]}
    primary_keys = [
        escape_postgres_identifier(key)
        for key in get_columns_names_with_prop(column_hints, "primary_key")
    ]
    primary_key_sql = ", ".join(primary_keys)
    dedup_sort = get_dedup_sort_tuple(column_hints)
    cursor_column = escape_postgres_identifier(dedup_sort[0]) if dedup_sort else None
    source_sql = _staging_source_sql(root_table_name, sql_client, staging_tables)
    if len(table_chain) == 1:
        order = f"{cursor_column} DESC NULLS LAST" if cursor_column else primary_key_sql
        source_sql = (
            f"SELECT * FROM ({source_sql}) "
            f"QUALIFY row_number() OVER (PARTITION BY {primary_key_sql} ORDER BY {order}) = 1"
        )
    statements = [f"CREATE OR REPLACE TABLE {staging_table_name} AS {source_sql};"]
    if len(table_chain) > 1:
        message = escape_duckdb_literal(
            f"Table {root_table_name}: nested upsert requires one row per primary key per load."
        )
        statements.append(
            f"SELECT CASE WHEN count(*) = count(DISTINCT row({primary_key_sql})) "
            f"THEN TRUE ELSE error({message}) END FROM {staging_table_name};"
        )
    if cursor_column:
        primary_key_match = " AND ".join(f"s.{key} = d.{key}" for key in primary_keys)
        incoming_is_newer = (
            f"(s.{cursor_column} > d.{cursor_column} "
            f"OR (d.{cursor_column} IS NULL AND s.{cursor_column} IS NOT NULL))"
        )
        statements.append(
            f"DELETE FROM {staging_table_name} AS s WHERE EXISTS "
            f"(SELECT 1 FROM {target_table_name} AS d "
            f"WHERE {primary_key_match} AND NOT ({incoming_is_newer} IS TRUE));"
        )
    return statements


def merge_statements(
    table_chain: Sequence[PreparedTableSchema],
    sql_client: AltertableSqlClient,
    staging_tables: Mapping[str, Sequence[str]],
) -> list[str]:
    statements = _root_staging_statements(table_chain, sql_client, staging_tables)
    root_table = table_chain[0]
    _, staging_root_table_name = sql_client.get_qualified_table_names(cast(str, root_table["name"]))
    for table in table_chain[1:]:
        table_name = cast(str, table["name"])
        _, staging_table_name = sql_client.get_qualified_table_names(table_name)
        source_sql = _staging_source_sql(table_name, sql_client, staging_tables)
        statements.append(f"CREATE OR REPLACE TABLE {staging_table_name} AS {source_sql};")
        root_key_column = escape_postgres_identifier(
            MergeThenDeleteJob.get_root_key_col(table_chain, table, "", "")
        )
        root_row_key_column = escape_postgres_identifier(
            MergeThenDeleteJob.get_row_key_col(table_chain, root_table, "", "")
        )
        statements.append(
            f"DELETE FROM {staging_table_name} WHERE {root_key_column} NOT IN "
            f"(SELECT {root_row_key_column} FROM {staging_root_table_name});"
        )
    statements.extend(MergeThenDeleteJob.generate_sql(table_chain, sql_client))
    return statements
