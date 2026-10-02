from collections.abc import Sequence
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

from dlt_altertable import api
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


def staged_files() -> dict[str, dict[str, bool]]:
    return destination_state().setdefault("staged_files", {})


def completed_staged_files() -> dict[str, list[str]]:
    files = staged_files()
    if not all(uploaded for shards in files.values() for uploaded in shards.values()):
        raise DestinationTerminalException("Cannot merge a load with incomplete staging uploads.")
    return {table_name: list(shards) for table_name, shards in files.items()}


def stage_file(config: AltertableClientConfiguration, path: object, table: TTableSchema) -> None:
    if not isinstance(path, str):
        raise TerminalValueError("Staged merges require a Parquet file path.")
    staged = staging_config(config, load_package_state()["load_id"])
    file_id = ParsedLoadJobFileName.parse(path).job_id()
    table_name = "_dlt_file_" + sha256(file_id.encode()).hexdigest()[:32]
    files = staged_files().setdefault(cast(str, table["name"]), {})
    files[table_name] = False
    commit_load_package_state()
    create_or_evolve_table(config, table, pq.read_schema(path))
    api.execute_sql(config, f"CREATE SCHEMA IF NOT EXISTS {qualified_schema_name(staged)}")
    with aligned_parquet(path, table) as aligned_path:
        api.post_parquet(
            staged,
            "upload",
            {
                "catalog": cast(str, staged.catalog),
                "schema": cast(str, staged.dataset_name),
                "table": table_name,
                "mode": "overwrite",
            },
            aligned_path,
            f"stage {table['name']}",
        )
    files[table_name] = True


def merge_statements(
    table_chain: Sequence[PreparedTableSchema],
    sql_client: AltertableSqlClient,
    files: dict[str, list[str]],
) -> list[str]:
    statements: list[str] = []
    root = table_chain[0]
    # dlt types its read-only column-hint helpers against the unprepared schema.
    root_schema = cast(TTableSchema, root)
    _, root_stage = sql_client.get_qualified_table_names(cast(str, root["name"]))
    keys = get_columns_names_with_prop(root_schema, "primary_key")
    quoted_keys = [escape_postgres_identifier(key) for key in keys]
    key_list = ", ".join(quoted_keys)
    dedup_sort = get_dedup_sort_tuple(root_schema)
    cursor = escape_postgres_identifier(dedup_sort[0]) if dedup_sort else None
    for table in table_chain:
        table_name = cast(str, table["name"])
        target, staging = sql_client.get_qualified_table_names(table_name)
        sources = files.get(table_name, [])
        source_sql = (
            " UNION ALL BY NAME ".join(
                f"SELECT * FROM {sql_client.get_qualified_table_names(name)[1]}" for name in sources
            )
            or f"SELECT * FROM {target} WHERE FALSE"
        )
        if table == root and len(table_chain) == 1:
            order = f"{cursor} DESC NULLS LAST" if cursor else key_list
            source_sql = (
                f"SELECT * FROM ({source_sql}) "
                f"QUALIFY row_number() OVER (PARTITION BY {key_list} ORDER BY {order}) = 1"
            )
        statements.append(f"CREATE OR REPLACE TABLE {staging} AS {source_sql};")
        if table == root:
            if len(table_chain) > 1:
                message = escape_duckdb_literal(
                    f"Table {root['name']}: nested upsert requires "
                    "one row per primary key per load."
                )
                statements.append(
                    f"SELECT CASE WHEN count(*) = count(DISTINCT row({key_list})) "
                    f"THEN TRUE ELSE error({message}) END FROM {staging};"
                )
            if cursor:
                matching = " AND ".join(f"s.{key} = d.{key}" for key in quoted_keys)
                newer = (
                    f"(s.{cursor} > d.{cursor} OR (d.{cursor} IS NULL AND s.{cursor} IS NOT NULL))"
                )
                statements.append(
                    f"DELETE FROM {staging} AS s WHERE EXISTS (SELECT 1 FROM {target} AS d "
                    f"WHERE {matching} AND NOT ({newer} IS TRUE));"
                )
        else:
            root_key = escape_postgres_identifier(
                MergeThenDeleteJob.get_root_key_col(table_chain, table, "", "")
            )
            root_id = escape_postgres_identifier(
                MergeThenDeleteJob.get_row_key_col(table_chain, root, "", "")
            )
            statements.append(
                f"DELETE FROM {staging} WHERE {root_key} NOT IN "
                f"(SELECT {root_id} FROM {root_stage});"
            )
    statements.extend(MergeThenDeleteJob.generate_sql(table_chain, sql_client))
    return statements
