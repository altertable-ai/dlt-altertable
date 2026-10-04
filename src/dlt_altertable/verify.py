from typing import Any, Final

import dlt
from dlt.common.data_writers.escape import escape_duckdb_literal, escape_postgres_identifier
from dlt.common.destination.reference import AnyDestination_CO
from dlt.common.pipeline import LoadInfo, NormalizeInfo
from dlt.common.schema import TSchemaTables, TTableSchema
from dlt.common.schema.typing import C_DLT_LOAD_ID, TWriteDisposition
from dlt.common.schema.utils import (
    DEFAULT_WRITE_DISPOSITION,
    fill_hints_from_parent_and_clone_table,
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    get_first_column_name_with_prop,
    has_column_with_prop,
)
from dlt.common.utils import merge_row_counts

from dlt_altertable import api as altertable_api
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.destination import altertable, primary_key_columns
from dlt_altertable.table_schema import qualified_table_name

MERGE: Final[TWriteDisposition] = "merge"
REPLACE: Final[TWriteDisposition] = "replace"
# DuckLake attaches its own metadata database beside every catalog it opens
DUCKLAKE_METADATA_PREFIX = "__ducklake_metadata_"


def resolved_config(destination: AnyDestination_CO) -> AltertableClientConfiguration:
    if not issubclass(destination.spec, AltertableClientConfiguration):
        raise TypeError(
            f"{destination.destination_type} is not an Altertable destination, so it has no "
            "lakehouse to read a load back from"
        )
    return destination.configuration(destination.spec())


def verify_catalog(**options: Any) -> list[str]:
    """Report why the configured catalog cannot be loaded into."""
    config = resolved_config(altertable(**options))
    attached = altertable_api.execute_sql(
        config,
        "SELECT database_name, readonly FROM duckdb_databases() "
        "WHERE NOT internal AND database_name <> 'memory'",
    )

    catalogs = dict(attached)
    loadable = {
        name: readonly
        for name, readonly in catalogs.items()
        if not name.startswith(DUCKLAKE_METADATA_PREFIX)
    }
    if config.catalog in catalogs and config.catalog not in loadable:
        return [
            f"catalog {config.catalog!r} is the DuckLake metadata store behind "
            f"{config.catalog.removeprefix(DUCKLAKE_METADATA_PREFIX)!r}, not a catalog to load "
            "into: writing tables there corrupts the bookkeeping the lakehouse reads"
        ]
    if config.catalog not in loadable:
        return [
            f"catalog {config.catalog!r} is not attached to {config.host}, which serves "
            f"{sorted(loadable)}"
        ]
    if loadable[config.catalog]:
        return [f"catalog {config.catalog!r} is attached read only, so a load cannot write to it"]
    return []


def existing_table_names(config: AltertableClientConfiguration) -> set[str]:
    rows = altertable_api.execute_sql(
        config,
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog = {escape_duckdb_literal(config.catalog)} "
        f"AND table_schema = {escape_duckdb_literal(config.dataset_name)}",
    )
    return {row[0] for row in rows}


def tables_in_load(load_info: LoadInfo) -> dict[str, list[TTableSchema]]:
    table_definitions_by_name: dict[str, list[TTableSchema]] = {}
    for package in load_info.load_packages:
        names = {job.job_file_info.table_name for jobs in package.jobs.values() for job in jobs}
        for name in names:
            table_definitions_by_name.setdefault(name, []).append(
                fill_hints_from_parent_and_clone_table(
                    package.schema.tables, package.schema.tables[name]
                )
            )
    return table_definitions_by_name


def normalized_row_counts(
    normalizations: list[NormalizeInfo], loads_ids: list[str]
) -> dict[str, int] | None:
    """dlt keeps every step of separately called pipeline stages in one trace, so the newest
    normalization is not necessarily the one that produced the load being verified. Reading its
    counts would measure a package this load never carried. Normalization metrics are keyed by
    load id, one entry per id, which is what ties a count back to the load it belongs to."""
    counts: dict[str, int] = {}
    for normalization in normalizations:
        for load_id in loads_ids:
            if load_id not in normalization.metrics:
                continue
            table_metrics = normalization.metrics[load_id][0]["table_metrics"]
            merge_row_counts(
                counts, {name: table.items_count for name, table in table_metrics.items()}
            )
    return counts or None


def load_context(
    pipeline: dlt.Pipeline,
) -> tuple[LoadInfo, dict[str, int] | None, AltertableClientConfiguration]:
    trace = pipeline.last_trace
    load_info = trace.last_load_info if trace is not None else None
    if load_info is None or not load_info.loads_ids:
        raise ValueError(
            "verify_load reads the load step of a pipeline.run() from the pipeline trace, "
            "and found none."
        )
    normalizations = [
        step.step_info for step in trace.steps if isinstance(step.step_info, NormalizeInfo)
    ]
    row_counts = normalized_row_counts(normalizations, load_info.loads_ids)
    return load_info, row_counts, resolved_config(pipeline.destination)


def table_contract(
    table_definitions: list[TTableSchema],
) -> tuple[TWriteDisposition, list[str], bool] | None:
    if len(table_definitions) != 1:
        return None
    table = table_definitions[0]
    disposition = table.get("write_disposition") or DEFAULT_WRITE_DISPOSITION
    key_columns = primary_key_columns(table) if disposition in (MERGE, REPLACE) else []
    if table.get("parent") and disposition in (MERGE, REPLACE):
        key_columns = get_columns_names_with_prop(table, "row_key")
    upsert_skips_stale_rows = disposition == MERGE and bool(
        get_dedup_sort_tuple(table)
        or has_column_with_prop(table, "hard_delete")
        or table.get("parent")
    )
    return disposition, key_columns, upsert_skips_stale_rows


def table_load_predicate(
    config: AltertableClientConfiguration,
    table: TTableSchema,
    tables: TSchemaTables,
    load_id_predicate: str,
) -> str | None:
    if C_DLT_LOAD_ID in table["columns"]:
        return load_id_predicate
    if not (parent_name := table.get("parent")):
        return None
    parent = tables[parent_name]
    parent_key = get_first_column_name_with_prop(table, "parent_key")
    row_key = get_first_column_name_with_prop(parent, "row_key")
    predicate = table_load_predicate(config, parent, tables, load_id_predicate)
    if not parent_key or not row_key or not predicate:
        return None
    return (
        f"{escape_postgres_identifier(parent_key)} IN "
        f"(SELECT {escape_postgres_identifier(row_key)} "
        f"FROM {qualified_table_name(config, parent_name)} WHERE {predicate})"
    )


def count_query(
    qualified_table: str,
    disposition: TWriteDisposition,
    key_columns: list[str],
    load_id_predicate: str,
) -> str:
    if not key_columns:
        where = "" if disposition == REPLACE else f" WHERE {load_id_predicate}"
        return f"SELECT count(*) FROM {qualified_table}{where}"

    keys = [escape_postgres_identifier(column) for column in key_columns]
    loaded_keys = [f"loaded.{key}" for key in keys]
    valid_key = " AND ".join(f"{key} IS NOT NULL" for key in loaded_keys)
    select = (
        f"SELECT count(*), count(DISTINCT row({', '.join(loaded_keys)})) "
        f"FILTER (WHERE {valid_key}) FROM {qualified_table} AS loaded"
    )
    if disposition == REPLACE:
        return select

    matching = " AND ".join(f"loaded.{key} IS NOT DISTINCT FROM current_load.{key}" for key in keys)
    return (
        f"{select} SEMI JOIN "
        f"(SELECT {', '.join(keys)} FROM {qualified_table} WHERE {load_id_predicate}) "
        f"AS current_load ON {matching}"
    )


def count_problems(
    table_name: str,
    disposition: TWriteDisposition,
    key_columns: list[str],
    normalized_rows: int | None,
    lakehouse_counts: list[Any],
    upsert_skips_stale_rows: bool,
) -> list[str]:
    row_count = lakehouse_counts[0]
    problems: list[str] = []
    if key_columns and row_count != lakehouse_counts[1]:
        problems.append(
            f"{table_name}: {disposition} left {row_count} rows, but the distinct, "
            f"non-null primary-key count is {lakehouse_counts[1]}"
        )

    if normalized_rows is None:
        return problems
    nothing_landed_under_this_load_id = normalized_rows > 0 and row_count == 0
    if disposition == MERGE and nothing_landed_under_this_load_id and not upsert_skips_stale_rows:
        problems.append(
            f"{table_name}: dlt normalized {normalized_rows} rows, none landed with this load id"
        )
    elif disposition != MERGE and row_count != normalized_rows:
        problems.append(f"{table_name}: dlt normalized {normalized_rows} rows, {row_count} landed")
    return problems


def verify_table(
    config: AltertableClientConfiguration,
    landed_table_names: set[str],
    table_name: str,
    table_definitions: list[TTableSchema],
    normalized_rows: int | None,
    load_id_predicate: str | None,
) -> list[str]:
    contract = table_contract(table_definitions)
    if contract is None:
        return [
            f"{table_name}: several load packages include this table, so it cannot be reconciled"
        ]
    if table_name not in landed_table_names:
        return [f"{table_name}: the load includes this table, but it does not exist"]

    disposition, key_columns, upsert_skips_stale_rows = contract
    if disposition != REPLACE and load_id_predicate is None:
        return [
            f"{table_name}: this table has no _dlt_load_id column, so this load cannot be "
            "reconciled. For Arrow inputs, enable NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID "
            "before loading."
        ]
    problems: list[str] = []
    if normalized_rows is None:
        problems.append(
            f"{table_name}: this run loaded a package it did not normalize, "
            "so it carries no row count to compare"
        )
        if not key_columns:
            return problems
    query = count_query(
        qualified_table_name(config, table_name), disposition, key_columns, load_id_predicate or ""
    )
    lakehouse_counts = altertable_api.execute_sql(config, query)[0]
    problems.extend(
        count_problems(
            table_name,
            disposition,
            key_columns,
            normalized_rows,
            lakehouse_counts,
            upsert_skips_stale_rows,
        )
    )
    return problems


def verify_load(pipeline: dlt.Pipeline) -> list[str]:
    """Reads the last load back and reports missing rows, tables, or primary-key integrity."""
    load_info, normalized_row_counts, config = load_context(pipeline)
    table_definitions_by_name = tables_in_load(load_info)
    schema_tables: TSchemaTables = {
        name: table
        for package in load_info.load_packages
        for name, table in package.schema.tables.items()
    }
    load_id_literals = ", ".join(escape_duckdb_literal(load_id) for load_id in load_info.loads_ids)
    load_id_predicate = f"{escape_postgres_identifier(C_DLT_LOAD_ID)} IN ({load_id_literals})"

    problems = ["dlt recorded failed load jobs"] if load_info.has_failed_jobs else []
    landed_table_names = existing_table_names(config)
    for table_name, table_definitions in sorted(table_definitions_by_name.items()):
        normalized_rows = (
            normalized_row_counts.get(table_name) if normalized_row_counts is not None else None
        )
        problems.extend(
            verify_table(
                config,
                landed_table_names,
                table_name,
                table_definitions,
                normalized_rows,
                table_load_predicate(
                    config, schema_tables[table_name], schema_tables, load_id_predicate
                ),
            )
        )
    return problems
