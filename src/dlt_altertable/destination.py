from typing import TYPE_CHECKING, Any, Literal, cast

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common.configuration import ConfigurationValueError
from dlt.common.data_writers.escape import escape_duckdb_literal, escape_postgres_identifier
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema import TTableSchema
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    has_column_with_prop,
)
from dlt.destinations.impl.destination.factory import destination as CustomDestination

from dlt_altertable import api
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.table_schema import aligned_parquet, create_or_evolve_table

if TYPE_CHECKING:
    from dlt_altertable.job_client import AltertableJobClient


type IngestMode = Literal["append", "overwrite", "upsert"]

DEFAULT_UPLOAD_FILE_SIZE_BYTES = 128 * 1024**2
NANOSECOND_TIMESTAMP_PRECISION = 9


def primary_key_columns(table: TTableSchema) -> list[str]:
    return get_columns_names_with_prop(table, "primary_key")


def unsupported_merge_configuration(
    table: TTableSchema, *, allow_hard_delete: bool = False
) -> str | None:
    strategy = table.get("x-merge-strategy")
    if strategy not in (None, "upsert"):
        return f"merge strategy {strategy!r}"
    if has_column_with_prop(table, "merge_key"):
        return "merge_key"
    if not allow_hard_delete and has_column_with_prop(table, "hard_delete"):
        return "the hard_delete column hint"
    dedup_sort = get_dedup_sort_tuple(table)
    if dedup_sort and dedup_sort[1] != "desc":
        return f"dedup_sort {dedup_sort[1]!r} (the server keeps the highest value, use 'desc')"
    if not primary_key_columns(table):
        return "merge without a primary_key"
    return None


def upsert_params(table: TTableSchema, *, allow_hard_delete: bool = False) -> dict[str, str] | None:
    if table.get("write_disposition") != "merge":
        return None
    if unsupported := unsupported_merge_configuration(table, allow_hard_delete=allow_hard_delete):
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


def _upload(
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

    if ingest_mode != "overwrite":
        parquet_schema = pq.read_schema(parquet_file_path)
        if table_name not in already_evolved or any(
            pa.types.is_uint64(field.type) for field in parquet_schema
        ):
            if create_or_evolve_table(config, table, parquet_schema):
                if table_name not in already_evolved:
                    already_evolved.append(table_name)

    params = {
        "catalog": cast(str, config.catalog),
        "schema": cast(str, config.dataset_name),
        "table": table_name,
    }
    action = f"{ingest_mode} {config.catalog}.{config.dataset_name}.{table_name}"
    if upsert is not None:
        api.post_parquet(config, "upsert", params | upsert, parquet_file_path, action)
    else:
        params["mode"] = ingest_mode
        with aligned_parquet(parquet_file_path, table) as upload_path:
            api.post_parquet(config, "upload", params, upload_path, action)

    if ingest_mode == "overwrite":
        already_replaced.append(table_name)


class altertable(CustomDestination):
    def __init__(
        self,
        destination_name: str = "altertable",
        naming_convention: str = "direct",
        max_table_nesting: int | None = 0,
        **kwargs: Any,
    ) -> None:
        options: dict[str, Any] = {
            "destination_callable": _upload,
            "loader_file_format": "parquet",
            "preferred_loader_file_format": "parquet",
            "supported_loader_file_formats": ["parquet"],
            "loader_file_format_selector": None,
            "batch_size": 0,
            "skip_dlt_columns_and_tables": False,
            "loader_parallelism_strategy": "table-sequential",
            "spec": AltertableClientConfiguration,
        }
        for name, value in options.items():
            if name in kwargs and kwargs[name] != value:
                raise ConfigurationValueError(f"altertable does not support overriding {name}.")
        super().__init__(
            destination_name=destination_name,
            naming_convention=naming_convention,
            max_table_nesting=max_table_nesting,
            **(kwargs | options),
        )

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        caps.sqlglot_dialect = "duckdb"
        caps.max_timestamp_precision = NANOSECOND_TIMESTAMP_PRECISION
        caps.recommended_file_size = DEFAULT_UPLOAD_FILE_SIZE_BYTES
        caps.max_parallel_load_jobs = 1
        caps.parquet_format = ParquetFormatConfiguration(version="2.6")
        caps.escape_identifier = escape_postgres_identifier
        caps.escape_literal = escape_duckdb_literal
        caps.has_case_sensitive_identifiers = False
        caps.supported_merge_strategies = ["upsert"]
        return caps

    @property
    def client_class(self) -> type["AltertableJobClient"]:
        from dlt_altertable.job_client import AltertableJobClient

        return AltertableJobClient


altertable.register()
