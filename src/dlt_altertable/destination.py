import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from altertable_flightsql import Client
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
from dlt.common.schema import TTableSchema

CURSOR_HINT = "x-altertable-cursor"


def primary_key_columns(table: TTableSchema) -> list[str]:
    return [name for name, column in table["columns"].items() if column.get("primary_key")]


def cursor_columns(table: TTableSchema) -> list[str]:
    cursor = table.get(CURSOR_HINT)
    if not cursor:
        return []
    return [cursor] if isinstance(cursor, str) else list(cursor)


def incremental_options(table: TTableSchema) -> IngestIncrementalOptions | None:
    if table.get("write_disposition") != "merge":
        return None
    primary_key = primary_key_columns(table)
    if not primary_key:
        return None
    return IngestIncrementalOptions(primary_key=primary_key, cursor_field=cursor_columns(table))


def ingest_mode(table: TTableSchema, table_already_replaced: bool) -> IngestTableMode:
    if table.get("write_disposition") != "replace":
        return IngestTableMode.CREATE_APPEND
    return IngestTableMode.APPEND if table_already_replaced else IngestTableMode.REPLACE


def tables_already_replaced() -> list[str]:
    return dlt.current.destination_state().setdefault("replaced_tables", [])


def declared_columns(table: TTableSchema, parquet_schema: pa.Schema) -> list[str]:
    """dlt writes _dlt_id and _dlt_load_id into the parquet file even when it hides them from the
    table schema, so the file is the wrong source of truth for what belongs in the lakehouse."""
    return [name for name in parquet_schema.names if name in table["columns"]]


@dlt.destination(
    name="altertable",
    loader_file_format="parquet",
    batch_size=0,
    skip_dlt_columns_and_tables=True,
    loader_parallelism_strategy="table-sequential",
)
def altertable(
    parquet_file_path: str,
    table: TTableSchema,
    host: str = dlt.config.value,
    catalog: str = dlt.config.value,
    schema: str = dlt.config.value,
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    port: int = 443,
    tls: bool = True,
) -> None:
    replaced_tables = tables_already_replaced()
    mode = ingest_mode(table, table["name"] in replaced_tables)
    if mode is IngestTableMode.REPLACE:
        replaced_tables.append(table["name"])

    parquet_file = pq.ParquetFile(parquet_file_path)
    columns = declared_columns(table, parquet_file.schema_arrow)
    arrow_schema = pa.schema([parquet_file.schema_arrow.field(name) for name in columns])

    with (
        Client(username, password, host=host, port=port, tls=tls) as client,
        client.begin_transaction() as transaction,
        client.ingest(
            table_name=table["name"],
            schema=arrow_schema,
            schema_name=schema,
            catalog_name=catalog,
            mode=mode,
            incremental_options=incremental_options(table),
            transaction=transaction,
        ) as writer,
    ):
        for batch in parquet_file.iter_batches(columns=columns):
            writer.write(batch)
