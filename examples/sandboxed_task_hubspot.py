# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "dlt[parquet]~=1.19",
#   "altertable-flightsql>=0.3.2",
#   "pyarrow",
#   "requests",
# ]
# ///
# Self-contained HubSpot CRM -> Altertable pipeline for a SandboxedScript task.
# The sink below is a copy of dlt_altertable/destination.py, inlined because the
# package repo is private and PEP 723 dependencies cannot carry git credentials.
# Task env vars expected: SOURCES__HUBSPOT__API_KEY, plus the ALTERTABLE_HOST,
# ALTERTABLE_CATALOG, ALTERTABLE_SCHEMA, ALTERTABLE_USERNAME and
# ALTERTABLE_PASSWORD variables the sandbox already provides.

import os
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import requests
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


def declared_columns(table: TTableSchema, parquet_schema: pa.Schema) -> list[str]:
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
    mode = ingest_mode(table, table["name"] in replaced_tables)

    try:
        with (
            pq.ParquetFile(parquet_file_path) as parquet_file,
            Client(username, password, host=host, port=port, tls=tls) as client,
        ):
            columns = declared_columns(table, parquet_file.schema_arrow)
            arrow_schema = pa.schema([parquet_file.schema_arrow.field(name) for name in columns])
            if mode is IngestTableMode.CREATE_APPEND:
                add_new_columns(client, catalog, dataset_name, table, columns)
            with (
                client.begin_transaction() as transaction,
                client.ingest(
                    table_name=table["name"],
                    schema=arrow_schema,
                    schema_name=dataset_name,
                    catalog_name=catalog,
                    mode=mode,
                    incremental_options=options,
                    transaction=transaction,
                ) as writer,
            ):
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


def epoch_ms(iso_timestamp: str) -> int:
    return int(datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00")).timestamp() * 1000)


SEARCH_URL = "https://api.hubapi.com/crm/v3/objects/{object_type}/search"
PAGE_LIMIT = 100
SEARCH_WINDOW_CAP = 10_000

CRM_OBJECTS: dict[str, dict[str, Any]] = {
    "contacts": {
        "modified_property": "lastmodifieddate",
        "properties": ["email", "firstname", "lastname", "lifecyclestage", "hubspot_owner_id"],
    },
    "companies": {
        "modified_property": "hs_lastmodifieddate",
        "properties": ["name", "domain", "industry", "hubspot_owner_id"],
    },
    "deals": {
        "modified_property": "hs_lastmodifieddate",
        "properties": [
            "dealname",
            "amount",
            "dealstage",
            "pipeline",
            "closedate",
            "hubspot_owner_id",
        ],
    },
}


def destination_cursor_ms(object_type: str) -> int:
    with Client(
        os.environ["ALTERTABLE_USERNAME"],
        os.environ["ALTERTABLE_PASSWORD"],
        host=os.environ["ALTERTABLE_HOST"],
        port=int(os.environ.get("ALTERTABLE_PORT", "443")),
        tls=os.environ.get("ALTERTABLE_TLS", "true").lower() != "false",
    ) as client:
        client.set_catalog(os.environ["ALTERTABLE_CATALOG"])
        client.set_schema(os.environ["ALTERTABLE_SCHEMA"])
        query = f'SELECT max(lastmodifieddate) AS cursor FROM "{object_type}"'
        try:
            table = client.query(query).read_all()
        except FlightError:
            return 0
        cursor = table.column("cursor")[0].as_py()
        return int(cursor) if cursor is not None else 0


def search_page(
    api_key: str,
    object_type: str,
    modified_property: str,
    properties: list[str],
    since_ms: int,
    after: str | None,
) -> dict[str, Any]:
    body = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": modified_property, "operator": "GT", "value": str(since_ms)}
                ]
            }
        ],
        "sorts": [{"propertyName": modified_property, "direction": "ASCENDING"}],
        "properties": properties + [modified_property],
        "limit": PAGE_LIMIT,
    }
    if after:
        body["after"] = after
    while True:
        response = requests.post(
            SEARCH_URL.format(object_type=object_type),
            json=body,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30,
        )
        if response.status_code == 429:
            time.sleep(int(response.headers.get("Retry-After", "10")))
            continue
        response.raise_for_status()
        return response.json()


def crm_object_resource(object_type: str, api_key: str) -> Any:
    spec = CRM_OBJECTS[object_type]
    modified_property = spec["modified_property"]

    @dlt.resource(
        name=object_type,
        write_disposition="merge",
        primary_key="id",
        columns={"lastmodifieddate": {"dedup_sort": "desc"}},
    )
    def rows(
        modified_at: dlt.sources.incremental[int] = dlt.sources.incremental(  # noqa: B008
            "lastmodifieddate", initial_value=0
        ),
    ) -> Iterator[dict[str, Any]]:
        since_ms = max(modified_at.last_value or 0, destination_cursor_ms(object_type))
        after: str | None = None
        yielded_in_window = 0
        while True:
            page = search_page(
                api_key, object_type, modified_property, spec["properties"], since_ms, after
            )
            for result in page["results"]:
                row = {"id": result["id"], **result["properties"]}
                row["lastmodifieddate"] = epoch_ms(result["properties"][modified_property])
                since_ms = max(since_ms, row["lastmodifieddate"])
                yield row
            yielded_in_window += len(page["results"])
            after = (page.get("paging") or {}).get("next", {}).get("after")
            if after and yielded_in_window >= SEARCH_WINDOW_CAP:
                after = None
                yielded_in_window = 0
            if not after:
                return


@dlt.source(name="hubspot_crm")
def hubspot_crm(api_key: str = dlt.secrets.value) -> Any:
    return [crm_object_resource(object_type, api_key) for object_type in CRM_OBJECTS]


if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="hubspot_crm", destination=altertable)
    info = pipeline.run(hubspot_crm())
    print(info)
