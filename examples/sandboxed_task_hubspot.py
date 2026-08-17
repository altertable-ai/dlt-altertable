# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "dlt[parquet]~=1.10",
#   "altertable-flightsql>=0.3.2",
#   "pyarrow",
#   "requests",
# ]
# ///
# Self-contained HubSpot CRM -> Altertable pipeline for a SandboxedScript task.
# The sink below is a copy of dlt_altertable/destination.py, inlined because the
# package repo is private and PEP 723 dependencies cannot carry git credentials.
# Task env vars expected:
#   SOURCES__HUBSPOT__API_KEY, DESTINATION__ALTERTABLE__HOST,
#   DESTINATION__ALTERTABLE__CATALOG, DESTINATION__ALTERTABLE__SCHEMA,
#   DESTINATION__ALTERTABLE__USERNAME, DESTINATION__ALTERTABLE__PASSWORD

import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import os

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from altertable_flightsql import Client
from altertable_flightsql.client import IngestIncrementalOptions, IngestTableMode
from altertable_flightsql.errors import AltertableNotFoundError
from dlt.common.destination.exceptions import DestinationTerminalException
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
    strategy = table.get("x-merge-strategy")
    merge_keys = [name for name, column in table["columns"].items() if column.get("merge_key")]
    primary_key = primary_key_columns(table)
    if strategy not in (None, "upsert") or merge_keys or not primary_key:
        raise DestinationTerminalException(
            f"altertable sink supports merge only as a primary-key upsert; "
            f"table {table['name']} uses strategy={strategy!r}, merge_key={merge_keys}, primary_key={primary_key}"
        )
    return IngestIncrementalOptions(primary_key=primary_key, cursor_field=cursor_columns(table))


def ingest_mode(table: TTableSchema, table_already_replaced: bool) -> IngestTableMode:
    if table.get("write_disposition") != "replace":
        return IngestTableMode.CREATE_APPEND
    return IngestTableMode.APPEND if table_already_replaced else IngestTableMode.REPLACE


def tables_already_replaced() -> list[str]:
    return dlt.current.destination_state().setdefault("replaced_tables", [])


def declared_columns(table: TTableSchema, parquet_schema: pa.Schema) -> list[str]:
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
        "properties": ["dealname", "amount", "dealstage", "pipeline", "closedate", "hubspot_owner_id"],
    },
}


def destination_cursor_ms(object_type: str) -> int:
    with Client(
        os.environ["DESTINATION__ALTERTABLE__USERNAME"],
        os.environ["DESTINATION__ALTERTABLE__PASSWORD"],
        host=os.environ["DESTINATION__ALTERTABLE__HOST"],
    ) as client:
        client.set_catalog(os.environ["DESTINATION__ALTERTABLE__CATALOG"])
        client.set_schema(os.environ["DESTINATION__ALTERTABLE__SCHEMA"])
        try:
            table = client.query(f'SELECT max(lastmodifieddate) AS cursor FROM "{object_type}"').read_all()
        except AltertableNotFoundError:
            return 0
        cursor = table.column("cursor")[0].as_py()
        return int(cursor) if cursor is not None else 0


def search_page(
    api_key: str, object_type: str, modified_property: str, properties: list[str], since_ms: int, after: str | None
) -> dict[str, Any]:
    body = {
        "filterGroups": [
            {"filters": [{"propertyName": modified_property, "operator": "GT", "value": str(since_ms)}]}
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

    @dlt.resource(name=object_type, write_disposition="merge", primary_key="id")
    def rows(
        modified_at: dlt.sources.incremental[int] = dlt.sources.incremental("lastmodifieddate", initial_value=0),
    ) -> Iterator[dict[str, Any]]:
        since_ms = max(modified_at.last_value or 0, destination_cursor_ms(object_type))
        after: str | None = None
        yielded_in_window = 0
        while True:
            page = search_page(api_key, object_type, modified_property, spec["properties"], since_ms, after)
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

    rows.apply_hints(additional_table_hints={CURSOR_HINT: "lastmodifieddate"})
    return rows


@dlt.source(name="hubspot_crm")
def hubspot_crm(api_key: str = dlt.secrets.value) -> Any:
    return [crm_object_resource(object_type, api_key) for object_type in CRM_OBJECTS]


if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="hubspot_crm", destination=altertable)
    info = pipeline.run(hubspot_crm())
    print(info)
