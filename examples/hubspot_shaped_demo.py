import os
from collections.abc import Iterator
from typing import Any

import dlt
from altertable_flightsql import Client

from dlt_altertable import CURSOR_HINT, altertable

HOST = os.environ.get("ALTERTABLE_HOST", "localhost")
PORT = int(os.environ.get("ALTERTABLE_PORT", "15102"))
TLS = os.environ.get("ALTERTABLE_TLS", "false").lower() == "true"
USERNAME = os.environ.get("ALTERTABLE_USERNAME", "dlt-demo")
PASSWORD = os.environ.get("ALTERTABLE_PASSWORD", "lk_demo")
CATALOG = os.environ.get("ALTERTABLE_CATALOG", "demo")
SCHEMA = os.environ.get("ALTERTABLE_SCHEMA", "crm")


@dlt.resource(name="contacts", write_disposition="merge", primary_key="id")
def contacts(batch: int) -> Iterator[list[dict[str, Any]]]:
    if batch == 1:
        yield [
            {"id": 1, "email": "ada@example.com", "lastmodifieddate": 100},
            {"id": 2, "email": "grace@example.com", "lastmodifieddate": 100},
        ]
    else:
        yield [
            {"id": 2, "email": "grace.hopper@example.com", "lastmodifieddate": 200},
            {"id": 3, "email": "katherine@example.com", "lastmodifieddate": 200},
        ]


def upserting_contacts(batch: int) -> Any:
    resource = contacts(batch)
    resource.apply_hints(additional_table_hints={CURSOR_HINT: "lastmodifieddate"})
    return resource


def prepare_target(client: Client) -> None:
    client.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {CATALOG}")
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    client.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.contacts")


def show(client: Client, title: str) -> None:
    rows = client.query(f"SELECT * FROM {CATALOG}.{SCHEMA}.contacts ORDER BY id").read_all()
    print(f"\n{title}")
    for row in rows.to_pylist():
        print(f"  {row}")


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="hubspot_shaped_demo",
        destination=altertable(
            host=HOST,
            port=PORT,
            tls=TLS,
            username=USERNAME,
            password=PASSWORD,
            catalog=CATALOG,
            schema=SCHEMA,
        ),
        dataset_name=SCHEMA,
    )

    with Client(USERNAME, PASSWORD, host=HOST, port=PORT, tls=TLS) as client:
        prepare_target(client)

        print(pipeline.run(upserting_contacts(batch=1)))
        show(client, "after the first load")

        print(pipeline.run(upserting_contacts(batch=2)))
        show(client, "after the second load (id=2 upserted, id=3 inserted)")


if __name__ == "__main__":
    main()
