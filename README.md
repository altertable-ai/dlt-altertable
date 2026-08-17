# dlt-altertable

A [dlt](https://dlthub.com) destination that loads into the [Altertable](https://altertable.ai)
lakehouse over Arrow Flight SQL. Each dlt load job is one parquet file, streamed as Arrow record
batches into a single Flight transaction.

## Install

```bash
uv add dlt-altertable
```

## Use

```python
import dlt
from dlt_altertable import altertable


@dlt.resource(name="contacts", write_disposition="merge", primary_key="id")
def contacts():
    yield [
        {"id": 1, "email": "ada@example.com", "lastmodifieddate": 100},
        {"id": 2, "email": "grace@example.com", "lastmodifieddate": 100},
    ]


pipeline = dlt.pipeline(
    pipeline_name="crm",
    destination=altertable(host="flight.altertable.ai", catalog="lakehouse", schema="crm"),
)
pipeline.run(contacts())
```

Credentials and connection settings can also come from the environment instead of the call:

```bash
export DESTINATION__ALTERTABLE__HOST=flight.altertable.ai
export DESTINATION__ALTERTABLE__CATALOG=lakehouse
export DESTINATION__ALTERTABLE__SCHEMA=crm
export DESTINATION__ALTERTABLE__USERNAME=...
export DESTINATION__ALTERTABLE__PASSWORD=...
export DESTINATION__ALTERTABLE__PORT=443   # optional, defaults to 443
export DESTINATION__ALTERTABLE__TLS=true   # optional, defaults to true
```

The pipeline's `dataset_name` is not used. The target schema is the `schema` setting above.

## Write dispositions

| dlt write disposition | Altertable ingest mode              | Notes                                                            |
| --------------------- | ----------------------------------- | ---------------------------------------------------------------- |
| `append`              | `CREATE_APPEND`                     | Creates the table on first load, appends afterwards.             |
| `replace`             | `REPLACE`, then `APPEND`            | The first file of each load recreates the table.                 |
| `merge`               | `CREATE_APPEND` + server-side upsert | Needs a `primary_key`. Other merge configurations are rejected.  |

Only the `upsert` merge strategy is supported. A `merge` resource configured with `merge_key`, with
the `scd2` strategy, or without a `primary_key` fails the load with a terminal error rather than
loading under different semantics.

## Merge cursor

A `merge` resource upserts on its `primary_key`. When two rows collide, Altertable keeps the one
with the highest cursor value. Name the cursor column with an `x-altertable-cursor` table hint:

```python
from dlt_altertable import CURSOR_HINT

resource = contacts()
resource.apply_hints(additional_table_hints={CURSOR_HINT: "lastmodifieddate"})
```

Without the hint the upsert conflicts on the primary key alone, and which row wins is up to the
server.

## Incremental state on ephemeral runners

A custom destination cannot read back what it already loaded, so incremental cursors live only in
dlt's own state under `pipelines_dir`. On an ephemeral runner that directory is gone on the next
run, and every load starts from scratch.

Either persist `pipelines_dir` between runs, or bootstrap the cursor from the destination itself
before extracting:

```python
with Client(username, password, host=host, port=port, tls=tls) as client:
    table = client.query(f'SELECT max(lastmodifieddate) AS cursor FROM "{table_name}"').read_all()
    since = table.column("cursor")[0].as_py() or 0
```

`examples/sandboxed_task_hubspot.py` takes the second route.

## dlt bookkeeping columns

`_dlt_id` and `_dlt_load_id` are not loaded. dlt writes them into the parquet file even though it
hides them from the table schema handed to a custom destination, so only the columns dlt declares
in that schema are ingested.

## Develop

```bash
uv sync
uv run pytest
uv run ruff check .
```

`examples/hubspot_shaped_demo.py` runs two merge loads and prints the resulting rows. It points at
`localhost:15102` by default, which suits
[altertable-mock](https://github.com/altertable-ai/altertable-mock):

```bash
docker run -d -p 15102:15002 \
  -e ALTERTABLE_MOCK_FLIGHT_PORT=15002 -e ALTERTABLE_MOCK_USERS="dlt-demo:lk_demo" \
  ghcr.io/altertable-ai/altertable-mock:latest
uv run python examples/hubspot_shaped_demo.py
```

Point it at a real deployment with `ALTERTABLE_HOST`, `ALTERTABLE_PORT`, `ALTERTABLE_TLS`,
`ALTERTABLE_USERNAME`, `ALTERTABLE_PASSWORD`, `ALTERTABLE_CATALOG` and `ALTERTABLE_SCHEMA`.
