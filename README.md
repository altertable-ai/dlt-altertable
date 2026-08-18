# dlt-altertable

[![CI](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml/badge.svg)](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dlt-altertable)](https://pypi.org/project/dlt-altertable/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg)](https://www.python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A [dlt](https://dlthub.com) destination that loads into the [Altertable](https://altertable.ai)
lakehouse over Arrow Flight SQL. Each dlt load job is one parquet file, streamed as Arrow record
batches through a single ingest statement. Types pass through Arrow end to end: the parquet file
dlt writes is the schema the server sees.

## Install

```bash
uv add dlt-altertable
```

Supported: Python 3.12 to 3.14, `dlt >= 1.19`, `altertable-flightsql >= 0.3.2`.

## Use

```python
import dlt
from dlt_altertable import altertable


@dlt.resource(
    name="contacts",
    write_disposition="merge",
    primary_key="id",
    columns={"lastmodifieddate": {"dedup_sort": "desc"}},
)
def contacts():
    yield [
        {"id": 1, "email": "ada@example.com", "lastmodifieddate": 100},
        {"id": 2, "email": "grace@example.com", "lastmodifieddate": 100},
    ]


pipeline = dlt.pipeline(pipeline_name="crm", destination=altertable)
pipeline.run(contacts())
```

Configure the connection in `.dlt/secrets.toml`:

```toml
[destination.altertable]
host = "flight.altertable.ai"
catalog = "lakehouse"
dataset_name = "crm"
username = "..."
password = "..."
# port = 443 and tls = true are the defaults
```

Every setting can also come from dlt's environment variables, which override the toml file
(`DESTINATION__ALTERTABLE__HOST`, `DESTINATION__ALTERTABLE__PASSWORD`, and so on), or be passed
directly: `altertable(host=..., catalog=..., dataset_name=...)`.

Inside an Altertable sandbox no configuration is needed at all: the destination picks up the
`ALTERTABLE_HOST`, `ALTERTABLE_PORT`, `ALTERTABLE_TLS`, `ALTERTABLE_USERNAME`,
`ALTERTABLE_PASSWORD`, `ALTERTABLE_CATALOG` and `ALTERTABLE_SCHEMA` variables the sandbox
already provides.

Two notes on naming:

- The pipeline's `dataset_name` is not used. dlt does not route it to custom destinations, so the
  target schema is the destination's own `dataset_name` setting.
- To point several pipelines at different backends, name each configuration:
  `altertable(destination_name="altertable_staging")` reads `[destination.altertable_staging]`.

## Write dispositions

| dlt write disposition | Altertable ingest mode               | Notes                                                |
| --------------------- | ------------------------------------ | ---------------------------------------------------- |
| `append`              | `CREATE_APPEND`                      | Creates the table on first load, appends afterwards. |
| `replace`             | `REPLACE`, then `APPEND`             | The first file of each load recreates the table.     |
| `merge`               | `CREATE_APPEND` + server-side upsert | Upserts on the `primary_key`.                        |

Merge always runs as a server-side upsert on the `primary_key`, which matches what dlt's default
merge does for a primary-key resource. Configurations with different semantics fail the load with
a terminal error instead of loading under different semantics: an explicit `delete-insert` or
`scd2` strategy, a `merge_key`, a `hard_delete` column, a missing `primary_key`, a table whose
columns are all part of the primary key, or an ascending `dedup_sort`.

A merge that sends a subset of columns updates only those columns on matched rows; omitted
columns keep their current values in the lakehouse.

### Picking the winning row

When two rows collide on the primary key, Altertable keeps the one with the highest value of the
column marked with dlt's standard [`dedup_sort` hint](https://dlthub.com/docs/general-usage/merge-loading):

```python
@dlt.resource(
    write_disposition="merge",
    primary_key="id",
    columns={"lastmodifieddate": {"dedup_sort": "desc"}},
)
```

Altertable arbitrates against existing table rows as well as within the batch, a superset of
dlt's batch-only deduplication. Without the hint, which row wins is up to the server.

## Schema evolution

When dlt's schema evolution adds a column, the destination issues `ALTER TABLE ... ADD COLUMN`
before ingesting, so existing tables follow the source. Removed columns stay in the table and
keep their values. Columns that only ever contained `NULL` are dropped by dlt at normalize time
with a warning, before they reach the destination.

Nested data does not become child tables: the destination sets `max_table_nesting=0`, dlt's
default for custom destinations, so lists and objects land as JSON strings in the parent table.
Child tables would need the dlt linking columns this destination deliberately skips.

## Atomicity and retries

Each parquet file is one ingest statement, applied atomically by the server: a file either lands
fully or not at all. A load split across several files is not atomic as a whole, and the first
file of a `replace` load recreates the table.

Transient failures (network, server errors) are retried 5 times by dlt
(`load.raise_on_max_retries`). Bad credentials and unsupported merge configurations fail
immediately with a terminal error naming the endpoint and table.

The destination declares `loader_parallelism_strategy="table-sequential"`, and the `replace`
bookkeeping depends on it. Do not override it with `LOAD__PARALLELISM_STRATEGY=parallel`: two
files of one replace load would both recreate the table and silently lose rows.

One caveat discovered the hard way: the target schema is created on demand by the server, so a
typo in `dataset_name` does not fail, it lands data in a new schema. The catalog, by contrast,
must exist.

## Incremental state on ephemeral runners

[Custom destinations cannot restore pipeline state](https://dlthub.com/docs/dlt-ecosystem/destinations/destination),
so incremental cursors live only in dlt's own state under `pipelines_dir`. On an ephemeral runner
that directory is gone on the next run, and every load starts from scratch.

Either persist `pipelines_dir` between runs, or bootstrap the cursor from the destination itself
before extracting:

```python
from altertable_flightsql import Client

with Client(username, password, host=host, port=port, tls=tls) as client:
    table = client.query(f'SELECT max(lastmodifieddate) AS cursor FROM "{table_name}"').read_all()
    since = table.column("cursor")[0].as_py() or 0
```

## dlt internal columns

`_dlt_id` and `_dlt_load_id` are not loaded. dlt writes them into the parquet file even though it
hides them from the table schema handed to a custom destination, so only the columns dlt declares
in that schema are ingested.

## Develop

```bash
uv sync
uv run pytest
uv run ruff check .
```

The test suite mocks the Flight client and needs no server. For a live check, run
[altertable-mock](https://github.com/altertable-ai/altertable-mock) and point a pipeline at it:

```bash
docker run -d -p 15102:15002 \
  -e ALTERTABLE_MOCK_FLIGHT_PORT=15002 -e ALTERTABLE_MOCK_USERS="dlt-demo:lk_demo" \
  ghcr.io/altertable-ai/altertable-mock:latest
```

## Resources

- [dlt custom destination docs](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
- [dlt merge loading](https://dlthub.com/docs/general-usage/merge-loading)
- [Altertable](https://altertable.ai)

## License

Apache 2.0, see [LICENSE](LICENSE).
