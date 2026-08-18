# dlt-altertable

[![CI](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml/badge.svg)](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dlt-altertable)](https://pypi.org/project/dlt-altertable/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg)](https://www.python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A [dlt](https://dlthub.com) destination that loads into the [Altertable](https://altertable.ai)
lakehouse through the HTTP Lakehouse API. Each dlt load job is one parquet file, posted verbatim
to `/upload` or `/upsert`. Types pass through untouched: the parquet file dlt writes is exactly
what the server ingests.

## Install

```bash
uv add dlt-altertable
```

Supported: Python 3.12 to 3.14, `dlt >= 1.19`. The only other dependency is `requests`.

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
host = "api.altertable.ai"
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
already provides. The host must be the HTTP API host (`api.…`), not the Flight endpoint.

Two notes on naming:

- The pipeline's `dataset_name` is not used. dlt does not route it to custom destinations, so the
  target schema is the destination's own `dataset_name` setting.
- To point several pipelines at different backends, name each configuration:
  `altertable(destination_name="altertable_staging")` reads `[destination.altertable_staging]`.

## Write dispositions

| dlt write disposition | HTTP call                                | Notes                                                |
| --------------------- | ---------------------------------------- | ---------------------------------------------------- |
| `append`              | `POST /upload?mode=append`               | The destination creates or evolves the table first.  |
| `replace`             | `mode=overwrite`, then `mode=append`     | The first file of each load recreates the table.     |
| `merge`               | `POST /upsert?primary_key=…&cursor_field=…` | Server-side upsert on the `primary_key`.          |

Merge always runs as a server-side upsert on the `primary_key`, which matches what dlt's default
merge does for a primary-key resource. Configurations with different semantics fail the load with
a terminal error instead of loading under different semantics: an explicit `delete-insert` or
`scd2` strategy, a `merge_key`, a `hard_delete` column, a missing `primary_key`, or an ascending
`dedup_sort`.

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

## dlt system columns and tables

Like dlt's SQL destinations, this destination loads dlt's lineage columns and system tables:
every row carries `_dlt_id` and `_dlt_load_id`, and the dataset gains `_dlt_loads`,
`_dlt_version` and `_dlt_pipeline_state` tables. They make every row traceable to the load that
produced it.

## Schema evolution

The destination owns the target table's schema. Before each load it syncs the table through
`POST /query`: a missing table is created with typed DDL derived from dlt's schema, and when
dlt's schema evolution adds a column, the destination issues `ALTER TABLE ... ADD COLUMN`, so
existing tables follow the source. Removed columns stay in the table and keep their values.
Columns that only ever contained `NULL` are dropped by dlt at normalize time with a warning,
before they reach the destination.

Nested data does not become child tables: the destination sets `max_table_nesting=0`, dlt's
default for custom destinations, so lists and objects land as JSON strings in the parent table.

## Atomicity and retries

Each parquet file is one HTTP POST, applied atomically by the server: a file either lands fully
or not at all. A load split across several files is not atomic as a whole, and the first file of
a `replace` load recreates the table.

Failures map onto dlt's retry contract: authentication and invalid-request errors (HTTP 4xx)
fail the job immediately with a terminal error, while server errors, capacity timeouts (HTTP
5xx) and connection failures are retried 5 times by dlt (`load.raise_on_max_retries`). The
upload read timeout is one hour, because the server only answers once the worker finished
ingesting; do not lower it aggressively, an aborted request that the server completes anyway
turns a retry into duplicate appended rows. `merge` tables are idempotent under retry, `append`
is at least once.

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
import json
import requests

response = requests.post(
    "https://api.altertable.ai/query",
    json={"statement": f'SELECT count(*) AS n, max(lastmodifieddate) AS m FROM "{table_name}"'},
    auth=(username, password),
    timeout=(10, 300),
)
count, since = [json.loads(line) for line in response.text.splitlines()][2]
```

Check the row count: a legitimately empty table gives `count = 0`, while `count` being `NULL`
signals a masked mid-stream error and the read should be retried.

## Develop

```bash
uv sync
uv run pytest
uv run ruff check .
```

The test suite mocks the HTTP API and needs no server. For a live check, run
[altertable-mock](https://github.com/altertable-ai/altertable-mock) and point a pipeline at its
Lakehouse REST port:

```bash
docker run -d -p 15100:15000 \
  -e ALTERTABLE_MOCK_LAKEHOUSE_PORT=15000 -e ALTERTABLE_MOCK_USERS="dlt-demo:lk_demo" \
  ghcr.io/altertable-ai/altertable-mock:latest
```

## Resources

- [dlt custom destination docs](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
- [dlt merge loading](https://dlthub.com/docs/general-usage/merge-loading)
- [Altertable](https://altertable.ai)

## License

Apache 2.0, see [LICENSE](LICENSE).
