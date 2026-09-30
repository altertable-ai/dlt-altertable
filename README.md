# dlt-altertable

[![CI](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml/badge.svg)](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dlt-altertable)](https://pypi.org/project/dlt-altertable/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg)](https://www.python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A [dlt](https://dlthub.com) destination that loads into the [Altertable](https://altertable.ai)
lakehouse through the HTTP Lakehouse API. Each dlt load job is one parquet file, posted verbatim
to `/upload` or `/upsert`. An append or replace file written before dlt evolved the load's schema
is padded with typed `NULL` columns first, because those uploads must match the table column for
column. Merge files always post as they are, so columns a file omits keep their stored values.

## Install

```bash
uv add dlt-altertable
```

Supported: Python 3.12 to 3.14, `dlt >= 1.30, < 2`.

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
# port = 443, tls = true and compute_size = "XS" (for schema queries) are the defaults
```

Every setting can also come from dlt's environment variables, which override the toml file
(`DESTINATION__ALTERTABLE__HOST`, `DESTINATION__ALTERTABLE__PASSWORD`, and so on), or be passed
directly: `altertable(host=..., catalog=..., dataset_name=...)`.

No configuration is needed in an environment that already exports the `ALTERTABLE_HOST`,
`ALTERTABLE_PORT`, `ALTERTABLE_TLS`, `ALTERTABLE_USERNAME`, `ALTERTABLE_PASSWORD`,
`ALTERTABLE_CATALOG` and `ALTERTABLE_SCHEMA` variables. The host must be the HTTP API host
(`api.…`), not the Flight SQL endpoint.

Two notes on naming:

- The pipeline's `dataset_name` is not used. dlt does not route it to custom destinations, so the
  target schema is the destination's own `dataset_name` setting.
- To point several pipelines at different backends, name each configuration:
  `altertable(destination_name="altertable_staging")` reads `[destination.altertable_staging]`.

## Check the catalog before loading

`verify_catalog` answers whether a load would reach the configured catalog, without running one.
It returns one message per catalog problem and an empty list when the catalog is writable.
Configuration, authentication, and query failures raise.

```python
from dlt_altertable import verify_catalog

if problems := verify_catalog():
    raise RuntimeError("\n".join(problems))
```

## Write dispositions

| dlt write disposition | HTTP call                                | Notes                                                |
| --------------------- | ---------------------------------------- | ---------------------------------------------------- |
| `append`              | `POST /upload?mode=append`               | The destination creates or evolves the table first.  |
| `replace`             | `mode=overwrite`, then `mode=append`     | The first file of each load recreates the table.     |
| `merge`               | `POST /upsert?primary_key=…&cursor_field=…` | Server-side upsert on the `primary_key`.          |

Merge always runs as a server-side upsert on the `primary_key`, dlt's `upsert` merge strategy.
A resource that does not name a strategy is accepted and upserted the same way. Configurations
the upsert cannot honor fail the load with a terminal error instead of loading under different
semantics: an explicit `delete-insert` or `scd2` strategy, a `merge_key`, a `hard_delete`
column, a missing `primary_key`, or an ascending `dedup_sort`.

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

Like dlt's SQL destinations, this destination loads dlt's lineage columns: every row carries
`_dlt_id` and `_dlt_load_id`, making it traceable to the load that produced it. The dataset also
contains `_dlt_pipeline_state`, `_dlt_loads` and `_dlt_version`. They store pipeline checkpoints,
completed loads and schema versions. State restoration only uses checkpoints belonging to a
completed load.

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

Failures map onto dlt's retry contract: errors a retry cannot fix, such as bad credentials or
an invalid request, fail the job immediately with a terminal error, while server errors,
capacity timeouts and connection failures are retried by dlt until the fifth attempt fails
(`load.raise_on_max_retries`). Errors streamed mid-response by `/query` raise as well. The upload
read timeout is one hour, because the server answers only once the file is fully ingested. Do
not lower it: a request aborted client side can still complete on the server, which turns dlt's
retry into duplicate appended rows. `merge` tables are idempotent under retry, `append` is at
least once, and `replace` is at least once after its first file, since only the first file of
the load recreates the table and a retried later file appends twice.

Rows that land twice raise no error. `verify_load` reads the tables back after a load and
reports what does not add up:

```python
from dlt_altertable import verify_load

pipeline.run(contacts())
if problems := verify_load(pipeline):
    raise RuntimeError("\n".join(problems))
```

It returns an empty list when the load reconciles and one message for each problem otherwise.
It raises when the last trace has no load to verify or belongs to a non-Altertable destination.

The destination declares `loader_parallelism_strategy="table-sequential"`, and the `replace`
bookkeeping depends on it. Do not override it with `LOAD__PARALLELISM_STRATEGY=parallel`: two
files of one replace load would both recreate the table and silently lose rows.

One caveat discovered the hard way: the target schema is created on demand, so a typo in
`dataset_name` does not fail, it lands data in a new schema. The catalog, by contrast,
must exist, which `verify_catalog` reports before a load discovers it.

## Incremental state on ephemeral runners

Keep the same `pipeline_name`, catalog and destination schema between runs. dlt restores the
latest completed checkpoint and stored schema before extracting, even with a fresh `pipelines_dir`.
You can request restoration explicitly with `pipeline.sync_destination()`.

Remote checkpoints let a fresh runner continue incremental extraction. Persist `pipelines_dir`
when you need to resume an unfinished load, because its pending parquet files and job progress
live locally. Authentication and query failures raise during restoration; they are not treated
as empty storage.

`client.drop_storage()` defaults to a dry run: it warns with the SQL and does not execute it.
Pass `dry_run=False` to delete the configured destination schema and all its tables, including
tables belonging to other pipelines. This opt-in prevents accidental calls, not unrestricted
code execution with privileged credentials.

## Develop

```bash
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check .
uvx ty check src
```

Unit tests need no server. CI also runs `tests/integration` against
[altertable-mock](https://github.com/altertable-ai/altertable-mock), with a separate schema per
test. These tests cover fresh-runner recovery, failed completion, and deleted storage.
Run them locally:

```bash
docker run --rm -d --name dlt-altertable-mock -p 127.0.0.1:15100:15000 \
  -e ALTERTABLE_MOCK_USERS="integration:integration" \
  ghcr.io/altertable-ai/altertable-mock:latest

DLT_ALTERTABLE_INTEGRATION=1 uv run --locked pytest tests/integration
docker stop dlt-altertable-mock
```

Override `DLT_ALTERTABLE_MOCK_URL`, `DLT_ALTERTABLE_MOCK_USERNAME`, and
`DLT_ALTERTABLE_MOCK_PASSWORD` for another local mock. Enabled tests fail on connection and
authentication errors.

## Resources

- [dlt custom destination docs](https://dlthub.com/docs/dlt-ecosystem/destinations/destination)
- [dlt merge loading](https://dlthub.com/docs/general-usage/merge-loading)
- [Altertable](https://altertable.ai)

## License

Apache 2.0, see [LICENSE](LICENSE).
