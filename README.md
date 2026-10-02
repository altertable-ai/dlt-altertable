# dlt-altertable

[![CI](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml/badge.svg)](https://github.com/altertable-ai/dlt-altertable/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dlt-altertable)](https://pypi.org/project/dlt-altertable/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg)](https://www.python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Load data into [Altertable](https://altertable.ai) with [dlt](https://dlthub.com).

## Install and configure

Requires Python 3.12+ and `dlt >= 1.30, < 2`.

```bash
uv add dlt-altertable
```

Set `.dlt/secrets.toml`:

```toml
[destination.altertable]
host = "api.altertable.ai"
catalog = "lakehouse"
dataset_name = "crm"
username = "..."
password = "..."
```

Use the HTTP API host. Defaults: HTTPS, port 443, compute size `XS`.
Set `dataset_name` on the destination. dlt's pipeline setting is ignored.
The catalog must exist. Schemas are created automatically.

Settings also accept dlt environment variables (`DESTINATION__ALTERTABLE__HOST`, etc.),
existing `ALTERTABLE_*` credentials, or arguments to `altertable(...)`.

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

## Loading

| Disposition | Behavior |
| --- | --- |
| `append` | Adds rows. |
| `replace` | Recreates the table, then appends subsequent files. |
| `merge` | Upserts by `primary_key`. Omitted columns keep their values. |

Merge requires a non-nullable primary key. In the example, `dedup_sort` keeps the highest
`lastmodifieddate` per `id`, comparing incoming and stored rows. `delete-insert`, `insert-only`, `scd2`,
`merge_key`, `hard_delete`, and ascending `dedup_sort` are unsupported.

New columns are added automatically. Nested data defaults to JSON strings. Set
`altertable(naming_convention="snake_case", max_table_nesting=None)` to flatten objects into columns
and lists into child tables. Child tables don't support `merge`.

Each file is atomic. A whole load is not. Replacement recreates the table on its first file, then
appends the rest. Retried appends can duplicate rows. Do not set
`LOAD__PARALLELISM_STRATEGY=parallel`: replacement requires sequential files per table.

## Performance

Override defaults with [dlt configuration](https://dlthub.com/docs/reference/performance):

```bash
export DATA_WRITER__FILE_MAX_BYTES=268435456  # 256 MiB (0 disables byte-based rotation)
export DATA_WRITER__COMPRESSION=zstd
```

`altertable(max_parallel_load_jobs=2)` allows parallel uploads, subject to `LOAD__WORKERS`.

## Read and verify

```python
from dlt_altertable import verify_catalog, verify_load

rows = pipeline.dataset().contacts.limit(10).fetchall()
print(verify_catalog())
print(verify_load(pipeline))
```

Verification helpers return a list of problems. `[]` means none found.
For Arrow inputs with `append` or `merge`, set
`NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID=true` before loading to use `verify_load`.

Reads buffer results in memory. Use `.arrow()` for Arrow or `.df()` for pandas.

## Operational notes

Destructive refresh requires `altertable(allow_destructive_refresh=True)`.
Use `refresh="drop_resources"` to drop and recreate selected resource tables and reset their state.

- Fresh runners restore incremental state with the same pipeline name, catalog, and schema.
  Persist `pipelines_dir` to resume unfinished loads.
- Keep the one-hour upload timeout: a timed-out request can still complete on the server.

## Develop

```bash
uv run --locked pytest
uv run --locked ruff check .
uv run --locked ruff format --check .
uvx ty check src
```

Run HTTP integration tests against [altertable-mock](https://github.com/altertable-ai/altertable-mock):

```bash
docker run --rm -d --name dlt-altertable-mock -p 127.0.0.1:15100:15000 \
  -e ALTERTABLE_MOCK_USERS=integration:integration \
  ghcr.io/altertable-ai/altertable-mock:latest
DLT_ALTERTABLE_INTEGRATION=1 uv run --locked pytest tests/integration
docker stop dlt-altertable-mock
```

[Apache 2.0](LICENSE).
