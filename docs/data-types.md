# Data types

For append and merge, unsigned 64-bit Arrow columns use `DECIMAL(20,0)` to preserve their full
range. Existing `BIGINT` columns widen to this type when a load supplies unsigned 64-bit data.
Other integer columns remain `BIGINT`.

The destination writes Parquet 2.6 by default. For nanosecond timestamps, set the column hint
`timezone=False` so dlt keeps the timestamps naive and the destination uses `TIMESTAMP_NS`.
Arrow timestamp precision is inferred; explicit timestamp schemas can set `precision=9`.
Without `timezone=False`, dlt uses UTC timestamps, which DuckDB stores at microsecond precision.
