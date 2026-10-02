from dlt_altertable import altertable_adapter, altertable_partition
from dlt_altertable.altertable_adapter import SortKey


def test_adapter_accepts_typed_lists() -> None:
    columns = ["category"]
    table = altertable_adapter([], partition=columns, sort=columns).compute_table_schema()
    assert table.get("x-altertable-partition") == [{"column": "category", "transform": "identity"}]
    assert table.get("x-altertable-sort") == [{"column": "category", "direction": "asc"}]

    partitions = [altertable_partition.year("created_at")]
    sort_keys: list[SortKey] = [{"column": "created_at", "direction": "desc"}]
    table = altertable_adapter([], partition=partitions, sort=sort_keys).compute_table_schema()
    assert table.get("x-altertable-partition") == [{"column": "created_at", "transform": "year"}]
    assert table.get("x-altertable-sort") == sort_keys


def test_adapter_accepts_tuples() -> None:
    table = altertable_adapter(
        [], partition=("category", altertable_partition.year("created_at")), sort=("created_at",)
    ).compute_table_schema()
    assert table.get("x-altertable-partition") == [
        {"column": "category", "transform": "identity"},
        {"column": "created_at", "transform": "year"},
    ]
    assert table.get("x-altertable-sort") == [{"column": "created_at", "direction": "asc"}]
