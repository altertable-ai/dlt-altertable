import pytest
from dlt.common.exceptions import TerminalValueError

from dlt_altertable import altertable_adapter, altertable_partition
from dlt_altertable.altertable_adapter import SortKey


def test_adapter_accepts_typed_lists() -> None:
    columns = ["category"]
    table = altertable_adapter([], partition=columns).compute_table_schema()
    assert table.get("x-altertable-partition") == [{"column": "category", "transform": "identity"}]

    partitions = [altertable_partition.year("created_at")]
    table = altertable_adapter([], partition=partitions).compute_table_schema()
    assert table.get("x-altertable-partition") == [{"column": "created_at", "transform": "year"}]


def test_typed_sort_keys_fail_explicitly() -> None:
    sort_keys: list[SortKey] = [{"column": "created_at", "direction": "desc"}]
    with pytest.raises(TerminalValueError, match="Sort hints are not supported yet"):
        altertable_adapter([], sort=sort_keys)


def test_adapter_accepts_tuples() -> None:
    table = altertable_adapter(
        [], partition=("category", altertable_partition.year("created_at"))
    ).compute_table_schema()
    assert table.get("x-altertable-partition") == [
        {"column": "category", "transform": "identity"},
        {"column": "created_at", "transform": "year"},
    ]
