from collections.abc import Sequence
from typing import NotRequired, TypedDict

from dlt.common.exceptions import TerminalValueError
from dlt.common.typing import TSortOrder
from dlt.destinations.adapters import iceberg_partition as altertable_partition
from dlt.destinations.impl.filesystem.iceberg_adapter import PartitionSpec
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource

from dlt_altertable.layout import PARTITION_HINT, partition_keys

__all__ = ["altertable_adapter", "altertable_partition"]


class SortKey(TypedDict):
    column: str
    direction: NotRequired[TSortOrder]


def altertable_adapter(
    data: object,
    *,
    partition: str | PartitionSpec | Sequence[str | PartitionSpec] | None = None,
    sort: str | SortKey | Sequence[str | SortKey] | None = None,
) -> DltResource:
    """Set ordered partition keys on a dlt resource."""
    if sort is not None:
        raise NotImplementedError("Sort hints are unsupported.")
    if partition is None:
        raise TerminalValueError("Specify partition in altertable_adapter.")
    resource = get_resource_for_adapter(data)
    partitions: list[str | dict[str, object]] = []
    keys = (
        partition
        if isinstance(partition, Sequence) and not isinstance(partition, str)
        else [partition]
    )
    for key in keys:
        if isinstance(key, str):
            partitions.append(key)
        elif isinstance(key, PartitionSpec) and key.partition_field is None:
            entry: dict[str, object] = {"column": key.source_column, "transform": key.transform}
            if key.param_value is not None:
                entry["buckets"] = key.param_value
            partitions.append(entry)
        else:
            raise TerminalValueError("Use a column name or altertable_partition helper.")
    resource.apply_hints(additional_table_hints={PARTITION_HINT: partition_keys(partitions)})
    return resource
