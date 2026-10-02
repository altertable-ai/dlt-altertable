from typing import Literal, NotRequired, TypedDict, TypeGuard

from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.destination.client import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TTableSchema

type PartitionTransform = Literal["identity", "year", "month", "day", "hour", "bucket"]

PARTITION_HINT: str = "x-altertable-partition"
MAX_SIGNED_INT32 = 2**31 - 1


class PartitionKey(TypedDict):
    column: str
    transform: PartitionTransform
    buckets: NotRequired[int]


def is_partition_transform(value: object) -> TypeGuard[PartitionTransform]:
    return isinstance(value, str) and value in (
        "identity",
        "year",
        "month",
        "day",
        "hour",
        "bucket",
    )


def partition_keys(keys: object) -> list[PartitionKey]:
    if not isinstance(keys, list):
        raise TerminalValueError("Altertable partition hints must be a list.")
    normalized: list[PartitionKey] = []
    for key in keys:
        entry = {"column": key} if isinstance(key, str) else key
        if not isinstance(entry, dict) or set(entry) - {"column", "transform", "buckets"}:
            raise TerminalValueError(f"Invalid Altertable partition key: {key!r}.")
        column = entry.get("column")
        if not isinstance(column, str) or not column or "\x00" in column:
            raise TerminalValueError(f"Invalid Altertable layout column: {column!r}.")
        transform = entry.get("transform", "identity")
        if not is_partition_transform(transform):
            raise TerminalValueError(f"Unsupported partition transform: {transform!r}.")
        item: PartitionKey = {"column": column, "transform": transform}
        buckets = entry.get("buckets")
        if transform == "bucket":
            if type(buckets) is not int or not 0 < buckets <= MAX_SIGNED_INT32:
                raise TerminalValueError(
                    f"Bucket count must be a signed 32-bit integer in 1..{MAX_SIGNED_INT32}."
                )
            item["buckets"] = buckets
        elif "buckets" in entry:
            raise TerminalValueError("Only bucket transforms accept a bucket count.")
        if item in normalized:
            raise TerminalValueError(f"Duplicate Altertable partition key: {key!r}.")
        normalized.append(item)
    return normalized


def partition_expressions(table: TTableSchema | PreparedTableSchema) -> list[str] | None:
    if "x-altertable-sort" in table or any(
        "sort" in column for column in table["columns"].values()
    ):
        raise NotImplementedError("Sort hints are unsupported.")
    if PARTITION_HINT in table:
        keys = partition_keys(table.get(PARTITION_HINT))
    elif any("partition" in column for column in table["columns"].values()):
        for column in table["columns"].values():
            if "partition" in column and type(column["partition"]) is not bool:
                raise TerminalValueError("Standard partition column hints must be bool.")
        keys = partition_keys(
            [name for name, column in table["columns"].items() if column.get("partition")]
        )
    else:
        return None
    if table.get("write_disposition") == "replace":
        raise TerminalValueError(
            "Altertable layout hints require append or merge. Replace recreates the table."
        )
    return [_partition_expression(table, key) for key in keys]


def _partition_expression(table: TTableSchema | PreparedTableSchema, key: PartitionKey) -> str:
    name = key["column"]
    if name not in table["columns"]:
        raise TerminalValueError(f"Table {table['name']}: layout column {name!r} does not exist.")
    column = escape_postgres_identifier(name)
    transform = key["transform"]
    data_type = table["columns"][name].get("data_type")
    if transform in ("year", "month", "day", "hour") and (
        data_type not in ("date", "timestamp") or (transform == "hour" and data_type == "date")
    ):
        raise TerminalValueError(
            f"Table {table['name']}: {transform} cannot partition {name!r} ({data_type})."
        )
    if transform == "identity":
        return column
    if transform == "bucket":
        return f"bucket({key['buckets']}, {column})"
    return f"{transform}({column})"
