from typing import Literal, NotRequired, TypedDict, TypeGuard

from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.destination.client import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import TTableSchema
from dlt.common.typing import TSortOrder

type LayoutKind = Literal["partition", "sort"]
type PartitionTransform = Literal["identity", "year", "month", "day", "hour", "bucket"]

LAYOUT_SETTINGS: dict[LayoutKind, str] = {"partition": "PARTITIONED", "sort": "SORTED"}


class LayoutKey(TypedDict):
    column: str
    transform: NotRequired[PartitionTransform]
    buckets: NotRequired[int]
    direction: NotRequired[TSortOrder]


def is_partition_transform(value: object) -> TypeGuard[PartitionTransform]:
    return isinstance(value, str) and value in (
        "identity",
        "year",
        "month",
        "day",
        "hour",
        "bucket",
    )


def is_sort_direction(value: object) -> TypeGuard[TSortOrder]:
    return isinstance(value, str) and value in ("asc", "desc")


def layout_keys(keys: object, kind: LayoutKind) -> list[LayoutKey]:
    if not isinstance(keys, list):
        raise TerminalValueError(f"Altertable {kind} hints must be a list.")
    normalized: list[LayoutKey] = []
    for key in keys:
        allowed = (
            {"column", "transform", "buckets"} if kind == "partition" else {"column", "direction"}
        )
        entry: dict[object, object]
        if isinstance(key, str):
            entry = {"column": key}
        elif isinstance(key, dict) and not set(key) - allowed:
            entry = dict(key)
        else:
            raise TerminalValueError(f"Invalid Altertable {kind} key: {key!r}.")
        column = entry.get("column")
        if not isinstance(column, str) or not column or "\x00" in column:
            raise TerminalValueError(f"Invalid Altertable layout column: {column!r}.")
        item: LayoutKey = {"column": column}
        if kind == "partition":
            transform = entry.get("transform", "identity")
            if not is_partition_transform(transform):
                raise TerminalValueError(f"Unsupported partition transform: {transform!r}.")
            item["transform"] = transform
            buckets = entry.get("buckets")
            if transform == "bucket":
                if type(buckets) is not int or not 0 < buckets <= 2**31 - 1:
                    raise TerminalValueError("Bucket count must be an integer in 1..2147483647.")
                item["buckets"] = buckets
            elif "buckets" in entry:
                raise TerminalValueError("Only bucket transforms accept a bucket count.")
        else:
            direction = entry.get("direction", "asc")
            if not is_sort_direction(direction):
                raise TerminalValueError(f"Unsupported sort direction: {direction!r}.")
            item["direction"] = direction
        if item in normalized:
            raise TerminalValueError(f"Duplicate Altertable {kind} key: {key!r}.")
        normalized.append(item)
    return normalized


def layout_expressions(table: TTableSchema | PreparedTableSchema) -> dict[LayoutKind, list[str]]:
    layouts: dict[LayoutKind, list[str]] = {}
    for kind in LAYOUT_SETTINGS:
        hint = f"x-altertable-{kind}"
        if hint in table:
            keys = layout_keys(table.get(hint), kind)
        elif any(kind in column for column in table["columns"].values()):
            for column in table["columns"].values():
                if kind in column and type(column[kind]) is not bool:
                    raise TerminalValueError(f"Standard {kind} column hints must be bool.")
            keys = layout_keys(
                [name for name, column in table["columns"].items() if column.get(kind)], kind
            )
        else:
            continue
        expressions = []
        for key in keys:
            name = key["column"]
            if name not in table["columns"]:
                raise TerminalValueError(
                    f"Table {table['name']}: layout column {name!r} does not exist."
                )
            column = escape_postgres_identifier(name)
            if kind == "sort":
                expressions.append(f"{column} {key['direction'].upper()}")
                continue
            transform = key["transform"]
            data_type = table["columns"][name].get("data_type")
            if transform in ("year", "month", "day", "hour") and (
                data_type not in ("date", "timestamp")
                or (transform == "hour" and data_type == "date")
            ):
                raise TerminalValueError(
                    f"Table {table['name']}: {transform} cannot partition {name!r} ({data_type})."
                )
            if transform == "identity":
                expressions.append(column)
            elif transform == "bucket":
                expressions.append(f"bucket({key['buckets']}, {column})")
            else:
                expressions.append(f"{transform}({column})")
        layouts[kind] = expressions
    if layouts and table.get("write_disposition") == "replace":
        raise TerminalValueError(
            "Altertable layout hints require append or merge; replace recreates the table."
        )
    return layouts
