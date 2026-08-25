from typing import Any

from dlt.common.destination.reference import AnyDestination_CO

from dlt_altertable.api import execute_sql
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.destination import altertable

# DuckLake attaches its own metadata database beside every catalog it opens
DUCKLAKE_METADATA_PREFIX = "__ducklake_metadata_"


def resolved_config(destination: AnyDestination_CO) -> AltertableClientConfiguration:
    if not issubclass(destination.spec, AltertableClientConfiguration):
        raise TypeError(
            f"{destination.destination_type} is not an Altertable destination, so it has no "
            "lakehouse to read a load back from"
        )
    return destination.configuration(destination.spec())


def verify_catalog(**options: Any) -> list[str]:
    """Report why the configured catalog cannot be loaded into."""
    config = resolved_config(altertable(**options))
    attached = execute_sql(
        config,
        "SELECT database_name, readonly FROM duckdb_databases() "
        "WHERE NOT internal AND database_name <> 'memory'",
    )

    catalogs = dict(attached)
    loadable = {
        name: readonly
        for name, readonly in catalogs.items()
        if not name.startswith(DUCKLAKE_METADATA_PREFIX)
    }
    if config.catalog in catalogs and config.catalog not in loadable:
        return [
            f"catalog {config.catalog!r} is the DuckLake metadata store behind "
            f"{config.catalog.removeprefix(DUCKLAKE_METADATA_PREFIX)!r}, not a catalog to load "
            "into: writing tables there corrupts the bookkeeping the lakehouse reads"
        ]
    if config.catalog not in loadable:
        return [
            f"catalog {config.catalog!r} is not attached to {config.host}, which serves "
            f"{sorted(loadable)}"
        ]
    if loadable[config.catalog]:
        return [f"catalog {config.catalog!r} is attached read only, so a load cannot write to it"]
    return []
