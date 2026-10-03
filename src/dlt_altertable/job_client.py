from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from functools import cached_property, partial
from typing import cast, override
from warnings import warn

from dlt.common import json
from dlt.common.data_writers.escape import escape_duckdb_literal, escape_postgres_identifier
from dlt.common.destination.client import (
    LoadJob,
    PreparedTableSchema,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
)
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationUndefinedEntity,
)
from dlt.common.schema import TSchemaTables, TTableSchema
from dlt.common.schema.utils import get_nested_tables, get_root_table, has_column_with_prop
from dlt.common.storages.load_package import destination_state
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.destinations.impl.destination.destination import DestinationClient
from dlt.destinations.job_impl import DestinationParquetLoadJob
from dlt.destinations.sql_client import WithSqlClient
from dlt.destinations.utils import verify_schema_merge_disposition

from dlt_altertable.api import execute_sql
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.destination import upsert_params
from dlt_altertable.layout import PARTITION_HINT, partition_expressions, partition_keys
from dlt_altertable.merge import (
    MergeThenDeleteJob,
    completed_staging_tables,
    merge_statements,
    stage_file,
    staging_config,
)
from dlt_altertable.sql_client import AltertableSqlClient
from dlt_altertable.table_schema import (
    create_or_evolve_table,
    qualified_schema_name,
    qualified_table_name,
    sql_type,
)


class AltertableJobClient(DestinationClient, WithStateSync, WithSqlClient):
    config: AltertableClientConfiguration

    @override
    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)
        if PARTITION_HINT in table:
            keys = partition_keys(table.get(PARTITION_HINT))
            for key in keys:
                key["column"] = self.schema.naming.normalize_identifier(key["column"])
            table.update({PARTITION_HINT: keys})
        return table

    @override
    def verify_schema(
        self,
        only_tables: Iterable[str] | None = None,
        new_jobs: Iterable[ParsedLoadJobFileName] | None = None,
    ) -> list[PreparedTableSchema]:
        tables = super().verify_schema(only_tables or (), new_jobs or ())
        for table in tables:
            table_name = cast(str, table["name"])
            if self._requires_staging(table_name):
                root = get_root_table(self.schema.tables, table_name)
                chain = self._table_chain(cast(str, root["name"]))
                upsert_params(cast(TTableSchema, chain[0]), allow_hard_delete=True)
                MergeThenDeleteJob.generate_sql(chain, self.sql_client)
            else:
                upsert_params(cast(TTableSchema, table))
            for column in table["columns"].values():
                sql_type(column)
            partition_expressions(table)
        if validation_errors := verify_schema_merge_disposition(
            self.schema, tables, self.capabilities
        ):
            raise validation_errors[0]
        return tables

    def _requires_staging(self, table_name: str) -> bool:
        root = get_root_table(self.schema.tables, table_name)
        return root.get("write_disposition") == "merge" and (
            has_column_with_prop(root, "hard_delete")
            or len(get_nested_tables(self.schema.tables, cast(str, root["name"]))) > 1
        )

    def _table_chain(self, root_name: str) -> list[PreparedTableSchema]:
        return [
            self.prepare_load_table(cast(str, table["name"]))
            for table in get_nested_tables(self.schema.tables, root_name)
        ]

    @override
    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        if self._requires_staging(cast(str, table["name"])):
            return DestinationParquetLoadJob(
                file_path,
                self.config,
                destination_state(),
                partial(stage_file, self.config),
            )
        return super().create_load_job(table, file_path, load_id, restore)

    @property
    def sql_client_class(self) -> type[AltertableSqlClient]:
        return AltertableSqlClient

    @cached_property
    def sql_client(self) -> AltertableSqlClient:
        return self.sql_client_class(self.config, self.capabilities)

    def _table_exists(self, table_name: str) -> bool:
        rows = execute_sql(
            self.config,
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog = {escape_duckdb_literal(self.config.catalog)} "
            f"AND table_schema = {escape_duckdb_literal(self.config.dataset_name)} "
            f"AND table_name = {escape_duckdb_literal(table_name)}",
        )
        return any(row[0] == table_name for row in rows)

    def initialize_storage(self, truncate_tables: Iterable[str] | None = None) -> None:
        for table_name in (self.schema.version_table_name, self.schema.loads_table_name):
            create_or_evolve_table(self.config, self.schema.tables[table_name])

    def is_storage_initialized(self) -> bool:
        return self._table_exists(self.schema.version_table_name)

    def drop_storage(self, *, dry_run: bool = True) -> None:
        statement = f"DROP SCHEMA IF EXISTS {qualified_schema_name(self.config)} CASCADE"
        if dry_run is not False:
            warn(
                f"Dry run: query was not executed: {statement}. "
                "Pass dry_run=False to delete this schema and all its tables.",
                stacklevel=2,
            )
            return
        execute_sql(self.config, statement)

    def drop_tables(self, *tables: str, delete_schema: bool = True) -> None:
        if self.config.allow_destructive_refresh is not True:
            raise DestinationTerminalException(
                "Dropping resource tables requires altertable(allow_destructive_refresh=True)."
            )
        for table_name in tables:
            execute_sql(
                self.config, f"DROP TABLE IF EXISTS {qualified_table_name(self.config, table_name)}"
            )
        if delete_schema and self._table_exists(self.schema.version_table_name):
            execute_sql(
                self.config,
                f"DELETE FROM {qualified_table_name(self.config, self.schema.version_table_name)} "
                f"WHERE schema_name = {escape_duckdb_literal(self.schema.name)}",
            )

    def update_stored_schema(
        self,
        only_tables: Iterable[str] | None = None,
        expected_update: TSchemaTables | None = None,
        force: bool = False,
    ) -> TSchemaTables | None:
        update = super().update_stored_schema(only_tables or (), expected_update or {}, force)
        if self.get_stored_schema_by_hash(self.schema.stored_version_hash) is None:
            schema_insert_sql = self._insert_statement(
                self.schema.version_table_name,
                {
                    "version_hash": self.schema.stored_version_hash,
                    "schema_name": self.schema.name,
                    "version": self.schema.version,
                    "engine_version": self.schema.ENGINE_VERSION,
                    "inserted_at": datetime.now(UTC),
                    "schema": json.dumps(self.schema.to_dict()),
                },
            )
            execute_sql(self.config, schema_insert_sql)
        return update

    def _insert_statement(self, table_name: str, values: Mapping[str, object]) -> str:
        columns = ", ".join(escape_postgres_identifier(column) for column in values)
        literals = ", ".join(escape_duckdb_literal(value) for value in values.values())
        return (
            f"INSERT INTO {qualified_table_name(self.config, table_name)} "
            f"({columns}) VALUES ({literals})"
        )

    @override
    def complete_load(self, load_id: str) -> None:
        load_receipt_sql = self._insert_statement(
            self.schema.loads_table_name,
            {
                "load_id": load_id,
                "schema_name": self.schema.name,
                "status": 0,
                "inserted_at": datetime.now(UTC),
                "schema_version_hash": self.schema.stored_version_hash,
            },
        )
        staging_tables = completed_staging_tables()
        if not staging_tables:
            execute_sql(self.config, load_receipt_sql)
            return
        staged = staging_config(self.config, load_id)
        receipt_sql = (
            f"SELECT 1 FROM {qualified_table_name(self.config, self.schema.loads_table_name)} "
            f"WHERE load_id = {escape_duckdb_literal(load_id)} AND status = 0"
        )
        if not execute_sql(self.config, receipt_sql):
            self.sql_client.staging_dataset_name = cast(str, staged.dataset_name)
            statements = [
                "BEGIN TRANSACTION;",
                f"SELECT CASE WHEN EXISTS ({receipt_sql}) THEN error('Load already committed') "
                "ELSE TRUE END;",
            ]
            roots = {
                cast(str, get_root_table(self.schema.tables, name)["name"])
                for name in staging_tables
            }
            for root in sorted(roots):
                chain = self._table_chain(root)
                for table in chain:
                    create_or_evolve_table(self.config, table)
                statements.extend(merge_statements(chain, self.sql_client, staging_tables))
            statements.extend([f"{load_receipt_sql};", "COMMIT;"])
            # Late commits need the transactional receipt check; overlapping snapshots
            # conflict on the shared staging tables created by merge_statements.
            try:
                execute_sql(self.config, "\n".join(statements))
            except Exception:
                if not execute_sql(self.config, receipt_sql):
                    raise
        execute_sql(self.config, f"DROP SCHEMA IF EXISTS {qualified_schema_name(staged)} CASCADE")

    def _stored_schema(self, where: str) -> StorageSchemaInfo | None:
        if not self._table_exists(self.schema.version_table_name):
            return None
        rows = execute_sql(
            self.config,
            "SELECT version_hash, schema_name, version, engine_version, inserted_at, schema "
            f"FROM {qualified_table_name(self.config, self.schema.version_table_name)} "
            f"{where} ORDER BY inserted_at DESC LIMIT 1",
        )
        if not rows:
            return None
        version_hash, schema_name, version, engine_version, inserted_at, serialized_schema = rows[0]
        return StorageSchemaInfo(
            version_hash=version_hash,
            schema_name=schema_name,
            version=version,
            engine_version=engine_version,
            inserted_at=datetime.fromisoformat(str(inserted_at)),
            schema=serialized_schema,
        )

    def get_stored_schema(self, schema_name: str | None = None) -> StorageSchemaInfo | None:
        return self._stored_schema(
            f"WHERE schema_name = {escape_duckdb_literal(schema_name)}" if schema_name else ""
        )

    def get_stored_schema_by_hash(  # ty: ignore[invalid-method-override]
        self, version_hash: str
    ) -> StorageSchemaInfo | None:
        return self._stored_schema(f"WHERE version_hash = {escape_duckdb_literal(version_hash)}")

    def get_stored_state(self, pipeline_name: str) -> StateInfo | None:
        for table_name in (self.schema.state_table_name, self.schema.loads_table_name):
            if not self._table_exists(table_name):
                raise DestinationUndefinedEntity(
                    f"Missing state table {qualified_table_name(self.config, table_name)}"
                )
        rows = execute_sql(
            self.config,
            "SELECT s.version, s.engine_version, s.pipeline_name, s.state, "
            "s.created_at, s.version_hash, s._dlt_load_id "
            f"FROM {qualified_table_name(self.config, self.schema.state_table_name)} AS s "
            f"JOIN {qualified_table_name(self.config, self.schema.loads_table_name)} AS l "
            "ON l.load_id = s._dlt_load_id "
            f"WHERE s.pipeline_name = {escape_duckdb_literal(pipeline_name)} AND l.status = 0 "
            "ORDER BY l.load_id DESC LIMIT 1",
        )
        if not rows:
            return None
        version, engine_version, stored_pipeline_name, state, created_at, version_hash, load_id = (
            rows[0]
        )
        return StateInfo(
            version=version,
            engine_version=engine_version,
            pipeline_name=stored_pipeline_name,
            state=state,
            created_at=datetime.fromisoformat(str(created_at)),
            version_hash=version_hash,
            _dlt_load_id=load_id,
        )
