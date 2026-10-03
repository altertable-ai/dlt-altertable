from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException

from dlt_altertable import altertable, api, verify_catalog, verify_load
from dlt_altertable.configuration import AltertableClientConfiguration
from tests.conftest import DESTINATION_OPTIONS, FakeServer


@pytest.fixture
def loaded_pipeline(tmp_path: Path, local_api):
    def load(resource: Any) -> dlt.Pipeline:
        pipeline = dlt.pipeline(
            pipeline_name="verify",
            destination=altertable(**DESTINATION_OPTIONS),
            dataset_name="raw",
            pipelines_dir=str(tmp_path / "dlt"),
        )
        pipeline.run(resource)
        return pipeline

    return load


@dlt.resource(name="events", write_disposition="append")
def appended_events() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": 1, "kind": "page_view"}, {"id": 2, "kind": "click"}]


@dlt.resource(name="people", write_disposition="replace", primary_key="record_id")
def replaced_people() -> Iterator[list[dict[str, Any]]]:
    yield [
        {"record_id": "person-1"},
        {"record_id": "person-1"},
        {"record_id": "person-2"},
    ]


@dlt.resource(name="places", write_disposition="replace", primary_key="place_id")
def replaced_places() -> Iterator[list[dict[str, Any]]]:
    yield [{"place_id": "place-1"}, {"place_id": "place-2"}]


@dlt.resource(
    name="contacts",
    write_disposition="merge",
    primary_key="id",
    columns={"lastmodified": {"dedup_sort": "desc"}},
    max_table_nesting=1,
)
def merged_contacts_by_cursor() -> Iterator[list[dict[str, Any]]]:
    yield [
        {"id": 1, "lastmodified": "2026-08-01", "items": ["first"]},
        {"id": 2, "lastmodified": "2026-08-02", "items": ["second"]},
    ]


@dlt.resource(
    name="visits",
    write_disposition="merge",
    primary_key=["day", "visitor"],
    columns={"deleted": {"hard_delete": True}},
)
def merged_visits() -> Iterator[list[dict[str, Any]]]:
    yield [
        {"day": "2026-08-19", "visitor": "ada", "hits": 1, "deleted": False},
        {"day": "2026-08-19", "visitor": "ada", "hits": 3, "deleted": False},
        {"day": "2026-08-20", "visitor": "grace", "hits": 1, "deleted": False},
    ]


@dlt.resource(name="contacts", write_disposition="merge", primary_key="id")
def merged_contacts() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": 3, "email": "linus@example.com"}]


@pytest.mark.ducklake
def test_an_append_load_counts_only_the_rows_it_wrote(connection, loaded_pipeline) -> None:
    pipeline = loaded_pipeline(appended_events())
    pipeline.run(appended_events())

    assert connection.execute("SELECT count(*) FROM lakehouse.raw.events").fetchone() == (4,)
    assert verify_load(pipeline) == []


@pytest.mark.ducklake
def test_a_row_count_that_misses_what_dlt_normalized_is_reported(
    connection, loaded_pipeline
) -> None:
    pipeline = loaded_pipeline(appended_events())
    connection.execute("DELETE FROM lakehouse.raw.events WHERE id = 2")

    assert verify_load(pipeline) == ["events: dlt normalized 2 rows, 1 landed"]


@pytest.mark.ducklake
def test_a_replace_load_counts_the_whole_replacement_table(connection, loaded_pipeline) -> None:
    pipeline = loaded_pipeline(replaced_places())
    connection.execute(
        "UPDATE lakehouse.raw.places SET _dlt_load_id = 'older' WHERE place_id = 'place-1'"
    )

    assert verify_load(pipeline) == []


@pytest.mark.ducklake
@pytest.mark.parametrize("null_key", [False, True])
def test_duplicate_or_null_primary_keys_in_a_replace_table_are_reported(
    connection, loaded_pipeline, null_key: bool
) -> None:
    pipeline = loaded_pipeline(replaced_people())
    if null_key:
        connection.execute(
            "UPDATE lakehouse.raw.people SET record_id = NULL WHERE record_id = 'person-2'"
        )
    distinct_count = 1 if null_key else 2

    assert verify_load(pipeline) == [
        "people: replace left 3 rows, but the distinct, "
        f"non-null primary-key count is {distinct_count}"
    ]


@pytest.mark.ducklake
def test_a_merge_that_deduplicated_rows_is_not_a_mismatch(connection, loaded_pipeline) -> None:
    pipeline = loaded_pipeline(merged_visits())

    assert pipeline.last_trace.last_normalize_info.row_counts["visits"] == 3
    assert connection.execute("SELECT count(*) FROM lakehouse.raw.visits").fetchone() == (2,)
    assert verify_load(pipeline) == []


@pytest.mark.ducklake
def test_duplicate_primary_keys_in_a_merge_table_are_reported(connection, loaded_pipeline) -> None:
    pipeline = loaded_pipeline(merged_visits())
    connection.execute(
        "INSERT INTO lakehouse.raw.visits "
        "SELECT visits.* REPLACE ('older' AS _dlt_load_id) "
        "FROM lakehouse.raw.visits AS visits CROSS JOIN range(3) WHERE visitor = 'ada'"
    )
    connection.execute(
        "INSERT INTO lakehouse.raw.visits "
        "SELECT * REPLACE ('unrelated' AS day, 'older' AS _dlt_load_id) "
        "FROM lakehouse.raw.visits"
    )

    assert verify_load(pipeline) == [
        "visits: merge left 5 rows, but the distinct, non-null primary-key count is 2"
    ]


@pytest.mark.ducklake
def test_a_failed_merge_load_is_reported(
    loaded_pipeline,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("LOAD__RAISE_ON_FAILED_JOBS", "false")
    seed_pipeline = dlt.pipeline(
        pipeline_name="seed",
        destination=altertable(**DESTINATION_OPTIONS),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "dlt"),
    )
    seed_pipeline.run(dlt.resource([{"id": 1}], name="contacts", write_disposition="append"))
    upload = api.post_parquet

    def fail_merge_upload(
        config: AltertableClientConfiguration,
        endpoint: str,
        params: dict[str, str],
        path: str,
        action: str,
    ) -> None:
        if endpoint == "upsert":
            raise DestinationTerminalException("injected terminal failure")
        return upload(config, endpoint, params, path, action)

    monkeypatch.setattr(api, "post_parquet", fail_merge_upload)

    pipeline = loaded_pipeline(merged_contacts())

    assert pipeline.last_trace.last_load_info.has_failed_jobs
    assert verify_load(pipeline) == [
        "dlt recorded failed load jobs",
        "contacts: dlt normalized 1 rows, none landed with this load id",
    ]


@pytest.mark.ducklake
def test_a_merge_whose_rows_all_lost_the_cursor_comparison_is_not_a_failure(
    connection, loaded_pipeline
) -> None:
    pipeline = loaded_pipeline(merged_contacts_by_cursor())
    connection.execute("UPDATE lakehouse.raw.contacts SET lastmodified = '2026-09-01'")

    pipeline.run(merged_contacts_by_cursor())

    assert pipeline.last_trace.last_normalize_info.row_counts["contacts"] == 2
    assert connection.execute(
        "SELECT lastmodified FROM lakehouse.raw.contacts ORDER BY id"
    ).fetchall() == [("2026-09-01",), ("2026-09-01",)]
    assert verify_load(pipeline) == []


def test_the_workers_own_memory_database_is_not_a_catalog(server: FakeServer) -> None:
    server.catalogs = {"memory": False, "lakehouse": False}

    assert verify_catalog(**{**DESTINATION_OPTIONS, "catalog": "memory"}) == [
        "catalog 'memory' is not attached to altertable.test, which serves ['lakehouse']"
    ]


def test_a_ducklake_metadata_catalog_is_reported_for_what_it_is(server: FakeServer) -> None:
    server.catalogs = {"lakehouse": False, "__ducklake_metadata_lakehouse": False}

    assert verify_catalog(
        **{**DESTINATION_OPTIONS, "catalog": "__ducklake_metadata_lakehouse"}
    ) == [
        "catalog '__ducklake_metadata_lakehouse' is the DuckLake metadata store behind "
        "'lakehouse', not a catalog to load into: writing tables there corrupts the "
        "bookkeeping the lakehouse reads"
    ]


def test_a_metadata_catalog_is_not_offered_as_somewhere_to_load(server: FakeServer) -> None:
    server.catalogs = {"lakehouse": False, "__ducklake_metadata_lakehouse": False}

    assert verify_catalog(**{**DESTINATION_OPTIONS, "catalog": "typo"}) == [
        "catalog 'typo' is not attached to altertable.test, which serves ['lakehouse']"
    ]


@pytest.mark.ducklake
def test_a_table_that_never_arrived_is_reported_rather_than_raised(
    connection, loaded_pipeline
) -> None:
    pipeline = loaded_pipeline(appended_events())
    connection.execute("DROP TABLE lakehouse.raw.events")

    assert verify_load(pipeline) == ["events: the load includes this table, but it does not exist"]


@pytest.mark.ducklake
def test_several_load_packages_for_one_table_cannot_be_reconciled(loaded_pipeline) -> None:
    @dlt.source(name="alpha")
    def alpha():
        yield dlt.resource([{"id": 1, "seen": "alpha"}], name="shared", write_disposition="append")

    @dlt.source(name="beta")
    def beta():
        yield dlt.resource(
            [{"id": 2, "seen": "beta", "deleted": False}],
            name="shared",
            write_disposition="merge",
            primary_key="id",
            columns={"deleted": {"hard_delete": True}},
        )

    pipeline = loaded_pipeline([alpha(), beta()])

    assert verify_load(pipeline) == [
        "shared: several load packages include this table, so it cannot be reconciled"
    ]


@pytest.mark.ducklake
@pytest.mark.usefixtures("local_api")
def test_a_resumed_load_reports_the_row_count_it_cannot_reconcile(tmp_path: Path) -> None:
    def build_pipeline() -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name="resumed",
            destination=altertable(**DESTINATION_OPTIONS),
            dataset_name="raw",
            pipelines_dir=str(tmp_path / "dlt"),
        )

    pipeline_with_pending_load = build_pipeline()
    pipeline_with_pending_load.extract(appended_events())
    pipeline_with_pending_load.normalize()

    resumed_pipeline = build_pipeline()
    resumed_pipeline.load()
    expected_problems = [
        "_dlt_pipeline_state: this run loaded a package it did not normalize, "
        "so it carries no row count to compare",
        "events: this run loaded a package it did not normalize, "
        "so it carries no row count to compare",
    ]

    assert resumed_pipeline.last_trace.last_normalize_info is None
    assert verify_load(resumed_pipeline) == expected_problems


@pytest.mark.parametrize("extract_first", [False, True])
def test_a_trace_without_a_load_says_so(tmp_path, extract_first: bool) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="never_ran",
        destination=altertable(**DESTINATION_OPTIONS),
        pipelines_dir=str(tmp_path),
    )
    if extract_first:
        pipeline.extract([{"id": 1}], table_name="events")

    with pytest.raises(ValueError, match=r"pipeline\.run\(\)"):
        verify_load(pipeline)


@pytest.mark.usefixtures("server")
def test_a_catalog_a_load_can_reach_reports_nothing() -> None:
    assert verify_catalog(**DESTINATION_OPTIONS) == []


def test_a_catalog_that_is_not_attached_names_the_ones_that_are(server: FakeServer) -> None:
    server.catalogs = {"analytics": False, "staging": False}

    assert verify_catalog(**DESTINATION_OPTIONS) == [
        "catalog 'lakehouse' is not attached to altertable.test, "
        "which serves ['analytics', 'staging']"
    ]


def test_a_read_only_catalog_is_reported(server: FakeServer) -> None:
    server.catalogs = {"lakehouse": True}

    assert verify_catalog(**DESTINATION_OPTIONS) == [
        "catalog 'lakehouse' is attached read only, so a load cannot write to it"
    ]


def test_rejected_credentials_raise_the_query_error(server: FakeServer) -> None:
    server.unauthenticated = True

    with pytest.raises(DestinationTerminalException, match="HTTP 401"):
        verify_catalog(**DESTINATION_OPTIONS)


@pytest.mark.ducklake
def test_a_later_normalization_is_not_counted_against_the_load_it_follows(loaded_pipeline) -> None:
    pipeline = loaded_pipeline(appended_events())
    pipeline.extract(
        dlt.resource(
            [{"id": id} for id in range(10, 15)], name="events", write_disposition="append"
        )
    )
    pipeline.normalize()
    trace = pipeline.last_trace

    assert trace.last_normalize_info.loads_ids != trace.last_load_info.loads_ids
    assert trace.last_normalize_info.row_counts["events"] == 5
    assert verify_load(pipeline) == []
