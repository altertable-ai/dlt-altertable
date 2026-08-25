from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException

from dlt_altertable import altertable, verify_catalog, verify_load
from tests.conftest import DESTINATION_OPTIONS, FakeServer


@pytest.fixture
def loaded_pipeline(tmp_path: Path):
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
)
def merged_contacts_by_cursor() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": 1, "lastmodified": "2026-08-01"}, {"id": 2, "lastmodified": "2026-08-02"}]


@dlt.resource(name="visits", write_disposition="merge", primary_key=["day", "visitor"])
def merged_visits() -> Iterator[list[dict[str, Any]]]:
    yield [
        {"day": "2026-08-19", "visitor": "ada", "hits": 1},
        {"day": "2026-08-19", "visitor": "ada", "hits": 3},
        {"day": "2026-08-20", "visitor": "grace", "hits": 1},
    ]


@dlt.resource(name="contacts", write_disposition="merge")
def keyless_contacts() -> Iterator[list[dict[str, Any]]]:
    yield [{"id": 3, "email": "linus@example.com"}]


def test_an_append_load_counts_only_the_rows_it_wrote(server: FakeServer, loaded_pipeline) -> None:
    pipeline = loaded_pipeline(appended_events())
    load_id = pipeline.last_trace.last_load_info.loads_ids[0]

    assert verify_load(pipeline) == []
    assert server.counts_asked["events"] == (
        f'SELECT count(*) FROM "lakehouse"."raw"."events" WHERE "_dlt_load_id" IN (E\'{load_id}\')'
    )


def test_a_row_count_that_misses_what_dlt_normalized_is_reported(
    server: FakeServer, loaded_pipeline
) -> None:
    server.counts = {"events": [1]}

    pipeline = loaded_pipeline(appended_events())

    assert verify_load(pipeline) == ["events: dlt normalized 2 rows, 1 landed"]


def test_a_replace_load_counts_the_whole_replacement_table(
    server: FakeServer, loaded_pipeline
) -> None:
    pipeline = loaded_pipeline(replaced_places())

    assert verify_load(pipeline) == []
    assert server.counts_asked["places"] == (
        'SELECT count(*), count(DISTINCT row(loaded."place_id")) '
        'FILTER (WHERE loaded."place_id" IS NOT NULL) '
        'FROM "lakehouse"."raw"."places" AS loaded'
    )


def test_duplicate_primary_keys_in_a_replace_table_are_reported(
    server: FakeServer, loaded_pipeline
) -> None:
    server.counts = {"people": [3, 2]}

    pipeline = loaded_pipeline(replaced_people())

    assert verify_load(pipeline) == [
        "people: replace left 3 rows, but the distinct, non-null primary-key count is 2"
    ]
    assert server.counts_asked["people"] == (
        'SELECT count(*), count(DISTINCT row(loaded."record_id")) '
        'FILTER (WHERE loaded."record_id" IS NOT NULL) '
        'FROM "lakehouse"."raw"."people" AS loaded'
    )


def test_a_merge_that_deduplicated_rows_is_not_a_mismatch(
    server: FakeServer, loaded_pipeline
) -> None:
    server.counts = {"visits": [2, 2]}

    pipeline = loaded_pipeline(merged_visits())

    assert pipeline.last_trace.last_normalize_info.row_counts["visits"] == 3
    assert verify_load(pipeline) == []


def test_duplicate_primary_keys_in_a_merge_table_are_reported(
    server: FakeServer, loaded_pipeline
) -> None:
    server.counts = {"visits": [5, 2]}

    pipeline = loaded_pipeline(merged_visits())
    load_id = pipeline.last_trace.last_load_info.loads_ids[0]

    assert verify_load(pipeline) == [
        "visits: merge left 5 rows, but the distinct, non-null primary-key count is 2"
    ]
    assert server.counts_asked["visits"] == (
        'SELECT count(*), count(DISTINCT row(loaded."day", loaded."visitor")) '
        'FILTER (WHERE loaded."day" IS NOT NULL AND loaded."visitor" IS NOT NULL) '
        'FROM "lakehouse"."raw"."visits" AS loaded SEMI JOIN '
        '(SELECT "day", "visitor" FROM "lakehouse"."raw"."visits" '
        f"WHERE \"_dlt_load_id\" IN (E'{load_id}')) AS current_load ON "
        'loaded."day" IS NOT DISTINCT FROM current_load."day" AND '
        'loaded."visitor" IS NOT DISTINCT FROM current_load."visitor"'
    )


def test_a_merge_without_a_primary_key_is_reported(
    server: FakeServer,
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

    pipeline = loaded_pipeline(keyless_contacts())

    assert pipeline.last_trace.last_load_info.has_failed_jobs
    assert verify_load(pipeline) == [
        "dlt recorded failed load jobs",
        "contacts: dlt normalized 1 rows, none landed with this load id",
    ]


def test_a_merge_whose_rows_all_lost_the_cursor_comparison_is_not_a_failure(
    server: FakeServer, loaded_pipeline
) -> None:
    server.counts = {"contacts": [0, 0]}

    pipeline = loaded_pipeline(merged_contacts_by_cursor())

    assert pipeline.last_trace.last_normalize_info.row_counts["contacts"] == 2
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


def test_a_table_that_never_arrived_is_reported_rather_than_raised(
    server: FakeServer, loaded_pipeline
) -> None:
    server.missing_tables = ["events"]

    pipeline = loaded_pipeline(appended_events())

    assert verify_load(pipeline) == ["events: the load includes this table, but it does not exist"]


def test_several_load_packages_for_one_table_cannot_be_reconciled(
    server: FakeServer, loaded_pipeline
) -> None:
    @dlt.source(name="alpha")
    def alpha():
        yield dlt.resource([{"id": 1, "seen": "alpha"}], name="shared", write_disposition="append")

    @dlt.source(name="beta")
    def beta():
        yield dlt.resource(
            [{"id": 2, "seen": "beta"}], name="shared", write_disposition="merge", primary_key="id"
        )

    pipeline = loaded_pipeline([alpha(), beta()])

    assert verify_load(pipeline) == [
        "shared: several load packages include this table, so it cannot be reconciled"
    ]
    assert "shared" not in server.counts_asked


@pytest.mark.usefixtures("server")
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
    resumed_pipeline.run()
    expected_problems = [
        "_dlt_pipeline_state: this run loaded a package it did not normalize, "
        "so it carries no row count to compare",
        "events: this run loaded a package it did not normalize, "
        "so it carries no row count to compare",
    ]

    assert resumed_pipeline.last_trace.last_normalize_info is None
    assert verify_load(resumed_pipeline) == expected_problems


def test_a_trace_without_a_load_says_so() -> None:
    never_ran = dlt.pipeline(
        pipeline_name="never_ran", destination=altertable(**DESTINATION_OPTIONS)
    )

    with pytest.raises(ValueError, match=r"pipeline\.run\(\)"):
        verify_load(never_ran)


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


def test_a_later_normalization_is_not_counted_against_the_load_it_follows(
    server: FakeServer, loaded_pipeline
) -> None:
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
