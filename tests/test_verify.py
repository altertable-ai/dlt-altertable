import pytest
from dlt.common.destination.exceptions import DestinationTerminalException

from dlt_altertable import verify_catalog
from tests.conftest import DESTINATION_OPTIONS, FakeServer


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
