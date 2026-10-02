import dlt
import pytest

from dlt_altertable import verify_load
from dlt_altertable.api import execute_sql
from dlt_altertable.table_schema import qualified_table_name

pytestmark = pytest.mark.integration


def test_nested_merge_and_tombstones_round_trip_over_http(
    pipeline_factory, mock_config, monkeypatch
):
    monkeypatch.setenv("SCHEMA__NAMING", "snake_case")
    pipeline = pipeline_factory("nested_merge")

    def resource(rows):
        return dlt.resource(
            rows,
            name="deals",
            primary_key="id",
            write_disposition="merge",
            max_table_nesting=2,
            columns={"deleted": {"hard_delete": True}},
        )

    pipeline.run(resource([{"id": 1, "deleted": False, "items": ["A", "B"]}]))
    pipeline.run(resource([{"id": 1, "deleted": False, "items": ["C"]}]))

    assert execute_sql(
        mock_config, f"SELECT value FROM {qualified_table_name(mock_config, 'deals__items')}"
    ) == [["C"]]
    assert verify_load(pipeline) == []

    pipeline.run(resource([{"id": 1, "deleted": True}, {"id": 99, "deleted": True}]))

    for name in ("deals", "deals__items"):
        assert execute_sql(
            mock_config, f"SELECT count(*) FROM {qualified_table_name(mock_config, name)}"
        ) == [[0]]
    assert verify_load(pipeline) == []
