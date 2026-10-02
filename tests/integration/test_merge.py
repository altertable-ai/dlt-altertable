import dlt
import pytest

from dlt_altertable import verify_load
from dlt_altertable.api import execute_sql
from dlt_altertable.table_schema import qualified_table_name

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("updated_items", [["D", "E"], []], ids=["replace", "empty"])
def test_nested_merge_replaces_children_across_files_over_http(
    pipeline_factory, mock_config, monkeypatch, updated_items
):
    monkeypatch.setenv("DATA_WRITER__FILE_MAX_ITEMS", "1")
    monkeypatch.setenv("DATA_WRITER__BUFFER_MAX_ITEMS", "1")
    pipeline = pipeline_factory(
        "nested_merge", naming_convention="snake_case", max_table_nesting=None
    )

    def resource(rows):
        return dlt.resource(rows, name="mrg_deals", primary_key="id", write_disposition="merge")

    parents = qualified_table_name(mock_config, "mrg_deals")
    children = qualified_table_name(mock_config, "mrg_deals__items")
    children_query = (
        f"SELECT p.id, c.value FROM {parents} p JOIN {children} c "
        "ON c._dlt_root_id = p._dlt_id ORDER BY p.id, c._dlt_list_idx"
    )
    rows = [
        {"id": 1, "properties": {"dealname": "d1"}, "items": ["A", "B"]},
        {"id": 2, "properties": {"dealname": "d2"}, "items": ["C"]},
    ]

    loaded = pipeline.run(resource(rows))

    assert (
        sum(
            job.job_file_info.table_name == "mrg_deals__items"
            for job in loaded.load_packages[0].jobs["completed_jobs"]
        )
        == 3
    )

    pipeline.run(resource(rows))

    assert execute_sql(mock_config, children_query) == [[1, "A"], [1, "B"], [2, "C"]]

    pipeline.run(
        resource([{"id": 1, "properties": {"dealname": "updated"}, "items": updated_items}])
    )

    assert execute_sql(
        mock_config, f"SELECT id, properties__dealname FROM {parents} ORDER BY id"
    ) == [[1, "updated"], [2, "d2"]]
    assert execute_sql(mock_config, children_query) == [
        *[[1, value] for value in updated_items],
        [2, "C"],
    ]
    assert execute_sql(mock_config, f"SELECT count(*) FROM {children}") == [
        [len(updated_items) + 1]
    ]
    assert verify_load(pipeline) == []


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
