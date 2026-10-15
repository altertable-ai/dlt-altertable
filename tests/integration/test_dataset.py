from pathlib import Path

import dlt
import pytest

from dlt_altertable import verify_load

pytestmark = pytest.mark.integration


def test_pipeline_dataset_restores_its_own_state_and_verifies_readback(
    mock_config, pipeline_factory, tmp_path: Path
) -> None:
    configured = pipeline_factory("configured", naming_convention="snake_case")
    configured.run([{"id": 99}], table_name="events")
    dataset_name = f"{mock_config.dataset_name}_Selected"
    starts = []
    records = [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}]
    incremental = dlt.sources.incremental("id", initial_value=0)

    @dlt.resource
    def events(cursor=incremental):
        starts.append(cursor.last_value)
        yield records

    selected = dlt.pipeline(
        pipeline_name=configured.pipeline_name,
        destination=configured.destination,
        dataset_name=dataset_name,
        pipelines_dir=str(tmp_path / "selected"),
    )
    try:
        selected.run(events())
        assert verify_load(selected) == []
        records.append({"id": 3})
        restored = dlt.pipeline(
            pipeline_name=configured.pipeline_name,
            destination=configured.destination,
            dataset_name=dataset_name,
            pipelines_dir=str(tmp_path / "restored"),
        )

        restored.run(events())

        assert starts == [0, 2]
        assert restored.sql_client().dataset_name == dataset_name.lower()
        assert "label" in restored.default_schema.tables["events"]["columns"]
        assert restored.dataset().events.select("id", "label").order_by("id").fetchall() == [
            (1, "one"),
            (2, "two"),
            (3, None),
        ]
        assert verify_load(restored) == []
        assert configured.dataset().events.select("id").fetchall() == [(99,)]
    finally:
        with selected.destination_client() as client:
            client.drop_storage(dry_run=False)
