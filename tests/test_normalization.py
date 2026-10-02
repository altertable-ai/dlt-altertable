from pathlib import Path

import dlt
import pytest

from dlt_altertable import altertable
from tests.conftest import DESTINATION_OPTIONS


@pytest.mark.parametrize("max_table_nesting", [2, None])
def test_naming_and_nesting_can_be_configured(server, tmp_path: Path, max_table_nesting):
    pipeline = dlt.pipeline(
        pipeline_name="normalization",
        destination=altertable(
            **DESTINATION_OPTIONS,
            naming_convention="snake_case",
            max_table_nesting=max_table_nesting,
        ),
        pipelines_dir=str(tmp_path),
    )

    pipeline.run(
        [{"id": 1, "properties": {"dealname": "Example"}, "items": ["A"]}], table_name="Deals"
    )

    assert "properties__dealname" in server.uploads_for("deals")[0].schema.names
    root_id = server.uploads_for("deals")[0].rows[0]["_dlt_id"]
    assert server.uploads_for("deals__items")[0].rows[0]["_dlt_parent_id"] == root_id
