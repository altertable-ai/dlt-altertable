import dlt
import pyarrow as pa
import pytest

from dlt_altertable import verify_load

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("write_disposition", ["append", "replace", "merge"])
@pytest.mark.parametrize("add_load_id", [False, True])
def test_arrow_load_verification_handles_optional_load_ids(
    pipeline_factory, monkeypatch, write_disposition, add_load_id
) -> None:
    monkeypatch.setenv("NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID", str(add_load_id))
    pipeline = pipeline_factory("arrow_verification")

    for value in [1, 2]:
        pipeline.run(
            dlt.resource(
                pa.table({"id": [value]}),
                name="events",
                write_disposition=write_disposition,
                primary_key="id",
            )
        )

    expected = []
    if not add_load_id and write_disposition != "replace":
        expected = [
            "events: this table has no _dlt_load_id column, so this load cannot be reconciled. "
            "For Arrow inputs, enable NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID "
            "before loading."
        ]

    assert verify_load(pipeline) == expected
