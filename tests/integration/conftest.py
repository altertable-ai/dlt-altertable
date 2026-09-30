import os
from collections.abc import Callable, Iterator
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import dlt
import pytest

from dlt_altertable import altertable
from dlt_altertable.api import execute_sql
from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.table_schema import qualified_schema_name


@pytest.fixture
def mock_config() -> Iterator[AltertableClientConfiguration]:
    if os.environ.get("DLT_ALTERTABLE_INTEGRATION") != "1":
        pytest.skip("set DLT_ALTERTABLE_INTEGRATION=1 to use a local altertable-mock")
    endpoint = urlsplit(os.environ.get("DLT_ALTERTABLE_MOCK_URL", "http://127.0.0.1:15100"))
    assert endpoint.hostname in {"localhost", "127.0.0.1"}, "mock must be local"
    assert endpoint.scheme == "http", "mock must use local HTTP"
    config = AltertableClientConfiguration(
        host=endpoint.hostname,
        port=endpoint.port or 80,
        tls=False,
        username=os.environ.get("DLT_ALTERTABLE_MOCK_USERNAME", "integration"),
        password=os.environ.get("DLT_ALTERTABLE_MOCK_PASSWORD", "integration"),
        catalog="memory",
        dataset_name=f"dlt_integration_{uuid4().hex}",
    )
    config.on_resolved()
    try:
        yield config
    finally:
        execute_sql(config, f"DROP SCHEMA IF EXISTS {qualified_schema_name(config)} CASCADE")


@pytest.fixture
def pipeline_factory(
    mock_config: AltertableClientConfiguration, tmp_path: Path
) -> Callable[..., dlt.Pipeline]:
    def create(directory: str, pipeline_name: str = "state_restore") -> dlt.Pipeline:
        return dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=altertable(
                host=mock_config.host,
                port=mock_config.port,
                tls=mock_config.tls,
                username=mock_config.username,
                password=mock_config.password,
                catalog=mock_config.catalog,
                dataset_name=mock_config.dataset_name,
            ),
            pipelines_dir=str(tmp_path / directory),
        )

    return create
