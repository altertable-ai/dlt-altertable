import json
from http import HTTPStatus
from typing import Any

import requests
from dlt.common.destination.exceptions import DestinationTerminalException
from requests.adapters import HTTPAdapter

from dlt_altertable.configuration import AltertableClientConfiguration

UPLOAD_TIMEOUT = (30, 3600)
QUERY_TIMEOUT = (10, 300)
UPLOAD_WRITE_BLOCK_BYTES = 1 << 20
LOADER_WORKERS = 20
TERMINAL_STATUSES = {
    HTTPStatus.BAD_REQUEST,
    HTTPStatus.UNAUTHORIZED,
    HTTPStatus.PAYMENT_REQUIRED,
    HTTPStatus.FORBIDDEN,
    HTTPStatus.NOT_FOUND,
    HTTPStatus.METHOD_NOT_ALLOWED,
}


class LargeBlockAdapter(HTTPAdapter):
    """A load job posts a whole parquet file in one stream, and urllib3's default blocksize is
    sized for ordinary requests, not for a file-sized body."""

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["blocksize"] = UPLOAD_WRITE_BLOCK_BYTES
        super().init_poolmanager(*args, **kwargs)


session = requests.Session()
_adapter = LargeBlockAdapter(pool_maxsize=LOADER_WORKERS)
session.mount("http://", _adapter)
session.mount("https://", _adapter)


def raise_for_failure(response: requests.Response, action: str) -> None:
    if response.status_code == HTTPStatus.OK:
        return
    detail = f"{action} failed with HTTP {response.status_code}: {response.text.strip()[:2000]}"
    if response.status_code in TERMINAL_STATUSES:
        raise DestinationTerminalException(detail)
    raise RuntimeError(detail)


def execute_sql(config: AltertableClientConfiguration, statement: str) -> list[list]:
    response = session.post(
        f"{config.base_url}/query",
        json={"statement": statement, "ephemeral": True, "compute_size": config.compute_size},
        auth=config.basic_auth,
        timeout=QUERY_TIMEOUT,
    )
    raise_for_failure(response, f"query {statement!r}")
    payload = [json.loads(line) for line in response.text.splitlines() if line.strip()]
    for entry in payload:
        if isinstance(entry, dict) and "error" in entry:
            raise RuntimeError(f"query {statement!r} failed mid-stream: {entry['error']}")
    return payload[2:]


def post_parquet(
    config: AltertableClientConfiguration,
    endpoint: str,
    params: dict[str, str],
    parquet_file_path: str,
    action: str,
) -> None:
    with open(parquet_file_path, "rb") as parquet_file:
        response = session.post(
            f"{config.base_url}/{endpoint}",
            params=params,
            data=parquet_file,
            auth=config.basic_auth,
            headers={"Content-Type": "application/parquet"},
            timeout=UPLOAD_TIMEOUT,
        )
    raise_for_failure(response, action)
