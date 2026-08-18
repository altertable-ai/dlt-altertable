import json
from http import HTTPStatus
from typing import Any

import requests
from dlt.common.destination.exceptions import DestinationTerminalException

UPLOAD_TIMEOUT = (30, 3600)
QUERY_TIMEOUT = (10, 300)
UPLOAD_BLOCK_BYTES = 1 << 20
TERMINAL_STATUSES = {
    HTTPStatus.BAD_REQUEST,
    HTTPStatus.UNAUTHORIZED,
    HTTPStatus.PAYMENT_REQUIRED,
    HTTPStatus.FORBIDDEN,
    HTTPStatus.NOT_FOUND,
    HTTPStatus.METHOD_NOT_ALLOWED,
}


class LargeBlockAdapter(requests.adapters.HTTPAdapter):
    """urllib3's default 16KiB blocksize measured 31% slower than 1MiB on loopback uploads."""

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["blocksize"] = UPLOAD_BLOCK_BYTES
        super().init_poolmanager(*args, **kwargs)


def make_session() -> requests.Session:
    http_session = requests.Session()
    adapter = LargeBlockAdapter()
    http_session.mount("http://", adapter)
    http_session.mount("https://", adapter)
    return http_session


session = make_session()


def raise_for_failure(response: requests.Response, action: str) -> None:
    if response.status_code == HTTPStatus.OK:
        return
    detail = f"{action} failed with HTTP {response.status_code}: {response.text.strip()}"
    if response.status_code in TERMINAL_STATUSES:
        raise DestinationTerminalException(detail)
    raise RuntimeError(detail)


def execute_sql(base_url: str, auth: tuple[str, str], statement: str) -> list[list]:
    response = session.post(
        f"{base_url}/query",
        json={"statement": statement, "ephemeral": True, "compute_size": "XS"},
        auth=auth,
        timeout=QUERY_TIMEOUT,
    )
    raise_for_failure(response, f"query {statement!r}")
    payload = [json.loads(line) for line in response.text.splitlines() if line.strip()]
    for entry in payload:
        if isinstance(entry, dict) and "error" in entry:
            raise RuntimeError(f"query {statement!r} failed mid-stream: {entry['error']}")
    return payload[2:]


def post_parquet(
    base_url: str,
    auth: tuple[str, str],
    endpoint: str,
    params: dict[str, str],
    parquet_file_path: str,
    action: str,
) -> None:
    with open(parquet_file_path, "rb") as parquet_file:
        response = session.post(
            f"{base_url}/{endpoint}",
            params=params,
            data=parquet_file,
            auth=auth,
            headers={"Content-Type": "application/parquet"},
            timeout=UPLOAD_TIMEOUT,
        )
    raise_for_failure(response, action)
