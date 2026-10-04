import json
from http import HTTPStatus
from importlib.metadata import version
from typing import Any

import requests
from dlt.destinations.exceptions import DatabaseTransientException
from requests.adapters import HTTPAdapter
from requests.utils import default_user_agent

from dlt_altertable.configuration import AltertableClientConfiguration
from dlt_altertable.exceptions import TRANSPORT_ERRORS, make_database_exception

UPLOAD_TIMEOUT = (30, 3600)
QUERY_TIMEOUT = (10, 300)
UPLOAD_WRITE_BLOCK_BYTES = 1 << 20
LOADER_WORKERS = 20


class LargeBlockAdapter(HTTPAdapter):
    """A load job posts a whole parquet file in one stream, and urllib3's default blocksize is
    sized for ordinary requests, not for a file-sized body."""

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["blocksize"] = UPLOAD_WRITE_BLOCK_BYTES
        super().init_poolmanager(*args, **kwargs)


session = requests.Session()
session.headers["User-Agent"] = f"dlt-altertable/{version('dlt-altertable')} {default_user_agent()}"
_adapter = LargeBlockAdapter(pool_maxsize=LOADER_WORKERS)
session.mount("http://", _adapter)
session.mount("https://", _adapter)


def raise_for_failure(response: requests.Response, action: str) -> None:
    if response.status_code == HTTPStatus.OK:
        return
    detail = f"HTTP {response.status_code}: {response.text.strip()[:2000]}\nOperation: {action}"
    error = requests.HTTPError(detail, response=response)
    raise make_database_exception(error) from error


def post_query(
    config: AltertableClientConfiguration,
    statement: str,
    *,
    output_format: str | None = None,
    dataset_name: str | None = None,
) -> requests.Response:
    payload = {"statement": statement, "ephemeral": True, "compute_size": config.compute_size}
    if output_format is not None:
        payload["format"] = output_format
    if dataset_name is not None:
        payload.update(catalog=config.catalog, schema=dataset_name)
    try:
        response = session.post(
            f"{config.base_url}/query",
            json=payload,
            auth=config.basic_auth,
            timeout=QUERY_TIMEOUT,
        )
    except TRANSPORT_ERRORS as ex:
        raise make_database_exception(ex) from ex
    raise_for_failure(response, f"query {statement!r}")
    return response


def execute_sql(config: AltertableClientConfiguration, statement: str) -> list[list]:
    response = post_query(config, statement)
    payload = [json.loads(line) for line in response.text.split("\n") if line.strip()]
    if len(payload) < 2 or not isinstance(payload[0], dict) or "error" in payload[0]:
        raise RuntimeError(f"Malformed query response for {statement!r}: missing headers or rows.")
    if isinstance(payload[1], list) and not all(
        isinstance(column, dict)
        and isinstance(column.get("name"), str)
        and isinstance(column.get("type"), str)
        for column in payload[1]
    ):
        raise RuntimeError(f"Malformed query response for {statement!r}: invalid column headers.")
    for index, entry in enumerate(payload[1:], 1):
        if isinstance(entry, list):
            continue
        if (
            index != len(payload) - 1
            or not isinstance(entry, dict)
            or not isinstance(entry.get("error"), str)
        ):
            raise RuntimeError(
                f"Malformed query response for {statement!r}: invalid rows or error."
            )
        error = RuntimeError(f"Query failed mid-stream: {entry['error']}\nSQL: {statement!r}")
        raise DatabaseTransientException(error) from error
    return payload[2:]


def post_parquet(
    config: AltertableClientConfiguration,
    endpoint: str,
    params: dict[str, str],
    parquet_file_path: str,
    action: str,
) -> None:
    with open(parquet_file_path, "rb") as parquet_file:
        try:
            response = session.post(
                f"{config.base_url}/{endpoint}",
                params=params,
                data=parquet_file,
                auth=config.basic_auth,
                headers={"Content-Type": "application/parquet"},
                timeout=UPLOAD_TIMEOUT,
            )
        except TRANSPORT_ERRORS as ex:
            raise make_database_exception(ex) from ex
    raise_for_failure(response, action)
