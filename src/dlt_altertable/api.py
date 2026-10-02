import json
import re
from http import HTTPStatus
from importlib.metadata import version
from typing import Any

import requests
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from requests.adapters import HTTPAdapter
from requests.utils import default_user_agent

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
TRANSPORT_ERRORS = (
    requests.Timeout,
    requests.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


class QueryError(RuntimeError):
    def __init__(self, statement: str, server_message: str) -> None:
        self.server_message = server_message
        super().__init__(f"Query failed mid-stream: {server_message}\nSQL: {statement!r}")


def make_database_exception(ex: Exception) -> Exception:
    if isinstance(ex, TRANSPORT_ERRORS):
        return DatabaseTransientException(ex)
    if isinstance(ex, requests.HTTPError) and ex.response is not None:
        status = ex.response.status_code
        if status != HTTPStatus.BAD_REQUEST:
            return (
                DatabaseTerminalException(ex)
                if status in TERMINAL_STATUSES
                else DatabaseTransientException(ex)
            )
        message = ex.response.text
    elif isinstance(ex, QueryError):
        message = ex.server_message
    else:
        return ex

    first_line = message.partition("\n")[0]
    if first_line.startswith(("TransactionContext Error: ", "Transaction Error: ")):
        return DatabaseTransientException(ex)
    if re.fullmatch(
        r'Catalog Error: (?:(?:Table|Schema) with name [^"\r\n]+ does not exist!|'
        r'Table with name "[^"\r\n]+" does not exist because schema "[^"\r\n]+" does not exist\.)|'
        r'Binder Error: Schema "[^"\r\n]+" not found in DuckLakeCatalog "[^"\r\n]+"',
        first_line,
    ):
        return DatabaseUndefinedRelation(ex)
    if isinstance(ex, requests.HTTPError) or first_line.startswith(
        (
            "Catalog Error: ",
            "Binder Error: ",
            "Conversion Error: ",
            "Constraint Error: ",
            "Out of Range Error: ",
            "Not implemented Error: ",
            "Permission Error: ",
        )
    ):
        return DatabaseTerminalException(ex)
    return DatabaseTransientException(ex)


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
    payload = [json.loads(line) for line in response.text.splitlines() if line.strip()]
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
        error = QueryError(statement, entry["error"])
        raise make_database_exception(error) from error
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
