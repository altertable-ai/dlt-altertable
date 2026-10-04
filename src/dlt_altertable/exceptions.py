from http import HTTPStatus

import requests
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException

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


def make_database_exception(ex: Exception) -> Exception:
    if isinstance(ex, TRANSPORT_ERRORS):
        return DatabaseTransientException(ex)
    if isinstance(ex, requests.HTTPError) and ex.response is not None:
        return (
            DatabaseTerminalException(ex)
            if ex.response.status_code in TERMINAL_STATUSES
            else DatabaseTransientException(ex)
        )
    return ex
