import os

from dlt.common.configuration import configspec
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.typing import TSecretStrValue
from dlt.destinations.impl.destination.configuration import CustomDestinationClientConfiguration

SANDBOX_ENV_VARS = {
    "host": "ALTERTABLE_HOST",
    "catalog": "ALTERTABLE_CATALOG",
    "dataset_name": "ALTERTABLE_SCHEMA",
    "username": "ALTERTABLE_USERNAME",
    "password": "ALTERTABLE_PASSWORD",
}


@configspec
class AltertableClientConfiguration(CustomDestinationClientConfiguration):
    """Connection settings, resolvable from destination arguments, `.dlt/secrets.toml`,
    `DESTINATION__ALTERTABLE__*` variables, or the `ALTERTABLE_*` variables an Altertable
    sandbox already provides."""

    host: str | None = None
    catalog: str | None = None
    dataset_name: str | None = None
    username: str | None = None
    password: TSecretStrValue | None = None
    port: int | None = None
    tls: bool | None = None

    def on_resolved(self) -> None:
        for parameter, env_var in SANDBOX_ENV_VARS.items():
            value = getattr(self, parameter) or os.environ.get(env_var)
            if not value:
                raise DestinationTerminalException(
                    f"{parameter} is not configured: pass {parameter}= to altertable(), set "
                    f"destination.altertable.{parameter} in .dlt/secrets.toml, or export "
                    f"{env_var}."
                )
            setattr(self, parameter, value)
        if self.port is None:
            self.port = int(os.environ.get("ALTERTABLE_PORT", "443"))
        if self.tls is None:
            self.tls = os.environ.get("ALTERTABLE_TLS", "true").lower() != "false"

    @property
    def base_url(self) -> str:
        return f"{'https' if self.tls else 'http'}://{self.host}:{self.port}"

    @property
    def basic_auth(self) -> tuple[str, str]:
        return (self.username, self.password)
