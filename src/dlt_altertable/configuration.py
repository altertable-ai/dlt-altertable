import os
from typing import cast

from dlt.common.configuration import ConfigurationValueError, configspec
from dlt.common.typing import TSecretStrValue
from dlt.destinations.impl.destination.configuration import CustomDestinationClientConfiguration

ENV_FALLBACKS = {
    "host": "ALTERTABLE_HOST",
    "catalog": "ALTERTABLE_CATALOG",
    "dataset_name": "ALTERTABLE_SCHEMA",
    "username": "ALTERTABLE_USERNAME",
    "password": "ALTERTABLE_PASSWORD",
}


@configspec
class AltertableClientConfiguration(CustomDestinationClientConfiguration):
    """Falls back to the `ALTERTABLE_*` environment variables after dlt's own providers,
    so a preconfigured environment needs no explicit destination configuration."""

    host: str | None = None
    catalog: str | None = None
    dataset_name: str | None = None
    username: str | None = None
    password: TSecretStrValue | None = None
    port: int | None = None
    tls: bool | None = None
    compute_size: str | None = None
    allow_destructive_refresh: bool = False

    def on_resolved(self) -> None:
        for parameter, env_var in ENV_FALLBACKS.items():
            value = getattr(self, parameter) or os.environ.get(env_var)
            if not value:
                raise ConfigurationValueError(
                    f"{parameter} is not configured: pass {parameter}= to altertable(), set "
                    f"destination.altertable.{parameter} in .dlt/secrets.toml, or export "
                    f"{env_var}."
                )
            setattr(self, parameter, value)
        if self.port is None:
            self.port = int(os.environ.get("ALTERTABLE_PORT", "443"))
        if self.tls is None:
            self.tls = os.environ.get("ALTERTABLE_TLS", "true").lower() != "false"
        if self.compute_size is None:
            self.compute_size = os.environ.get("ALTERTABLE_COMPUTE_SIZE", "XS")

    @property
    def base_url(self) -> str:
        return f"{'https' if self.tls else 'http'}://{self.host}:{self.port}"

    @property
    def basic_auth(self) -> tuple[str, str]:
        return cast(tuple[str, str], (self.username, self.password))
