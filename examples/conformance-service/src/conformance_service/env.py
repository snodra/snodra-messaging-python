"""The service's settings, from the ``CONFORMANCE_*`` variables the runner sets."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from snodra import ClientCertificate, ClientCredentials, ConfigurationError, RetryPolicy, Settings, TLSConfig, Token

DEFAULT_CAPACITY = 64
"""The handlers the instance runs at once when ``CONFORMANCE_CAPACITY`` is not set."""


def settings_from_env(environ: Mapping[str, str]) -> Settings:
    """The library's settings for the runner's variables; an absent (or blank) variable is not configured."""

    def get(name: str) -> str | None:
        value = environ.get(name, "").strip()
        return value or None

    values: dict[str, Any] = {"service": get("CONFORMANCE_SERVICE_NAME") or ""}
    if (instance := get("CONFORMANCE_INSTANCE_ID")) is not None:
        values["instance"] = instance
    if (hubs := get("CONFORMANCE_HUBS")) is not None:
        values["hubs"] = tuple(h.strip() for h in hubs.split(",") if h.strip())
    capacity = get("CONFORMANCE_CAPACITY")
    values["max_concurrent"] = DEFAULT_CAPACITY if capacity is None else int(capacity)
    if (subscriptions := get("CONFORMANCE_MAX_SUBSCRIPTIONS")) is not None:
        values["max_subscriptions"] = _positive("CONFORMANCE_MAX_SUBSCRIPTIONS", subscriptions)
    values["default_retry"] = _retry(get)
    if (credit := get("CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS")) is not None:
        values["stream_credit_timeout"] = _positive_ms("CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS", credit) / 1000
    # Every call goes through the hub, also one of the service's own types: the runner sees what is sent.
    values["always_route_through_hub"] = True
    # The callers' tokens are verified only where the runner gives its issuer (conformance.Guarded needs it).
    if (jwks_url := get("CONFORMANCE_JWKS_URL")) is not None:
        values["jwks_url"] = jwks_url
    if (issuer := get("CONFORMANCE_ISSUER")) is not None:
        values["issuer"] = issuer
    _credentials(get, values)
    return Settings(**values)


def _credentials(get: Callable[[str], str | None], values: dict[str, Any]) -> None:
    """The service's credential towards the hubs: a static token, a JWT of the runner's token endpoint, TLS with the
    runner's CA and a client certificate, as the runner sets them."""
    if (token := get("CONFORMANCE_HUB_TOKEN")) is not None:
        values["credentials"] = Token(token)
    if (url := get("CONFORMANCE_OAUTH2_TOKEN_URL")) is not None:
        values["credentials"] = ClientCredentials(
            url,
            get("CONFORMANCE_OAUTH2_CLIENT_ID") or values["service"],
            client_secret=get("CONFORMANCE_OAUTH2_CLIENT_SECRET"),
        )
    ca, name = get("CONFORMANCE_TLS_CA"), get("CONFORMANCE_TLS_SERVER_NAME")
    if ca is not None or name is not None:
        values["tls"] = TLSConfig(ca, name)
    cert, key = get("CONFORMANCE_TLS_CLIENT_CERT"), get("CONFORMANCE_TLS_CLIENT_KEY")
    if cert is not None or key is not None:
        values["client_certificate"] = ClientCertificate(cert or "", key or "")


def _positive_ms(name: str, value: str) -> int:
    """A positive whole number of milliseconds, as the runner's timeout variables are."""
    if not (value.isascii() and value.isdigit()) or int(value) < 1:
        raise ConfigurationError(f"{name} must be a positive integer of milliseconds, got {value!r}")
    return int(value)


def _positive(name: str, value: str) -> int:
    """A positive whole number, as a count of the runner's variables is."""
    if not (value.isascii() and value.isdigit()) or int(value) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {value!r}")
    return int(value)


def _retry(get: Callable[[str], str | None]) -> RetryPolicy | None:
    """The policy of every send: ``CONFORMANCE_RETRY_MAX_ATTEMPTS`` attempts, the first backoff
    ``CONFORMANCE_RETRY_INITIAL_BACKOFF_MS`` (100 by default); None when the first is not set."""
    attempts = get("CONFORMANCE_RETRY_MAX_ATTEMPTS")
    if attempts is None:
        return None
    if not (attempts.isascii() and attempts.isdigit()) or int(attempts) < 1:
        raise ConfigurationError(f"CONFORMANCE_RETRY_MAX_ATTEMPTS must be a positive integer, got {attempts!r}")
    backoff = get("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS") or "100"
    if not (backoff.isascii() and backoff.isdigit()):
        raise ConfigurationError(
            f"CONFORMANCE_RETRY_INITIAL_BACKOFF_MS must be a non-negative integer, got {backoff!r}"
        )
    return RetryPolicy(max_attempts=int(attempts), initial_backoff=int(backoff) / 1000)
