"""The service's settings, from the ``CONFORMANCE_*`` variables the runner sets."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from snodra import ConfigurationError, RetryPolicy, Settings

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
    values["default_retry"] = _retry(get)
    # Every call goes through the hub, also one of the service's own types: the runner sees what is sent.
    values["always_route_through_hub"] = True
    # The callers' tokens are verified only where the runner gives its issuer (conformance.Guarded needs it).
    if (jwks_url := get("CONFORMANCE_JWKS_URL")) is not None:
        values["jwks_url"] = jwks_url
    if (issuer := get("CONFORMANCE_ISSUER")) is not None:
        values["issuer"] = issuer
    return Settings(**values)


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
