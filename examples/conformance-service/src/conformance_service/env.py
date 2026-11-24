"""The service's settings, from the ``CONFORMANCE_*`` variables the runner sets."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from snodra import Settings

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
    return Settings(**values)
