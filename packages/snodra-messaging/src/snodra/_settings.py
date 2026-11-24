"""The settings of an :class:`snodra.App`, and how they are read from the environment."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final

from snodra._errors import ConfigurationError
from snodra._ids import new_id

_UINT32_MAX: Final = 2**32 - 1
_STATIC: Final = "static://"
_DNS: Final = "dns:///"
_DNS_TARGET: Final = re.compile(r"dns:///([^/:]+|\[[^/\]]+\]):(\d+)")
_ACCEPTED_FORMS: Final = "host:port, static://host:port, or one dns:///name:port"
_DEFAULT_BACKOFF: Final = (0.5, 5.0)


@dataclass(frozen=True, kw_only=True)
class Settings:
    """What an :class:`snodra.App` registers as and where it connects; checked when built (:class:`ConfigurationError`
    names the field and its ``SNODRA_*`` variable). Durations are seconds."""

    service: str
    """``Register.service_name`` (``SNODRA_SERVICE_NAME``)."""
    instance: str = field(default_factory=new_id)
    """``Register.instance_id`` (``SNODRA_INSTANCE_ID``); a fresh UUID by default."""
    hubs: tuple[str, ...] = ("static://snodra-server:7000",)
    """The hubs (``SNODRA_SERVER_ADDRESS``): ``host:port`` entries (``static://`` optional), or one
    ``dns:///name:port``."""
    max_concurrent: int = 256
    """Handlers run at once (``SNODRA_MAX_CONCURRENT_HANDLERS``): ``Register.initial_permits``."""
    reconnect_backoff: tuple[float, float] = _DEFAULT_BACKOFF
    """The first and the longest reconnect wait (``SNODRA_RECONNECT_BACKOFF_MIN_MS``, ``_MAX_MS``)."""
    load_factor: int = 100
    """``Register.load_factor`` (``SNODRA_SERVER_LOAD_FACTOR``); 0 reads as 100 at the hub."""
    tags: Mapping[str, str] = field(default_factory=dict)
    """``Register.tags`` (``SNODRA_SERVER_TAGS``, ``key=value,key=value``)."""

    def __post_init__(self) -> None:
        if not isinstance(self.service, str) or not self.service.strip():
            raise ConfigurationError("the service name (service, SNODRA_SERVICE_NAME) is required")
        if not isinstance(self.instance, str) or not self.instance.strip():
            raise ConfigurationError("the instance id (instance, SNODRA_INSTANCE_ID) must not be empty")
        if isinstance(self.hubs, str):
            raise ConfigurationError(
                f"hubs (SNODRA_SERVER_ADDRESS) is a sequence of addresses, not one text: {self.hubs!r}"
            )
        object.__setattr__(self, "hubs", tuple(self.hubs))
        hub_targets(self.hubs)
        if not _is_int(self.max_concurrent) or not 1 <= self.max_concurrent <= _UINT32_MAX:
            raise ConfigurationError(
                f"max_concurrent (SNODRA_MAX_CONCURRENT_HANDLERS) must be an integer from 1 to {_UINT32_MAX}, "
                f"got {self.max_concurrent!r}"
            )
        _check_backoff(self.reconnect_backoff)
        lo, hi = self.reconnect_backoff
        object.__setattr__(self, "reconnect_backoff", (float(lo), float(hi)))
        if not _is_int(self.load_factor) or not 0 <= self.load_factor <= _UINT32_MAX:
            raise ConfigurationError(
                f"load_factor (SNODRA_SERVER_LOAD_FACTOR) must be an integer from 0 to {_UINT32_MAX}, "
                f"got {self.load_factor!r}"
            )
        for k, v in self.tags.items():
            if not isinstance(k, str) or not k.strip() or not isinstance(v, str):
                raise ConfigurationError(f"tags (SNODRA_SERVER_TAGS): {k!r}={v!r} is not a non-empty name and a text")
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None, **overrides: Any) -> Settings:
        """The settings the ``SNODRA_*`` variables of ``environ`` (the process environment by default) give; a blank
        variable changes nothing, and ``overrides`` (fields) win over the environment."""
        env = os.environ if environ is None else environ

        def get(name: str) -> str | None:
            value = env.get(name, "").strip()
            return value or None

        values: dict[str, Any] = {}
        if (v := get("SNODRA_SERVICE_NAME")) is not None:
            values["service"] = v
        if (v := get("SNODRA_INSTANCE_ID")) is not None:
            values["instance"] = v
        if (v := get("SNODRA_SERVER_ADDRESS")) is not None:
            values["hubs"] = tuple(e.strip() for e in v.split(",") if e.strip())
        if (v := get("SNODRA_MAX_CONCURRENT_HANDLERS")) is not None:
            values["max_concurrent"] = _env_int("SNODRA_MAX_CONCURRENT_HANDLERS", v)
        lo = get("SNODRA_RECONNECT_BACKOFF_MIN_MS")
        hi = get("SNODRA_RECONNECT_BACKOFF_MAX_MS")
        if lo is not None or hi is not None:
            default_lo, default_hi = _DEFAULT_BACKOFF
            values["reconnect_backoff"] = (
                default_lo if lo is None else _env_int("SNODRA_RECONNECT_BACKOFF_MIN_MS", lo) / 1000,
                default_hi if hi is None else _env_int("SNODRA_RECONNECT_BACKOFF_MAX_MS", hi) / 1000,
            )
        if (v := get("SNODRA_SERVER_LOAD_FACTOR")) is not None:
            values["load_factor"] = _env_int("SNODRA_SERVER_LOAD_FACTOR", v)
        if (v := get("SNODRA_SERVER_TAGS")) is not None:
            values["tags"] = _env_pairs("SNODRA_SERVER_TAGS", v)
        values |= overrides
        if "service" not in values:
            raise ConfigurationError("the service name (service, SNODRA_SERVICE_NAME) is required")
        return cls(**values)


def hub_targets(hubs: tuple[str, ...]) -> list[str]:
    """The gRPC targets of the hub addresses, in order, repeats dropped: ``host:port`` (``static://`` removed), or the
    one ``dns:///name:port``. Raises :class:`ConfigurationError` for anything else."""
    entries = [h.strip() if isinstance(h, str) else h for h in hubs]
    if not entries or any(not isinstance(e, str) or not e for e in entries):
        raise ConfigurationError(
            f"the hub addresses (hubs, SNODRA_SERVER_ADDRESS) name no hub, or an empty one: {hubs!r}"
        )
    if any(e.startswith("dns:") for e in entries):
        if len(entries) > 1:
            raise ConfigurationError(
                f"the hub addresses (hubs, SNODRA_SERVER_ADDRESS) {hubs!r}: a dns:/// address must be the only one"
            )
        m = _DNS_TARGET.fullmatch(entries[0])
        if m is None or not 1 <= int(m.group(2)) <= 65535:
            raise ConfigurationError(
                f"hub address {entries[0]!r} (hubs, SNODRA_SERVER_ADDRESS) is not dns:///name:port with a port from 1"
                " to 65535"
            )
        return [entries[0]]
    targets: list[str] = []
    for e in entries:
        target = e.removeprefix(_STATIC).strip()
        # TODO(spec-gap): SG-2
        if not target or "://" in target:
            raise ConfigurationError(
                f"hub address {e!r} (hubs, SNODRA_SERVER_ADDRESS) is not accepted: "
                f"the accepted forms are {_ACCEPTED_FORMS}"
            )
        if target not in targets:
            targets.append(target)
    return targets


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_backoff(backoff: object) -> None:
    if not isinstance(backoff, tuple) or len(backoff) != 2:
        raise ConfigurationError(
            "reconnect_backoff (SNODRA_RECONNECT_BACKOFF_MIN_MS, SNODRA_RECONNECT_BACKOFF_MAX_MS) must be "
            f"(min, max) in seconds, got {backoff!r}"
        )
    lo, hi = backoff
    if not _is_number(lo) or not math.isfinite(lo) or lo <= 0:
        raise ConfigurationError(
            "the reconnect backoff minimum (reconnect_backoff, SNODRA_RECONNECT_BACKOFF_MIN_MS) must be positive, "
            f"got {lo!r}"
        )
    if not _is_number(hi) or not math.isfinite(hi) or hi < lo:
        raise ConfigurationError(
            "the reconnect backoff maximum (reconnect_backoff, SNODRA_RECONNECT_BACKOFF_MAX_MS) must be finite and not "
            f"below the minimum {lo!r}, got {hi!r}"
        )


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _env_int(name: str, value: str) -> int:
    if not re.fullmatch(r"\d+", value):
        raise ConfigurationError(f"{name} must be a non-negative integer, got {value!r}")
    return int(value)


def _env_pairs(name: str, value: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for entry in value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        k, sep, v = entry.partition("=")
        if not sep or not k.strip():
            raise ConfigurationError(f"{name}: entry {entry!r} is not key=value")
        pairs[k.strip()] = v.strip()
    return pairs
