"""The settings of an :class:`snodra.App`, and how they are read from the environment."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import urlsplit

from snodra._errors import ConfigurationError
from snodra._ids import new_id
from snodra._retry import RetryPolicy

_UINT32_MAX: Final = 2**32 - 1
_INT32: Final = range(-(2**31), 2**31)
_TRUE: Final = frozenset({"true", "1", "yes", "on"})
_FALSE: Final = frozenset({"false", "0", "no", "off"})
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
    default_timeout: float | None = None
    """The timeout of a call that chose none (``SNODRA_SERVER_REQUEST_TIMEOUT_MS``, 0 for none); without one the hub
    applies its own per type and the call waits at most 5 s for a hub."""
    default_retry: RetryPolicy | None = None
    """The retry policy of a call that chose none (``SNODRA_SERVER_RETRY_ENABLED`` and ``SNODRA_SERVER_RETRY_*``);
    None: no retry."""
    priorities: Mapping[str, int] = field(default_factory=dict)
    """``Request.priority`` per type, for a call that chose none (``SNODRA_SERVER_PRIORITIES``, ``type=n,type=n``)."""
    jwks_url: str | None = None
    """The issuer's signing keys (``SNODRA_JWKS_URL``, ``http://`` or ``https://``): every forwarded token is verified
    against them (RS256) before a handler is looked up. Without it no token is an identity: every caller is anonymous,
    and a handler with a guard cannot be registered. The keys are fetched without following a redirect, through the
    proxy the environment names (``https_proxy``, ``http_proxy``, ``no_proxy``)."""
    issuer: str | None = None
    """The ``iss`` a token must carry (``SNODRA_ISSUER``); required with ``jwks_url``."""
    roles_claim: str = "roles"
    """Where a token's roles are (``SNODRA_SECURITY_ROLES_CLAIM``): a dotted path to a list of texts
    (``realm_access.roles``), or ``scope`` for the space-separated scopes; blank is ``roles``."""
    forward_jwt: bool = True
    """A request sent while one is handled carries the handled request's ``authorization`` on
    (``SNODRA_FORWARD_JWT``)."""
    always_route_through_hub: bool = False
    """Every command and query goes through a hub, also one of a type this instance handles
    (``SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB``); by default those are answered in-process."""

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
        timeout = self.default_timeout
        if timeout is not None and (not _is_number(timeout) or not math.isfinite(timeout) or timeout <= 0):
            raise ConfigurationError(
                "default_timeout (SNODRA_SERVER_REQUEST_TIMEOUT_MS) must be a positive number of seconds or None, "
                f"got {timeout!r}"
            )
        if self.default_retry is not None and not isinstance(self.default_retry, RetryPolicy):
            raise ConfigurationError(
                f"default_retry (SNODRA_SERVER_RETRY_*) must be a RetryPolicy or None, got {self.default_retry!r}"
            )
        for k, n in self.priorities.items():
            if not isinstance(k, str) or not k.strip() or not _is_int(n) or n not in _INT32:
                raise ConfigurationError(
                    f"priorities (SNODRA_SERVER_PRIORITIES): {k!r}={n!r} is not a type name and a 32-bit integer"
                )
        object.__setattr__(self, "priorities", MappingProxyType(dict(self.priorities)))
        self._check_identity()
        for name, variable in (
            ("forward_jwt", "SNODRA_FORWARD_JWT"),
            ("always_route_through_hub", "SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB"),
        ):
            if not isinstance(getattr(self, name), bool):
                raise ConfigurationError(f"{name} ({variable}) must be True or False, got {getattr(self, name)!r}")

    def _check_identity(self) -> None:
        url, issuer = self.jwks_url, self.issuer
        if url is not None:
            if not isinstance(url, str) or not url.strip():
                raise ConfigurationError(f"jwks_url (SNODRA_JWKS_URL) must be a URL or None, got {url!r}")
            url = url.strip()
            parts = urlsplit(url)
            if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
                raise ConfigurationError(f"jwks_url (SNODRA_JWKS_URL) must be an http:// or https:// URL, got {url!r}")
            object.__setattr__(self, "jwks_url", url)
            if not isinstance(issuer, str) or not issuer.strip():
                raise ConfigurationError(
                    "the issuer (issuer, SNODRA_ISSUER) is required with jwks_url (SNODRA_JWKS_URL): tokens of other "
                    f"issuers are refused, got {issuer!r}"
                )
        if issuer is not None:
            if not isinstance(issuer, str):
                raise ConfigurationError(f"issuer (SNODRA_ISSUER) must be a text or None, got {issuer!r}")
            object.__setattr__(self, "issuer", issuer.strip() or None)
        if not isinstance(self.roles_claim, str):
            raise ConfigurationError(
                f"roles_claim (SNODRA_SECURITY_ROLES_CLAIM) must be a claim path or scope, got {self.roles_claim!r}"
            )
        object.__setattr__(self, "roles_claim", self.roles_claim.strip() or "roles")

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
        if (v := get("SNODRA_SERVER_REQUEST_TIMEOUT_MS")) is not None:
            ms = _env_int("SNODRA_SERVER_REQUEST_TIMEOUT_MS", v)
            values["default_timeout"] = ms / 1000 if ms > 0 else None
        retry = _env_retry(get)
        if retry is not None:
            values["default_retry"] = retry
        if (v := get("SNODRA_SERVER_PRIORITIES")) is not None:
            values["priorities"] = _env_priorities("SNODRA_SERVER_PRIORITIES", v)
        if (v := get("SNODRA_JWKS_URL")) is not None:
            values["jwks_url"] = v
        if (v := get("SNODRA_ISSUER")) is not None:
            values["issuer"] = v
        if (v := get("SNODRA_SECURITY_ROLES_CLAIM")) is not None:
            values["roles_claim"] = v
        if (v := get("SNODRA_FORWARD_JWT")) is not None:
            values["forward_jwt"] = _env_bool("SNODRA_FORWARD_JWT", v)
        if (v := get("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB")) is not None:
            values["always_route_through_hub"] = _env_bool("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB", v)
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


def _env_bool(name: str, value: str) -> bool:
    if value.lower() not in _TRUE | _FALSE:
        raise ConfigurationError(f"{name} must be true or false, got {value!r}")
    return value.lower() in _TRUE


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


def _env_retry(get: Callable[[str], str | None]) -> RetryPolicy | None:
    """The policy of ``SNODRA_SERVER_RETRY_*``, None unless ``SNODRA_SERVER_RETRY_ENABLED`` is true; every variable
    is checked even while retry is off."""
    enabled = get("SNODRA_SERVER_RETRY_ENABLED")
    on = enabled is not None and _env_bool("SNODRA_SERVER_RETRY_ENABLED", enabled)
    fields: dict[str, Any] = {}
    if (v := get("SNODRA_SERVER_RETRY_MAX_ATTEMPTS")) is not None:
        fields["max_attempts"] = _env_int("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", v)
    if (v := get("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS")) is not None:
        fields["initial_backoff"] = _env_int("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS", v) / 1000
    if (v := get("SNODRA_SERVER_RETRY_MULTIPLIER")) is not None:
        try:
            fields["multiplier"] = float(v)
        except ValueError:
            raise ConfigurationError(f"SNODRA_SERVER_RETRY_MULTIPLIER must be a number, got {v!r}") from None
    if (v := get("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS")) is not None:
        fields["max_backoff"] = _env_int("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS", v) / 1000
    try:
        policy = RetryPolicy(**fields)
    except ValueError as e:
        names = {
            "max_attempts": "SNODRA_SERVER_RETRY_MAX_ATTEMPTS",
            "initial_backoff": "SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS",
            "multiplier": "SNODRA_SERVER_RETRY_MULTIPLIER",
            "max_backoff": "SNODRA_SERVER_RETRY_MAX_BACKOFF_MS",
        }
        variable = next((env for name, env in names.items() if f"RetryPolicy.{name} " in str(e)), "SNODRA_SERVER_RETRY")
        raise ConfigurationError(f"{variable}: {e}") from None
    return policy if on else None


def _env_priorities(name: str, value: str) -> dict[str, int]:
    priorities: dict[str, int] = {}
    for k, v in _env_pairs(name, value).items():
        if not re.fullmatch(r"[+-]?\d+", v) or int(v) not in _INT32:
            raise ConfigurationError(f"{name}: the priority of {k} must be a 32-bit integer, got {v!r}")
        priorities[k] = int(v)
    return priorities
