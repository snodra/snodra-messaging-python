"""The settings of an :class:`snodra.App`, and how they are read from the environment."""

from __future__ import annotations

import logging
import math
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Final
from urllib.parse import urlsplit

from snodra._credentials import ClientCredentials, Credential, Token, TokenFile, is_token_source
from snodra._errors import ConfigurationError
from snodra._ids import new_id
from snodra._outbox import OUTBOX_SEND_TIMEOUT
from snodra._retry import RetryPolicy
from snodra._targets import parse_targets
from snodra._tls import ClientCertificate, TLSConfig

log = logging.getLogger(__name__)

_UINT32_MAX: Final = 2**32 - 1
_INT32: Final = range(-(2**31), 2**31)
_TRUE: Final = frozenset({"true", "1", "yes", "on"})
_FALSE: Final = frozenset({"false", "0", "no", "off"})
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
    max_subscriptions: int = 1024
    """Subscription queries this instance serves at once (``SNODRA_SERVER_MAX_SUBSCRIPTIONS``); one beyond is ended
    ``RESOURCE_EXHAUSTED``. The hub's own default is 1024 per instance."""
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
    stream_credit_timeout: float = 600.0
    """The longest a streaming handler waits for a credit before it ends its stream with ``TIMEOUT``
    (``SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS``, 10 minutes by default: the hub's default maximum stream duration)."""
    status_interval: float = 5.0
    """How often every hub stream gets an ``InstanceStatus`` (``SNODRA_SERVER_STATUS_INTERVAL_MS``, at least 1 ms); an
    extra report goes out on every stream whenever the acknowledged hubs change."""
    events_source: str = "snodra.events"
    """The event store's source the service publishes its events to and its processors read
    (``SNODRA_EVENTS_SOURCE``)."""
    outbox_enabled: bool = True
    """Every instance runs the outbox relay (``SNODRA_OUTBOX_ENABLED``). Turning it off on all but one keeps the order
    per key of what one instance publishes."""
    outbox_poll_interval: float = 0.2
    """The longest wait between two claims of the outbox (``SNODRA_OUTBOX_POLL_INTERVAL_MS``)."""
    outbox_batch_size: int = 100
    """The most outbox entries one claim returns (``SNODRA_OUTBOX_BATCH_SIZE``)."""
    outbox_max_attempts: int = 10
    """Sends of one outbox entry in all before it is given up (``SNODRA_OUTBOX_MAX_ATTEMPTS``)."""
    outbox_lease: float = 120.0
    """How long a claimed outbox batch stays one relay's (``SNODRA_OUTBOX_LEASE_MS``); longer than a send (40 s)."""
    consumer_enabled: bool = True
    """Every instance reads its event processors' subscriptions (``SNODRA_CONSUMER_ENABLED``)."""
    consumer_max_attempts: int = 5
    """Event handler invocations per delivered record in all (``SNODRA_CONSUMER_MAX_ATTEMPTS``)."""
    store_init_schema: bool = True
    """The app prepares the message store's schema at start (``SNODRA_STORE_INIT_SCHEMA``, true by default); when
    false it checks the schema the service applies itself instead."""
    credentials: Credential | None = None
    """The instance's credential towards the hubs, for a hub whose access control asks for one: a :class:`Token`
    (``SNODRA_SERVER_TOKEN``), a :class:`TokenFile` (``SNODRA_SERVER_TOKEN_FILE``, which wins over the token), a JWT
    from :class:`ClientCredentials` (``SNODRA_SERVER_OAUTH2_*``) or from a :class:`TokenSource`. It excludes
    ``client_certificate``. Without TLS it travels unencrypted: a warning says so when the app is built."""
    tls: TLSConfig | None = None
    """TLS to every hub (``SNODRA_SERVER_TLS``, ``SNODRA_SERVER_TLS_CA``, ``SNODRA_SERVER_TLS_SERVER_NAME``); a client
    certificate implies it (with grpcio's bundled roots unless set)."""
    client_certificate: ClientCertificate | None = None
    """The client certificate presented to every hub (``SNODRA_SERVER_TLS_CLIENT_CERT``,
    ``SNODRA_SERVER_TLS_CLIENT_KEY``): the service's credential, excluding ``credentials``."""

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
        if not _is_int(self.max_subscriptions) or not 1 <= self.max_subscriptions <= _UINT32_MAX:
            raise ConfigurationError(
                f"max_subscriptions (SNODRA_SERVER_MAX_SUBSCRIPTIONS) must be an integer from 1 to {_UINT32_MAX}, "
                f"got {self.max_subscriptions!r}"
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
        credit = self.stream_credit_timeout
        if not _is_number(credit) or not math.isfinite(credit) or credit <= 0:
            raise ConfigurationError(
                "stream_credit_timeout (SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS) must be a positive number of seconds, "
                f"got {credit!r}"
            )
        status = self.status_interval
        if not _is_number(status) or not math.isfinite(status) or status <= 0:
            raise ConfigurationError(
                "status_interval (SNODRA_SERVER_STATUS_INTERVAL_MS) must be a positive number of seconds, "
                f"got {status!r}"
            )
        self._check_events()
        self._check_identity()
        self._check_credentials()
        for name, variable in (
            ("forward_jwt", "SNODRA_FORWARD_JWT"),
            ("always_route_through_hub", "SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB"),
        ):
            if not isinstance(getattr(self, name), bool):
                raise ConfigurationError(f"{name} ({variable}) must be True or False, got {getattr(self, name)!r}")

    def _check_events(self) -> None:
        source = self.events_source
        if not isinstance(source, str) or not source.strip():
            raise ConfigurationError("events_source (SNODRA_EVENTS_SOURCE) must not be empty")
        object.__setattr__(self, "events_source", source.strip())
        for name, variable in (
            ("outbox_enabled", "SNODRA_OUTBOX_ENABLED"),
            ("consumer_enabled", "SNODRA_CONSUMER_ENABLED"),
            ("store_init_schema", "SNODRA_STORE_INIT_SCHEMA"),
        ):
            if not isinstance(getattr(self, name), bool):
                raise ConfigurationError(f"{name} ({variable}) must be True or False, got {getattr(self, name)!r}")
        for name, variable in (
            ("outbox_batch_size", "SNODRA_OUTBOX_BATCH_SIZE"),
            ("outbox_max_attempts", "SNODRA_OUTBOX_MAX_ATTEMPTS"),
            ("consumer_max_attempts", "SNODRA_CONSUMER_MAX_ATTEMPTS"),
        ):
            value = getattr(self, name)
            if not _is_int(value) or value < 1:
                raise ConfigurationError(f"{name} ({variable}) must be a positive integer, got {value!r}")
        for name, variable in (
            ("outbox_poll_interval", "SNODRA_OUTBOX_POLL_INTERVAL_MS"),
            ("outbox_lease", "SNODRA_OUTBOX_LEASE_MS"),
        ):
            value = getattr(self, name)
            if not _is_number(value) or not math.isfinite(value) or value <= 0:
                raise ConfigurationError(f"{name} ({variable}) must be a positive number of seconds, got {value!r}")
        if self.outbox_lease <= OUTBOX_SEND_TIMEOUT:
            raise ConfigurationError(
                f"outbox_lease (SNODRA_OUTBOX_LEASE_MS) must be longer than a send ({OUTBOX_SEND_TIMEOUT:g} s), "
                f"got {self.outbox_lease:g} s"
            )

    def _check_identity(self) -> None:
        url, issuer = self.jwks_url, self.issuer
        if url is not None:
            if not isinstance(url, str) or not url.strip():
                raise ConfigurationError("jwks_url (SNODRA_JWKS_URL) must be a URL or None")
            url = url.strip()
            parts = urlsplit(url)
            if _carries_userinfo(url) or parts.username is not None or parts.password is not None:
                # Before the scheme check: a URL with credentials is never written into an error, whatever its scheme.
                raise ConfigurationError(
                    "jwks_url (SNODRA_JWKS_URL) must carry no userinfo: the credentials of the issuer's endpoint are "
                    "not read from a URL"
                )
            if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
                raise ConfigurationError("jwks_url (SNODRA_JWKS_URL) must be an http:// or https:// URL")
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

    def _check_credentials(self) -> None:
        credential = self.credentials
        kinds = "Token, TokenFile, ClientCredentials or a TokenSource (an object with async def token())"
        match credential:
            case None:
                pass
            case Token(value=value):
                if not isinstance(value, str) or not value.strip():
                    raise ConfigurationError("credentials Token (SNODRA_SERVER_TOKEN) is blank")
                object.__setattr__(self, "credentials", Token(value.strip()))
            case TokenFile(path=path):
                if not isinstance(path, str) or not path.strip():
                    raise ConfigurationError("credentials TokenFile (SNODRA_SERVER_TOKEN_FILE) names no file")
                object.__setattr__(self, "credentials", TokenFile(path.strip()))
            case ClientCredentials():
                object.__setattr__(self, "credentials", _checked_grant(credential))
            case _ if not is_token_source(credential):
                raise ConfigurationError(f"credentials must be {kinds}, got a {type(credential).__name__}")
        tls = self.tls
        if tls is not None:
            if not isinstance(tls, TLSConfig) or not all(_optional_text(v) for v in (tls.ca_file, tls.server_name)):
                raise ConfigurationError(
                    "tls (SNODRA_SERVER_TLS, SNODRA_SERVER_TLS_CA, SNODRA_SERVER_TLS_SERVER_NAME) must be a TLSConfig "
                    f"of texts or None, got a {type(tls).__name__}"
                )
            object.__setattr__(self, "tls", TLSConfig(_blank_none(tls.ca_file), _blank_none(tls.server_name)))
        pair = self.client_certificate
        if pair is not None:
            if not isinstance(pair, ClientCertificate):
                raise ConfigurationError(
                    f"client_certificate must be a ClientCertificate or None, got a {type(pair).__name__}"
                )
            for name, variable in (
                ("cert_file", "SNODRA_SERVER_TLS_CLIENT_CERT"),
                ("key_file", "SNODRA_SERVER_TLS_CLIENT_KEY"),
            ):
                if _blank_none(getattr(pair, name)) is None:
                    raise ConfigurationError(f"client_certificate.{name} ({variable}) names no file")
            object.__setattr__(
                self, "client_certificate", ClientCertificate(pair.cert_file.strip(), pair.key_file.strip())
            )
            if credential is not None:
                raise ConfigurationError(
                    "credentials (SNODRA_SERVER_TOKEN, SNODRA_SERVER_TOKEN_FILE, SNODRA_SERVER_OAUTH2_*) and "
                    "client_certificate (SNODRA_SERVER_TLS_CLIENT_CERT, SNODRA_SERVER_TLS_CLIENT_KEY) exclude each "
                    "other: the hub reads the service from the certificate; configure one of them"
                )
            if self.tls is None:
                object.__setattr__(self, "tls", TLSConfig())

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
        if (v := get("SNODRA_SERVER_MAX_SUBSCRIPTIONS")) is not None:
            values["max_subscriptions"] = _env_int("SNODRA_SERVER_MAX_SUBSCRIPTIONS", v)
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
        if (v := get("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS")) is not None:
            values["stream_credit_timeout"] = _env_positive_int("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", v) / 1000
        if (v := get("SNODRA_SERVER_STATUS_INTERVAL_MS")) is not None:
            values["status_interval"] = _env_positive_int("SNODRA_SERVER_STATUS_INTERVAL_MS", v) / 1000
        if (v := get("SNODRA_EVENTS_SOURCE")) is not None:
            values["events_source"] = v
        if (v := get("SNODRA_OUTBOX_ENABLED")) is not None:
            values["outbox_enabled"] = _env_bool("SNODRA_OUTBOX_ENABLED", v)
        if (v := get("SNODRA_OUTBOX_POLL_INTERVAL_MS")) is not None:
            values["outbox_poll_interval"] = _env_positive_int("SNODRA_OUTBOX_POLL_INTERVAL_MS", v) / 1000
        if (v := get("SNODRA_OUTBOX_BATCH_SIZE")) is not None:
            values["outbox_batch_size"] = _env_positive_int("SNODRA_OUTBOX_BATCH_SIZE", v)
        if (v := get("SNODRA_OUTBOX_MAX_ATTEMPTS")) is not None:
            values["outbox_max_attempts"] = _env_positive_int("SNODRA_OUTBOX_MAX_ATTEMPTS", v)
        if (v := get("SNODRA_OUTBOX_LEASE_MS")) is not None:
            values["outbox_lease"] = _env_positive_int("SNODRA_OUTBOX_LEASE_MS", v) / 1000
        if (v := get("SNODRA_CONSUMER_ENABLED")) is not None:
            values["consumer_enabled"] = _env_bool("SNODRA_CONSUMER_ENABLED", v)
        if (v := get("SNODRA_CONSUMER_MAX_ATTEMPTS")) is not None:
            values["consumer_max_attempts"] = _env_positive_int("SNODRA_CONSUMER_MAX_ATTEMPTS", v)
        if (v := get("SNODRA_STORE_INIT_SCHEMA")) is not None:
            values["store_init_schema"] = _env_bool("SNODRA_STORE_INIT_SCHEMA", v)
        grant = _env_credentials(get, values)
        _env_tls(get, values)
        values |= overrides
        if "service" not in values:
            raise ConfigurationError("the service name (service, SNODRA_SERVICE_NAME) is required")
        if grant is not None and "credentials" not in overrides:
            uri, client_id, fields = grant
            values["credentials"] = ClientCredentials(uri, client_id or values["service"], **fields)
        return cls(**values)


def hub_targets(hubs: tuple[str, ...]) -> list[str]:
    """The gRPC targets of the hub addresses, in order, repeats dropped: ``host:port`` (``static://`` removed), or the
    one ``dns:///name:port``. Raises :class:`ConfigurationError` for anything else."""
    return [target.address for target in parse_targets(hubs)]


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


def _optional_text(value: object) -> bool:
    return value is None or isinstance(value, str)


def _carries_userinfo(url: str) -> bool:
    """Whether the authority of url carries ``@``, read from the text so that every form is refused whatever
    :func:`~urllib.parse.urlsplit` makes of it: a scheme-less ``user:pw@host/x`` is read as the scheme ``user``, with
    the credentials in its path. Callers also check the parsed userinfo, for the forms no authority is read from
    (``//id:pw@host/x``)."""
    rest = url[url.find("://") + 3 :] if "://" in url else url
    ends = [i for i in (rest.find(c) for c in "/?#") if i != -1]
    return "@" in rest[: min(ends, default=len(rest))]


def _blank_none(value: str | None) -> str | None:
    return None if value is None or not value.strip() else value.strip()


def _checked_grant(grant: ClientCredentials) -> ClientCredentials:
    """grant with its texts trimmed; :class:`ConfigurationError` for one that cannot fetch a token."""
    for name in ("token_url", "client_id", "client_secret", "client_secret_file", "scope", "ca_file"):
        value = getattr(grant, name)
        if not _optional_text(value) or (name in ("token_url", "client_id") and value is None):
            raise ConfigurationError(f"ClientCredentials.{name} must be a text, got a {type(value).__name__}")
    url = grant.token_url.strip()
    parts = urlsplit(url)
    if _carries_userinfo(url) or parts.username is not None or parts.password is not None:
        # Before the scheme check: a URL with credentials is never written into an error, whatever its scheme.
        raise ConfigurationError(
            "ClientCredentials.token_url (SNODRA_SERVER_OAUTH2_TOKEN_URI) must carry no userinfo: the client is "
            "authenticated by client_id and its secret, not by a URL"
        )
    if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
        raise ConfigurationError(
            "ClientCredentials.token_url (SNODRA_SERVER_OAUTH2_TOKEN_URI) must be an http:// or https:// URL"
        )
    if parts.scheme.lower() == "http":
        log.warning(
            "The token endpoint of ClientCredentials (SNODRA_SERVER_OAUTH2_TOKEN_URI) is plain http://: the client "
            "secret and the access token travel unencrypted; use https://"
        )
    if not grant.client_id.strip():
        raise ConfigurationError("ClientCredentials.client_id (SNODRA_SERVER_OAUTH2_CLIENT_ID) must not be blank")
    secret, secret_file = _blank_none(grant.client_secret), _blank_none(grant.client_secret_file)
    if secret is None and secret_file is None:
        raise ConfigurationError(
            "ClientCredentials needs a client secret: client_secret (SNODRA_SERVER_OAUTH2_CLIENT_SECRET) or "
            "client_secret_file (SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE)"
        )
    return replace(
        grant,
        token_url=url,
        client_id=grant.client_id.strip(),
        client_secret=secret,
        client_secret_file=secret_file,
        scope=_blank_none(grant.scope),
        ca_file=_blank_none(grant.ca_file),
    )


def _env_credentials(
    get: Callable[[str], str | None], values: dict[str, Any]
) -> tuple[str, str | None, dict[str, str | None]] | None:
    """Sets the token of ``SNODRA_SERVER_TOKEN`` / ``_TOKEN_FILE`` in values; returns the grant of
    ``SNODRA_SERVER_OAUTH2_*`` (its URI, client id and other fields), built once the service is known."""
    token, token_file = get("SNODRA_SERVER_TOKEN"), get("SNODRA_SERVER_TOKEN_FILE")
    oauth = {name: get(f"SNODRA_SERVER_OAUTH2_{name}") for name in _OAUTH2}
    uri = oauth.pop("TOKEN_URI")
    if uri is None:
        if any(v is not None for v in oauth.values()):
            raise ConfigurationError("the SNODRA_SERVER_OAUTH2_* variables need SNODRA_SERVER_OAUTH2_TOKEN_URI")
        if token_file is not None:
            values["credentials"] = TokenFile(token_file)
        elif token is not None:
            values["credentials"] = Token(token)
        return None
    if token is not None or token_file is not None:
        raise ConfigurationError(
            "SNODRA_SERVER_TOKEN / SNODRA_SERVER_TOKEN_FILE and SNODRA_SERVER_OAUTH2_TOKEN_URI exclude each other: "
            "configure one credential"
        )
    fields = {
        "client_secret": oauth["CLIENT_SECRET"],
        "client_secret_file": oauth["CLIENT_SECRET_FILE"],
        "scope": oauth["SCOPE"],
        "ca_file": oauth["CA"],
    }
    return uri, oauth["CLIENT_ID"], fields


def _env_tls(get: Callable[[str], str | None], values: dict[str, Any]) -> None:
    flag = get("SNODRA_SERVER_TLS")
    on = None if flag is None else _env_bool("SNODRA_SERVER_TLS", flag)
    ca, name = get("SNODRA_SERVER_TLS_CA"), get("SNODRA_SERVER_TLS_SERVER_NAME")
    cert, key = get("SNODRA_SERVER_TLS_CLIENT_CERT"), get("SNODRA_SERVER_TLS_CLIENT_KEY")
    if cert is not None or key is not None:
        if on is False:
            raise ConfigurationError(
                "SNODRA_SERVER_TLS_CLIENT_CERT and SNODRA_SERVER_TLS_CLIENT_KEY need TLS, but SNODRA_SERVER_TLS=false"
            )
        if cert is None or key is None:
            missing = "SNODRA_SERVER_TLS_CLIENT_CERT" if cert is None else "SNODRA_SERVER_TLS_CLIENT_KEY"
            raise ConfigurationError(f"a client certificate needs both files: {missing} is not set")
        values["client_certificate"] = ClientCertificate(cert, key)
        values["tls"] = TLSConfig(ca, name)
    elif on:
        values["tls"] = TLSConfig(ca, name)
    elif ca is not None or name is not None:
        raise ConfigurationError("SNODRA_SERVER_TLS_CA and SNODRA_SERVER_TLS_SERVER_NAME need SNODRA_SERVER_TLS=true")


_OAUTH2: Final = ("TOKEN_URI", "CLIENT_ID", "CLIENT_SECRET", "CLIENT_SECRET_FILE", "SCOPE", "CA")


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _env_int(name: str, value: str) -> int:
    if not re.fullmatch(r"\d+", value):
        raise ConfigurationError(f"{name} must be a non-negative integer, got {value!r}")
    return int(value)


def _env_positive_int(name: str, value: str) -> int:
    n = _env_int(name, value)
    if n < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {value!r}")
    return n


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
