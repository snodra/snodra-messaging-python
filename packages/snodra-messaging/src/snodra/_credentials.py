"""The instance's credential towards the hubs: a static token, a token file, or a JWT (an OAuth2 client-credentials
token, or one from a :class:`TokenSource`) renewed before it expires. A client certificate is the other kind
(:mod:`snodra._tls`); an instance uses one kind.

No credential is ever logged or shown: the types holding one print ``***`` in its place, and an error names the
endpoint or the file, never the token or the secret.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Credentials").
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import functools
import inspect
import json
import logging
import math
import random
import ssl
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, Protocol, runtime_checkable

from snodra import _http
from snodra._backoff import jitter, next_wait
from snodra._errors import ConfigurationError
from snodra._hubpb import hub_pb2
from snodra._meta import client_version
from snodra._timings import DEFAULT_TIMINGS, Timings

log = logging.getLogger(__name__)

MAX_TOKEN_RESPONSE: Final = 1 << 20
"""The largest answer of a token endpoint read."""

MAX_LIFETIME: Final = 10 * 365 * 24 * 3600.0
"""The longest lifetime an access token is given, whatever its endpoint says."""

_CLIP: Final = 200
_SLICE: Final = 5.0
"""The renewal looks at the clocks at least this often (a suspended machine stops the monotonic one)."""

MIN_RENEWAL_INTERVAL: Final = 1.0
"""The shortest time between two fetches of the renewal: a shorter-lived token is renewed at that pace, not in a
loop (a stream's own fetch after a refusal is not held back)."""


@dataclass(frozen=True)
class Token:
    """A static token, sent in ``Register.credentials.token`` on every stream to every hub (``SNODRA_SERVER_TOKEN``);
    surrounding blanks are dropped. Shown as ``Token(value='***')``."""

    value: str = field(repr=False)

    def __repr__(self) -> str:
        return "Token(value='***')"


@dataclass(frozen=True)
class TokenFile:
    """A file holding the static token (``SNODRA_SERVER_TOKEN_FILE``), for a mounted secret: read when the app is built
    (a file that cannot be read or is empty is a :class:`ConfigurationError`), before every connect and on every
    heartbeat of a registered stream, whose hub gets a changed token as ``ClientMessage.credentials``. Later, a file
    that cannot be read or is empty (a rotation caught half way) keeps the last token read, with one warning."""

    path: str


@dataclass(frozen=True)
class AccessToken:
    """A JWT the instance registers with (``Register.credentials.jwt``), and when it expires (seconds since the
    epoch; None: unknown, read as 60 s after it was fetched). Shown without its value."""

    value: str = field(repr=False)
    expires_at: float | None

    def __repr__(self) -> str:
        return f"AccessToken(value='***', expires_at={self.expires_at!r})"


@runtime_checkable
class TokenSource(Protocol):
    """Gives the instance a JWT to register with (``credentials=`` of :class:`Settings`): asked before a stream opens
    when no token still fresh is held, and again ahead of its expiry; one call at a time, at most 10 s. An exception it
    raises is a failed fetch."""

    async def token(self) -> AccessToken: ...


@dataclass(frozen=True)
class ClientCredentials:
    """The OAuth2 client-credentials grant that gives the instance its JWT: a ``POST`` of
    ``grant_type=client_credentials`` (and ``scope``) to ``token_url`` (``SNODRA_SERVER_OAUTH2_TOKEN_URI``), the client
    authenticated with HTTP Basic (``client_id``, ``SNODRA_SERVER_OAUTH2_CLIENT_ID``, by default the service name,
    which the hub requires as the token's ``azp``; ``client_secret``, ``SNODRA_SERVER_OAUTH2_CLIENT_SECRET``, or
    ``client_secret_file``, ``SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE``, read at every fetch and winning over the
    secret). ``ca_file`` (``SNODRA_SERVER_OAUTH2_CA``) adds PEM CA certificates to the system's for an ``https://``
    endpoint. The fetch follows no redirect and takes at most 10 s; the proxy environment variables apply. Shown
    without the secret."""

    token_url: str
    client_id: str
    client_secret: str | None = field(default=None, repr=False)
    client_secret_file: str | None = None
    scope: str | None = None
    ca_file: str | None = None

    def __repr__(self) -> str:
        secret = None if self.client_secret is None else "'***'"
        return (
            f"ClientCredentials(token_url={self.token_url!r}, client_id={self.client_id!r}, client_secret={secret}, "
            f"client_secret_file={self.client_secret_file!r}, scope={self.scope!r}, ca_file={self.ca_file!r})"
        )


Credential = Token | TokenFile | ClientCredentials | TokenSource
"""What ``Settings.credentials`` takes."""


def is_token_source(value: object) -> bool:
    """True for an object with an ``async def token()``."""
    return isinstance(value, TokenSource) and inspect.iscoroutinefunction(value.token)


def refresh_at(lifetime: float, min_margin: float = DEFAULT_TIMINGS.min_refresh_margin) -> float:
    """When, after it was fetched, a token of ``lifetime`` seconds is renewed: when a fifth of its lifetime is left, at
    least ``min_margin`` seconds before its expiry and at most half of its lifetime before it."""
    margin = min(max(lifetime / 5, min_margin), lifetime / 2)
    return lifetime - margin


class TokenError(Exception):
    """No access token could be had; the message is short and names no credential."""


class NoToken(Exception):
    """A stream is not opened: no access token could be had (the message says why)."""


def clip(text: str) -> str:
    """text with every control character a space (an endpoint's text must not forge a log line), at most 200
    characters."""
    text = "".join(" " if not c.isprintable() else c for c in text)
    return text if len(text) <= _CLIP else text[: _CLIP - 1] + "…"


# What a connection asks of the credential.


class HubCredentials:
    """The credential of every hub connection of an app: what a new stream's ``Register`` carries, and what a
    registered stream is sent when it changed."""

    kind: str = "token"
    """How the credential is named in a log line (never its value)."""

    async def for_register(self) -> hub_pb2.Credentials:
        """The credentials of the next stream's ``Register``; :class:`NoToken` when there is none to give."""
        raise NotImplementedError

    async def on_tick(self) -> hub_pb2.Credentials | None:
        """On a heartbeat tick of a registered stream: the credentials it is to hold now (None: nothing to follow)."""
        return None

    def latest(self) -> hub_pb2.Credentials | None:
        """The newest credentials a registered stream is to hold, at its ack and when they change (None: nothing to
        follow)."""
        return None

    def listen(self, changed: Callable[[], None]) -> None:
        """changed is called whenever :meth:`latest` changes."""

    def refused(self, presented: hub_pb2.Credentials) -> None:
        """A hub closed a stream that presented these with ``UNAUTHENTICATED``."""

    async def close(self) -> None:
        """Ends the work in the background; nothing is fetched any more."""


class StaticToken(HubCredentials):
    def __init__(self, value: str) -> None:
        self._credentials = hub_pb2.Credentials(token=value)

    async def for_register(self) -> hub_pb2.Credentials:
        return self._credentials

    def __repr__(self) -> str:
        return "StaticToken(***)"


class TokenFileCredentials(HubCredentials):
    """The token in a file: read now (:class:`ConfigurationError` when it cannot be read or is empty), then before every
    connect and on every tick, keeping the last token read when the file cannot be read or is empty (a warning per
    distinct problem).

    A tick never waits on the file: it starts one read (task ``snodra:token-file``) when none is under way, and a
    changed token is followed (``ClientMessage.credentials``) as soon as the read came back, however slow the mount.
    """

    kind = "token (file)"

    def __init__(self, path: str) -> None:
        self._path = path
        try:
            token = Path(path).read_text(encoding="utf-8").strip()
        except (OSError, ValueError) as e:
            raise ConfigurationError(
                f"the token file (TokenFile, SNODRA_SERVER_TOKEN_FILE) {path} cannot be read: {type(e).__name__}"
            ) from None
        if not token:
            raise ConfigurationError(
                f"the token file (TokenFile, SNODRA_SERVER_TOKEN_FILE) {path} is empty: it must hold the hub token"
            )
        self._last = token
        self._problem: str | None = None
        self._current = hub_pb2.Credentials(token=token)
        self._listeners: list[Callable[[], None]] = []
        self._task: asyncio.Task[None] | None = None

    async def for_register(self) -> hub_pb2.Credentials:
        self._adopt(await asyncio.to_thread(self._read))
        return self._current

    async def on_tick(self) -> hub_pb2.Credentials | None:
        if self._task is None or self._task.done():  # one read at a time, however many ticks pass meanwhile
            self._task = asyncio.create_task(self._reread(), name="snodra:token-file")
        return None  # a changed token goes out when its read comes back (latest/listen)

    def latest(self) -> hub_pb2.Credentials | None:
        return self._current

    def listen(self, changed: Callable[[], None]) -> None:
        self._listeners.append(changed)

    async def close(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.wait({task})

    async def _reread(self) -> None:
        self._adopt(await asyncio.to_thread(self._read))

    def _adopt(self, token: str) -> None:
        """token, once it is not the one the streams hold, is followed from here on."""
        if token == self._current.token:
            return
        self._current = hub_pb2.Credentials(token=token)
        for changed in list(self._listeners):
            try:
                changed()
            except Exception:
                log.exception("Following a changed hub token failed")

    def _read(self) -> str:
        try:
            token = Path(self._path).read_text(encoding="utf-8").strip()
            problem = None if token else "it is empty"
        except (OSError, ValueError) as e:
            token, problem = "", f"it cannot be read ({type(e).__name__})"
        if problem is None:
            self._last, self._problem = token, None
        elif problem != self._problem:
            self._problem = problem
            log.warning(
                "The hub token file %s is not usable: %s; the last token read from it is used", self._path, problem
            )
        return self._last

    def __repr__(self) -> str:
        return f"TokenFileCredentials({self._path!r})"


class JwtCredentials(HubCredentials):
    """A JWT from a :class:`TokenProvider`."""

    kind = "access token"

    def __init__(self, provider: TokenProvider) -> None:
        self._provider = provider

    async def for_register(self) -> hub_pb2.Credentials:
        try:
            issued = await self._provider.token()
        except TokenError as e:
            raise NoToken(str(e)) from None
        return hub_pb2.Credentials(jwt=issued.value)

    def latest(self) -> hub_pb2.Credentials | None:
        issued = self._provider.published
        return None if issued is None else hub_pb2.Credentials(jwt=issued.value)

    def listen(self, changed: Callable[[], None]) -> None:
        self._provider.listen(changed)

    def refused(self, presented: hub_pb2.Credentials) -> None:
        if presented.WhichOneof("kind") == "jwt":
            self._provider.invalidate(presented.jwt)

    async def close(self) -> None:
        await self._provider.close()

    def __repr__(self) -> str:
        return "JwtCredentials(***)"


def build(credential: Credential | None, *, service: str, timings: Timings) -> HubCredentials | None:
    """The credential of an app's connections, its files checked now (:class:`ConfigurationError`)."""
    if credential is None:
        return None
    if isinstance(credential, Token):
        return StaticToken(credential.value)
    if isinstance(credential, TokenFile):
        return TokenFileCredentials(credential.path)
    if isinstance(credential, ClientCredentials):
        source: TokenSource = ClientCredentialsSource(credential, timeout=timings.token_fetch_timeout)
    else:
        source = credential
    return JwtCredentials(TokenProvider(source, timings))


# The access token: fetched, kept while fresh, renewed in the background.


class _Issued:
    """An access token the provider fetched: its value, its generation (1, 2, ... in fetch order, a token equal to
    the one before keeping its generation) and when it stops being fresh, by the monotonic and by the wall clock."""

    __slots__ = ("_refresh_loop", "_refresh_wall", "expires_at", "generation", "value")

    def __init__(self, value: str, generation: int, lifetime: float, refresh_after: float) -> None:
        self.value = value
        self.generation = generation
        now = time.time()
        self.expires_at = now + lifetime
        self._refresh_loop = asyncio.get_running_loop().time() + refresh_after
        self._refresh_wall = now + refresh_after

    def until_refresh(self) -> float:
        """Seconds until it is renewed (the earlier of the two clocks' view); zero or less once it is due."""
        return min(self._refresh_loop - asyncio.get_running_loop().time(), self._refresh_wall - time.time())

    def __repr__(self) -> str:
        return f"_Issued(***, generation={self.generation})"


class TokenProvider:
    """The instance's access token, one for every hub connection.

    :meth:`token` gives the cached token while it is fresh (before :func:`refresh_at` of its lifetime), else fetches
    one: callers that arrive while a fetch runs wait for that one (a waiter that is cancelled leaves it to the others),
    and its failure is theirs too. A fetch (task ``snodra:token``) asks the source, at most ``token_fetch_timeout``.
    After the first fetch, ``snodra:token-renewal`` renews the token when it stops being fresh and publishes each new
    one (:attr:`published`, the listeners called), at most once a second whatever the lifetime; a renewal that fails
    keeps the current token and is tried again after the first ``token_retry``, doubling (jittered) up to the second,
    one warning per distinct error. A token a hub refused (:meth:`invalidate`) is never handed out again."""

    def __init__(self, source: TokenSource, timings: Timings) -> None:
        self._source = source
        self._timeout = timings.token_fetch_timeout
        self._default_lifetime = timings.default_token_lifetime
        self._min_margin = timings.min_refresh_margin
        self._retry = timings.token_retry
        self._current: _Issued | None = None
        self._published: _Issued | None = None
        self._generation = 0
        self._listeners: list[Callable[[], None]] = []
        self._fetch: asyncio.Task[_Issued] | None = None
        self._renewer: asyncio.Task[None] | None = None
        self._closed = False
        self._no_expiry_warned = False

    @property
    def published(self) -> _Issued | None:
        """The newest token fetched, which the registered streams follow; None before the first."""
        return self._published

    def listen(self, changed: Callable[[], None]) -> None:
        self._listeners.append(changed)

    async def token(self) -> _Issued:
        """A token still worth registering with; :class:`TokenError` when none can be had."""
        current = self._current
        if current is not None and current.until_refresh() > 0:
            return current
        return await self._shared()

    def invalidate(self, value: str) -> None:
        """A hub refused value (``UNAUTHENTICATED``): while it is the current token, it is not handed out again."""
        if self._current is not None and self._current.value == value:
            self._current = None

    async def close(self) -> None:
        self._closed = True
        tasks = [t for t in (self._fetch, self._renewer) if t is not None and not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks)

    async def _shared(self) -> _Issued:
        if self._closed:
            raise TokenError("the app is shutting down")
        task = self._fetch
        if task is None:
            task = asyncio.create_task(self._fetch_once(), name="snodra:token")
            self._fetch = task
            task.add_done_callback(self._fetched)
        try:
            return await asyncio.shield(task)  # a waiter's cancellation never cancels the fetch the others wait for
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if task.cancelled() and (current is None or not current.cancelling()):
                raise TokenError("the app is shutting down") from None
            raise

    def _fetched(self, task: asyncio.Task[_Issued]) -> None:
        if self._fetch is task:
            self._fetch = None
        if not task.cancelled():
            task.exception()  # read here: a failure is its waiters'

    async def _fetch_once(self) -> _Issued:
        try:
            async with asyncio.timeout(self._timeout):
                token = await self._source.token()
        except TokenError:
            raise
        except TimeoutError:
            raise TokenError(f"no access token within {self._timeout:g} s") from None
        except Exception as e:
            raise TokenError(clip(f"the token source failed: {type(e).__name__}: {e}")) from None
        return self._publish(token)

    def _publish(self, token: AccessToken) -> _Issued:
        if not isinstance(token, AccessToken) or not isinstance(token.value, str) or not token.value.strip():
            raise TokenError("the token source gave no access token")
        if token.expires_at is None:
            lifetime = self._default_lifetime
            if not self._no_expiry_warned:
                self._no_expiry_warned = True
                log.warning("The access token for the hubs names no expiry: renewed as one of %g s", lifetime)
        else:
            lifetime = float(token.expires_at) - time.time()
            if math.isnan(lifetime) or lifetime <= 0:
                raise TokenError("the access token has already expired")
        lifetime = min(lifetime, MAX_LIFETIME)
        previous = self._published
        renewed = previous is None or previous.value != token.value
        if renewed:
            self._generation += 1
        # TODO(spec-gap): SG-11
        issued = _Issued(
            token.value,
            self._generation,
            lifetime,
            max(refresh_at(lifetime, self._min_margin), MIN_RENEWAL_INTERVAL),
        )
        self._current = self._published = issued
        log.debug("Access token for the hubs fetched (generation %d), valid for %.0f s", issued.generation, lifetime)
        if renewed:
            for changed in list(self._listeners):
                try:
                    changed()
                except Exception:
                    log.exception("Following a renewed access token failed")
        if self._renewer is None and not self._closed:
            self._renewer = asyncio.create_task(self._renew(), name="snodra:token-renewal")
        return issued

    async def _renew(self) -> None:
        lo, hi = self._retry
        backoff = lo
        retry: float | None = None  # the wait after a failure
        logged: str | None = None
        while True:
            if retry is not None:
                await asyncio.sleep(retry)
            else:
                current = self._current
                wait = 0.0 if current is None else current.until_refresh()
                if wait > 0:
                    await asyncio.sleep(min(wait, _SLICE))
                    continue  # look again: renewed meanwhile, or a clock moved
            current = self._current
            if current is not None and current.until_refresh() > 0:
                retry = None  # a stream's own fetch renewed it meanwhile
                continue
            try:
                await self._shared()
            except TokenError as e:
                retry = jitter(backoff, hi, random.random())
                backoff = next_wait(backoff, lo, hi)
                text = str(e)
                log.log(
                    logging.WARNING if text != logged else logging.DEBUG,
                    "Renewing the access token for the hubs failed (%s): keeping the current one, retrying in %.2f s",
                    text,
                    retry,
                )
                logged = text
                continue
            if logged is not None:
                log.info("The access token for the hubs is renewed again")
            retry, backoff, logged = None, lo, None


# The OAuth2 client-credentials grant.


class ClientCredentialsSource:
    """The :class:`TokenSource` of a :class:`ClientCredentials` grant (RFC 6749, 4.4): one fetch per call."""

    def __init__(self, grant: ClientCredentials, *, timeout: float) -> None:
        self._grant = grant
        self._timeout = timeout
        self._host = urllib.parse.urlsplit(grant.token_url).hostname or grant.token_url
        if grant.client_secret_file is not None:
            self._read_secret()  # now, for the startup error
        if grant.ca_file is not None:
            try:
                ssl.create_default_context().load_verify_locations(cafile=grant.ca_file)
            except (OSError, ssl.SSLError, ValueError) as e:
                raise ConfigurationError(
                    f"the token endpoint's CA file (ClientCredentials.ca_file, SNODRA_SERVER_OAUTH2_CA) "
                    f"{grant.ca_file} cannot be used: {type(e).__name__}"
                ) from None

    def __repr__(self) -> str:
        return f"ClientCredentialsSource({self._grant!r})"

    def _read_secret(self) -> str:
        path = self._grant.client_secret_file
        assert path is not None
        name = "the client secret file (ClientCredentials.client_secret_file, SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE)"
        try:
            secret = Path(path).read_text(encoding="utf-8").strip()
        except (OSError, ValueError) as e:
            raise ConfigurationError(f"{name} {path} cannot be read: {type(e).__name__}") from None
        if not secret:
            raise ConfigurationError(f"{name} {path} is empty")
        return secret

    async def token(self) -> AccessToken:
        grant = self._grant
        if grant.client_secret_file is not None:
            try:
                secret = await asyncio.to_thread(self._read_secret)
            except ConfigurationError as e:
                raise TokenError(str(e)) from None
        else:
            secret = grant.client_secret or ""
        user = f"{_form(grant.client_id)}:{_form(secret)}"
        basic = base64.b64encode(user.encode()).decode("ascii")
        hidden = [basic, user, _form(secret), secret]  # what an endpoint might echo back in a text
        fields = {"grant_type": "client_credentials"} | ({} if grant.scope is None else {"scope": grant.scope})
        try:
            return await _http.exchange(
                grant.token_url,
                method="POST",
                data=urllib.parse.urlencode(fields).encode("ascii"),
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "User-Agent": client_version(),
                },
                timeout=self._timeout,
                max_bytes=MAX_TOKEN_RESPONSE,
                ca_file=grant.ca_file,
                read=functools.partial(self._read, hidden),
            )
        except TokenError:
            raise
        except _http.TooLarge:
            raise TokenError(
                f"the token endpoint {self._host} answered with more than {MAX_TOKEN_RESPONSE} bytes"
            ) from None
        except TimeoutError:
            raise TokenError(f"no answer from the token endpoint {self._host} within {self._timeout:g} s") from None
        except Exception as e:
            raise TokenError(
                _masked(f"the token endpoint {self._host} cannot be reached: {_describe(e)}", hidden)
            ) from None

    def _read(self, hidden: list[str], response: _http.Response) -> AccessToken:
        """The token of the endpoint's answer (in the exchange's thread); :class:`TokenError` for any other answer."""
        try:
            doc = json.loads(response.body)
        except ValueError:
            doc = None
        if response.status != 200:
            error = doc.get("error") if isinstance(doc, dict) else None
            if not isinstance(error, str) or not error:
                raise TokenError(f"the token endpoint {self._host} answered HTTP {response.status}")
            description = doc.get("error_description") if isinstance(doc, dict) else None
            text = error if not isinstance(description, str) else f"{error}: {description}"
            raise TokenError(
                _masked(f"the token endpoint {self._host} refused (HTTP {response.status}): {text}", hidden)
            )
        if not isinstance(doc, dict):
            raise TokenError(f"the token endpoint {self._host} answered something that is not a JSON object")
        value = doc.get("access_token")
        if not isinstance(value, str) or not value.strip():
            raise TokenError(f"the token endpoint {self._host} answered without an access_token")
        kind = doc.get("token_type")
        if kind is not None and not (isinstance(kind, str) and kind.lower() == "bearer"):
            raise TokenError(f"the token endpoint {self._host} answered a token_type that is not Bearer")
        expires_in = doc.get("expires_in")
        if expires_in is None:
            return AccessToken(value, None)
        if isinstance(expires_in, bool) or not isinstance(expires_in, int | float) or not math.isfinite(expires_in):
            raise TokenError(f"the token endpoint {self._host} answered an expires_in that is not a positive number")
        seconds = min(int(expires_in), int(MAX_LIFETIME))
        if seconds <= 0:
            raise TokenError(f"the token endpoint {self._host} answered an expires_in that is not a positive number")
        return AccessToken(value, time.time() + seconds)


def _form(value: str) -> str:
    """value form-urlencoded (RFC 6749, 2.3.1: the client id and secret, before the Basic encoding)."""
    return urllib.parse.quote_plus(value, safe="")


def _masked(text: str, hidden: list[str]) -> str:
    for needle in hidden:
        if len(needle) >= 3:  # shorter would mask any text
            text = text.replace(needle, "***")
    return clip(text)


def _describe(e: BaseException) -> str:
    reason = getattr(e, "reason", None)  # urllib's URLError: the cause, without the URL
    with contextlib.suppress(Exception):
        if reason is not None:
            return f"{type(e).__name__}: {reason}"
    return f"{type(e).__name__}: {e}"
