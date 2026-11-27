"""Helpers of the tests that run an :class:`snodra.App` against fake hubs.

``start_app`` starts an app with the scaled timings below; the ``_stop_apps`` fixture of ``conftest.py`` shuts every
app started in a test down when the test ends, whatever its outcome, and then checks that no task of the library is
left. Waits are bounded, and a bound only stops a hang.
"""

from __future__ import annotations

import asyncio
import base64
import http.server
import json
import socketserver
import threading
import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Final

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from fakehub import FakeHub, FakeStream, ack
from snodra import App, Codec, Settings, Telemetry, message_type
from snodra._hubpb import hub_pb2
from snodra._processors import Processors
from snodra._timings import DEFAULT_TIMINGS, Timings

SCALED: Final = replace(
    DEFAULT_TIMINGS,
    heartbeat_before_ack=0.1,
    handshake_timeout=0.3,
    deregister_wait=0.1,
    subscription_end=0.2,
    handler_drain=1.0,
    close_wait=0.2,
    status_interval=300.0,
    processor_control_poll=0.05,
    dead_letter_sample=0.1,
    dns_refresh=300.0,
)
"""The shipped timings, shortened for the tests. The status reports and a DNS re-resolution are pushed far away: a test
that observes them names its own interval (the frames would otherwise land in the middle of a scripted stream)."""

_apps: list[App] = []


def new_app(
    hubs: FakeHub | Sequence[FakeHub],
    /,
    *,
    store: Any = None,
    event_store: Any = None,
    codec: Codec | None = None,
    telemetry: Telemetry | None = None,
    _timings: Timings | None = None,
    _clock: Callable[[], int] | None = None,
    **settings: Any,
) -> App:
    """An app on hubs, not started, with the test defaults (``orders-service``, instance ``i-1``, reconnect backoff
    0.05 s to 0.2 s, :data:`SCALED` timings) under ``settings`` (which may name other ``hubs``); shut down when the
    test ends."""
    targets = [hubs] if isinstance(hubs, FakeHub) else list(hubs)
    values: dict[str, Any] = {
        "service": "orders-service",
        "instance": "i-1",
        "reconnect_backoff": (0.05, 0.2),
        "hubs": tuple(h.address for h in targets),
    }
    app = App(
        Settings(**(values | settings)),
        store=store,
        event_store=event_store,
        codec=codec,
        telemetry=telemetry,
        _timings=SCALED if _timings is None else _timings,
        _clock=_clock,
    )
    _apps.append(app)
    return app


async def start_app(
    hubs: FakeHub | Sequence[FakeHub],
    /,
    *,
    store: Any = None,
    event_store: Any = None,
    telemetry: Telemetry | None = None,
    _timings: Timings | None = None,
    _clock: Callable[[], int] | None = None,
    **settings: Any,
) -> App:
    """:func:`new_app`, started."""
    app = new_app(
        hubs,
        store=store,
        event_store=event_store,
        telemetry=telemetry,
        _timings=_timings,
        _clock=_clock,
        **settings,
    )
    await app.start()
    return app


async def stop_apps() -> None:
    """Shuts down every app the test made, then fails if a task of the library is left."""
    try:
        while _apps:
            await _apps.pop().shutdown(timeout=5.0)
    finally:
        _apps.clear()
    assert_no_library_tasks()


def processors_of(app: App) -> Processors:
    """The app's event processors (asserting it built them)."""
    assert app._processors is not None, "the app runs no event processor"
    return app._processors


def library_tasks() -> set[str]:
    """The names of the library's tasks (``snodra:...``) still running."""
    return {t.get_name() for t in asyncio.all_tasks() if t.get_name().startswith("snodra:") and not t.done()}


def assert_no_library_tasks() -> None:
    """Fails if a task named ``snodra:...`` is still running."""
    left = sorted(library_tasks())
    assert not left, f"library tasks left: {left}"


def within(value: float, lo: float, hi: float) -> None:
    """Fails unless ``lo <= value <= hi``."""
    assert lo <= value <= hi, f"{value:.4f} is not between {lo:.4f} and {hi:.4f}"


async def eventually(predicate: Callable[[], bool], within: float = 1.0) -> None:
    """Waits until predicate holds; fails after ``within`` seconds."""
    deadline = time.monotonic() + within
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"the condition did not hold within {within} s")
        await asyncio.sleep(0.005)


async def acknowledged(hub: FakeHub, hub_id: str = "hub-1") -> FakeStream:
    """The hub's next stream, its ``Register`` read and acknowledged."""
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack(hub_id))
    return s


_BESIDE_ANSWERS: Final = frozenset({"heartbeat", "status", "permits"})


async def next_reply(s: FakeStream, within: float = 5.0) -> hub_pb2.Reply:
    """The next ``Reply`` on s, skipping heartbeats, statuses and ``Permits`` (which a batching timer writes at any
    moment); any other frame fails."""
    deadline = time.monotonic() + within
    while True:
        m = await s.recv(within=max(0.0, deadline - time.monotonic()))
        kind = m.WhichOneof("message")
        if kind == "reply":
            return m.reply
        if kind not in _BESIDE_ANSWERS:
            raise AssertionError(f"expected a reply, got {kind}: {m}")


# A token issuer for the tests: an RSA key, its JWKS served over HTTP, and tokens signed with it.

ISSUER: Final = "https://issuer.test"
"""The issuer of the tokens :class:`Jwks` mints."""

_CUT_PROBE: Final = 0.02
"""How long a delayed JWKS answer waits before its body, so that a client that gave up is seen as gone."""

_RSA_KEYS: list[rsa.RSAPrivateKey] = []


def rsa_key(index: int) -> rsa.RSAPrivateKey:
    """The test session's RSA key number index (made once: making one takes tens of milliseconds)."""
    while len(_RSA_KEYS) <= index:
        _RSA_KEYS.append(rsa.generate_private_key(public_exponent=65537, key_size=2048))
    return _RSA_KEYS[index]


def b64url(data: bytes) -> str:
    """Base64url without padding, as JWS writes its segments."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def jwk(key: rsa.RSAPrivateKey, kid: str) -> dict[str, str]:
    """The public JWK of key under kid."""
    numbers = key.public_key().public_numbers()
    return {
        "kty": "RSA",
        "kid": kid,
        "use": "sig",
        "alg": "RS256",
        "n": b64url(numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")),
        "e": b64url(numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big")),
    }


def sign(header: Mapping[str, Any], claims: Mapping[str, Any], key: rsa.RSAPrivateKey) -> str:
    """A compact JWS of claims under header, signed with key (RS256)."""
    signing_input = f"{b64url(json.dumps(header).encode())}.{b64url(json.dumps(claims).encode())}"
    signature = key.sign(signing_input.encode("ascii"), padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input}.{b64url(signature)}"


class _JwksHandler(http.server.BaseHTTPRequestHandler):
    server: _JwksServer

    def do_GET(self) -> None:
        jwks = self.server.jwks
        jwks.paths.append(self.path)
        jwks.fetches += 1
        try:
            if jwks.delay > 0:
                time.sleep(jwks.delay)
            self._answer(jwks)
        except OSError:  # the client cut the connection first: nothing can be written any more
            jwks.write_failed = True
            self.close_connection = True

    def _answer(self, jwks: Jwks) -> None:
        if jwks.down:
            self.send_error(503, "down")
            return
        if jwks.redirect is not None and self.path == "/jwks":
            self.send_response(302)
            self.send_header("Location", jwks.redirect)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if jwks.dribble:
            self._dribble(jwks)
            return
        body = json.dumps({"keys": list(jwks.published)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if jwks.delay > 0:
            # A client that gave up during the delay closed its socket: the headers went out, its reset comes back,
            # and the body write below is the one that fails (that is how ``write_failed`` is observed).
            time.sleep(_CUT_PROBE)
        self.wfile.write(body)

    def _dribble(self, jwks: Jwks) -> None:
        """Announces a large body and sends one byte of it every 20 ms, for at most 3 s; records whether the client
        cut the connection meanwhile."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "100000")
        self.end_headers()
        until = time.monotonic() + 3.0
        try:
            while time.monotonic() < until:
                self.wfile.write(b" ")
                self.wfile.flush()
                time.sleep(0.02)
        except OSError:
            jwks.dribble_cut = True
        self.close_connection = True

    def log_message(self, format: str, *args: Any) -> None:
        pass


class _JwksServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, jwks: Jwks) -> None:
        super().__init__(("127.0.0.1", 0), _JwksHandler)
        self.jwks = jwks

    def server_bind(self) -> None:
        # Without HTTPServer's reverse lookup of the address, which can take seconds.
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name, self.server_port = str(host), port


class Jwks:
    """An issuer of the tests: its JWKS (the key ``k-1``) served by a local HTTP server in a thread, at :attr:`url`.

    ``delay`` (seconds) holds every answer back; ``down`` answers 503; ``redirect`` answers ``/jwks`` with a 302 to
    that location; ``dribble`` sends the body one byte every 20 ms and sets ``dribble_cut`` when the client cuts the
    connection; ``write_failed`` is set when an answer, its delay over, could not be written because the client cut the
    connection meanwhile; ``fetches`` counts the requests and ``paths`` lists their paths; ``published`` is the JWKS's
    keys, which a test may change."""

    def __init__(self) -> None:
        self.key = rsa_key(0)
        self.other = rsa_key(1)
        """A key the JWKS does not publish."""
        self.published: list[dict[str, str]] = [jwk(self.key, "k-1")]
        self.delay = 0.0
        self.down = False
        self.redirect: str | None = None
        self.dribble = False
        self.dribble_cut = False
        self.write_failed = False
        self.fetches = 0
        self.paths: list[str] = []
        self._server = _JwksServer(self)
        self._thread = threading.Thread(target=self._server.serve_forever, args=(0.05,), name="tests:jwks", daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}/jwks"

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def mint(
        self,
        sub: str = "u-1",
        roles: tuple[str, ...] = ("orders-user",),
        *,
        exp: float | None = None,
        kid: str = "k-1",
        issuer: str = ISSUER,
        key: rsa.RSAPrivateKey | None = None,
        claims: Mapping[str, Any] | None = None,
    ) -> str:
        """A token of sub with roles (claim ``roles``), expiring at exp (default: in 5 minutes), under kid, signed
        with key (default: the published one); ``claims`` are added over these."""
        now = time.time()
        body: dict[str, Any] = {
            "iss": issuer,
            "sub": sub,
            "iat": int(now),
            "exp": int(now + 300 if exp is None else exp),
        }
        body["roles"] = list(roles)
        body |= dict(claims or {})
        return sign({"alg": "RS256", "typ": "JWT", "kid": kid}, body, self.key if key is None else key)


@pytest.fixture
async def jwks() -> AsyncIterator[Jwks]:
    """A :class:`Jwks` issuer, stopped when the test ends."""
    issuer = await asyncio.to_thread(Jwks)
    try:
        yield issuer
    finally:
        await asyncio.to_thread(issuer.stop)


# An OAuth2 token endpoint for the tests (client credentials), answering a script.


@dataclass
class TokenAnswer:
    """One answer of :class:`TokenEndpoint`: after ``delay`` seconds, ``status``; a 200 carries ``access_token``
    (``token``, or the next ``jwt-<n>`` when None) and ``expires_in`` (left out when None); another status carries the
    OAuth2 ``error`` when one is given. ``body``, when set, is the whole body instead."""

    status: int = 200
    token: str | None = None
    expires_in: float | None = 60
    error: str | None = None
    delay: float = 0.0
    body: bytes | None = None


@dataclass(frozen=True)
class TokenRequest:
    """A request the endpoint received, and when (``time.monotonic()``)."""

    at: float
    method: str
    headers: dict[str, str]
    body: bytes


class _TokenHandler(http.server.BaseHTTPRequestHandler):
    server: _TokenServer

    def do_POST(self) -> None:
        self._answer("POST")

    def do_GET(self) -> None:
        self._answer("GET")

    def _answer(self, method: str) -> None:
        endpoint = self.server.endpoint
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        endpoint.requests.append(TokenRequest(time.monotonic(), method, dict(self.headers.items()), body))
        answer = endpoint.script.pop(0) if endpoint.script else endpoint.default
        if answer.delay > 0:
            time.sleep(answer.delay)
        doc: dict[str, Any] = {}
        if answer.status == 200:
            token = answer.token or f"jwt-{len(endpoint.issued) + 1}"
            endpoint.issued.append(token)
            doc = {"access_token": token, "token_type": "Bearer"}
            if answer.expires_in is not None:
                doc["expires_in"] = answer.expires_in
        elif answer.error is not None:
            doc = {"error": answer.error, "error_description": "refused by the test"}
        out = json.dumps(doc).encode() if answer.body is None else answer.body
        self.send_response(answer.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, format: str, *args: Any) -> None:
        pass


class _TokenServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, endpoint: TokenEndpoint) -> None:
        super().__init__(("127.0.0.1", 0), _TokenHandler)
        self.endpoint = endpoint

    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name, self.server_port = str(host), port


class TokenEndpoint:
    """A token endpoint served by a local HTTP server in a thread, at :attr:`url`: each request takes the first answer
    of ``script`` (``default`` once it is empty); ``requests`` records every request, ``issued`` every token given."""

    def __init__(self) -> None:
        self.script: list[TokenAnswer] = []
        self.default = TokenAnswer()
        self.requests: list[TokenRequest] = []
        self.issued: list[str] = []
        self._server = _TokenServer(self)
        self._thread = threading.Thread(
            target=self._server.serve_forever, args=(0.05,), name="tests:token-endpoint", daemon=True
        )
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}/token"

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
async def token_endpoint() -> AsyncIterator[TokenEndpoint]:
    """A :class:`TokenEndpoint`, stopped when the test ends."""
    endpoint = await asyncio.to_thread(TokenEndpoint)
    try:
        yield endpoint
    finally:
        await asyncio.to_thread(endpoint.stop)


TP: Final = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

"""A valid W3C traceparent."""

INVALID_TRACEPARENTS: Final = [
    "",
    "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",  # another version
    "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",  # upper case
    "00-00000000000000000000000000000000-00f067aa0ba902b7-01",  # a zero trace id
    "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",  # a zero parent id
    "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",  # a trace id one digit short
    TP + "\n",
    TP + "-00",
    " " + TP,
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
]
"""W3C traceparent values a library must not forward."""


# The example messages of the tests.


@message_type("orders.PlaceOrder")
@dataclass
class PlaceOrder:
    order_id: str
    lines: list[str]


@dataclass
class Placed:
    order_id: str
    total: int


@message_type("orders.GetOrder")
@dataclass
class GetOrder:
    order_id: str


@message_type("orders.CancelOrder")
@dataclass
class CancelOrder:
    order_id: str


@message_type("orders.GetStock")
@dataclass
class GetStock:
    """A query many services may answer (a shared type)."""

    sku: str


@message_type("orders.ListOrders")
@dataclass
class ListOrders:
    """A streaming query: its handler yields one item per order."""

    order_id: str


@message_type("orders.WatchOrders")
@dataclass
class WatchOrders:
    """A subscription query: the topic whose updates a subscription watches."""

    topic: str


@message_type("orders.WatchArchive")
@dataclass
class WatchArchive:
    """A shared subscription query."""

    topic: str


@message_type("orders.Noop")
@dataclass
class Noop:
    """A command without a result, for a request that must run while something else is served."""


@dataclass
class WatchInitial:
    """The initial result of a subscription to orders.WatchOrders."""

    topic: str
    value: int


@dataclass
class WatchUpdate:
    """One update of a subscription to orders.WatchOrders."""

    topic: str
    value: object


@dataclass
class Stock:
    count: int
