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
from snodra import App, Codec, Settings, message_type
from snodra._hubpb import hub_pb2
from snodra._timings import DEFAULT_TIMINGS, Timings

SCALED: Final = replace(
    DEFAULT_TIMINGS,
    heartbeat_before_ack=0.1,
    handshake_timeout=0.3,
    deregister_wait=0.1,
    subscription_end=0.2,
    handler_drain=1.0,
    close_wait=0.2,
)
"""The shipped timings, shortened for the tests."""

_apps: list[App] = []


def new_app(
    hubs: FakeHub | Sequence[FakeHub],
    /,
    *,
    codec: Codec | None = None,
    _timings: Timings | None = None,
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
    app = App(Settings(**(values | settings)), codec=codec, _timings=SCALED if _timings is None else _timings)
    _apps.append(app)
    return app


async def start_app(hubs: FakeHub | Sequence[FakeHub], /, *, _timings: Timings | None = None, **settings: Any) -> App:
    """:func:`new_app`, started."""
    app = new_app(hubs, _timings=_timings, **settings)
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


def assert_no_library_tasks() -> None:
    """Fails if a task named ``snodra:...`` is still running."""
    left = sorted(t.get_name() for t in asyncio.all_tasks() if t.get_name().startswith("snodra:") and not t.done())
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
        jwks.fetches += 1
        if jwks.delay > 0:
            time.sleep(jwks.delay)
        if jwks.down:
            self.send_error(503, "down")
            return
        body = json.dumps({"keys": list(jwks.published)}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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

    ``delay`` (seconds) holds every answer back; ``down`` answers 503; ``fetches`` counts the requests;
    ``published`` is the JWKS's keys, which a test may change."""

    def __init__(self) -> None:
        self.key = rsa_key(0)
        self.other = rsa_key(1)
        """A key the JWKS does not publish."""
        self.published: list[dict[str, str]] = [jwk(self.key, "k-1")]
        self.delay = 0.0
        self.down = False
        self.fetches = 0
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
