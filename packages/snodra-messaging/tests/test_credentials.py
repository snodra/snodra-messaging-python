"""The instance's credential towards the hubs: a static token, a token file, a JWT from an OAuth2 token endpoint (its
renewal, its retirement after UNAUTHENTICATED, an endpoint that fails), and the rule of one credential kind.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Credentials").
"""

import asyncio
import base64
import itertools
import json
import logging
import threading
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import grpc
import pytest
from fakehub import FakeHub, FakeStream, ack, forward
from helpers import (
    SCALED,
    GetOrder,
    Placed,
    TokenAnswer,
    TokenEndpoint,
    acknowledged,
    eventually,
    new_app,
    next_reply,
    start_app,
    within,
)
from snodra import (
    AccessToken,
    ClientCertificate,
    ClientCredentials,
    ConfigurationError,
    Settings,
    Token,
    TokenFile,
    _credentials,
)
from snodra._credentials import MAX_TOKEN_RESPONSE, ClientCredentialsSource, TokenError, refresh_at
from snodra._hubpb import hub_pb2

SECRET = "s3cr3t-token"


def _client(endpoint: TokenEndpoint, **fields: str) -> ClientCredentials:
    values = {"client_secret": "p@ss w:rd"} | fields
    return ClientCredentials(endpoint.url, "orders-service", **values)


async def _registered(hub: FakeHub, interval_ms: int = 3000) -> tuple[FakeStream, hub_pb2.Register]:
    """The hub's next stream, acknowledged, and its Register."""
    s = await hub.next_stream()
    r = await s.recv_register()
    await s.ack(ack("hub-1", interval_ms=interval_ms))
    return s, r


async def test_the_token_is_on_every_stream(hub: FakeHub, hub2: FakeHub) -> None:
    await start_app([hub, hub2], credentials=Token(SECRET))
    streams = []
    for h in (hub, hub2):
        s, r = await _registered(h)
        assert r.credentials.WhichOneof("kind") == "token"
        assert r.credentials.token == SECRET
        streams.append(s)
    await streams[0].close(grpc.StatusCode.UNAVAILABLE, "restart")
    _, r = await _registered(hub)
    assert r.credentials.token == SECRET  # and after a reconnect


async def test_the_token_is_never_logged(hub: FakeHub, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    app = await start_app(hub, credentials=Token(SECRET))
    s, _ = await _registered(hub)
    await s.close(grpc.StatusCode.UNAUTHENTICATED, "token not accepted")
    await eventually(lambda: app.rejection is not None)
    s, r = await _registered(hub)
    assert r.credentials.token == SECRET  # it was sent
    await app.shutdown()

    raised: list[BaseException] = []
    bad: Callable[[], object]
    for bad in (
        lambda: Settings(
            service="orders-service", credentials=Token(SECRET), client_certificate=ClientCertificate("c", "k")
        ),
        lambda: Settings(service="", credentials=Token(SECRET)),
        lambda: Settings(service="orders-service", credentials=Token(SECRET), hubs=("http://h:7000",)),
    ):
        with pytest.raises(ConfigurationError) as e:
            bad()
        raised.append(e.value)
    texts = [r.getMessage() for r in caplog.records]
    texts += [logging.Formatter().formatException(r.exc_info) for r in caplog.records if r.exc_info]
    texts += [str(e) for e in raised] + [repr(e) for e in raised]
    texts.append(repr(app._settings))
    for value in (
        Token(SECRET),
        AccessToken(SECRET, time.time() + 60),
        AccessToken(SECRET, None),
        ClientCredentials("https://idp.test/token", "orders-service", client_secret=SECRET),
    ):
        texts += [repr(value), str(value), format(value), f"{value!r}", f"{value}"]
    assert texts
    for text in texts:
        assert SECRET not in text, text
    assert "***" in repr(Token(SECRET))


async def test_the_token_file_is_read_before_every_connect_and_on_every_tick(
    hub: FakeHub, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    file = tmp_path / "token"
    file.write_text("t-1\n")
    await start_app(hub, credentials=TokenFile(str(file)))
    s, r = await _registered(hub, interval_ms=100)
    assert r.credentials.token == "t-1"
    await s.nothing(0.25, "credentials")  # an unchanged file sends nothing
    file.write_text("t-2\n")
    assert (await s.recv_credentials(within=1.0)).token == "t-2"  # on the next tick
    await s.nothing(0.35, "credentials")  # once
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s, r = await _registered(hub, interval_ms=100)
    assert r.credentials.token == "t-2"  # read before the connect

    caplog.set_level(logging.WARNING)
    file.write_text("")  # a rotation caught half way
    await s.nothing(0.35, "credentials")
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s, r = await _registered(hub, interval_ms=100)
    assert r.credentials.token == "t-2"  # the last token read
    await s.nothing(0.25, "credentials")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and str(file) in r.getMessage()]
    assert len(warnings) == 1, [r.getMessage() for r in warnings]

    empty = tmp_path / "empty"
    empty.write_text("  \n")
    for path in (empty, tmp_path / "missing"):
        with pytest.raises(ConfigurationError, match="SNODRA_SERVER_TOKEN_FILE") as e:
            new_app(hub, credentials=TokenFile(str(path)))
        assert str(path) in str(e.value)


def test_refresh_at() -> None:
    # Renewed when a fifth of the lifetime is left, at least 5 s, at most half of it.
    assert (refresh_at(100), refresh_at(60), refresh_at(10), refresh_at(8)) == (80, 48, 5, 4)


async def test_the_jwt_registers_and_each_renewal_is_sent_once(hub: FakeHub, token_endpoint: TokenEndpoint) -> None:
    token_endpoint.default = TokenAnswer(expires_in=2)  # renewed every second
    await start_app(hub, credentials=_client(token_endpoint, scope="hub.connect orders"))
    s, r = await _registered(hub, interval_ms=200)
    assert r.credentials.WhichOneof("kind") == "jwt"
    assert r.credentials.jwt == "jwt-1"

    first = token_endpoint.requests[0]
    assert first.method == "POST"
    basic = base64.b64encode(
        f"{urllib.parse.quote_plus('orders-service')}:{urllib.parse.quote_plus('p@ss w:rd')}".encode()
    ).decode()
    assert first.headers["Authorization"] == f"Basic {basic}"
    assert first.headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert urllib.parse.parse_qs(first.body.decode()) == {
        "grant_type": ["client_credentials"],
        "scope": ["hub.connect orders"],
    }

    seen: list[str] = []
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            m = await s.recv(within=max(0.01, deadline - time.monotonic()))
        except AssertionError as e:
            if "no frame" not in str(e):
                raise
            break
        kind = m.WhichOneof("message")
        assert kind in ("heartbeat", "credentials"), m
        if kind == "credentials":
            assert m.credentials.WhichOneof("kind") == "jwt"
            seen.append(m.credentials.jwt)
    renewed = token_endpoint.issued[1:]
    assert len(seen) >= 3, seen
    assert seen == renewed[: len(seen)]  # every new token, in order, exactly once
    assert len(renewed) - len(seen) <= 1  # at most one issued at the very end, still on its way
    assert len(token_endpoint.requests) <= len(seen) + 2  # no fetch beyond the renewals (and the first)


async def test_an_unauthenticated_close_registers_with_a_newly_fetched_token(
    hub: FakeHub, token_endpoint: TokenEndpoint
) -> None:
    await start_app(hub, credentials=_client(token_endpoint))
    s, r = await _registered(hub)
    assert r.credentials.jwt == "jwt-1"
    closed = time.monotonic()
    await s.close(grpc.StatusCode.UNAUTHENTICATED, "token not accepted")
    s, r = await _registered(hub)
    assert r.credentials.jwt == "jwt-2"
    assert len(token_endpoint.requests) == 2
    assert token_endpoint.requests[1].at > closed


async def test_a_permission_denied_close_fetches_nothing(hub: FakeHub, token_endpoint: TokenEndpoint) -> None:
    await start_app(hub, credentials=_client(token_endpoint))
    s, r = await _registered(hub)
    await s.close(grpc.StatusCode.PERMISSION_DENIED, "not in the ACL")
    s, r = await _registered(hub)
    assert r.credentials.jwt == "jwt-1"  # a new token would not help
    assert len(token_endpoint.requests) == 1


async def test_a_token_endpoint_failure_is_exposed_and_retried(hub: FakeHub, token_endpoint: TokenEndpoint) -> None:
    token_endpoint.default = TokenAnswer(status=500)
    app = await start_app(hub, credentials=_client(token_endpoint), reconnect_backoff=(0.05, 1.0))
    await eventually(lambda: app.rejection is not None)
    rejection = app.rejection
    assert rejection is not None
    assert rejection.code == "UNAUTHENTICATED"
    assert "access token" in rejection.message
    assert "500" in rejection.message
    assert "p@ss" not in rejection.message
    await hub.no_stream_within(0.3)  # no stream without a token
    # Retried at the ordinary backoff (0.05 s doubling), not at a refusal's wait (80 to 100 % of 1 s).
    await eventually(lambda: len(token_endpoint.requests) >= 4, within=2.0)
    at = [r.at for r in token_endpoint.requests[:4]]
    gaps = [b - a for a, b in itertools.pairwise(at)]
    assert gaps[0] < 0.2, gaps
    assert sum(gaps) < 0.7, gaps

    token_endpoint.script = [TokenAnswer(status=401, error="invalid_client")]
    await eventually(lambda: app.rejection is not None and "invalid_client" in app.rejection.message, within=2.0)
    token_endpoint.default = TokenAnswer()
    _, r = await _registered(hub)
    assert r.credentials.jwt == "jwt-1"


async def test_a_failed_renewal_keeps_the_token_and_retries(hub: FakeHub, token_endpoint: TokenEndpoint) -> None:
    token_endpoint.script = [TokenAnswer(expires_in=2)] + [TokenAnswer(status=503)] * 4
    token_endpoint.default = TokenAnswer(expires_in=60)
    await start_app(hub, credentials=_client(token_endpoint), _timings=replace(SCALED, token_retry=(0.1, 0.4)))
    s, r = await _registered(hub, interval_ms=200)
    assert r.credentials.jwt == "jwt-1"
    c = await s.recv_credentials(within=4.0)  # renewed at 1 s, then failures retried: 0.1, 0.2, 0.4, 0.4 s
    assert c.jwt == "jwt-2"
    at = [r.at for r in token_endpoint.requests]
    assert len(at) == 6
    gaps = [b - a for a, b in itertools.pairwise(at[1:])]
    for gap, (lo, hi) in zip(gaps, [(0.08, 0.12), (0.16, 0.24), (0.32, 0.4), (0.32, 0.4)], strict=True):
        within(gap, lo, hi + 0.05)
    await s.nothing(0.3, "credentials", "register")  # the stream kept its token and was never closed


async def test_a_slow_token_endpoint_never_delays_heartbeats(hub: FakeHub, token_endpoint: TokenEndpoint) -> None:
    token_endpoint.script = [TokenAnswer(expires_in=2), TokenAnswer(expires_in=60, delay=1.0)]
    app = new_app(hub, credentials=_client(token_endpoint))

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 2)

    await app.start()
    s, _ = await _registered(hub, interval_ms=100)
    await eventually(lambda: len(token_endpoint.requests) == 2, within=2.0)  # the renewal is under way
    started = time.monotonic()
    request = hub_pb2.Request(id="q-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-1"}')
    await s.send(forward(request))
    beats: list[float] = []
    answered = 0.0
    renewed = False
    while not renewed:
        m = await s.recv(within=3.0)
        match m.WhichOneof("message"):
            case "heartbeat":
                beats.append(time.monotonic())
            case "reply":
                assert m.reply.WhichOneof("result") == "payload"
                answered = time.monotonic()
            case "credentials":
                assert m.credentials.jwt == "jwt-2"
                renewed = True
            case kind:
                assert kind == "permits", m
    assert 0 < answered - started < 0.2  # served while the fetch goes on
    assert time.monotonic() - started >= 0.8
    assert len(beats) >= 7
    gaps = [b - a for a, b in itertools.pairwise(beats)]
    assert max(gaps) <= 0.15, gaps


async def test_one_credential_kind(tmp_path: Path) -> None:
    for credentials in (Token("t"), TokenFile(str(tmp_path / "t")), ClientCredentials("https://idp.test/t", "c", "s")):
        with pytest.raises(ConfigurationError) as e:
            Settings(
                service="orders-service",
                credentials=credentials,
                client_certificate=ClientCertificate("c.pem", "k.pem"),
            )
        assert "credentials" in str(e.value)
        assert "client_certificate" in str(e.value)
    with pytest.raises(ConfigurationError, match="credentials"):
        Settings(service="orders-service", credentials="t")  # type: ignore[arg-type]


async def test_a_plain_text_credential_warns_once(hub: FakeHub, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    app = new_app(hub, credentials=Token(SECRET))
    warned = [r for r in caplog.records if "TLS" in r.getMessage()]
    assert len(warned) == 1
    assert hub.address in warned[0].getMessage()
    await app.start()
    s = await acknowledged(hub)
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await acknowledged(hub)
    assert len([r for r in caplog.records if "TLS" in r.getMessage()]) == 1


async def test_a_token_source_gives_the_jwt(hub: FakeHub) -> None:
    calls = 0

    class Source:
        async def token(self) -> AccessToken:
            nonlocal calls
            calls += 1
            return AccessToken(f"own-{calls}", time.time() + 60)

    app = new_app(hub, credentials=Source())

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 2)

    await app.start()
    s, r = await _registered(hub)
    assert r.credentials.jwt == "own-1"
    await s.send(forward(hub_pb2.Request(id="q-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b"{}")))
    assert (await next_reply(s)).request_id == "q-1"
    assert calls == 1


async def test_a_hung_token_file_never_delays_heartbeats(
    hub: FakeHub, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file = tmp_path / "token"
    file.write_text("t-1\n")
    await start_app(hub, credentials=TokenFile(str(file)))
    s, _ = await _registered(hub, interval_ms=100)
    release = threading.Event()
    reads = 0
    read = _credentials.TokenFileCredentials._read

    def hung(self: _credentials.TokenFileCredentials) -> str:
        nonlocal reads
        reads += 1
        release.wait(5.0)  # a mount that does not answer
        return read(self)

    monkeypatch.setattr(_credentials.TokenFileCredentials, "_read", hung)
    try:
        beats: list[float] = []
        while len(beats) < 8:
            m = await s.recv(within=1.0)
            if m.WhichOneof("message") == "heartbeat":
                beats.append(time.monotonic())
        gaps = [b - a for a, b in itertools.pairwise(beats)]
        assert max(gaps) <= 0.2, gaps
        assert reads == 1  # one read at a time, however many ticks pass
        file.write_text("t-2\n")
    finally:
        release.set()
    assert (await s.recv_credentials(within=1.0)).token == "t-2"  # followed once the read came back


async def test_a_connect_waits_for_the_read_under_way(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    file = tmp_path / "token"
    file.write_text("t-1\n")
    credentials = _credentials.TokenFileCredentials(str(file))
    release = threading.Event()
    reads = 0
    read = _credentials.TokenFileCredentials._read

    def hung(self: _credentials.TokenFileCredentials) -> str:
        nonlocal reads
        reads += 1
        release.wait(5.0)  # a mount that does not answer
        return read(self)

    monkeypatch.setattr(_credentials.TokenFileCredentials, "_read", hung)
    await credentials.on_tick()  # a tick starts the read, which hangs
    await eventually(lambda: reads == 1)
    file.write_text("t-2\n")
    register = asyncio.create_task(credentials.for_register())
    try:
        await asyncio.sleep(0.05)
        assert reads == 1  # the connect waits for the read under way instead of starting a second one
        assert not register.done()
    finally:
        release.set()
    assert (await register).token == "t-2"  # and the newest read is the one adopted
    await credentials.close()


async def test_short_lived_tokens_are_renewed_at_most_once_a_second(hub: FakeHub) -> None:
    assert _credentials.MIN_RENEWAL_INTERVAL == 1.0  # the shipped floor, not just its effect below
    calls = 0

    class Source:
        async def token(self) -> AccessToken:
            nonlocal calls
            calls += 1
            return AccessToken(f"own-{calls}", time.time() + 0.05)

    await start_app(hub, credentials=Source())
    s, r = await _registered(hub, interval_ms=100)
    assert r.credentials.jwt == "own-1"
    sent: list[float] = []
    started = time.monotonic()
    deadline = started + 1.6
    while (left := deadline - time.monotonic()) > 0:
        try:
            m = await s.recv(within=left)
        except AssertionError as e:
            if "no frame" not in str(e):
                raise
            break
        if m.WhichOneof("message") == "credentials":
            sent.append(time.monotonic() - started)
    assert 1 <= len(sent) <= 2, sent  # not one every 25 ms
    assert calls <= 3, calls


async def test_each_renewal_goes_to_every_stream(hub: FakeHub, hub2: FakeHub, token_endpoint: TokenEndpoint) -> None:
    token_endpoint.default = TokenAnswer(expires_in=2)  # renewed every second
    await start_app([hub, hub2], credentials=_client(token_endpoint))
    s1, r1 = await _registered(hub, interval_ms=200)
    s2, r2 = await _registered(hub2, interval_ms=200)
    assert (r1.credentials.jwt, r2.credentials.jwt) == ("jwt-1", "jwt-1")  # one token for both hubs
    assert (await s1.recv_credentials(within=3.0)).jwt == "jwt-2"
    assert (await s2.recv_credentials(within=3.0)).jwt == "jwt-2"


async def test_the_token_endpoints_answers(token_endpoint: TokenEndpoint) -> None:
    source = ClientCredentialsSource(_client(token_endpoint), timeout=2.0)

    async def answered(body: object, status: int = 200) -> AccessToken:
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        token_endpoint.script = [TokenAnswer(status=status, body=raw)]
        return await source.token()

    async def refused(body: object, status: int = 200) -> str:
        with pytest.raises(TokenError) as e:
            await answered(body, status)
        assert token_endpoint.url not in str(e.value)  # the host only
        return str(e.value)

    token = await answered({"access_token": "a-1", "token_type": "bearer", "expires_in": 30.7})
    assert token.value == "a-1"
    assert token.expires_at is not None
    within(token.expires_at - time.time(), 29.0, 30.1)  # whole seconds
    assert (await answered({"access_token": "a-2"})).expires_at is None  # no expiry named
    for expires_in in ("60", True, 0, -5):
        assert "expires_in" in await refused({"access_token": "a", "expires_in": expires_in}), expires_in
    assert "not Bearer" in await refused({"access_token": "a", "token_type": "mac"})
    assert "without an access_token" in await refused({"token_type": "Bearer"})
    assert "not a JSON object" in await refused(b"[1]")
    assert "HTTP 201" in await refused({"access_token": "a"}, status=201)
    assert "HTTP 302" in await refused(b"", status=302)
    assert f"more than {MAX_TOKEN_RESPONSE} bytes" in await refused(b"x" * (MAX_TOKEN_RESPONSE + 1))
    echoed = await refused({"error": "invalid_client", "error_description": "secret p@ss w:rd refused"}, status=401)
    assert "invalid_client" in echoed
    assert "p@ss w:rd" not in echoed
    assert "***" in echoed
    assert "\n" not in await refused({"error": "bad\nline"}, status=400)


async def test_a_token_url_or_jwks_url_with_userinfo_is_refused() -> None:
    for url in (
        "https://id:pw@idp.test/token",
        "https://id@idp.test/token",
        "ftp://id:pw@idp.test/token",  # another scheme: refused before it, so it is never echoed either
        "user:pw@idp.test/token",  # no scheme: parsed as scheme "user", the credentials in its path
    ):
        with pytest.raises(ConfigurationError, match="user") as e:
            Settings(service="orders-service", credentials=ClientCredentials(url, "c", "s"))
        assert "pw" not in str(e.value), url
    for url in (
        "https://id:pw@idp.test/jwks",
        "https://id@idp.test/jwks",
        "ftp://id:pw@idp.test/jwks",
        "user:pw@idp.test/jwks",
    ):
        with pytest.raises(ConfigurationError, match="user") as e:
            Settings(service="orders-service", jwks_url=url, issuer="https://idp.test")
        assert "pw" not in str(e.value), url
    # No authority in the text (a scheme-relative authority, a path where one was expected): the parts decide, or the
    # scheme error refuses the URL without it. Either way the URL is never echoed.
    for url, message in (
        ("//id:pw@idp.test/token", "userinfo"),
        ("https:///id:pw@idp.test/token", r"http:// or https://"),
    ):
        with pytest.raises(ConfigurationError, match=message) as e:
            Settings(service="orders-service", credentials=ClientCredentials(url, "c", "s"))
        assert "pw" not in str(e.value), url
    for url, message in (
        ("//id:pw@idp.test/jwks", "userinfo"),
        ("https:///id:pw@idp.test/jwks", r"http:// or https://"),
    ):
        with pytest.raises(ConfigurationError, match=message) as e:
            Settings(service="orders-service", jwks_url=url, issuer="https://idp.test")
        assert "pw" not in str(e.value), url


async def test_a_plain_http_token_url_warns(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    Settings(service="orders-service", credentials=ClientCredentials("http://idp.test/token", "c", "s"))
    assert any("http://" in r.getMessage() and "secret" in r.getMessage() for r in caplog.records)
    caplog.clear()
    Settings(service="orders-service", credentials=ClientCredentials("https://idp.test/token", "c", "s"))
    assert not caplog.records
