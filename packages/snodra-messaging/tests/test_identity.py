"""The caller of a forwarded request: its token checked before the handler lookup, the guards (an anonymous caller
refused before decoding, the rule after it), and the JWKS behind them.

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Forwarded identity").
"""

import asyncio
import functools
import itertools
import json
import socket
import time
from dataclasses import dataclass, replace
from typing import Any

import pytest
from fakehub import FakeHub, ack, cancel, forward
from helpers import (
    ISSUER,
    SCALED,
    CancelOrder,
    GetOrder,
    Jwks,
    Placed,
    PlaceOrder,
    acknowledged,
    b64url,
    eventually,
    new_app,
    next_reply,
    rsa_key,
    sign,
)
from snodra import (
    App,
    Caller,
    ConfigurationError,
    Settings,
    authenticated,
    caller_matches,
    current_caller,
    message_type,
    role,
)
from snodra._hubpb import hub_pb2

ORDER = b'{"orderId":"o-1","lines":[]}'
QUERY = b'{"orderId":"o-1"}'


@message_type("orders.ShipOrder")
@dataclass
class ShipOrder:
    order_id: str


def _request(
    id: str,
    *,
    kind: int = hub_pb2.COMMAND,
    type: str = "orders.PlaceOrder",
    payload: bytes = ORDER,
    authorization: str | None = None,
) -> hub_pb2.ServerMessage:
    headers = {} if authorization is None else {"authorization": authorization}
    return forward(hub_pb2.Request(id=id, kind=kind, type=type, payload=payload, headers=headers))  # type: ignore[arg-type]


def _error(r: hub_pb2.Reply) -> tuple[int, str]:
    assert r.WhichOneof("result") == "error", r
    return r.error.code, r.error.message


def _segment(value: object) -> str:
    return b64url(json.dumps(value).encode())


SIGNED = sign({"alg": "RS256", "kid": "k-1"}, {"sub": "u-1", "roles": ["orders-user"]}, rsa_key(0))
"""A signed token no instance of these tests can verify without a JWKS."""


async def test_the_token_is_checked_before_the_handler_lookup(hub: FakeHub, jwks: Jwks) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        return Placed(cmd.order_id, 1)

    await app.start()
    s = await acknowledged(hub)
    bad = {
        "Bearer not-a-jwt": "unreadable token",
        f"Bearer {jwks.mint(exp=time.time() - 120)}": "token expired",
        f"Bearer {jwks.mint(kid='k-unknown')}": "unknown signing key",
        f"Bearer {jwks.mint(key=jwks.other)}": "invalid token",
        f"Bearer {jwks.mint(issuer='https://other.test')}": "token issuer not accepted",
        "Basic abc": "malformed authorization header",
    }
    n = 0
    for type_ in ("orders.PlaceOrder", "orders.Unknown"):
        for header, text in bad.items():
            n += 1
            await s.send(_request(f"r-{n}", type=type_, authorization=header))
            assert _error(await next_reply(s)) == (hub_pb2.UNAUTHENTICATED, text), (type_, header)
    for scheme in ("Bearer", "bearer"):
        await s.send(_request(f"ok-{scheme}", authorization=f"{scheme} {jwks.mint()}"))
        r = await next_reply(s)
        assert r.WhichOneof("result") == "payload", r
        assert json.loads(r.payload) == {"orderId": "o-1", "total": 1}


async def test_without_a_jwks_no_token_is_an_identity(hub: FakeHub) -> None:
    app = new_app(hub)
    seen: list[Caller | None] = []

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        seen.append(current_caller())
        await app.send(CancelOrder(cmd.order_id))  # another service's type: through the hub

    await app.start()
    s = await acknowledged(hub)
    refused = {
        "Basic abc": "malformed authorization header",
        " Bearer x": "malformed authorization header",
        "Bearer ": "malformed authorization header",
        "Bearer a.b.c.d.e": "unsupported token",
        "Bearer a.b": "unreadable token",
        f"Bearer not-base64-json.{_segment({})}.c2ln": "unreadable token",
        f"Bearer {_segment({'alg': 5})}.{_segment({})}.c2ln": "unreadable token",
        f"Bearer {_segment({'alg': 'none'})}.{_segment({'sub': 'u-1'})}.c2ln": "unsigned token",
        f"Bearer {_segment({'alg': 'NONE'})}.{_segment({'sub': 'u-1'})}.c2ln": "unsigned token",
        f"Bearer {_segment({'alg': 'RS256'})}.{_segment({'sub': 'u-1'})}.": "unsigned token",
    }
    for n, (header, text) in enumerate(refused.items()):
        await s.send(_request(f"r-{n}", authorization=header))
        assert _error(await next_reply(s)) == (hub_pb2.UNAUTHENTICATED, text), header
    assert seen == []

    cases: list[tuple[str, str | None]] = [("signed", f"Bearer {SIGNED}"), ("blank", "  "), ("none", None)]
    for id_, authorization in cases:
        await s.send(_request(id_, authorization=authorization))
        nested = await s.recv_request()
        if id_ == "signed":
            assert nested.headers["authorization"] == f"Bearer {SIGNED}"  # carried on, never read
        else:
            assert "authorization" not in nested.headers
        await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=nested.id, payload=b"{}")))
        r = await next_reply(s)
        assert r.request_id == id_
        assert r.WhichOneof("result") == "payload", r
    assert seen == [None, None, None]


async def test_a_guard_without_a_jwks_is_a_startup_error() -> None:
    app = App(Settings(service="orders-service"))

    with pytest.raises(ConfigurationError) as raised:

        @app.command_handler(guard=authenticated())
        async def place_order(cmd: PlaceOrder) -> None:
            return None

    assert "place_order" in str(raised.value)
    assert "SNODRA_JWKS_URL" in str(raised.value)

    with pytest.raises(ConfigurationError, match=r"get_order.*SNODRA_JWKS_URL"):

        @app.query_handler(guard=role("orders-user"))
        async def get_order(q: GetOrder) -> Placed:
            return Placed(q.order_id, 1)

    assert (app._registry.types("command"), app._registry.types("query")) == ([], [])

    @app.command_handler
    async def cancel_order(cmd: CancelOrder) -> None:
        return None

    assert app._registry.types("command") == ["orders.CancelOrder"]
    with pytest.raises(TypeError, match="guard"):
        app.command_handler(guard=lambda c: True)  # type: ignore[call-overload]


def test_a_caller_rule_is_a_plain_function() -> None:
    async def rule(c: Caller) -> bool:
        return True

    class AsyncRule:
        async def __call__(self, c: Caller) -> bool:
            return True

    async def rule_of(name: str, c: Caller) -> bool:
        return c.subject == name

    for r in (rule, AsyncRule(), functools.partial(rule_of, "u-1")):
        with pytest.raises(TypeError, match="async"):
            caller_matches(r)  # type: ignore[arg-type]


def test_a_caller_rule_that_returns_an_awaitable_refuses_the_caller() -> None:
    async def later(c: Caller) -> bool:
        return True

    def sneaky(c: Caller) -> Any:
        return later(c)  # a plain function handing back a coroutine

    guard = caller_matches(sneaky)
    with pytest.raises(TypeError, match="awaitable"):
        guard.allows(Caller("u-1", ("orders-user",), {}))


async def test_the_guard_order(hub: FakeHub, jwks: Jwks) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER)
    seen: list[Caller | None] = []

    @app.command_handler(guard=authenticated())
    async def place_order(cmd: PlaceOrder) -> Placed:
        seen.append(current_caller())
        return Placed(cmd.order_id, 1)

    @app.query_handler(guard=role("orders-user"))
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 2)

    @app.command_handler(guard=caller_matches(lambda c: c.subject != "conformance-refused"))
    async def cancel_order(cmd: CancelOrder) -> None:
        return None

    @app.command_handler(guard=caller_matches(lambda c: 1 / 0 > 0))
    async def ship_order(cmd: ShipOrder) -> None:
        return None

    await app.start()
    s = await acknowledged(hub)
    no_role = f"Bearer {jwks.mint(roles=())}"
    cases = [
        # Anonymous: refused before the payload is decoded.
        (
            _request("a", payload=b"not json"),
            (hub_pb2.UNAUTHENTICATED, "handler test_the_guard_order.place_order requires authentication"),
        ),
        (
            _request("b", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b"not json"),
            (hub_pb2.UNAUTHENTICATED, "handler test_the_guard_order.get_order requires authentication"),
        ),
        # Authenticated: the payload is decoded first, then the rule.
        (
            _request("c", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b"not json", authorization=no_role),
            (hub_pb2.INVALID_REQUEST, None),
        ),
        (
            _request("d", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=QUERY, authorization=no_role),
            (hub_pb2.PERMISSION_DENIED, "handler test_the_guard_order.get_order: access denied"),
        ),
        (
            _request(
                "e",
                type="orders.CancelOrder",
                payload=QUERY,
                authorization=f"Bearer {jwks.mint('conformance-refused')}",
            ),
            (hub_pb2.PERMISSION_DENIED, "handler test_the_guard_order.cancel_order: access denied"),
        ),
        (
            _request("f", type="orders.ShipOrder", payload=QUERY, authorization=f"Bearer {jwks.mint()}"),
            (hub_pb2.PERMISSION_DENIED, "handler test_the_guard_order.ship_order: access denied (ZeroDivisionError)"),
        ),
    ]
    for frame, (code, text) in cases:
        await s.send(frame)
        got_code, got_text = _error(await next_reply(s))
        assert got_code == code, frame
        if text is not None:
            assert got_text == text
    assert seen == []

    await s.send(_request("g", type="orders.CancelOrder", payload=QUERY, authorization=f"Bearer {jwks.mint()}"))
    assert (await next_reply(s)).WhichOneof("result") == "payload"
    await s.send(_request("h", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=QUERY, authorization=no_role[:0]))
    assert _error(await next_reply(s))[0] == hub_pb2.UNAUTHENTICATED  # a blank header: anonymous
    await s.send(_request("i", authorization=f"Bearer {jwks.mint()}"))
    assert (await next_reply(s)).WhichOneof("result") == "payload"
    [caller] = seen
    assert caller is not None
    assert caller == Caller("u-1", ("orders-user",), caller.claims)
    assert caller.has_role("orders-user")
    assert not caller.has_role("admin")
    assert caller.claims["iss"] == ISSUER


@pytest.mark.parametrize(
    ("claim", "claims"),
    [
        ("roles", {"roles": ["orders-user"]}),
        ("realm_access.roles", {"roles": [], "realm_access": {"roles": ["other", "orders-user"]}}),
        ("scope", {"roles": [], "scope": "read orders-user  write"}),
        ("SCOPE", {"roles": [], "scope": "orders-user"}),
        ("  ", {"roles": ["orders-user"]}),  # blank: roles
    ],
)
async def test_the_roles_claim(hub: FakeHub, jwks: Jwks, claim: str, claims: dict[str, Any]) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER, roles_claim=claim)
    seen: list[Caller | None] = []

    @app.query_handler(guard=role("orders-user"))
    async def get_order(q: GetOrder) -> Placed:
        seen.append(current_caller())
        return Placed(q.order_id, 2)

    await app.start()
    s = await acknowledged(hub)
    token = jwks.mint(roles=(), claims=claims)
    await s.send(
        _request("q", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=QUERY, authorization=f"Bearer {token}")
    )
    assert (await next_reply(s)).WhichOneof("result") == "payload"
    assert seen[0] is not None
    assert "orders-user" in seen[0].roles


async def test_a_roles_claim_of_another_shape_gives_no_roles(hub: FakeHub, jwks: Jwks) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER, roles_claim="realm_access.roles")
    seen: list[Caller | None] = []

    @app.command_handler(guard=authenticated())
    async def place_order(cmd: PlaceOrder) -> None:
        seen.append(current_caller())

    await app.start()
    s = await acknowledged(hub)
    shapes: list[dict[str, Any]] = [
        {"realm_access": "orders-user"},
        {"realm_access": {"roles": "orders-user"}},
        {"realm_access": {"roles": [1]}},
    ]
    for n, claims in enumerate(shapes):
        await s.send(_request(f"r-{n}", authorization=f"Bearer {jwks.mint(roles=(), claims=claims)}"))
        assert (await next_reply(s)).WhichOneof("result") == "payload"
    assert [c.roles if c else None for c in seen] == [(), (), ()]


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


async def test_an_unreachable_jwks_is_unauthenticated(hub: FakeHub, jwks: Jwks) -> None:
    for url in (f"http://127.0.0.1:{_closed_port()}/jwks", jwks.url):
        jwks.down = url == jwks.url  # a refused connection, then an endpoint answering 503
        app = new_app(hub, jwks_url=url, issuer=ISSUER)

        @app.command_handler(guard=authenticated())
        async def place_order(cmd: PlaceOrder) -> None:
            return None

        @app.query_handler
        async def get_order(q: GetOrder) -> Placed:
            return Placed(q.order_id, 2)

        await app.start()
        s = await acknowledged(hub)
        await s.send(_request("r-1", authorization=f"Bearer {jwks.mint()}"))
        assert _error(await next_reply(s)) == (hub_pb2.UNAUTHENTICATED, "signing keys unavailable")
        await s.send(_request("q-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=QUERY))
        assert (await next_reply(s)).WhichOneof("result") == "payload"  # a request without a token is served
        await app.shutdown()


async def test_an_unknown_kid_refreshes_the_jwks_at_most_once_per_minute(hub: FakeHub, jwks: Jwks) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER, _timings=replace(SCALED, jwks_min_refresh=0.5))

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        return None

    await app.start()
    s = await acknowledged(hub)

    async def ask(id_: str, kid: str) -> hub_pb2.Reply:
        await s.send(_request(id_, authorization=f"Bearer {jwks.mint(kid=kid)}"))
        return await next_reply(s)

    assert (await ask("a", "k-1")).WhichOneof("result") == "payload"
    loaded = time.monotonic()
    assert jwks.fetches == 1
    assert _error(await ask("b", "k-2")) == (hub_pb2.UNAUTHENTICATED, "unknown signing key")
    assert jwks.fetches == 1  # the first load was the refresh: not again within the minimum refresh

    async def until_fetched(n: int, id_: str) -> float:
        """Asks with k-2 until the JWKS was fetched n times; when the last fetch happened (at the latest)."""
        deadline = time.monotonic() + 3.0
        i = 0
        while jwks.fetches < n:
            assert time.monotonic() < deadline, f"the JWKS was not fetched {n} times"
            await asyncio.sleep(0.05)
            assert _error(await ask(f"{id_}-{i}", "k-2")) == (hub_pb2.UNAUTHENTICATED, "unknown signing key")
            i += 1
        return time.monotonic()

    refreshed = await until_fetched(2, "c")  # an unknown kid fetches again once the minimum refresh passed
    assert refreshed - loaded >= 0.5
    jwks.published.append(dict(jwks.published[0], kid="k-2"))  # the issuer publishes k-2
    assert _error(await ask("d", "k-2")) == (hub_pb2.UNAUTHENTICATED, "unknown signing key")
    assert jwks.fetches == 2  # not again within the minimum refresh

    answered: list[hub_pb2.Reply] = []

    async def until_answered() -> bool:
        r = await ask(f"e-{len(answered)}", "k-2")
        answered.append(r)
        return bool(r.WhichOneof("result") == "payload")

    deadline = time.monotonic() + 3.0
    while not await until_answered():
        assert time.monotonic() < deadline, "k-2 was never accepted"
        await asyncio.sleep(0.05)
    assert time.monotonic() - refreshed >= 0.5
    assert jwks.fetches == 3


async def test_a_slow_jwks_never_delays_heartbeats(hub: FakeHub, jwks: Jwks) -> None:
    jwks.delay = 1.0
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER)

    @app.command_handler(guard=authenticated())
    async def place_order(cmd: PlaceOrder) -> Placed:
        return Placed(cmd.order_id, 1)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 2)

    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", interval_ms=100))
    token = f"Bearer {jwks.mint()}"
    await s.send(_request("r-1", authorization=token))
    await s.send(_request("r-2", authorization=token))
    await eventually(lambda: jwks.fetches == 1)  # both wait for the one fetch
    await s.send(cancel("r-2", "the caller gave up"))  # its handler task is cancelled while it waits
    sent = time.monotonic()
    await s.send(_request("q-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=QUERY))

    beats: list[float] = []
    replies: dict[str, tuple[float, hub_pb2.Reply]] = {}
    deadline = time.monotonic() + 3.0
    while "r-1" not in replies:
        m = await s.recv(within=max(0.0, deadline - time.monotonic()))
        kind = m.WhichOneof("message")
        if kind == "heartbeat":
            beats.append(time.monotonic())
        elif kind == "reply":
            replies[m.reply.request_id] = (time.monotonic(), m.reply)
    assert replies["q-1"][0] - sent < 0.2  # served at once, while the fetch goes on
    assert replies["q-1"][0] < replies["r-1"][0]
    assert replies["r-1"][1].WhichOneof("result") == "payload"  # the cancelled waiter did not fail the fetch
    assert "r-2" not in replies
    assert len(beats) >= 8
    gaps = [b - a for a, b in itertools.pairwise(beats)]
    assert max(gaps) <= 0.15, gaps
    await s.nothing(0.15, "reply")  # nothing for the cancelled request
    assert jwks.fetches == 1
