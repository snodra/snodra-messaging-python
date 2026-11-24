"""What a request carries on from the request being handled: the correlation id, the caller's token and the trace
context; and the header names a call may not set.

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Request headers", "Correlation id",
"Trace context", "Forwarded identity").
"""

import asyncio
import uuid
from typing import Any

import pytest
from fakehub import FakeHub, FakeStream, forward
from helpers import INVALID_TRACEPARENTS as INVALID
from helpers import TP, CancelOrder, PlaceOrder, acknowledged, new_app, next_reply, rsa_key, sign
from snodra import App, ReservedHeader
from snodra._hubpb import hub_pb2
from snodra._propagation import outgoing_headers
from snodra._traceparent import trace_id

TOKEN = sign({"alg": "RS256", "kid": "k-1"}, {"sub": "u-1"}, rsa_key(0))
"""A signed token (not verified: these instances have no JWKS)."""


def _handled(id: str, headers: dict[str, str]) -> hub_pb2.ServerMessage:
    return forward(
        hub_pb2.Request(
            id=id,
            kind=hub_pb2.COMMAND,
            type="orders.PlaceOrder",
            payload=b'{"orderId":"o-1","lines":[]}',
            headers=headers,
        )
    )


async def _relaying(hub: FakeHub, **settings: Any) -> tuple[App, FakeStream]:
    """An app whose PlaceOrder handler sends a CancelOrder (another service's type) with an own header."""
    app = new_app(hub, **settings)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        await app.send(CancelOrder(cmd.order_id), headers={"x-tenant": "t-1"})

    await app.start()
    return app, await acknowledged(hub)


async def _nested(s: FakeStream, id: str, headers: dict[str, str]) -> dict[str, str]:
    """The headers of the request the handler sends while handling request id with headers."""
    await s.send(_handled(id, headers))
    nested = await s.recv_request()
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=nested.id, payload=b"{}")))
    r = await next_reply(s)
    assert (r.request_id, r.WhichOneof("result")) == (id, "payload"), r
    return dict(nested.headers)


async def test_a_nested_request_carries_the_handled_context(hub: FakeHub) -> None:
    _, s = await _relaying(hub)
    headers = await _nested(
        s, "r-1", {"correlationId": "c-1", "authorization": f"Bearer {TOKEN}", "traceparent": TP, "x-other": "o"}
    )
    assert headers["correlationId"] == "c-1"
    assert headers["authorization"] == f"Bearer {TOKEN}"
    assert trace_id(headers["traceparent"]) == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert headers["x-tenant"] == "t-1"
    assert "x-other" not in headers  # only the library's own headers are carried on
    # The scheme in any case; it goes on as "Bearer".
    headers = await _nested(s, "r-2", {"authorization": f"bearer {TOKEN}"})
    assert headers["authorization"] == f"Bearer {TOKEN}"


async def test_the_handled_id_stands_in_for_a_missing_correlation_id(hub: FakeHub) -> None:
    _, s = await _relaying(hub)
    assert await _nested(s, "r-9", {}) == {"correlationId": "r-9", "x-tenant": "t-1"}
    assert (await _nested(s, "r-10", {"correlationId": ""}))["correlationId"] == "r-10"


async def test_outside_any_handling_the_correlation_id_is_fresh(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    ids: list[str] = []
    for _ in range(2):
        call = asyncio.create_task(app.send(CancelOrder("o-1")))
        r = await s.recv_request()
        assert set(r.headers) == {"correlationId"}
        ids.append(r.headers["correlationId"])
        await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=r.id, payload=b"{}")))
        await call
    assert all(uuid.UUID(i).version == 4 for i in ids)
    assert ids[0] != ids[1]


async def test_an_invalid_traceparent_is_not_carried_on(hub: FakeHub) -> None:
    _, s = await _relaying(hub)
    for n, bad in enumerate(INVALID):
        assert "traceparent" not in await _nested(s, f"r-{n}", {"traceparent": bad}), bad


async def test_forward_jwt_off(hub: FakeHub) -> None:
    _, s = await _relaying(hub, forward_jwt=False)
    headers = await _nested(s, "r-1", {"correlationId": "c-1", "authorization": f"Bearer {TOKEN}", "traceparent": TP})
    assert headers == {"correlationId": "c-1", "traceparent": TP, "x-tenant": "t-1"}


RESERVED = [
    "authorization",
    "traceparent",
    "tracestate",
    "correlationId",
    "correlation-id",
    "AUTHORIZATION",
    "CORRELATIONID",
    "Correlation-Id",
    "TraceState",
]


async def test_reserved_headers_are_refused_before_anything_is_sent(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:  # an own type: answered in-process
        raise AssertionError("never called")

    await app.start()
    s = await acknowledged(hub)
    for h in RESERVED:
        with pytest.raises(ReservedHeader, match=h):
            await app.send(CancelOrder("o-1"), headers={h: "x"})
        with pytest.raises(ReservedHeader, match=h):
            await app.query(CancelOrder("o-1"), dict, headers={"x-tenant": "t-1", h: "x"})
        with pytest.raises(ReservedHeader, match=h):
            await app.send(PlaceOrder("o-1", []), headers={h: "x"})
    await s.nothing(0.2, "request")
    call = asyncio.create_task(app.send(CancelOrder("o-1"), headers={"x-tenant": "t-1", "correlationIdx": "c"}))
    r = await s.recv_request()
    assert (r.headers["x-tenant"], r.headers["correlationIdx"]) == ("t-1", "c")
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=r.id, payload=b"{}")))
    await call


def test_outgoing_headers_outside_any_handling() -> None:
    headers = outgoing_headers({"x-tenant": "t-1"}, forward_jwt=True)
    assert set(headers) == {"x-tenant", "correlationId"}
    assert outgoing_headers(None, forward_jwt=False).keys() == {"correlationId"}
    with pytest.raises(ReservedHeader):
        outgoing_headers({"Authorization": "Bearer x"}, forward_jwt=True)
    with pytest.raises(TypeError):
        outgoing_headers({"x-tenant": 1}, forward_jwt=True)  # type: ignore[dict-item]
