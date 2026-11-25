"""Every cell of the mode table of protocol.md ("A request in a mode its handler does not serve"), and the point
query of an own subscription type answered in-process.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Handling a request", "Requests a service answers
itself").
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest
from fakehub import FakeHub, forward
from helpers import (
    CancelOrder,
    GetOrder,
    ListOrders,
    Placed,
    WatchInitial,
    WatchOrders,
    WatchUpdate,
    acknowledged,
    new_app,
)
from snodra import App, ErrorCode, HubError, Updates
from snodra._hubpb import hub_pb2

UNARY = hub_pb2.UNARY
STREAMING = hub_pb2.STREAMING
SUBSCRIPTION = hub_pb2.SUBSCRIPTION

RUNS = "runs"
ADMITTED = "admitted"

PAYLOADS = {
    "orders.CancelOrder": b'{"orderId":"o-1"}',
    "orders.GetOrder": b'{"orderId":"o-1"}',
    "orders.ListOrders": b'{"orderId":"o-1"}',
    "orders.WatchOrders": b'{"topic":"t"}',
    "orders.NotServed": b"{}",
}


async def _command(m: CancelOrder) -> None:
    return None


async def _query(q: GetOrder) -> Placed:
    return Placed(q.order_id, 1)


async def _stream(q: ListOrders) -> AsyncIterator[Placed]:
    yield Placed(q.order_id, 1)


async def _watch(q: WatchOrders) -> WatchInitial:
    return WatchInitial(q.topic, 0)


def _register(app: App) -> Updates[WatchOrders, WatchUpdate]:
    """The four kinds of handler of the table: a command, a query, a streaming one and a subscription one."""
    app.command_handler(_command)
    app.query_handler(_query)
    app.stream_handler(_stream)
    register = app.subscription_handler(update=WatchUpdate)
    return register(_watch)


CELLS = [
    # The row, the column and the type it names, the mode and kind forwarded, and the cell's answer.
    ("UNARY COMMAND", "command handler", "orders.CancelOrder", UNARY, hub_pb2.COMMAND, RUNS),
    ("UNARY COMMAND", "query handler", "orders.GetOrder", UNARY, hub_pb2.COMMAND, hub_pb2.NO_HANDLER),
    ("UNARY COMMAND", "streaming handler", "orders.ListOrders", UNARY, hub_pb2.COMMAND, hub_pb2.NO_HANDLER),
    ("UNARY COMMAND", "subscription handler", "orders.WatchOrders", UNARY, hub_pb2.COMMAND, hub_pb2.NO_HANDLER),
    ("UNARY COMMAND", "type not served", "orders.NotServed", UNARY, hub_pb2.COMMAND, hub_pb2.NO_HANDLER),
    ("UNARY QUERY", "command handler", "orders.CancelOrder", UNARY, hub_pb2.QUERY, hub_pb2.NO_HANDLER),
    ("UNARY QUERY", "query handler", "orders.GetOrder", UNARY, hub_pb2.QUERY, RUNS),
    ("UNARY QUERY", "streaming handler", "orders.ListOrders", UNARY, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
    ("UNARY QUERY", "subscription handler #0", "orders.WatchOrders", UNARY, hub_pb2.QUERY, RUNS),
    (
        "UNARY QUERY",
        "subscription handler other id",
        "orders.WatchOrders",
        UNARY,
        hub_pb2.QUERY,
        hub_pb2.INVALID_REQUEST,
    ),
    ("UNARY QUERY", "type not served", "orders.NotServed", UNARY, hub_pb2.QUERY, hub_pb2.NO_HANDLER),
    ("STREAMING", "command handler", "orders.CancelOrder", STREAMING, hub_pb2.QUERY, hub_pb2.NO_HANDLER),
    ("STREAMING", "query handler", "orders.GetOrder", STREAMING, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
    ("STREAMING", "streaming handler", "orders.ListOrders", STREAMING, hub_pb2.QUERY, RUNS),
    ("STREAMING", "subscription handler", "orders.WatchOrders", STREAMING, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
    ("STREAMING", "type not served", "orders.NotServed", STREAMING, hub_pb2.QUERY, hub_pb2.NO_HANDLER),
    ("SUBSCRIPTION", "command handler", "orders.CancelOrder", SUBSCRIPTION, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
    ("SUBSCRIPTION", "query handler", "orders.GetOrder", SUBSCRIPTION, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
    ("SUBSCRIPTION", "streaming handler", "orders.ListOrders", SUBSCRIPTION, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
    ("SUBSCRIPTION", "subscription handler", "orders.WatchOrders", SUBSCRIPTION, hub_pb2.QUERY, ADMITTED),
    ("SUBSCRIPTION", "type not served", "orders.NotServed", SUBSCRIPTION, hub_pb2.QUERY, hub_pb2.INVALID_REQUEST),
]


@pytest.mark.parametrize(("row", "column", "type", "mode", "kind", "expected"), CELLS)
async def test_every_cell_of_the_mode_table(
    hub: FakeHub,
    row: str,
    column: str,
    type: str,
    mode: int,
    kind: int,
    expected: int | str,
) -> None:
    app = new_app(hub)
    updates = _register(app)
    await app.start()
    s = await acknowledged(hub)
    # "<id>#0" is the hub's initial request of a subscription; a "#0" elsewhere runs too, so the cell names the suffix.
    suffix = "#0" if column.endswith("#0") else ("#1" if mode is not UNARY else "")
    if column == "subscription handler other id":
        suffix = ""
    id = f"cell{suffix}"
    request = hub_pb2.Request(
        id=id,
        kind=kind,  # type: ignore[arg-type]
        type=type,
        payload=PAYLOADS[type],
        mode=mode,  # type: ignore[arg-type]
        initial_credit=1,
    )
    view = f"{row} / {column} ({type})"
    await s.send(forward(request))

    if mode == UNARY:
        reply = await s.recv_reply()
        assert reply.request_id == id, view
        if expected is RUNS:
            assert reply.WhichOneof("result") == "payload", view
            answer = json.loads(reply.payload)
            if type == "orders.CancelOrder":
                assert answer == {}, view
            elif type == "orders.WatchOrders":
                assert answer["topic"] == "t", view  # the initial result
            else:
                assert answer["orderId"] == "o-1", view
        else:
            assert reply.WhichOneof("result") == "error", view
            assert reply.error.code == expected, view
        await s.nothing(0.1, "reply", "part", "end")  # one terminal frame, and no part
        return

    if expected is ADMITTED:
        await s.nothing(0.1, "reply", "part", "end")  # nothing is answered for an admitted target
        assert app.subscriptions_served == 1, view
        assert updates.emit(lambda w: w.topic == "t", WatchUpdate("t", 1)) == 1
        assert (await s.recv_part()).request_id == id
        assert updates.complete(lambda w: True) == 1
        end = await s.recv_end()
        assert (end.request_id, end.parts, end.HasField("error")) == (id, 1, False), view
        return

    if expected is RUNS:  # the streaming handler: its one item, then the clean end
        part = await s.recv_part()
        assert (part.request_id, part.seq, json.loads(part.payload)) == (id, 1, {"orderId": "o-1", "total": 1}), view
    end = await s.recv_end()
    assert end.request_id == id, view
    assert end.parts == (1 if expected is RUNS else 0), view
    if expected is RUNS:
        assert not end.HasField("error"), view
    else:
        assert end.HasField("error"), view
        assert end.error.code == expected, view
    await s.nothing(0.1, "end", "reply", "part")  # one terminal frame, and no part

    # The subscription's texts name the handler and the mode to use, or the type it does not serve.
    if row == "SUBSCRIPTION" and column == "query handler":
        assert end.error.message == (
            "handler _query for orders.GetOrder is not a subscription query handler (it answers mode UNARY)"
        ), view
    if row == "SUBSCRIPTION" and column == "type not served":
        assert end.error.message == "no subscription handler for orders.NotServed on this instance", view


async def test_the_stream_stays_up_after_every_refusal(hub: FakeHub) -> None:
    app = new_app(hub)
    _register(app)
    await app.start()
    s = await acknowledged(hub)
    refused = [cell for cell in CELLS if isinstance(cell[-1], int)]
    for n, (_, _, type_, mode, kind, _) in enumerate(refused):
        id = f"refused-{n}" + ("#1" if mode != UNARY else "")
        await s.send(
            forward(
                hub_pb2.Request(
                    id=id,
                    kind=kind,
                    type=type_,
                    payload=PAYLOADS[type_],
                    mode=mode,
                    initial_credit=1,
                )
            )
        )
        while True:
            m = await s.recv()
            if m.WhichOneof("message") in ("reply", "end"):
                break
    # The stream is still served after every wrong mode.
    await s.send(
        forward(hub_pb2.Request(id="ping-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-9"}'))
    )
    reply = await s.recv_reply()
    assert (reply.request_id, json.loads(reply.payload)) == ("ping-1", {"orderId": "o-9", "total": 1})


async def test_a_point_query_of_an_own_subscription_type_is_invalid_in_process(hub: FakeHub) -> None:
    app = new_app(hub)
    _register(app)
    await app.start()
    s = await acknowledged(hub)
    with pytest.raises(HubError) as raised:
        await app.query(WatchOrders("t"), WatchInitial)
    assert (raised.value.code, raised.value.message) == (
        ErrorCode.INVALID_REQUEST,
        "orders.WatchOrders is a subscription query: send it with App.subscribe (mode SUBSCRIPTION)",
    )
    await s.nothing(0.2, "request", "permits")  # nothing went to the hub
