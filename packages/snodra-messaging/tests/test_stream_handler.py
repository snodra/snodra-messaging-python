"""A streaming request this instance serves: its handler's items as parts numbered from 1 under the sender's credit, the
one ``ReplyEnd``, the ``Cancel`` and the permit held until the end.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Streaming", "Handling a request").
"""

import asyncio
import json
import time
from collections.abc import AsyncIterator

import pytest
from fakehub import FakeHub, FakeStream, cancel, flow, forward
from helpers import CancelOrder, GetOrder, ListOrders, Placed, acknowledged, library_tasks, new_app, next_reply
from snodra import ErrorCode, HubError
from snodra._hubpb import hub_pb2

_BESIDE_ENDS = frozenset({"heartbeat", "status", "permits"})


def _totals(*parts: hub_pb2.ReplyPart) -> list[int]:
    return [json.loads(p.payload)["total"] for p in parts]


async def _next_end(s: FakeStream, within: float = 5.0) -> hub_pb2.ReplyEnd:
    """The next ``ReplyEnd`` on s, skipping heartbeats, statuses and ``Permits`` (which a batching timer writes at any
    moment); any other frame fails."""
    deadline = time.monotonic() + within
    while True:
        m = await s.recv(within=max(0.0, deadline - time.monotonic()))
        kind = m.WhichOneof("message")
        if kind == "end":
            return m.end
        if kind not in _BESIDE_ENDS:
            raise AssertionError(f"expected an end, got {kind}: {m}")


def _stream(
    id: str = "s#1",
    *,
    type: str = "orders.ListOrders",
    payload: bytes = b'{"orderId":"o-1"}',
    credit: int = 1,
) -> hub_pb2.ServerMessage:
    """A streaming request as the hub forwards it: the sender's id with ``#1``, its payload and its credit."""
    return forward(
        hub_pb2.Request(
            id=id,
            kind=hub_pb2.QUERY,
            type=type,
            payload=payload,
            mode=hub_pb2.STREAMING,
            initial_credit=credit,
        )
    )


async def test_stream_parts_are_numbered_under_the_credit(hub: FakeHub) -> None:
    produced: list[int] = []
    app = new_app(hub)

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        for n in range(1, 6):
            produced.append(n)
            yield Placed(q.order_id, n)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_stream(credit=2))
    one, two = await s.recv_part(), await s.recv_part()
    assert [(p.request_id, p.seq) for p in (one, two)] == [("s#1", 1), ("s#1", 2)]
    assert _totals(one, two) == [1, 2]
    await s.nothing(0.2, "part")  # the credit is spent: nothing more until a Flow
    assert produced == [1, 2, 3]  # the item the handler holds while it waits, and no part beyond it
    await s.send(flow("s#1", 2))
    three, four = await s.recv_part(), await s.recv_part()
    assert [p.seq for p in (three, four)] == [3, 4]
    await s.send(flow("s#1", 1))
    five = await s.recv_part()
    end = await s.recv_end()  # an exactly spent credit: the end needs no further credit
    assert (five.seq, _totals(five)) == (5, [5])
    assert (end.request_id, end.parts, end.HasField("error")) == ("s#1", 5, False)
    assert produced == [1, 2, 3, 4, 5]


async def test_a_stream_stops_on_cancel(hub: FakeHub) -> None:
    stopped = asyncio.Event()
    release = asyncio.Event()
    app = new_app(hub, max_concurrent=1)

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        try:
            yield Placed(q.order_id, 1)
            await release.wait()  # until the Cancel lands in it
            yield Placed(q.order_id, 2)
        finally:
            stopped.set()

    await app.start()
    s = await acknowledged(hub)
    before = library_tasks()
    await s.send(_stream(credit=2))
    assert _totals(await s.recv_part()) == [1]
    await s.send(cancel("s#1", "conformance"))
    await asyncio.wait_for(stopped.wait(), 1.0)  # the handler's task is cancelled
    # No part, no End: the hub has forgotten the request. Its permit goes back, once, and no task is left behind.
    await s.nothing(0.5, "part", "end")
    assert (await s.recv_permits()).count == 1
    assert library_tasks() == before


async def test_no_credit_within_the_bound_ends_with_timeout(hub: FakeHub) -> None:
    app = new_app(hub, stream_credit_timeout=0.3)

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        for n in range(1, 3):
            yield Placed(q.order_id, n)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_stream(credit=1))
    assert _totals(await s.recv_part()) == [1]
    end = await s.recv_end(within=3.0)
    assert (end.request_id, end.parts) == ("s#1", 1)
    assert end.HasField("error")
    assert (end.error.code, end.error.message) == (hub_pb2.TIMEOUT, "no credit within 300 ms")
    await s.nothing(0.2, "part", "end")  # the handler is over: its second item never came, and it ends once


async def test_a_stream_handlers_failure_ends_the_stream(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        yield Placed(q.order_id, 1)
        raise RuntimeError("out of stock")

    await app.start()
    s = await acknowledged(hub)
    await s.send(_stream(credit=2))
    assert _totals(await s.recv_part()) == [1]
    end = await s.recv_end()
    assert (end.request_id, end.parts) == ("s#1", 1)
    assert end.HasField("error")
    assert (end.error.code, end.error.message) == (hub_pb2.HANDLER_ERROR, "RuntimeError: out of stock")
    await s.nothing(0.2, "part", "end")  # nothing after the end
    assert (await s.recv_permits()).count == 1


async def test_a_stream_holds_its_permit_until_it_ends(hub: FakeHub) -> None:
    release = asyncio.Event()
    app = new_app(hub, max_concurrent=1)

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        yield Placed(q.order_id, 1)
        await release.wait()
        yield Placed(q.order_id, 2)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 9)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_stream(credit=2))
    assert _totals(await s.recv_part()) == [1]
    await s.send(
        forward(hub_pb2.Request(id="q-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-2"}'))
    )
    await s.nothing(0.3, "reply")  # the stream holds the instance's only handler slot
    release.set()
    assert _totals(await s.recv_part()) == [2]
    assert (await s.recv_end()).parts == 2
    assert json.loads((await next_reply(s)).payload) == {"orderId": "o-2", "total": 9}


async def test_the_streaming_cells_of_the_mode_table(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.stream_handler
    async def list_orders(q: ListOrders) -> AsyncIterator[Placed]:
        yield Placed(q.order_id, 1)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 2)

    @app.command_handler
    async def cancel_order(cmd: CancelOrder) -> None:
        return None

    await app.start()
    s = await acknowledged(hub)
    # A unary query to a streaming handler: one Reply, INVALID_REQUEST, and the text names App.stream.
    await s.send(
        forward(hub_pb2.Request(id="u-1", kind=hub_pb2.QUERY, type="orders.ListOrders", payload=b'{"orderId":"o-1"}'))
    )
    reply = await next_reply(s)
    assert reply.WhichOneof("result") == "error"
    assert (reply.error.code, reply.error.message) == (
        hub_pb2.INVALID_REQUEST,
        "orders.ListOrders is a streaming query: send it with App.stream (mode STREAMING)",
    )
    # A unary command to a streaming handler: NO_HANDLER.
    await s.send(forward(hub_pb2.Request(id="u-2", kind=hub_pb2.COMMAND, type="orders.ListOrders", payload=b"{}")))
    reply = await next_reply(s)
    assert reply.WhichOneof("result") == "error"
    assert (reply.error.code, reply.error.message) == (hub_pb2.NO_HANDLER, "no handler for type orders.ListOrders")
    # Streaming to a query handler: one End, INVALID_REQUEST, no part.
    await s.send(_stream("s-1#1", type="orders.GetOrder"))
    end = await _next_end(s)
    assert (end.request_id, end.parts, end.HasField("error")) == ("s-1#1", 0, True)
    assert end.error.code == hub_pb2.INVALID_REQUEST
    assert end.error.message == (
        "handler test_the_streaming_cells_of_the_mode_table.get_order for orders.GetOrder is not a streaming query "
        "handler (it answers mode UNARY)"
    )
    await s.nothing(0.2, "part", "end")
    # Streaming to a command handler, and to a type nobody serves: one End, NO_HANDLER, no part each.
    for id, type in (("s-2#1", "orders.CancelOrder"), ("s-3#1", "orders.Unknown")):
        await s.send(_stream(id, type=type, payload=b"{}"))
        end = await _next_end(s)
        assert (end.request_id, end.parts, end.error.code) == (id, 0, hub_pb2.NO_HANDLER)
        assert end.error.message == f"no handler for type {type}"
    await s.nothing(0.2, "part", "end")
    # Answered on the stream they arrived on, which is still up and served.
    await s.send(
        forward(hub_pb2.Request(id="q-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-3"}'))
    )
    assert json.loads((await next_reply(s)).payload) == {"orderId": "o-3", "total": 2}


async def test_a_point_query_of_an_own_streaming_type_is_invalid_in_process(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        raise AssertionError("a streaming type is never answered in-process")
        yield Placed(q.order_id, 1)  # pragma: no cover

    await app.start()
    s = await acknowledged(hub)
    with pytest.raises(HubError) as raised:
        await app.query(ListOrders("o-1"), Placed)
    assert (raised.value.code, raised.value.message) == (
        ErrorCode.INVALID_REQUEST,
        "orders.ListOrders is a streaming query: send it with App.stream (mode STREAMING)",
    )
    await s.nothing(0.2, "request", "permits")  # nothing went to the hub


async def test_a_stream_answers_on_its_arrival_stream(hub: FakeHub, hub2: FakeHub) -> None:
    app = new_app([hub, hub2])

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        yield Placed(q.order_id, 1)
        yield Placed(q.order_id, 2)

    await app.start()
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    await s2.send(_stream(credit=2))
    one, two = await s2.recv_part(), await s2.recv_part()
    assert [p.seq for p in (one, two)] == [1, 2]
    assert (await s2.recv_end()).parts == 2
    assert (await s2.recv_permits()).count == 1  # the permit goes back on the stream it arrived on
    await s1.nothing(0.3, "part", "end", "permits")  # and nothing at all on the other hub's stream
