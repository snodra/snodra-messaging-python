"""Shared query types and scatter-gather: the ``Register``'s lists, the gather's parts until the hub's end, its counts,
its failures, its retries and its caller's cancellation.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Shared query types", "Scatter-gather").
"""

import asyncio
import json
from dataclasses import replace

import grpc
import pytest
from fakehub import FakeHub, ack, forward, hub_error
from helpers import (
    SCALED,
    GetOrder,
    GetStock,
    Placed,
    Stock,
    acknowledged,
    eventually,
    new_app,
    next_reply,
    start_app,
)
from snodra import App, DecodeError, ErrorCode, Gathered, HubError, OutcomeUnknown, Part, RetryPolicy
from snodra._connection import OutboundRequest
from snodra._hubpb import hub_pb2

STOCK = GetStock("sku-1")


def _part(
    id: str,
    seq: int,
    service: str,
    instance: str,
    payload: bytes = b"",
    *,
    error: tuple[int, str] | None = None,
    details: bytes = b"",
) -> hub_pb2.ServerMessage:
    part = hub_pb2.ReplyPart(
        request_id=id, seq=seq, responder_service=service, responder_instance=instance, payload=payload
    )
    if error is not None:
        part.error.CopyFrom(hub_pb2.Error(request_id=id, code=error[0], message=error[1], details=details))  # type: ignore[arg-type]
    return hub_pb2.ServerMessage(part=part)


def _end(id: str, parts: int, failed: int = 0, timed_out: int = 0) -> hub_pb2.ServerMessage:
    return hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=id, parts=parts, failed=failed, timed_out=timed_out))


def _pending(app: App) -> dict[str, OutboundRequest]:
    return app._sender.pending


# Shared query types.


async def test_a_shared_query_is_registered_as_shared(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.query_handler(shared=True)
    async def get_stock(q: GetStock) -> Stock:
        return Stock(len(q.sku))

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 1)

    await app.start()
    s = await hub.next_stream()
    r = await s.recv_register()
    assert list(r.shared_query_types) == ["orders.GetStock"]
    assert list(r.query_types) == ["orders.GetOrder"]
    assert list(r.command_types) == []
    await s.ack(ack("hub-1"))
    # Served like any query: a point query, and a gather's target <id>#<n>.
    for id in ("p-1", "g-1#0"):
        request = hub_pb2.Request(id=id, kind=hub_pb2.QUERY, type="orders.GetStock", payload=b'{"sku":"abc"}')
        await s.send(forward(request))
        reply = await next_reply(s)
        assert (reply.request_id, json.loads(reply.payload)) == (id, {"count": 3})


# Scatter-gather.


async def test_gather_collects_the_parts_until_the_end(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    assert (r.mode, r.kind, r.type, r.max_results, r.routing_key, r.timeout_ms) == (
        hub_pb2.SCATTER_GATHER,
        hub_pb2.QUERY,
        "orders.GetStock",
        0,
        "",
        0,
    )
    assert json.loads(r.payload) == {"sku": "sku-1"}
    assert "correlationId" in r.headers
    await s.send(_part(r.id, 0, "B", "b2", b'{"count":2}'))
    await s.send(_part(r.id, 1, "A", "a1", error=(hub_pb2.HANDLER_ERROR, "out of stock"), details=b'{"sku":"sku-1"}'))
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=r.id, payload=b'{"count":9}')))  # no answer
    await s.send(_part(r.id, 2, "B", "b1", b'{"count":1}'))
    await s.send(_end(r.id, 3, 1, 0))
    g = await call
    assert isinstance(g, Gathered)
    assert [(p.service, p.instance) for p in g.parts] == [("B", "b2"), ("A", "a1"), ("B", "b1")]  # arrival order
    assert g.parts[0] == Part("B", "b2", Stock(2), None)
    assert g.parts[2] == Part("B", "b1", Stock(1), None)
    failed = g.parts[1]
    assert failed.value is None
    assert isinstance(failed.error, HubError)
    assert (failed.error.code, failed.error.message, failed.error.details) == (
        ErrorCode.HANDLER_ERROR,
        "out of stock",
        b'{"sku":"sku-1"}',
    )
    assert (g.targets, g.failed, g.timed_out) == (3, 1, 0)
    assert not _pending(app)


async def test_a_targets_failure_keeps_its_code_and_message(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", error=(hub_pb2.HANDLER_DISCONNECTED, "the instance left")))
    await s.send(_part(r.id, 1, "A", "a2", error=(99, "a code of a later hub"), details=b"[1]"))
    await s.send(_part(r.id, 2, "A", "a3", error=(hub_pb2.RESOURCE_EXHAUSTED, "queue full"), details=b"[2]"))
    await s.send(_end(r.id, 3, 3))
    g = await call
    errors = [p.error for p in g.parts]
    assert all(isinstance(e, HubError) for e in errors)
    assert [(e.code, e.message, e.details) for e in errors if isinstance(e, HubError)] == [
        (ErrorCode.HANDLER_DISCONNECTED, "the instance left", b""),
        (ErrorCode.HANDLER_ERROR, "a code of a later hub", b"[1]"),  # an unknown code reads as HANDLER_ERROR
        (ErrorCode.RESOURCE_EXHAUSTED, "queue full", b""),  # details only with HANDLER_ERROR
    ]
    assert (g.targets, g.failed, g.timed_out) == (3, 3, 0)


async def test_an_undecodable_part_is_a_failed_part(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":"many"}'))
    await s.send(_part(r.id, 1, "B", "b1", b'{"count":4}'))
    await s.send(_end(r.id, 2))
    g = await call
    first = g.parts[0]
    assert (first.service, first.instance, first.value) == ("A", "a1", None)
    assert isinstance(first.error, DecodeError)
    assert "A/a1" in str(first.error)
    assert g.parts[1] == Part("B", "b1", Stock(4), None)
    assert (g.targets, g.failed, g.timed_out) == (2, 1, 0)  # failed counts the parts with an error


async def test_max_results_and_the_deadline_count_apart(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    # max_results: the hub cancels the targets still working and counts them nowhere.
    call = asyncio.create_task(app.gather(STOCK, Stock, max_results=2, timeout=1.5, priority=3))
    r = await s.recv_request()
    assert (r.max_results, r.timeout_ms, r.priority) == (2, 1500, 3)
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":1}'))
    await s.send(_part(r.id, 1, "A", "a2", b'{"count":2}'))
    await s.send(_end(r.id, 2))
    g = await call
    assert (len(g.parts), g.targets, g.failed, g.timed_out) == (2, 2, 0, 0)
    # The deadline: the targets that had not answered are timed out, and count as targets.
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":1}'))
    await s.send(_end(r.id, 1, 0, 2))
    g = await call
    assert (len(g.parts), g.targets, g.failed, g.timed_out) == (1, 3, 0, 2)


async def test_a_gather_refuses_bad_options_before_anything_is_sent(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    for bad in (-1, 2**32, True, 1.0, "2"):
        with pytest.raises((ValueError, TypeError), match="max_results"):
            await app.gather(STOCK, Stock, max_results=bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="priority"):
        await app.gather(STOCK, Stock, priority=2**31)
    await s.nothing(0.1, "request")


async def test_late_frames_are_ignored(hub: FakeHub) -> None:
    app = new_app(hub, _timings=replace(SCALED, guard_margin=0.1))
    await app.start()
    s = await acknowledged(hub)
    # After the end: a part and a second end.
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":1}'))
    await s.send(_end(r.id, 1))
    await s.send(_part(r.id, 1, "A", "a2", b'{"count":2}'))
    await s.send(_end(r.id, 2))
    g = await call
    assert (g.parts, g.targets) == ([Part("A", "a1", Stock(1), None)], 1)
    # After the local guard (the hub's bound plus the margin): TIMEOUT, then its part is dropped.
    with pytest.raises(HubError) as timed_out:
        await app.gather(STOCK, Stock, timeout=0.05)
    assert timed_out.value.code == ErrorCode.TIMEOUT
    late = await s.recv_request()
    assert late.timeout_ms == 50
    assert not _pending(app)
    # After the caller's cancellation.
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    for id in (late.id, r.id):
        await s.send(_part(id, 0, "A", "a1", b'{"count":1}'))
        await s.send(_end(id, 1))
    assert not _pending(app)
    # The stream is up and the next gather gets only its own parts.
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_end(r.id, 0, 0, 1))
    g = await call
    assert (g.parts, g.targets, g.timed_out) == ([], 1, 1)


async def test_gather_without_targets_is_no_handler(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(hub_error(r.id, hub_pb2.NO_HANDLER, "no live instance has registered orders.GetStock"))
    with pytest.raises(HubError) as e:
        await call
    assert (e.value.code, e.value.message) == (ErrorCode.NO_HANDLER, "No handler registered for 'orders.GetStock'")
    # A command type, or an end that carries an error: the gather fails as a whole.
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(hub_error(r.id, hub_pb2.INVALID_REQUEST, "invalid request: orders.GetStock is a command"))
    with pytest.raises(HubError) as e:
        await call
    assert e.value.code == ErrorCode.INVALID_REQUEST
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":1}'))
    error = hub_pb2.Error(request_id=r.id, code=hub_pb2.CANCELLED, message="ended by the hub")
    await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=r.id, parts=1, error=error)))
    with pytest.raises(HubError) as e:
        await call
    assert (e.value.code, e.value.message) == (ErrorCode.CANCELLED, "ended by the hub")
    assert not _pending(app)


async def test_a_retried_gather_holds_only_its_own_parts(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    policy = RetryPolicy(max_attempts=2, initial_backoff=0.01)
    call = asyncio.create_task(app.gather(STOCK, Stock, timeout=2.0, retry=policy))
    first = await s.recv_request()
    assert first.timeout_ms == 2000
    await s.send(hub_error(first.id, hub_pb2.NO_HANDLER, "no live instance"))
    second = await s.recv_request()
    assert second.id != first.id  # a new gather
    assert (second.mode, second.type) == (hub_pb2.SCATTER_GATHER, "orders.GetStock")
    assert 0 < second.timeout_ms < 2000  # what is left of the budget
    await s.send(_part(first.id, 0, "A", "a1", b'{"count":7}'))  # the first gather's: nobody waits for it
    await s.send(_part(second.id, 0, "A", "a1", b'{"count":1}'))
    await s.send(_part(second.id, 1, "B", "b1", b'{"count":2}'))
    await s.send(_end(second.id, 2))
    g = await call
    assert [p.value for p in g.parts] == [Stock(1), Stock(2)]
    assert g.targets == 2


async def test_gather_on_a_lost_stream_is_outcome_unknown(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock, timeout=2.0))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":1}'))
    await s.close(grpc.StatusCode.UNAVAILABLE, "hub restart")
    with pytest.raises(OutcomeUnknown, match=r"^hub stream closed: UNAVAILABLE \(hub restart\)$"):
        await call
    assert not _pending(app)
    s2 = await acknowledged(hub)
    await s2.nothing(0.2, "request")  # bound to the hub it was written to: never moved


async def test_a_cancelled_gather_leaves_nothing_pending(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()
    await s.send(_part(r.id, 0, "A", "a1", b'{"count":1}'))
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert not _pending(app)
    await s.nothing(0.2, "cancel")  # a gather has no Cancel: the hub ends it at its deadline
    await s.send(_part(r.id, 1, "A", "a2", b'{"count":2}'))
    await s.send(_end(r.id, 2))
    assert not _pending(app)

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.1):
            await app.gather(STOCK, Stock)
    await s.recv_request()
    assert not _pending(app)


async def test_a_gather_always_goes_through_the_hub(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.query_handler
    async def get_stock(q: GetStock) -> Stock:
        return Stock(0)

    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s.recv_request()  # this instance owns the type, and the hub still gets the gather
    assert r.mode == hub_pb2.SCATTER_GATHER
    await s.send(_end(r.id, 0))
    g = await call
    assert (g.parts, g.targets) == ([], 0)


async def test_a_hubs_no_handler_is_the_gathers_answer(hub: FakeHub, hub2: FakeHub) -> None:
    app = await start_app([hub, hub2])
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    call = asyncio.create_task(app.gather(STOCK, Stock))
    r = await s1.recv_request()
    await s1.send(hub_error(r.id, hub_pb2.NO_HANDLER, "no live target"))
    with pytest.raises(HubError) as e:
        await call
    assert e.value.code == ErrorCode.NO_HANDLER
    await s2.nothing(0.2, "request")  # unlike a unary request, the other hub never gets it
