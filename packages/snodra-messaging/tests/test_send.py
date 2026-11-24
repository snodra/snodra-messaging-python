"""Sending a command or a query: the request, its answer, timeouts, never sent and outcome unknown, the hubs tried
in turn after ``NO_HANDLER``, retries and the caller's cancellation.

See https://snodra.damoreira.com/library-authors/protocol/ ("Requests", "Several hubs").
"""

import asyncio
import json
import time
from collections.abc import AsyncIterator
from dataclasses import replace

import grpc
import pytest
from fakehub import FakeHub, FakeStream, ack, forward, hub_error
from helpers import (
    SCALED,
    CancelOrder,
    GetOrder,
    Placed,
    PlaceOrder,
    acknowledged,
    assert_no_library_tasks,
    eventually,
    new_app,
    next_reply,
    start_app,
    within,
)
from snodra import App, ErrorCode, HubError, NeverSent, OutcomeUnknown, RetryPolicy, SnodraError
from snodra._connection import HubConnection, OutboundRequest
from snodra._hubpb import hub_pb2
from snodra._send import call_deadline, request_timing, retry_budget

PLACE = PlaceOrder("o-1", ["sku-1"])

PATIENT = replace(SCALED, handshake_timeout=5.0)
"""A stream waits long for its ``RegisterAck``."""


def _reply(id: str, payload: bytes = b"{}") -> hub_pb2.ServerMessage:
    return hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=id, payload=payload))


def _reply_error(id: str, code: ErrorCode, message: str, details: bytes = b"") -> hub_pb2.ServerMessage:
    error = hub_pb2.Error(request_id=id, code=int(code), message=message, details=details)  # type: ignore[arg-type]
    return hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=id, error=error))


def _pending(app: App) -> dict[str, OutboundRequest]:
    return app._sender.pending


def _connection(app: App, index: int = 0) -> HubConnection:
    return app._connections()[index]


def _ack(app: App) -> hub_pb2.RegisterAck:
    stream = _connection(app)._stream
    return hub_pb2.RegisterAck() if stream is None or stream.ack is None else stream.ack


def _buffered_on_current_stream(app: App, index: int = 0) -> bool:
    current = _connection(app, index)._stream
    return current is not None and any(r.stream is current and not r.written for r in _pending(app).values())


async def _close_held(s: FakeStream, conn: HubConnection) -> None:
    """Closes s while conn's writer is held, waits until the library saw the end, then lets the writer go."""
    stream = conn._stream
    assert stream is not None
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await eventually(stream.ended.is_set)
    conn._block_writer(False)


async def _requests_until_end(s: FakeStream) -> list[hub_pb2.Request]:
    """The requests left to read on a stream that ended."""
    requests: list[hub_pb2.Request] = []
    while True:
        try:
            m = await s.recv(within=2.0)
        except AssertionError as e:
            if "stream ended" not in str(e):
                raise
            return requests
        if m.WhichOneof("message") == "request":
            requests.append(m.request)


@pytest.fixture
async def hub3() -> AsyncIterator[FakeHub]:
    h = await FakeHub.start()
    try:
        yield h
    finally:
        await h.stop()


# The timing rules, without a hub.


def test_request_timing() -> None:
    a = hub_pb2.RegisterAck(max_timeout_ms=2000, default_timeout_ms=5000, type_timeouts_ms={"orders.GetOrder": 1500})
    assert request_timing(a, "orders.PlaceOrder", None) == (0, 3.0)
    assert request_timing(a, "orders.GetOrder", None) == (0, 2.5)
    assert request_timing(a, "orders.PlaceOrder", 1.2) == (1200, 2.2)
    assert request_timing(a, "orders.GetOrder", 1.8) == (1800, 2.5)
    assert request_timing(a, "orders.PlaceOrder", 0.0004)[0] == 1  # never 0 when chosen
    assert request_timing(hub_pb2.RegisterAck(), "x.Y", None)[1] == 61.0  # no maximum: 60 s
    assert request_timing(None, "x.Y", 2.0) == (2000, 3.0)
    assert request_timing(a, "orders.PlaceOrder", 1e12)[0] == 2**32 - 1  # uint32 on the wire


def test_retry_budget() -> None:
    a = hub_pb2.RegisterAck(default_timeout_ms=5000, type_timeouts_ms={"orders.GetOrder": 1500})
    assert (
        retry_budget(a, "x.Y", 3.0),
        retry_budget(a, "orders.GetOrder", None),
        retry_budget(a, "x.Y", None),
        retry_budget(None, "x.Y", None),
    ) == (3.0, 1.5, 5.0, 5.0)
    assert retry_budget(hub_pb2.RegisterAck(), "x.Y", None) == 5.0


def test_call_deadline() -> None:
    assert (call_deadline(None), call_deadline(1.5)) == (5.0, 1.5)


# One hub.


async def test_send_writes_the_request_and_decodes_the_reply(hub: FakeHub) -> None:
    app = await start_app(hub, priorities={"orders.PlaceOrder": 7})
    s = await acknowledged(hub)
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key="o-1"))
    r = await s.recv_request()
    assert (r.kind, r.type, r.routing_key, r.timeout_ms, r.mode, r.priority) == (
        hub_pb2.COMMAND,
        "orders.PlaceOrder",
        "o-1",
        0,
        hub_pb2.UNARY,
        7,
    )
    assert r.id
    assert json.loads(r.payload) == {"orderId": "o-1", "lines": ["sku-1"]}
    await s.send(_reply(r.id, b'{"orderId":"o-1","total":42}'))
    assert await call == Placed("o-1", 42)

    query = asyncio.create_task(app.query(GetOrder("o-2"), Placed, priority=3, headers={"x-tenant": "t-1"}))
    r = await s.recv_request()
    assert (r.kind, r.type, r.routing_key, r.priority, dict(r.headers)) == (
        hub_pb2.QUERY,
        "orders.GetOrder",
        "",
        3,
        {"x-tenant": "t-1"},
    )
    await s.send(_reply(r.id, b'{"orderId":"o-2","total":7}'))
    assert await query == Placed("o-2", 7)

    no_result = asyncio.create_task(app.send(CancelOrder("o-3")))  # no result type: None
    r = await s.recv_request()
    assert r.priority == 0
    await s.send(_reply(r.id, b"{}"))
    assert await no_result is None  # type: ignore[func-returns-value]
    assert not _pending(app)


async def test_a_call_refuses_bad_options_before_anything_is_sent(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    for options in (
        {"timeout": float("nan")},
        {"timeout": float("inf")},
        {"timeout": True},
        {"priority": 2**31},
        {"priority": True},
        {"retry": True},
    ):
        with pytest.raises((ValueError, TypeError)):
            await app.send(PLACE, **options)
    with pytest.raises(TypeError, match="message_type"):
        await app.send(Placed("o-1", 1))
    await s.nothing(0.1, "request")


async def test_every_attempt_has_a_fresh_id(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    calls = [asyncio.create_task(app.send(PlaceOrder(f"o-{i}", []))) for i in range(10)]
    first = [await s.recv_request() for _ in range(10)]
    assert len(_pending(app)) == 10  # ten pending at once
    for r in first:
        await s.send(_reply(r.id))
    await asyncio.gather(*calls)
    calls = [asyncio.create_task(app.send(PlaceOrder(f"o-{i}", []))) for i in range(5)]
    rest = [await s.recv_request() for _ in range(5)]
    for r in rest:
        await s.send(_reply(r.id))
    await asyncio.gather(*calls)
    ids = [r.id for r in first + rest]
    assert all(ids)
    assert len(set(ids)) == 15
    assert not _pending(app)


async def test_the_local_guard_waits_the_hubs_bound_plus_one_second(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", max_ms=200))
    await app.ready(timeout=1.0)
    started = time.monotonic()
    call = asyncio.create_task(app.send(PLACE))
    r = await s.recv_request()
    assert r.timeout_ms == 0
    with pytest.raises(HubError) as raised:
        await call
    within(time.monotonic() - started, 1.05, 1.4)
    assert (raised.value.code, str(raised.value)) == (
        ErrorCode.TIMEOUT,
        "Timed out waiting for reply to 'orders.PlaceOrder'",
    )
    assert not _pending(app)
    await s.send(_reply(r.id))  # a late reply is dropped
    call = asyncio.create_task(app.send(PLACE))  # the stream is fine
    r = await s.recv_request()
    await s.send(_reply(r.id))
    await call


async def test_no_hub_waits_until_the_deadline_then_never_sent(hub: FakeHub) -> None:
    app = await start_app(hub, _timings=PATIENT)
    s = await hub.next_stream()
    await s.recv_register()
    started = time.monotonic()
    with pytest.raises(NeverSent, match=r"^no hub connected within 300 ms$"):
        await app.send(PLACE, timeout=0.3)
    within(time.monotonic() - started, 0.28, 0.45)
    await s.nothing(0.05, "request")
    assert not _pending(app)
    # Woken by the ack, not by the deadline.
    call = asyncio.create_task(app.send(PLACE, timeout=1.0))
    await asyncio.sleep(0.1)  # the call waits for a hub meanwhile
    acked = time.monotonic()
    await s.ack(ack("hub-1"))
    r = await s.recv_request()
    assert time.monotonic() - acked < 0.5  # woken by the change, far before the deadline
    assert r.timeout_ms == 1000
    await s.send(_reply(r.id))
    await call


async def test_a_reconnected_hub_wakes_a_waiting_call(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s = await hub.next_stream()
    await s.recv_register()
    await eventually(lambda: app.hub_ids() == [])
    call = asyncio.create_task(app.send(PLACE, timeout=2.0))
    await asyncio.sleep(0.1)  # the call waits for a hub meanwhile
    acked = time.monotonic()
    await s.ack(ack("hub-1"))
    r = await s.recv_request()
    assert time.monotonic() - acked < 0.5  # woken by the change, far before the deadline
    await s.send(_reply(r.id))
    await call


async def test_a_buffered_request_is_never_sent_when_the_stream_breaks(hub: FakeHub) -> None:
    app = await start_app(hub, _timings=PATIENT)
    s = await acknowledged(hub)
    conn = _connection(app)
    conn._block_writer(True)
    started = time.monotonic()
    call = asyncio.create_task(app.send(PLACE, timeout=0.4))
    await eventually(lambda: _buffered_on_current_stream(app))
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s2 = await hub.next_stream()  # the writer is still held: no Register, no ack
    with pytest.raises(NeverSent, match=r"^no hub took the request within 400 ms$"):
        await call
    within(time.monotonic() - started, 0.38, 0.6)
    assert not _pending(app)
    conn._block_writer(False)
    assert not await _requests_until_end(s)
    await s2.recv_register()
    await s2.nothing(0.1, "request")


async def test_an_unwritten_request_moves_to_the_next_stream_with_its_id_and_full_timeout(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    conn = _connection(app)
    conn._block_writer(True)
    call = asyncio.create_task(app.send(PLACE, timeout=2.0))
    await eventually(lambda: _buffered_on_current_stream(app))
    (id_,) = _pending(app)
    await _close_held(s, conn)
    s2 = await acknowledged(hub)
    r = await s2.recv_request()
    assert (r.id, r.timeout_ms) == (id_, 2000)
    await s2.send(_reply(r.id))
    await call
    assert not await _requests_until_end(s)


async def test_the_ninth_lost_buffer_is_never_sent(hub: FakeHub) -> None:
    app = await start_app(hub)
    conn = _connection(app)
    s = await hub.next_stream()
    await s.recv_register()
    conn._block_writer(True)
    await s.ack(ack("hub-1"))
    call = asyncio.create_task(app.send(PLACE, timeout=10.0))
    ids: set[str] = set()
    for lost in range(1, 10):
        await eventually(lambda: _buffered_on_current_stream(app), within=2.0)
        ids |= set(_pending(app))
        await _close_held(s, conn)
        if lost < 9:
            assert not call.done()
            s = await hub.next_stream()
            await s.recv_register()
            conn._block_writer(True)
            await s.ack(ack("hub-1"))
    with pytest.raises(NeverSent, match=r"^'orders.PlaceOrder' not written: 9 hub streams lost before it was$"):
        await call
    assert len(ids) == 1  # one id, moved eight times
    assert not _pending(app)


async def test_a_written_request_is_outcome_unknown_when_the_stream_breaks(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.send(PLACE, timeout=2.0))
    await s.recv_request()
    await s.close(grpc.StatusCode.UNAVAILABLE, "hub restart")
    with pytest.raises(OutcomeUnknown, match=r"^hub stream closed: UNAVAILABLE \(hub restart\)$"):
        await call
    assert not _pending(app)
    s2 = await acknowledged(hub)
    await s2.nothing(0.2, "request")  # never moved


async def test_every_error_code_reaches_the_caller(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    expected = {
        1: (ErrorCode.NO_HANDLER, "No handler registered for 'orders.PlaceOrder'"),
        2: (ErrorCode.TIMEOUT, "Timed out waiting for reply to 'orders.PlaceOrder'"),
        3: (ErrorCode.HANDLER_DISCONNECTED, "Handler for 'orders.PlaceOrder' disconnected"),
        4: (ErrorCode.HANDLER_ERROR, "hub said 4"),
        5: (ErrorCode.UNAUTHENTICATED, "hub said 5"),
        6: (ErrorCode.INVALID_REQUEST, "hub said 6"),
        7: (ErrorCode.PERMISSION_DENIED, "hub said 7"),
        8: (
            ErrorCode.RESOURCE_EXHAUSTED,
            "Handler for 'orders.PlaceOrder' is busy: its hub queue or session buffer is full",
        ),
        9: (ErrorCode.CANCELLED, "hub said 9"),
        0: (ErrorCode.HANDLER_ERROR, "hub said 0"),
        42: (ErrorCode.HANDLER_ERROR, "hub said 42"),
    }
    for wire, (code, message) in expected.items():
        call = asyncio.create_task(app.send(PLACE))
        r = await s.recv_request()
        await s.send(hub_error(r.id, wire, f"hub said {wire}"))
        with pytest.raises(HubError) as raised:
            await call
        assert (raised.value.code, raised.value.message, raised.value.details) == (code, message, b""), wire
    for code, message, details in (
        (ErrorCode.HANDLER_ERROR, "insufficient stock", b'{"sku":"a"}'),
        (ErrorCode.UNAUTHENTICATED, "token expired", b""),
        (ErrorCode.PERMISSION_DENIED, "not your order", b""),
        (ErrorCode.INVALID_REQUEST, "unknown sku", b""),
    ):
        call = asyncio.create_task(app.send(PLACE))
        r = await s.recv_request()
        await s.send(_reply_error(r.id, code, message, details))
        with pytest.raises(HubError) as raised:
            await call
        assert (raised.value.code, raised.value.message, raised.value.details) == (code, message, details)
    assert not _pending(app)


async def test_a_handlers_no_handler_answer_is_not_tried_elsewhere(hub: FakeHub, hub2: FakeHub) -> None:
    app = await start_app([hub, hub2])
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    call = asyncio.create_task(app.send(PLACE))
    r = await s1.recv_request()
    await s1.send(_reply_error(r.id, ErrorCode.NO_HANDLER, "no handler for type orders.PlaceOrder"))
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.NO_HANDLER
    await s2.nothing(0.1, "request")


# Several hubs: the walk after NO_HANDLER, one deadline, each hub once.


async def _two_hubs(hub: FakeHub, hub2: FakeHub) -> tuple[App, FakeStream, FakeStream]:
    app = await start_app([hub, hub2])
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    return app, s1, s2


async def test_after_no_handler_the_other_hub_gets_the_same_request_with_the_full_timeout(
    hub: FakeHub, hub2: FakeHub
) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    call = asyncio.create_task(app.send(PLACE, Placed, timeout=1.0))
    r1 = await s1.recv_request()
    await asyncio.sleep(0.2)  # the first hub answers late: the next hop still carries the full timeout
    await s1.send(hub_error(r1.id, ErrorCode.NO_HANDLER, "no handler"))
    r2 = await s2.recv_request()
    assert (r2.id, r2.timeout_ms) == (r1.id, 1000)
    assert r1.timeout_ms == 1000
    await s2.send(_reply(r2.id, b'{"orderId":"o-1","total":3}'))
    assert await call == Placed("o-1", 3)
    await s1.nothing(0.1, "request")
    assert not _pending(app)


async def test_when_every_live_hub_declined_the_last_no_handler_comes_at_once(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    started = time.monotonic()
    call = asyncio.create_task(app.send(PLACE, timeout=0.3))
    r = await s1.recv_request()
    await s1.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    r = await s2.recv_request()
    await asyncio.sleep(max(0.0, started + 0.5 - time.monotonic()))  # past the call's deadline
    answered = time.monotonic()
    await s2.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.NO_HANDLER
    assert time.monotonic() - answered < 0.5  # at once: the call's deadline is long past
    await s1.nothing(0.1, "request")
    assert not _pending(app)


async def test_no_new_hop_after_the_deadline(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    started = time.monotonic()
    call = asyncio.create_task(app.send(PLACE, timeout=0.3))
    r = await s1.recv_request()
    await asyncio.sleep(max(0.0, started + 0.5 - time.monotonic()))
    await s1.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    with pytest.raises(NeverSent, match=r"^no hub took the request within 300 ms$"):
        await call
    await s2.nothing(0.1, "request")
    assert not _pending(app)


async def test_a_hub_that_did_not_take_the_request_is_not_tried_again(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    first = _connection(app, 0)
    first._block_writer(True)
    call = asyncio.create_task(app.send(PLACE, timeout=2.0))
    await eventually(lambda: _buffered_on_current_stream(app, 0))
    (id_,) = _pending(app)
    await _close_held(s1, first)
    r = await s2.recv_request()  # lost in the first hub's buffer: the second hub, same id
    assert (r.id, r.timeout_ms) == (id_, 2000)
    s1 = await acknowledged(hub, "hub-1")  # the first hub is back
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    await s2.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.NO_HANDLER
    await s1.nothing(0.2, "request")  # never back to the first hub


async def test_the_next_unkeyed_hub_is_taken_from_the_untried_ones_by_the_shared_turn(
    hub: FakeHub, hub2: FakeHub, hub3: FakeHub
) -> None:
    app = await start_app([hub, hub2, hub3])
    a = await acknowledged(hub, "hub-a")
    b = await acknowledged(hub2, "hub-b")
    c = await acknowledged(hub3, "hub-c")
    await eventually(lambda: app.hub_ids() == ["hub-a", "hub-b", "hub-c"])
    call = asyncio.create_task(app.send(PLACE))
    r = await a.recv_request()  # turn 0 of three
    await a.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    r = await c.recv_request()  # the untried [b, c], turn 1: c
    await c.send(_reply(r.id))
    await call
    await b.nothing(0.1, "request")


async def test_an_answer_counts_only_from_the_stream_the_request_was_written_to(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    call = asyncio.create_task(app.send(PLACE, Placed))
    r = await s1.recv_request()
    await s2.send(_reply(r.id, b'{"orderId":"o-1","total":2}'))  # the same id on another stream: dropped
    await s1.send(_reply(r.id, b'{"orderId":"o-1","total":1}'))
    assert await call == Placed("o-1", 1)


async def test_a_half_closed_stream_is_never_picked(hub: FakeHub) -> None:
    app = await start_app(hub)
    await acknowledged(hub)
    await eventually(lambda: app._pool.pick() is not None)
    _connection(app).stop_reconnecting()
    assert app._pool.pick() is not None  # no reconnect any more, but the stream still takes requests
    _connection(app).half_close()
    assert app._pool.pick() is None


# Retries.


async def test_retry_after_no_handler_with_a_fresh_id_and_the_remaining_budget(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.send(PLACE, Placed, timeout=5.0, retry=RetryPolicy(3, 0.02)))
    r1 = await s.recv_request()
    assert 4900 < r1.timeout_ms <= 5000
    await s.send(hub_error(r1.id, ErrorCode.NO_HANDLER, "no handler"))
    r2 = await s.recv_request()
    assert r2.id
    assert r2.id != r1.id
    assert 0 < r2.timeout_ms < r1.timeout_ms
    await s.send(_reply(r2.id, b'{"orderId":"o-1","total":1}'))
    assert await call == Placed("o-1", 1)
    assert not _pending(app)


async def test_retry_without_a_caller_timeout_never_sends_zero(hub: FakeHub) -> None:
    app = await start_app(hub, default_retry=RetryPolicy(3, 0.02))
    s = await acknowledged(hub)  # default_timeout_ms 5000
    call = asyncio.create_task(app.send(PLACE))
    r1 = await s.recv_request()
    assert 4900 < r1.timeout_ms <= 5000
    await s.send(hub_error(r1.id, ErrorCode.NO_HANDLER, "no handler"))
    r2 = await s.recv_request()
    assert 0 < r2.timeout_ms < 5000
    await s.send(_reply(r2.id))
    await call
    # The type's override is the budget.
    await s.send(hub_pb2.ServerMessage(register_ack=ack("hub-1", type_timeouts_ms={"orders.PlaceOrder": 1500})))
    await eventually(lambda: bool(_ack(app).type_timeouts_ms))
    call = asyncio.create_task(app.send(PLACE))
    r = await s.recv_request()
    assert 1400 < r.timeout_ms <= 1500
    await s.send(_reply(r.id))
    await call
    # retry=False sends once, and 0.
    call = asyncio.create_task(app.send(PLACE, retry=False))
    r = await s.recv_request()
    assert r.timeout_ms == 0
    await s.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    with pytest.raises(HubError):
        await call
    await s.nothing(0.2, "request")


async def test_a_command_is_never_retried_after_an_unknown_outcome(hub: FakeHub) -> None:
    app = await start_app(hub, default_retry=RetryPolicy(3, 0.02))
    s = await acknowledged(hub)
    for code in (ErrorCode.TIMEOUT, ErrorCode.HANDLER_DISCONNECTED):
        call = asyncio.create_task(app.send(PLACE, timeout=2.0))
        r = await s.recv_request()
        await s.send(hub_error(r.id, code, "lost"))
        with pytest.raises(HubError) as raised:
            await call
        assert raised.value.code == code
        await s.nothing(0.2, "request")
    call = asyncio.create_task(app.send(PLACE, timeout=2.0))
    await s.recv_request()
    await s.close(grpc.StatusCode.UNAVAILABLE, "broken after the write")
    with pytest.raises(OutcomeUnknown):
        await call
    s = await acknowledged(hub)
    await s.nothing(0.2, "request")
    # A query is retried after the same answers.
    for code in (ErrorCode.TIMEOUT, ErrorCode.HANDLER_DISCONNECTED):
        query = asyncio.create_task(app.query(GetOrder("o-1"), Placed, timeout=2.0))
        r = await s.recv_request()
        await s.send(hub_error(r.id, code, "lost"))
        r2 = await s.recv_request()
        assert r2.id != r.id
        await s.send(_reply(r2.id, b'{"orderId":"o-1","total":1}'))
        assert await query == Placed("o-1", 1)


async def test_no_retry_with_less_than_the_minimum_left(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    started = time.monotonic()
    call = asyncio.create_task(app.send(PLACE, timeout=0.3, retry=RetryPolicy(3, initial_backoff=0.1)))
    r = await s.recv_request()
    await asyncio.sleep(max(0.0, started + 0.1 - time.monotonic()))  # the first answer at 0.1 s
    await s.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.NO_HANDLER
    await s.nothing(0.3, "request")
    # Given up at once, without the wait: budget 1 s, the first answer at 0.5 s, a wait of 0.24 to 0.36 s would leave
    # less than max(0.3, 0.25) s. Waiting first would end the call 0.74 s in at the earliest.
    started = time.monotonic()
    call = asyncio.create_task(app.send(PLACE, timeout=1.0, retry=RetryPolicy(3, initial_backoff=0.3)))
    r = await s.recv_request()
    await asyncio.sleep(max(0.0, started + 0.5 - time.monotonic()))
    await s.send(hub_error(r.id, ErrorCode.NO_HANDLER, "no handler"))
    with pytest.raises(HubError):
        await call
    within(time.monotonic() - started, 0.5, 0.65)
    await s.nothing(0.3, "request")


async def test_a_zero_timeout_lets_the_hub_decide_over_the_default(hub: FakeHub) -> None:
    app = await start_app(hub, default_timeout=5.0)
    s = await acknowledged(hub)
    for timeout in (0, -1.0):
        call = asyncio.create_task(app.send(PLACE, timeout=timeout))
        r = await s.recv_request()
        assert r.timeout_ms == 0
        await s.send(_reply(r.id))
        await call


async def test_a_zero_timeout_still_waits_the_default_for_a_hub(hub: FakeHub) -> None:
    app = await start_app(hub, default_timeout=0.3, _timings=PATIENT)
    s = await hub.next_stream()
    await s.recv_register()
    started = time.monotonic()
    with pytest.raises(NeverSent, match=r"^no hub connected within 300 ms$"):
        await app.send(PLACE, timeout=0)
    within(time.monotonic() - started, 0.28, 0.6)


async def test_the_per_call_timeout_wins_over_the_default(hub: FakeHub) -> None:
    app = await start_app(hub, default_timeout=5.0)
    s = await acknowledged(hub)
    for timeout, sent in ((1.2, 1200), (None, 5000)):
        call = asyncio.create_task(app.send(PLACE, timeout=timeout))
        r = await s.recv_request()
        assert r.timeout_ms == sent
        await s.send(_reply(r.id))
        await call


# The outbound buffer and shutdown.


async def test_a_full_outbound_buffer_makes_send_wait_until_the_stream_ends(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    conn = _connection(app)
    conn._block_writer(True)
    calls = [asyncio.create_task(app.send(PlaceOrder(f"o-{i}", []), timeout=5.0)) for i in range(257)]
    await eventually(lambda: len(_pending(app)) == 256)
    stream = conn._stream
    assert stream is not None
    assert stream.buffered == 256
    assert not calls[-1].done()  # the 257th waits for room
    await _close_held(s, conn)
    s2 = await acknowledged(hub)  # the stream's end woke the 257th: it goes out with the 256 moved ones
    requests = [await s2.recv_request() for _ in range(257)]
    assert {json.loads(r.payload)["orderId"] for r in requests} == {f"o-{i}" for i in range(257)}
    for r in requests:
        await s2.send(_reply(r.id))
    assert await asyncio.gather(*calls) == [None] * 257
    assert not _pending(app)


async def test_shutdown_wakes_a_send_waiting_for_room(hub: FakeHub) -> None:
    app = await start_app(hub)
    await acknowledged(hub)
    conn = _connection(app)
    conn._block_writer(True)
    try:
        calls = [asyncio.create_task(app.send(PlaceOrder(f"o-{i}", []), timeout=5.0)) for i in range(257)]
        await eventually(lambda: len(_pending(app)) == 256)
        started = time.monotonic()
        await app.shutdown()
        outcomes = await asyncio.gather(*calls, return_exceptions=True)
        assert time.monotonic() - started < 1.5
        assert all(isinstance(o, NeverSent) for o in outcomes), {repr(o) for o in outcomes}
        assert str(outcomes[-1]) == "'orders.PlaceOrder' not sent: the app is shutting down"
        assert not _pending(app)
    finally:
        conn._block_writer(False)


async def test_a_handler_being_drained_can_still_send(hub: FakeHub) -> None:
    # The empty Register takes the instance out of the routing; the handlers still running finish their work, sends to
    # other services included, until the streams are closed after the drain.
    app = new_app(hub)
    started = asyncio.Event()
    release = asyncio.Event()

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        started.set()
        await release.wait()
        order = await app.query(GetOrder(cmd.order_id), Placed, timeout=2.0)
        return Placed(cmd.order_id, order.total + 1)

    await app.start()
    s = await acknowledged(hub)
    request = hub_pb2.Request(
        id="r-1", kind=hub_pb2.COMMAND, type="orders.PlaceOrder", payload=b'{"orderId":"o-1","lines":[]}'
    )
    await s.send(forward(request))
    await asyncio.wait_for(started.wait(), 1.0)
    shutdown = asyncio.create_task(app.shutdown())
    assert not (await s.recv_register()).command_types  # the empty Register first
    release.set()
    nested = await s.recv_request()  # sent during the drain
    assert nested.type == "orders.GetOrder"
    await s.send(_reply(nested.id, b'{"orderId":"o-1","total":41}'))
    answer = await next_reply(s)
    assert answer.request_id == "r-1"
    assert json.loads(answer.payload) == {"orderId": "o-1", "total": 42}
    await shutdown
    assert not _pending(app)


async def test_a_send_after_the_drain_is_never_sent(hub: FakeHub, monkeypatch: pytest.MonkeyPatch) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    drain = app._dispatcher.drain
    after: list[asyncio.Task[None]] = []

    async def drain_then_send(timeout: float) -> None:  # noqa: ASYNC109 (the drain's bound)
        await drain(timeout)
        after.append(asyncio.create_task(app.send(PLACE, timeout=1.0)))  # runs once the drain is over

    monkeypatch.setattr(app._dispatcher, "drain", drain_then_send)
    started = time.monotonic()
    await app.shutdown()
    with pytest.raises(NeverSent, match=r"^'orders.PlaceOrder' not sent: the app is shutting down$"):
        await after[0]
    assert time.monotonic() - started < 0.6  # at once, not at the call's deadline
    assert not await _requests_until_end(s)
    with pytest.raises(NeverSent, match="shutting down"):
        await app.send(PLACE, timeout=1.0)
    assert not _pending(app)


async def test_a_written_request_is_outcome_unknown_at_shutdown(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.send(PLACE, timeout=5.0))
    await s.recv_request()
    await app.shutdown()
    with pytest.raises(OutcomeUnknown, match=r"^hub connection stopped$"):
        await call
    assert not _pending(app)


# The caller's cancellation.


async def test_a_cancelled_caller_gets_cancelled_error_and_nothing_leaks(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    call = asyncio.create_task(app.send(PLACE))
    r = await s.recv_request()
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert not _pending(app)
    await s.send(_reply(r.id))  # the late reply is dropped quietly

    started = time.monotonic()
    with pytest.raises(TimeoutError) as timed_out:
        async with asyncio.timeout(0.1):
            await app.send(PLACE)
    assert not isinstance(timed_out.value, SnodraError)
    within(time.monotonic() - started, 0.09, 0.3)
    r = await s.recv_request()
    assert not _pending(app)
    await s.send(_reply(r.id))

    async def sibling() -> None:
        await s.recv_request()
        raise RuntimeError("sibling failed")

    sent: list[asyncio.Task[None]] = []

    async def group_of_two() -> None:
        async with asyncio.TaskGroup() as tg:
            sent.append(tg.create_task(app.send(PLACE)))
            tg.create_task(sibling())

    with pytest.raises(ExceptionGroup) as group:
        await group_of_two()
    assert [type(e) for e in group.value.exceptions] == [RuntimeError]
    assert sent[0].cancelled()
    assert not _pending(app)
    await app.shutdown()
    assert_no_library_tasks()


async def test_a_call_cancelled_while_buffered_is_never_written(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await acknowledged(hub)
    conn = _connection(app)
    conn._block_writer(True)
    call = asyncio.create_task(app.send(PLACE))
    await eventually(lambda: _buffered_on_current_stream(app))
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    stream = conn._stream
    assert stream is not None
    assert stream.buffered == 0
    conn._block_writer(False)
    await s.nothing(0.2, "request")
    assert not _pending(app)
