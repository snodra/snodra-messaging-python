"""A streaming query this instance sends: its credit and refills, its parts and end, the Cancels of a caller that stops,
the parts the hub lost, the local guard and the retries before the first part.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Streaming", "Never sent or outcome unknown").
"""

import asyncio
import json
import time

import grpc
import pytest
from fakehub import FakeHub, FakeStream, ack
from helpers import ListOrders, Placed, acknowledged, eventually, library_tasks, new_app
from snodra import App, ErrorCode, HubError, LostParts, OutcomeUnknown, RetryPolicy
from snodra._hubpb import hub_pb2


def _part(id: str, seq: int, total: int) -> hub_pb2.ServerMessage:
    """One part of a stream, as the hub relays it: the sender's id and the seq the handler gave it."""
    return hub_pb2.ServerMessage(
        part=hub_pb2.ReplyPart(request_id=id, seq=seq, payload=json.dumps({"orderId": "o-1", "total": total}).encode())
    )


def _end(id: str, parts: int, *, code: int | None = None, message: str = "") -> hub_pb2.ServerMessage:
    """The end of a stream, without an error unless code names one."""
    end = hub_pb2.ReplyEnd(request_id=id, parts=parts)
    if code is not None:
        error = hub_pb2.Error(request_id=id, code=code, message=message)  # type: ignore[arg-type]
        end.error.CopyFrom(error)
    return hub_pb2.ServerMessage(end=end)


async def _collect(app: App, *, read: int | None = None, **options: object) -> list[Placed]:
    """The items of a stream of orders.ListOrders, read inside its block; stops after ``read`` items."""
    received: list[Placed] = []
    async with app.stream(ListOrders("o-1"), Placed, **options) as items:  # type: ignore[arg-type]
        async for item in items:
            received.append(item)
            if read is not None and len(received) >= read:
                break
    return received


_BESIDE_CANCELS = frozenset({"heartbeat", "status", "permits", "flow"})


async def _next_cancel(s: FakeStream, within: float = 5.0) -> hub_pb2.Cancel:
    """The next ``Cancel`` on s, skipping heartbeats, statuses, ``Permits`` and the ``Flow``s a stream grants as it is
    read; any other frame fails."""
    deadline = time.monotonic() + within
    while True:
        m = await s.recv(within=max(0.0, deadline - time.monotonic()))
        kind = m.WhichOneof("message")
        if kind == "cancel":
            return m.cancel
        if kind not in _BESIDE_CANCELS:
            raise AssertionError(f"expected a cancel, got {kind}: {m}")


async def test_the_sender_sends_the_credit_and_refills(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(_collect(app, initial_credit=4, max_results=3))
    r = await s.recv_request()
    assert (r.mode, r.kind, r.type, r.routing_key, r.timeout_ms) == (
        hub_pb2.STREAMING,
        hub_pb2.QUERY,
        "orders.ListOrders",
        "",
        0,
    )
    assert (r.initial_credit, r.max_results) == (4, 3)
    assert json.loads(r.payload) == {"orderId": "o-1"}
    assert "correlationId" in r.headers
    for n in range(1, 13):  # read under the credit: a Flow of max(1, 4 // 2) after every 2 parts consumed
        await s.send(_part(r.id, n, n))
        if n % 2 == 0:
            flow = await s.recv_flow()
            assert (flow.request_id, flow.credits) == (r.id, 2)
    await s.send(_end(r.id, 12))
    assert await call == [Placed("o-1", n) for n in range(1, 13)]
    await s.nothing(0.2, "cancel")  # the stream ended: the caller sends no Cancel
    assert not app._sender.pending

    call = asyncio.create_task(_collect(app))  # no credit asked for: the protocol's 64
    r = await s.recv_request()
    assert (r.initial_credit, r.max_results) == (64, 0)
    await s.send(_end(r.id, 0))  # a stream of no item: open, then over at once
    assert await call == []
    await s.nothing(0.2, "cancel")


async def test_leaving_the_stream_block_early_cancels_and_leaks_nothing(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    before = library_tasks()

    # A break after 2 items: a Cancel at once, and nothing of the stream left here or in a task.
    call = asyncio.create_task(_collect(app, read=2))
    r = await s.recv_request()
    for n in range(1, 4):
        await s.send(_part(r.id, n, n))
    assert await call == [Placed("o-1", 1), Placed("o-1", 2)]
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector cancelled")
    assert not app._sender.pending
    assert library_tasks() == before  # no task of the stream's own is left behind

    # An exception in the body: the Cancel names the failure, and the exception reaches the caller.
    async def failing() -> None:
        async with app.stream(ListOrders("o-1"), Placed, initial_credit=2) as items:
            async for _ in items:
                raise RuntimeError("the body broke")

    broken = asyncio.create_task(failing())
    r = await s.recv_request()
    await s.send(_part(r.id, 1, 1))
    with pytest.raises(RuntimeError, match="the body broke"):
        await broken
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector failed: RuntimeError: the body broke")
    assert not app._sender.pending

    # The caller's task cancelled while it waits for the next item: a Cancel at once.
    seen: list[Placed] = []

    async def waiting() -> None:
        async with app.stream(ListOrders("o-1"), Placed, initial_credit=2) as items:
            async for item in items:
                seen.append(item)

    waiting_call = asyncio.create_task(waiting())
    r = await s.recv_request()
    await s.send(_part(r.id, 1, 1))
    await eventually(lambda: seen == [Placed("o-1", 1)])
    waiting_call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_call
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector cancelled")
    assert not app._sender.pending

    # Never entering the block sends no Request; iterating outside it, or entering it twice, is a RuntimeError.
    never = app.stream(ListOrders("o-1"), Placed)
    await s.nothing(0.2, "request")
    with pytest.raises(RuntimeError, match="async with"):
        async for _ in never:  # pragma: no cover
            pass
    entering = asyncio.create_task(never.__aenter__())
    hand = await s.recv_request()  # entered by hand: its one request went out, and it waits for the first frame
    await s.send(_part(hand.id, 1, 1))
    assert await entering is never
    with pytest.raises(RuntimeError, match="async with"):
        await never.__aenter__()
    await never.__aexit__(None, None, None)
    assert not app._sender.pending  # the one request it opened is gone, and its Cancel went out
    assert (await _next_cancel(s)).reason == "collector cancelled"
    with pytest.raises(RuntimeError, match="async with"):
        async for _ in never:  # pragma: no cover
            pass


async def _lost_after(app: App, *, delay: float = 0.0) -> tuple[list[Placed], str]:
    """The items a stream handed the caller before ``LostParts``, with ``delay`` seconds spent on each item (a consumer
    slower than the stream's reader, which keeps queueing frames meanwhile)."""
    received: list[Placed] = []

    async def collect() -> None:
        async with app.stream(ListOrders("o-1"), Placed) as items:
            async for item in items:
                received.append(item)
                if delay:
                    await asyncio.sleep(delay)

    with pytest.raises(LostParts) as raised:
        await collect()
    return received, str(raised.value)


async def test_a_gap_or_a_short_end_is_lost_parts(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)

    # A gap in the seq the target gave: the parts before it stay, then a Cancel with the loss and LostParts; a consumer
    # slower than the reader loses none of them.
    for delay in (0.0, 0.05):
        call = asyncio.create_task(_lost_after(app, delay=delay))
        r = await s.recv_request()
        for seq in (1, 2, 4):
            await s.send(_part(r.id, seq, seq))
        received, failure = await call
        assert received == [Placed("o-1", 1), Placed("o-1", 2)]
        assert failure == "stream orders.ListOrders lost 1 part(s)"
        cancel = await _next_cancel(s)
        assert (cancel.request_id, cancel.reason) == (r.id, failure)
        assert not app._sender.pending

    # An end that counts more parts than arrived: the stream has ended, so nothing is cancelled, and the parts read
    # before the end stay too.
    for seqs, parts, lost, delay in (((1, 2), 3, 1, 0.0), ((1, 2, 3), 5, 2, 0.05)):
        call = asyncio.create_task(_lost_after(app, delay=delay))
        r = await s.recv_request()
        for seq in seqs:
            await s.send(_part(r.id, seq, seq))
        await s.send(_end(r.id, parts))
        received, failure = await call
        assert received == [Placed("o-1", n) for n in seqs]
        assert failure == f"stream orders.ListOrders lost {lost} part(s)"
        await s.nothing(0.2, "cancel")
        assert not app._sender.pending


async def test_the_ends_error_reaches_the_caller(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(_collect(app))
    r = await s.recv_request()
    await s.send(_part(r.id, 1, 1))
    await s.send(_end(r.id, 1, code=hub_pb2.TIMEOUT, message="idle timeout"))
    with pytest.raises(HubError) as raised:
        await call
    assert (raised.value.code, raised.value.message) == (
        ErrorCode.TIMEOUT,
        "Timed out waiting for reply to 'orders.ListOrders'",
    )
    await s.nothing(0.2, "cancel")


async def test_a_stream_on_a_lost_hub_stream_fails(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(_collect(app))
    r = await s.recv_request()
    await s.send(_part(r.id, 1, 1))
    await s.close(grpc.StatusCode.UNAVAILABLE, "hub restart")
    with pytest.raises(OutcomeUnknown, match=r"^hub stream closed: UNAVAILABLE \(hub restart\)$"):
        await call
    assert not app._sender.pending
    s2 = await acknowledged(hub)
    await s2.nothing(0.2, "request")  # bound to the hub it was written to: never moved


async def test_the_streams_local_guard(hub: FakeHub) -> None:
    app = new_app(hub)  # the guard margin is the shipped 1 s
    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", max_ms=200))
    # No part at all: twice the hub's 200 ms bound (its queue wait, then its idle timeout) plus the 1 s margin.
    started = time.monotonic()
    call = asyncio.create_task(_collect(app))
    r = await s.recv_request()
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.TIMEOUT
    assert time.monotonic() - started >= 1.35
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "no part within the local guard")
    # Between two parts: the hub's 200 ms bound plus the 1 s margin.
    started = time.monotonic()
    call = asyncio.create_task(_collect(app))
    r = await s.recv_request()
    await s.send(_part(r.id, 1, 1))
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.TIMEOUT
    assert time.monotonic() - started >= 1.15
    assert (await _next_cancel(s)).reason == "no part within the local guard"


async def test_a_stream_is_retried_only_before_its_first_part(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    policy = RetryPolicy(max_attempts=2, initial_backoff=0.01)
    call = asyncio.create_task(_collect(app, timeout=2.0, retry=policy))
    first = await s.recv_request()
    assert first.timeout_ms == 2000
    await s.send(_end(first.id, 0, code=hub_pb2.NO_HANDLER, message="no live instance"))
    second = await s.recv_request()
    assert second.id != first.id  # a new request, with an id of its own
    assert second.timeout_ms == 2000  # the caller's idle timeout, whole, on every attempt
    await s.send(_part(second.id, 1, 1))
    await s.send(_end(second.id, 1))
    assert await call == [Placed("o-1", 1)]

    # Once a part arrived, an error end is the answer: no new attempt, no new request.
    call = asyncio.create_task(_collect(app, timeout=2.0, retry=policy))
    r = await s.recv_request()
    await s.send(_part(r.id, 1, 1))
    await s.send(_end(r.id, 1, code=hub_pb2.HANDLER_ERROR, message="out of stock"))
    with pytest.raises(HubError) as raised:
        await call
    assert (raised.value.code, raised.value.message) == (ErrorCode.HANDLER_ERROR, "out of stock")
    await s.nothing(0.3, "request")


async def test_a_stream_refuses_bad_options_before_anything_is_sent(hub: FakeHub) -> None:
    app = new_app(hub)
    for bad in (-1, 2**32, True, 1.0, "4"):
        with pytest.raises((ValueError, TypeError), match="initial_credit"):
            app.stream(ListOrders("o-1"), Placed, initial_credit=bad)  # type: ignore[arg-type]
    for bad in (-1, 2**32, True, 1.0, "2"):
        with pytest.raises((ValueError, TypeError), match="max_results"):
            app.stream(ListOrders("o-1"), Placed, max_results=bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timeout"):  # the options of a call are checked here, before anything is sent
        app.stream(ListOrders("o-1"), Placed, timeout=float("nan"))
    await app.start()
    s = await acknowledged(hub)
    await s.nothing(0.2, "request")


async def test_a_stream_request_carries_the_calls_priority_and_headers(hub: FakeHub) -> None:
    app = new_app(hub, priorities={"orders.ListOrders": 7})
    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(_collect(app, headers={"x-probe": "p-1"}))
    r = await s.recv_request()
    assert (r.priority, dict(r.headers)["x-probe"]) == (7, "p-1")
    await s.send(_end(r.id, 0))
    assert await call == []
