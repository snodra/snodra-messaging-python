"""A subscription query this instance sends: the initial result, the updates, the credit and refills, the Cancels of a
caller that stops, the parts the hub lost, the local guard and the retries until the initial result.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Subscription", "Never sent or outcome unknown").
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator

import pytest
from fakehub import FakeHub, FakeStream, ack
from helpers import WatchInitial, WatchOrders, WatchUpdate, acknowledged, eventually, library_tasks, new_app
from snodra import App, ErrorCode, HubError, LostParts, RetryPolicy
from snodra._hubpb import hub_pb2


def _part(id: str, seq: int, value: object, *, topic: str = "t") -> hub_pb2.ServerMessage:
    """One part of a subscription, as the hub relays it: the sender's id and the hub's own seq."""
    payload = json.dumps({"topic": topic, "value": value}).encode()
    return hub_pb2.ServerMessage(part=hub_pb2.ReplyPart(request_id=id, seq=seq, payload=payload))


def _initial(id: str, *, topic: str = "t") -> hub_pb2.ServerMessage:
    """The initial result's part: the hub always numbers it 1."""
    return _part(id, 1, 0, topic=topic)


def _end(id: str, parts: int, *, code: int | None = None, message: str = "") -> hub_pb2.ServerMessage:
    """The end of a subscription, without an error unless code names one."""
    end = hub_pb2.ReplyEnd(request_id=id, parts=parts)
    if code is not None:
        error = hub_pb2.Error(request_id=id, code=code, message=message)  # type: ignore[arg-type]
        end.error.CopyFrom(error)
    return hub_pb2.ServerMessage(end=end)


async def _collect(app: App, *, read: int | None = None, **options: object) -> tuple[WatchInitial, list[WatchUpdate]]:
    """The initial result and the updates of a subscription, read inside its block; stops after ``read`` of them."""
    initial: WatchInitial | None = None
    updates: list[WatchUpdate] = []
    async with app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate, **options) as sub:  # type: ignore[arg-type]
        initial = sub.initial
        async for update in sub:
            updates.append(update)
            if read is not None and len(updates) >= read:
                break
    assert initial is not None
    return initial, updates


BESIDE_CANCELS = frozenset({"heartbeat", "status", "permits", "flow"})


async def _next_cancel(s: FakeStream, within: float = 5.0) -> hub_pb2.Cancel:
    """The next ``Cancel`` on s, skipping heartbeats, statuses, ``Permits`` and the ``Flow``s a subscription grants as
    it is read; any other frame fails."""
    deadline = time.monotonic() + within
    while True:
        m = await s.recv(within=max(0.0, deadline - time.monotonic()))
        kind = m.WhichOneof("message")
        if kind == "cancel":
            return m.cancel
        if kind not in BESIDE_CANCELS:
            raise AssertionError(f"expected a cancel, got {kind}: {m}")


async def test_subscribe_returns_the_initial_then_the_updates(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(_collect(app, initial_credit=2, max_results=4))
    r = await s.recv_request()
    assert (r.mode, r.kind, r.type, r.routing_key, r.timeout_ms) == (
        hub_pb2.SUBSCRIPTION,
        hub_pb2.QUERY,
        "orders.WatchOrders",
        "",
        0,
    )
    assert (r.initial_credit, r.max_results) == (2, 4)
    assert json.loads(r.payload) == {"topic": "t"}
    assert "correlationId" in r.headers
    await s.send(_initial(r.id))  # seq 1 is the initial result, counted as consumed at once
    flow = await s.recv_flow()
    assert (flow.request_id, flow.credits) == (r.id, 1)  # max(1, 2 // 2) after the initial part
    for seq in range(2, 7):  # 5 updates, each one refilling a window of 1
        await s.send(_part(r.id, seq, seq - 1))
        flow = await s.recv_flow()
        assert (flow.request_id, flow.credits) == (r.id, 1)
    await s.send(_end(r.id, 6))
    initial, updates = await call
    assert initial == WatchInitial("t", 0)
    assert updates == [WatchUpdate("t", n) for n in range(1, 6)]
    await s.nothing(0.2, "cancel")  # the subscription ended: no Cancel
    assert not app._sender.pending


async def test_subscribe_detects_lost_parts(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)

    async def lost() -> tuple[WatchInitial, list[WatchUpdate]]:
        return await _collect(app, initial_credit=8)

    # A gap in the hub's seq: the parts before it stay, then a Cancel with the loss and LostParts.
    call = asyncio.create_task(lost())
    r = await s.recv_request()
    for seq in (1, 2, 4):
        await s.send(_part(r.id, seq, seq))
    with pytest.raises(LostParts) as raised:
        await call
    assert str(raised.value) == "subscription orders.WatchOrders lost 1 part(s)"
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "subscription orders.WatchOrders lost 1 part(s)")
    await s.nothing(0.2, "cancel")
    assert not app._sender.pending

    # A clean end that counts more parts than arrived: the subscription ended, so nothing is cancelled.
    call = asyncio.create_task(lost())
    r = await s.recv_request()
    for seq in (1, 2):
        await s.send(_part(r.id, seq, seq))
    await s.send(_end(r.id, 4))
    with pytest.raises(LostParts) as raised:
        await call
    assert str(raised.value) == "subscription orders.WatchOrders lost 2 part(s)"
    await s.nothing(0.2, "cancel")
    assert not app._sender.pending


async def test_a_failed_initial_result_fails_the_enter(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(_collect(app))
    r = await s.recv_request()
    await s.send(_end(r.id, 0, code=hub_pb2.NO_HANDLER, message="no live instance"))
    with pytest.raises(HubError) as raised:
        await call
    assert (raised.value.code, raised.value.message) == (
        ErrorCode.NO_HANDLER,
        "No handler registered for 'orders.WatchOrders'",
    )
    await s.nothing(0.2, "cancel")  # the initial result failed: nothing to cancel
    assert not app._sender.pending


async def test_leaving_the_subscription_block_early_cancels(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    before = library_tasks()

    # A break after 2 updates: a Cancel at once, and nothing of the subscription left here or in a task.
    call = asyncio.create_task(_collect(app, read=2))
    r = await s.recv_request()
    for seq in range(1, 4):
        await s.send(_part(r.id, seq, seq - 1))
    _, updates = await call
    assert updates == [WatchUpdate("t", 1), WatchUpdate("t", 2)]
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector cancelled")
    assert not app._sender.pending
    assert library_tasks() == before

    # An exception in the body: the Cancel names the failure, and the exception reaches the caller.
    async def failing() -> None:
        async with app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate, initial_credit=2) as sub:
            async for _ in sub:
                raise RuntimeError("the body broke")

    broken = asyncio.create_task(failing())
    r = await s.recv_request()
    await s.send(_initial(r.id))
    await s.send(_part(r.id, 2, 1))
    with pytest.raises(RuntimeError, match="the body broke"):
        await broken
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector failed: RuntimeError: the body broke")
    assert not app._sender.pending

    # The caller's task cancelled while it waits for the next update: a Cancel at once.
    seen: list[WatchUpdate] = []

    async def waiting() -> None:
        async with app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate, initial_credit=2) as sub:
            async for update in sub:
                seen.append(update)

    waiting_call = asyncio.create_task(waiting())
    r = await s.recv_request()
    await s.send(_initial(r.id))
    await s.send(_part(r.id, 2, 1))
    await eventually(lambda: seen == [WatchUpdate("t", 1)])
    waiting_call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_call
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector cancelled")
    assert not app._sender.pending

    # Never entering the block sends no Request; iterating outside it, or entering it twice, is a RuntimeError.
    never = app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate)
    await s.nothing(0.2, "request")
    with pytest.raises(RuntimeError, match="async with"):
        async for _ in never:  # pragma: no cover
            pass
    entering = asyncio.create_task(never.__aenter__())
    hand = await s.recv_request()
    await s.send(_initial(hand.id))
    assert await entering is never
    with pytest.raises(RuntimeError, match="async with"):
        await never.__aenter__()
    await never.__aexit__(None, None, None)
    assert not app._sender.pending  # the one request it opened is gone, and its Cancel went out
    assert (await _next_cancel(s)).reason == "collector cancelled"
    with pytest.raises(RuntimeError, match="async with"):
        async for _ in never:  # pragma: no cover
            pass


async def test_a_subscription_never_entered_sends_nothing(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.start()
    s = await acknowledged(hub)
    dropped = app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate)
    await s.nothing(0.2, "request")
    with pytest.raises(RuntimeError, match="async with"):
        async for _ in dropped:  # pragma: no cover
            pass


async def _enter(app: App, **options: object) -> AsyncIterator[WatchUpdate]:
    """Enters a subscription and yields its updates (the block the guards and retries run around)."""
    async with app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate, **options) as sub:  # type: ignore[arg-type]
        async for update in sub:
            yield update


async def test_the_subscriptions_guard_and_retries(hub: FakeHub) -> None:
    app = new_app(hub)  # the guard margin is the shipped 1 s
    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", max_ms=200))
    # No initial result at all: the unary bound of 200 ms plus the 1 s margin.
    started = time.monotonic()
    call = asyncio.create_task(_collect(app))
    r = await s.recv_request()
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.TIMEOUT
    assert time.monotonic() - started >= 1.15
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "no initial result within the local guard")
    assert not app._sender.pending

    # Retried only until the initial result, each attempt sending what is left of the caller's budget.
    policy = RetryPolicy(max_attempts=2, initial_backoff=0.01)
    call = asyncio.create_task(_collect(app, timeout=2.0, retry=policy))
    first = await s.recv_request()
    assert first.timeout_ms == 2000
    await s.send(_end(first.id, 0, code=hub_pb2.NO_HANDLER, message="no live instance"))
    second = await s.recv_request()
    assert second.id != first.id  # a new request, with an id of its own
    assert 0 < second.timeout_ms < 2000  # what is left of the budget after the backoff
    await s.send(_initial(second.id))
    await s.send(_part(second.id, 2, 1))
    await s.send(_end(second.id, 2))
    initial, updates = await call
    assert (initial, updates) == (WatchInitial("t", 0), [WatchUpdate("t", 1)])

    # Once the initial result arrived, an error end is the answer: no new attempt.
    call = asyncio.create_task(_collect(app, timeout=2.0, retry=policy))
    r = await s.recv_request()
    await s.send(_initial(r.id))
    await s.send(_end(r.id, 1, code=hub_pb2.HANDLER_ERROR, message="feed closed"))
    with pytest.raises(HubError) as raised:
        await call
    assert (raised.value.code, raised.value.message) == (ErrorCode.HANDLER_ERROR, "feed closed")
    await s.nothing(0.3, "request")
