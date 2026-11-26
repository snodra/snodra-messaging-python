"""The outbox relay: order, attempts, leases, wake-ups and cancellation."""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import suppress

import pytest
from eventhelpers import Clock, OrderPlaced, RecordingEventStore, SlowOutbox
from eventhelpers import outbox_entry as entry_of
from fakehub import FakeHub, ack
from helpers import eventually, library_tasks, new_app, start_app
from snodra import MemoryEventStore, MemoryStore
from snodra._eventstore import Failed, Rejected
from snodra._outbox import OUTBOX_SEND_TIMEOUT, OutboxRelay, OutboxSettings


def _settings(
    *,
    poll_interval: float = 0.01,
    batch_size: int = 100,
    max_attempts: int = 5,
    lease: float = 120.0,
) -> OutboxSettings:
    return OutboxSettings(
        source="snodra.events",
        service="orders-service",
        poll_interval=poll_interval,
        batch_size=batch_size,
        max_attempts=max_attempts,
        lease=lease,
    )


def _payloads(events: RecordingEventStore) -> list[str]:
    return [json.loads(event.payload)["orderId"] for event in events.events]


async def test_only_events_whose_transaction_committed_are_relayed(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    async with store.transaction():
        await app.publish(OrderPlaced(order_id="committed"))

    async def fail() -> None:
        async with store.transaction():
            await app.publish(OrderPlaced(order_id="rolled-back"))
            raise RuntimeError("the handler failed")

    with pytest.raises(RuntimeError):
        await fail()
    await eventually(lambda: len(events.events) == 1)
    await asyncio.sleep(0.05)  # a relayed entry left the outbox pending
    assert _payloads(events) == ["committed"]
    assert await store.outbox.pending() == 0


async def test_events_are_relayed_in_created_at_seq_order(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    for number in range(5):
        await app.publish(OrderPlaced(order_id=f"o-{number}"), key="one-key")
    await eventually(lambda: len(events.events) == 5)
    assert _payloads(events) == [f"o-{number}" for number in range(5)]
    assert [event.key for event in events.events] == ["one-key"] * 5


async def test_one_relay_keeps_per_key_order_when_a_send_fails(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    events.failures.append(Failed("the broker is down"))
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    await app.publish(OrderPlaced(order_id="first"), key="order-1")
    await app.publish(OrderPlaced(order_id="second"), key="order-1")
    await eventually(lambda: len(events.events) == 2)
    assert _payloads(events) == ["first", "second"]
    assert events.attempts == 3


async def test_a_rejected_event_is_given_up_at_once_and_the_next_one_goes(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    events.failures.append(Rejected("too large"))
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    await app.publish(OrderPlaced(order_id="rejected"))
    await app.publish(OrderPlaced(order_id="accepted"))
    await eventually(lambda: len(events.events) == 1)
    await asyncio.sleep(0.05)
    assert _payloads(events) == ["accepted"]
    assert events.attempts == 2
    assert await store.outbox.pending() == 0


async def test_a_failed_event_is_retried_until_it_is_sent(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    events.failures.extend([Failed("down"), Failed("still down")])
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    await app.publish(OrderPlaced(order_id="o-1"))
    await eventually(lambda: len(events.events) == 1)
    assert events.attempts == 3


async def test_an_event_failing_its_attempts_is_given_up(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    events.failures.append(Failed("down"))
    app = await start_app(
        hub,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        outbox_max_attempts=1,
    )
    await app.publish(OrderPlaced(order_id="o-1"))
    await asyncio.sleep(0.1)
    assert events.events == []
    assert await store.outbox.pending() == 0


async def test_a_committed_append_wakes_the_relay_before_the_poll(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=5.0)
    await asyncio.sleep(0.05)  # the relay polled an empty outbox and waits
    begin = time.monotonic()
    async with store.transaction():
        await app.publish(OrderPlaced(order_id="woken"))
    await asyncio.wait_for(events.published.wait(), 1.0)
    assert time.monotonic() - begin < 1.0


async def test_a_crash_between_publish_and_mark_does_not_lose_the_event() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    events = RecordingEventStore()
    entry = entry_of(1)
    await store.outbox.append_now(entry)
    relay = OutboxRelay(store.outbox, events, _settings(), clock=clock)
    events.block = asyncio.Event()
    task = relay.start()
    await eventually(lambda: len(events.events) == 1)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    assert await store.outbox.pending() == 1  # published, never marked: pending still
    events.block = None
    clock.advance(121.0)
    second = OutboxRelay(store.outbox, events, _settings(), clock=clock)
    assert await second.relay_once() == 1
    assert [event.id for event in events.events] == [entry.id, entry.id]
    assert await store.outbox.pending() == 0


async def test_a_batch_stops_before_its_lease_could_run_out_under_a_send() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    events = RecordingEventStore()
    events.on_publish = lambda event: clock.advance(30.0)
    for number in range(5):
        await store.outbox.append_now(entry_of(number))
    relay = OutboxRelay(store.outbox, events, _settings(), clock=clock)
    assert await relay.relay_once() == 3
    assert [event.id for event in events.events] == ["e-0", "e-1", "e-2"]
    assert await store.outbox.pending() == 2


def test_a_lease_not_longer_than_a_send_is_refused() -> None:
    relay = OutboxRelay
    with pytest.raises(ValueError, match="longer than a send"):
        relay(
            MemoryStore().outbox,
            RecordingEventStore(),
            _settings(lease=OUTBOX_SEND_TIMEOUT),
        )


async def test_the_shutdown_cancels_the_relay_and_leaves_no_library_task(hub: FakeHub) -> None:
    app = await start_app(hub, store=MemoryStore(), event_store=RecordingEventStore(), outbox_poll_interval=5.0)
    await asyncio.sleep(0.05)
    assert "snodra:outbox" in library_tasks()
    await app.shutdown(timeout=5.0)
    assert "snodra:outbox" not in library_tasks()


async def test_an_event_store_raising_a_cancelled_error_does_not_end_the_relay(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    events.failures.append(asyncio.CancelledError())
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    await app.publish(OrderPlaced(order_id="o-1"))
    await eventually(lambda: len(events.events) == 1)
    assert events.attempts == 2
    assert "snodra:outbox" in library_tasks()


async def test_the_shutdown_budget_bounds_the_event_tasks(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore())

    async def stubborn() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(10.0)

    task = asyncio.create_task(stubborn(), name="snodra:test-stubborn")
    app._event_tasks.append(task)
    begin = time.monotonic()
    await app._stop_events(0.05)
    assert time.monotonic() - begin < 0.5
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_a_blocking_outbox_store_does_not_delay_heartbeats(hub: FakeHub) -> None:
    store = MemoryStore()
    store.outbox = SlowOutbox(store.outbox, 0.4)
    events = RecordingEventStore()
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    stream = await hub.next_stream()
    await stream.recv_register()
    await stream.ack(ack("hub-1", interval_ms=100))
    await app.ready(1.0)
    before = len(stream.heartbeats)
    begin = time.monotonic()
    await app.publish(OrderPlaced(order_id="o-1"))
    await asyncio.wait_for(events.published.wait(), 2.0)
    end = time.monotonic()
    assert end - begin >= 0.35
    during = [at for at in stream.heartbeats[before:] if begin <= at <= end]
    assert len(during) >= 2, f"the heartbeats during the blocking store call: {during}"
