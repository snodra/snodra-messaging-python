"""Pause and resume: persisted in ``processor_control``, read at start and every poll, never leaving the subscription.

Check: L3-PAUSE.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", Pause and resume) and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Pause and resume).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace

from eventhelpers import OrderPlaced, ScriptedEventStore, ScriptedSubscription, assigned, delivery
from fakehub import FakeHub
from helpers import SCALED, eventually, new_app, processors_of
from snodra import App, MemoryStore
from snodra._hubpb import hub_pb2
from snodra._timings import Timings

NOW = 1_793_700_000_000


def _timings(**changes: object) -> Timings:
    values: dict[str, object] = {
        "event_handler_backoff": 0.01,
        "event_resubscribe_min": 0.01,
        "event_resubscribe_max": 0.02,
        "event_resubscribe_stable": 0.05,
        "processor_control_poll": 0.05,
        "dead_letter_sample": 0.05,
    }
    values.update(changes)
    return replace(SCALED, **values)  # type: ignore[arg-type]


async def _started(hub: FakeHub, app: App, events: ScriptedEventStore) -> ScriptedSubscription:
    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)
    return sub


async def test_a_pause_is_persisted_holds_the_records_and_keeps_the_subscription(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings(), _clock=lambda: NOW)
    calls: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        calls.append(event.order_id)

    sub = await _started(hub, app, events)
    await processors_of(app).pause("default")
    doc = await store.control.get("default")
    assert (doc.paused, doc.paused_at_ms, doc.paused_by) == (True, NOW, "local")
    assert sub.pause_calls == 1
    assert sub.closes == 0, "a pause never leaves the subscription"
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_PAUSED

    # A record handed out before the pause took effect is handed back, not handled.
    sub.deliver(delivery(event_id="e-1", position="0"))
    await eventually(lambda: sub.redeliveries == [("0", "0")])
    assert calls == []
    assert sub.acks == []

    # Resume consumes again in place: the same subscription, no new one.
    await processors_of(app).resume("default")
    doc = await store.control.get("default")
    assert (doc.paused, doc.paused_at_ms, doc.paused_by) == (False, None, None)
    assert sub.resume_calls == 1
    assert len(events.subscriptions) == 1
    sub.deliver(delivery(event_id="e-1", position="0"))
    await eventually(lambda: sub.acks == [("0", "0")])
    assert calls == ["o-1"]


async def test_a_pause_written_by_another_instance_reaches_this_one_within_a_poll(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings(), _clock=lambda: NOW)
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    await store.control.set_paused("default", True, "another-instance", NOW)
    await eventually(lambda: sub.pause_calls == 1)
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_PAUSED
    sub.deliver(delivery())
    await eventually(lambda: len(sub.redeliveries) == 1)
    assert calls == 0

    await store.control.set_paused("default", False, "another-instance", NOW)
    await eventually(lambda: sub.resume_calls == 1)
    sub.deliver(delivery())
    await eventually(lambda: len(sub.acks) == 1)
    assert calls == 1


async def test_an_instance_starts_paused_when_the_document_says_so(hub: FakeHub) -> None:
    store = MemoryStore()
    await store.control.set_paused("default", True, "operator", NOW)
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings(), _clock=lambda: NOW)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise AssertionError("paused")

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    assert sub.pause_calls == 1, "the subscription is paused before its first read (a segment assigned while paused is)"
    assert (await store.control.get("default")).paused_by == "operator", "starting reads, never rewrites, the document"
    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_PAUSED)
    sub.deliver(delivery())
    await eventually(lambda: len(sub.redeliveries) == 1)
    assert sub.acks == []


async def test_a_pause_does_not_touch_the_reset_lock(hub: FakeHub) -> None:
    store = MemoryStore()
    await store.control.lock("default", "op-1", NOW + 60_000, NOW)
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings(), _clock=lambda: NOW)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.sleep(0.05)
    # Under the lock the processor does not join; a pause persists without touching the operation.
    assert events.subscriptions == []
    await processors_of(app).pause("default")
    state = await store.control.get("default")
    assert state.paused is True
    assert state.operation is not None
    assert state.operation.id == "op-1"
    await processors_of(app).resume("default")
    state = await store.control.get("default")
    assert state.operation is not None
    assert state.operation.id == "op-1"
    assert processors_of(app).statuses()[0].operation_id == "op-1"
