"""The named processors: their subscriptions, their markers, their independence, their names and their start.

Check: L3-PROCESSORS.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor status") and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Named processors).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from eventhelpers import OrderPlaced, ScriptedEventStore, assigned, delivery
from fakehub import FakeHub
from helpers import SCALED, eventually, new_app, processors_of
from snodra import MemoryEventStore, MemoryStore, MessageStoreUnsupported
from snodra._eventstore import SubscriptionSpec
from snodra._hubpb import hub_pb2
from snodra._store import DeadLetter
from snodra._timings import Timings


def _timings() -> Timings:
    return replace(
        SCALED,
        event_handler_backoff=0.01,
        event_resubscribe_min=0.01,
        event_resubscribe_max=0.02,
        event_resubscribe_stable=0.05,
        dead_letter_sample=0.05,
    )


async def test_one_event_type_in_two_processors_reaches_both_with_its_own_marker(hub: FakeHub) -> None:
    store = MemoryStore()
    events = MemoryEventStore(segments=1)
    app = new_app(
        hub,
        store=store,
        event_store=events,
        _timings=_timings(),
        outbox_poll_interval=0.01,
    )
    seen: list[str] = []

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        seen.append("default")

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        seen.append("mail")

    await app.start()
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    await eventually(lambda: sorted(seen) == ["default", "mail"])
    event_id = _event_id(app)
    for processor in ("default", "mail"):
        await _wait_marker(store, f"processor:{processor}:{event_id}")
    assert sorted(spec.name for spec in events._shared.groups) == [
        "orders-service.default",
        "orders-service.mail",
    ]


async def _wait_marker(store: MemoryStore, marker: str) -> None:
    deadline = asyncio.get_running_loop().time() + 2.0
    while not await store.processed.exists(marker):
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"no marker {marker} within 2 s")
        await asyncio.sleep(0.005)


def _event_id(app: object) -> str:
    """The id of the only event the app published (read from its outbox)."""
    rows = list(app._store._data.outbox.rows.values())  # type: ignore[attr-defined]
    assert len(rows) == 1
    return str(rows[0].entry.id)


async def _letters(store: MemoryStore, processor: str) -> list[DeadLetter]:
    deadline = asyncio.get_running_loop().time() + 2.0
    while True:
        found = await store.dead_letters.list(processor, 10)
        if found:
            return found
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"no dead letter of {processor} within 2 s")
        await asyncio.sleep(0.01)


async def test_a_failure_in_one_processor_does_not_hold_the_other(hub: FakeHub) -> None:
    store = MemoryStore()
    events = MemoryEventStore(segments=1)
    app = new_app(
        hub,
        store=store,
        event_store=events,
        consumer_max_attempts=2,
        _timings=_timings(),
        outbox_poll_interval=0.01,
    )
    seen: list[str] = []

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        seen.append("default")

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        raise RuntimeError("mail is down")

    await app.start()
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    await eventually(lambda: seen == ["default"])
    (letter,) = await _letters(store, "mail")
    assert (letter.processor, letter.reason.value) == ("mail", "HANDLER_FAILED")
    assert await store.dead_letters.count("default") == 0
    status = {row.name: row for row in processors_of(app).statuses()}
    assert status["default"].processed_total == 1
    assert status["mail"].failed_total == 1
    assert status["default"].last_error == ""
    assert status["mail"].last_error == "RuntimeError: mail is down"


async def test_only_processors_with_handlers_start(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore(), outbox_enabled=False)

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        pass

    await app.start()
    assert processors_of(app).names == ("mail",)
    names = {task.get_name() for task in asyncio.all_tasks()}
    assert "snodra:consumer:mail" in names
    assert "snodra:consumer:default" not in names


def test_the_processor_name_is_checked_and_default_is_the_unnamed_one(hub: FakeHub) -> None:
    # TODO(spec-gap): SG-43
    # Python names the default processor with the parameter's own default; an explicit "default" is that same
    # processor (the parameter cannot tell them apart), where Kotlin and Rust refuse the explicit name.
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore())
    for processor in ("", "Default", "mail_", "a" * 49, "-mail", "mail.x"):
        with pytest.raises(ValueError, match="processor name"):

            @app.event_handler(processor=processor)
            async def bad(event: OrderPlaced) -> None:
                pass

    @app.event_handler(processor="a" * 48)
    async def longest(event: OrderPlaced) -> None:
        pass

    @app.event_handler(processor="default")
    async def explicit_default(event: OrderPlaced) -> None:
        pass

    assert app._events.handlers("default", "orders.OrderPlaced")[0].fn is explicit_default
    assert app._events.handlers("a" * 48, "orders.OrderPlaced")[0].fn is longest


async def test_the_subscription_and_the_markers_of_a_processor_are_its_own(hub: FakeHub) -> None:
    from snodra._events import processor_marker, processor_subscription

    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore(), outbox_enabled=False)

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        pass

    await app.start()
    consumer = processors_of(app).consumer("mail")
    assert consumer.subscription == SubscriptionSpec("orders-service.mail", "snodra.events")
    assert processor_subscription("orders-service", "mail") == "orders-service.mail"
    assert processor_marker("mail", "e-1") == "processor:mail:e-1"


async def test_a_new_processor_reports_starting_until_its_assignment(hub: FakeHub) -> None:
    events = ScriptedEventStore()
    app = new_app(hub, store=MemoryStore(), event_store=events, outbox_enabled=False, _timings=_timings())

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_STARTING
    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)


async def test_a_processor_started_under_a_reset_lock_does_not_join_and_reports_stopped(hub: FakeHub) -> None:
    store = MemoryStore()
    now = 1_793_700_000_000
    await store.control.lock("default", "op-1", now + 60_000, now)
    app = new_app(
        hub,
        store=store,
        event_store=MemoryEventStore(),
        outbox_enabled=False,
        _timings=_timings(),
        _clock=lambda: now,
    )

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        pass

    await app.start()
    status = processors_of(app).statuses()[0]
    assert (status.state, status.operation_id) == (hub_pb2.PROCESSOR_STOPPED, "op-1")
    assert "snodra:consumer:default" not in {task.get_name() for task in asyncio.all_tasks()}
    operation = (await store.control.get("default")).operation
    assert operation is not None
    assert (operation.id, operation.kind, operation.lease_until_ms) == ("op-1", "reset", now + 60_000)


class _UnsupportedControl:
    """A processor control port that the store does not implement (the level-2 add-ons' state)."""

    def __init__(self) -> None:
        self.get_calls = 0

    async def get(self, processor: str) -> object:
        self.get_calls += 1
        raise MessageStoreUnsupported("processor control in this store (level 3)")

    def __getattr__(self, name: str) -> object:
        async def refuse(*args: object, **kwargs: object) -> object:
            raise MessageStoreUnsupported(f"processor control in this store (level 3): {name}")

        return refuse


async def test_a_store_without_processor_control_starts_the_processors_unpaused(hub: FakeHub) -> None:
    store = MemoryStore()
    control = _UnsupportedControl()
    store.control = control  # type: ignore[assignment]
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)
    assert processors_of(app).consumer("default").paused is False
    await eventually(lambda: processors_of(app)._polls >= 3)
    assert control.get_calls == 1, "the unsupported control port is tried once, not on every poll"
    sub.deliver(delivery())
    await eventually(lambda: len(sub.acks) == 1)
    assert calls == 1


async def test_a_lock_this_instance_holds_keeps_its_processor_out_of_the_subscription(hub: FakeHub) -> None:
    store = MemoryStore()
    now = 1_793_700_000_000
    await store.control.lock("default", "op-1", now + 60_000, now)
    app = new_app(
        hub,
        store=store,
        event_store=MemoryEventStore(),
        outbox_enabled=False,
        _timings=_timings(),
        _clock=lambda: now,
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    processors = processors_of(app)
    assert processors.statuses()[0].operation_id == "op-1"
    assert processors.consumer("default").running is False

    # The reset state machine (level 3) marks the operation held here: the document names it, so it stays stopped.
    processors.reset.hold("default", "op-1", 60_000)
    await processors._read_and_apply(initial=False)
    assert processors.consumer("default").running is False
    assert processors.statuses()[0].operation_id == "op-1"

    # The document no longer names it (finished elsewhere): the processor consumes again and reports no operation.
    await store.control.release("default", "op-1", now)
    await processors._read_and_apply(initial=False)
    assert processors.consumer("default").running is True
    assert processors.statuses()[0].operation_id == ""
    assert processors.reset.held("default") is None
