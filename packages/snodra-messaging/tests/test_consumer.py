"""The event consumer: stage 1, the marker, the acknowledgement and the processor context."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import replace

import pytest
from eventhelpers import (
    OrderPlaced,
    OrderShipped,
    RecordingProcessed,
    ScriptedEventStore,
    ScriptedSubscription,
    SlowProcessed,
    delivery,
)
from fakehub import FakeHub, ack
from helpers import SCALED, eventually, library_tasks, new_app
from snodra import App, Caller, DecodeError, JsonCodec, MemoryStore, MessageInfo, current_caller, current_message
from snodra._consumer import EventConsumer
from snodra._events import EventRegistry
from snodra._eventstore import ReplayRange, SubscriptionSpec
from snodra._store import ProcessedMarker, ProcessedStore, Tx
from snodra._timings import Timings


def _fast_timings(**changes: object) -> Timings:
    values: dict[str, object] = {
        "event_handler_backoff": 0.01,
        "event_resubscribe_min": 0.01,
        "event_resubscribe_max": 0.02,
        "event_resubscribe_stable": 0.05,
    }
    values.update(changes)
    return replace(SCALED, **values)  # type: ignore[arg-type]


async def _started(hub: FakeHub, app: App, events: ScriptedEventStore) -> ScriptedSubscription:
    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    return events.subscriptions[0]


async def _acked(sub: ScriptedSubscription, count: int = 1) -> None:
    await eventually(lambda: len(sub.acks) >= count)


class _RefusingProcessed:
    """A processed store that answers every record false, as a duplicate marker does."""

    def __init__(self, inner: ProcessedStore) -> None:
        self._inner = inner

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        return False

    async def exists(self, marker_id: str) -> bool:
        return await self._inner.exists(marker_id)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        return await self._inner.delete_markers(processor, ranges)

    async def purge(self, before_ms: int) -> int:
        return await self._inner.purge(before_ms)


class _NoCommitStore(MemoryStore):
    """A memory store counting its transactions' commits and rollbacks."""

    def __init__(self) -> None:
        super().__init__()
        self.commits = 0
        self.rollbacks = 0
        self.processed = _RefusingProcessed(self.processed)

    async def commit(self, tx: Tx) -> None:
        self.commits += 1
        await super().commit(tx)

    async def rollback(self, tx: Tx) -> None:
        self.rollbacks += 1
        await super().rollback(tx)


async def test_the_marker_and_the_handlers_writes_commit_together(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    published: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        published.append(event.order_id)
        await app.publish(OrderShipped(order_id=event.order_id))

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert published == ["o-1"]
    assert await store.processed.exists("processor:default:e-1")
    assert await store.outbox.pending() == 1


async def test_a_failing_handler_rolls_back_the_marker_and_the_published_event(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=3,
        _timings=_fast_timings(),
    )
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        await app.publish(OrderShipped(order_id=event.order_id))
        raise RuntimeError("the projection store is down")

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert calls == 3
    assert not await store.processed.exists("processor:default:e-1")
    assert await store.outbox.pending() == 0


async def test_a_payload_that_does_not_fit_the_handler_is_skipped_at_once(hub: FakeHub) -> None:
    store = MemoryStore()
    processed = RecordingProcessed(store.processed)
    store.processed = processed
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=5,
        _timings=_fast_timings(),
    )
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery(payload=b'{"orderId":42}'))
    await _acked(sub)
    assert calls == 0
    assert len(processed.markers) == 1  # one transaction, no retry
    assert not await store.processed.exists("processor:default:e-1")


async def test_a_decode_error_raised_by_a_handler_is_an_ordinary_failure(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=3,
        _timings=_fast_timings(),
    )
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise DecodeError("the handler's own decode")

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert calls == 3
    assert not await store.processed.exists("processor:default:e-1")


async def test_the_record_is_acknowledged_only_after_the_commit(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    seen: list[bool] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    sub = await _started(hub, app, events)

    async def check_committed(subscription: ScriptedSubscription, segment: str, position: str) -> None:
        seen.append(await store.processed.exists("processor:default:e-1"))

    sub.on_ack = check_committed
    sub.deliver(delivery())
    await _acked(sub)
    assert seen == [True]


async def test_a_redelivered_event_whose_marker_exists_skips_the_handlers(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery(position="0"))
    await _acked(sub, 1)
    sub.deliver(delivery(position="1"))
    await _acked(sub, 2)
    assert calls == 1
    assert sub.acks == [("0", "0"), ("0", "1")]


async def test_a_duplicate_marker_aborts_its_transaction(hub: FakeHub) -> None:
    store = _NoCommitStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert store.commits == 0
    assert store.rollbacks >= 1
    assert calls == 0
    assert sub.acks == [("0", "0")]


async def test_two_handlers_of_one_type_run_in_registration_order(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls: list[str] = []

    @app.event_handler
    async def first(event: OrderPlaced) -> None:
        calls.append("first")

    @app.event_handler
    async def second(event: OrderPlaced) -> None:
        calls.append("second")

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert calls == ["first", "second"]


async def test_an_event_type_no_handler_handles_is_acknowledged_and_runs_nothing(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery(type_="orders.Unknown"))
    await _acked(sub)
    assert calls == 0
    assert not await store.processed.exists("processor:default:e-1")


async def test_a_malformed_record_is_acknowledged_and_runs_nothing(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery(type_=None))
    sub.deliver(delivery(event_id="e-2", position="1", payload=b"[]"))
    await _acked(sub, 2)
    assert calls == 0
    assert sub.acks == [("0", "0"), ("0", "1")]
    assert not await store.processed.exists("processor:default:e-1")
    assert not await store.processed.exists("processor:default:e-2")


async def test_the_handler_reads_the_event_as_the_current_message(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    seen: list[tuple[str, MessageInfo | None, Caller | None]] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append((event.order_id, current_message(), current_caller()))

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    order_id, info, caller = seen[0]
    assert order_id == "o-1"
    assert info is not None
    assert (info.id, info.type, info.correlation_id) == ("e-1", "orders.OrderPlaced", "c-1")
    assert info.headers["userId"] == "u-1"
    assert caller is None


async def test_the_marker_carries_the_processor_the_source_and_the_position(hub: FakeHub) -> None:
    store = MemoryStore()
    processed = RecordingProcessed(store.processed)
    store.processed = processed
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    sub = await _started(hub, app, events)
    sub.deliver(delivery(segment="2", position="7"))
    await _acked(sub)
    (marker,) = processed.markers
    assert marker.id == "processor:default:e-1"
    assert (marker.processor, marker.source, marker.segment, marker.position) == ("default", "snodra.events", "2", "7")
    assert marker.type_name == "orders.OrderPlaced"


async def test_the_marker_of_an_event_without_an_id_names_its_position(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery(event_id=None, segment="1", position="4"))
    await _acked(sub)
    assert calls == 1
    assert await store.processed.exists("processor:default:snodra.events-1-4")


async def test_a_failed_subscription_is_joined_again(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    events.subscribe_failures.append(RuntimeError("the event store is down"))
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    assert len(events.subscriptions) == 1


async def test_the_first_resubscribe_waits_the_minimum(hub: FakeHub) -> None:
    events = ScriptedEventStore()
    for _ in range(2):
        events.subscribe_failures.append(RuntimeError("the event store is down"))
    app = new_app(
        hub,
        store=MemoryStore(),
        event_store=events,
        outbox_enabled=False,
        _timings=_fast_timings(event_resubscribe_min=0.2, event_resubscribe_max=0.8),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await eventually(lambda: len(events.subscribe_times) >= 3, within=2.0)
    times = list(events.subscribe_times[:3])
    first_gap = times[1] - times[0]
    second_gap = times[2] - times[1]
    assert 0.15 <= first_gap < 0.35, f"the first wait was {first_gap:.3f} s, not the 0.2 s minimum"
    assert second_gap >= 0.35, f"the second wait was {second_gap:.3f} s, not twice the minimum"


async def test_a_stable_subscription_resets_the_resubscribe_wait(hub: FakeHub) -> None:
    events = ScriptedEventStore()
    events.subscribe_failures.append(RuntimeError("the event store is down"))
    app = new_app(
        hub,
        store=MemoryStore(),
        event_store=events,
        outbox_enabled=False,
        _timings=_fast_timings(
            event_resubscribe_min=0.15,
            event_resubscribe_max=0.8,
            event_resubscribe_stable=0.2,
        ),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await eventually(lambda: len(events.subscriptions) == 1, within=2.0)
    subscription = events.subscriptions[0]
    await asyncio.sleep(0.25)  # the subscription lived past the 0.2 s stability window
    failed_at = time.monotonic()
    subscription.fail(RuntimeError("the subscription broke"))
    await eventually(lambda: len(events.subscribe_times) >= 3, within=2.0)
    gap = events.subscribe_times[2] - failed_at
    assert 0.12 <= gap < 0.26, f"the wait after a stable subscription was {gap:.3f} s, not the 0.15 s minimum"


async def test_a_subscription_that_fails_while_reading_is_closed_and_joined_again(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.fail(RuntimeError("the subscription broke"))
    await eventually(lambda: len(events.subscriptions) == 2)
    assert sub.closes == 1
    second = events.subscriptions[1]
    second.deliver(delivery())
    await _acked(second)
    assert calls == 1


async def test_a_cancelled_consumer_leaves_no_task_and_a_cancelled_error(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    entered = asyncio.Event()

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        entered.set()
        await asyncio.Event().wait()

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await asyncio.wait_for(entered.wait(), 1.0)
    tasks = [task for task in asyncio.all_tasks() if task.get_name() == "snodra:consumer:default"]
    assert len(tasks) == 1
    tasks[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await tasks[0]
    assert not await store.processed.exists("processor:default:e-1")  # the transaction rolled back
    assert "snodra:consumer:default" not in library_tasks()


async def test_a_handler_that_raises_a_cancelled_error_is_retried_and_the_consumer_lives(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=2,
        _timings=_fast_timings(),
    )
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert calls == 2
    assert "snodra:consumer:default" in library_tasks()
    assert not await store.processed.exists("processor:default:e-1")


async def test_a_blocking_consumer_store_does_not_delay_heartbeats(hub: FakeHub) -> None:
    store = MemoryStore()
    store.processed = SlowProcessed(store.processed, 0.4)
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    stream = await hub.next_stream()
    await stream.recv_register()
    await stream.ack(ack("hub-1", interval_ms=100))
    await app.ready(1.0)
    before = len(stream.heartbeats)
    begin = time.monotonic()
    sub.deliver(delivery())
    await _acked(sub)
    end = time.monotonic()
    assert end - begin >= 0.35
    during = [at for at in stream.heartbeats[before:] if begin <= at <= end]
    assert len(during) >= 2, f"the heartbeats during the blocking store call: {during}"


async def test_one_event_type_in_two_processors_reaches_both(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())
    calls: list[str] = []

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        calls.append("default")

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        calls.append("mail")

    await app.start()
    await eventually(lambda: len(events.subscriptions) == 2)
    assert sorted(spec.name for spec in events.specs) == ["orders-service.default", "orders-service.mail"]
    for subscription in events.subscriptions:
        subscription.deliver(delivery())
    await _acked(events.subscriptions[0])
    await _acked(events.subscriptions[1])
    assert sorted(calls) == ["default", "mail"]


async def test_a_handler_registered_in_a_named_processor_gets_its_own_subscription(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_fast_timings())

    @app.event_handler(processor="mail")
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    assert events.specs == [SubscriptionSpec("orders-service.mail", "snodra.events")]
    assert "snodra:consumer:mail" in library_tasks()


async def test_an_old_consumer_task_does_not_report_a_new_one_stopped() -> None:
    consumer = EventConsumer(
        service="orders-service",
        processor="default",
        store=MemoryStore(),
        events=ScriptedEventStore(),
        registry=EventRegistry(),
        source="snodra.events",
        codec=JsonCodec(),
        max_attempts=1,
        backoff=0.01,
        resubscribe_min=0.01,
        resubscribe_max=0.02,
        resubscribe_stable=0.05,
    )
    first = consumer.start()
    second = consumer.start()
    assert consumer.running is True
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    assert consumer.running is True, "the older task's end reported the newer one stopped"
    second.cancel()
    await asyncio.gather(second, return_exceptions=True)
    assert consumer.running is False
