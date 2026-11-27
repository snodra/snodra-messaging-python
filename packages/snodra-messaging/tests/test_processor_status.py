"""The processors' status: one ``ProcessorStatus`` per started processor, its fields, its states and its bounds.

Check: L3-STATUS.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor status") and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Status).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace

from eventhelpers import Clock, OrderPlaced, ScriptedEventStore, assigned, dead_letter
from fakehub import FakeHub
from helpers import SCALED, eventually, new_app, processors_of
from snodra import DeadLetterReason, MemoryEventStore, MemoryStore
from snodra._eventstore import SegmentStatus
from snodra._hubpb import hub_pb2
from snodra._status import StatusView, build_status
from snodra._timings import Timings


def _timings(**changes: object) -> Timings:
    values: dict[str, object] = {
        "event_handler_backoff": 0.01,
        "event_resubscribe_min": 0.01,
        "event_resubscribe_max": 0.02,
        "event_resubscribe_stable": 0.05,
        "dead_letter_sample": 0.05,
        "status_interval": 0.1,
    }
    values.update(changes)
    return replace(SCALED, **values)  # type: ignore[arg-type]


async def test_every_tick_carries_one_status_per_processor_with_its_fields(hub: FakeHub) -> None:
    store = MemoryStore()
    events = MemoryEventStore(segments=2)
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        _timings=_timings(),
    )

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        pass

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        pass

    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(hub_pb2.RegisterAck(hub_id="hub-1", heartbeat_interval_ms=3000, default_timeout_ms=5000))
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    await app.publish(OrderPlaced(order_id="o-2"), key="o-2")

    def both_processed() -> bool:
        if not s.statuses:
            return False
        latest = s.statuses[-1][1]
        return len(latest.processors) == 2 and all(row.processed_total == 2 for row in latest.processors)

    await eventually(both_processed, within=3.0)
    status = s.statuses[-1][1]
    rows = {row.name: row for row in status.processors}
    assert list(rows) == ["default", "mail"]
    source_log = events._shared.sources["snodra.events"]
    heads = [str(len(source_log[segment])) for segment in range(2)]
    assert sum(int(head) for head in heads) == 2
    for name in ("default", "mail"):
        row = rows[name]
        assert row.subscription == f"orders-service.{name}"
        assert row.source == "snodra.events"
        assert row.event_store == "memory"
        assert list(row.capabilities) == ["lag", "replay"]
        assert row.state == hub_pb2.PROCESSOR_RUNNING
        assert [segment.segment for segment in row.segments] == ["0", "1"]  # the hub's segment order
        assert [segment.head for segment in row.segments] == heads
        assert [segment.committed for segment in row.segments] == heads
        assert [segment.lag for segment in row.segments] == [0, 0]
        assert row.processed_total == 2
        assert row.failed_total == 0
        assert row.dead_letters == 0
        assert row.last_error == ""
        assert row.last_error_at_ms == 0
        assert list(row.event_types) == ["orders.OrderPlaced"]
        assert row.dead_letter_mode == "collection"
        assert row.replaying is False
        assert row.operation_id == ""
        assert all(segment.replay_until == "" for segment in row.segments)
    # A later tick carries the same one status per processor, not an accumulating list.
    assert all(len(status.processors) == 2 for _, status in s.statuses)


async def test_the_status_is_built_without_waiting_for_the_subscription(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings())

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)
    began = time.monotonic()
    for _ in range(50):
        processors_of(app).statuses()
    assert time.monotonic() - began < 0.5


async def test_states_starting_failing_paused_and_stopped(hub: FakeHub) -> None:
    clock = Clock()
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=MemoryStore(),
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
        _timings=_timings(),
        _clock=clock,
    )
    calls = 0

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_STARTING

    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)

    from eventhelpers import delivery

    sub.deliver(delivery())
    await eventually(lambda: len(sub.acks) == 1)
    status = processors_of(app).statuses()[0]
    assert status.state == hub_pb2.PROCESSOR_FAILING
    assert status.failed_total == 1
    assert status.last_error == "RuntimeError: boom"
    assert status.last_error_at_ms == clock.now_ms

    clock.advance(61)
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING, "FAILING lasts 60 s"

    await processors_of(app).pause("default")
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_PAUSED
    await processors_of(app).resume("default")
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)

    await app.shutdown()
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_STOPPED


async def test_the_dead_letter_count_is_sampled_and_zero_outside_collection(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings())

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    events.subscriptions[0].queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)
    await store.dead_letters.park(dead_letter("1"))
    await store.dead_letters.park(dead_letter("2"))
    await eventually(lambda: processors_of(app).statuses()[0].dead_letters == 2, within=2.0)
    # An INVALID letter counts too: it is a letter.
    invalid = dead_letter("3", reason=DeadLetterReason.INVALID)
    await store.dead_letters.park(invalid)
    await eventually(lambda: processors_of(app).statuses()[0].dead_letters == 3, within=2.0)

    other = ScriptedEventStore()
    log_app = new_app(
        hub,
        store=MemoryStore(),
        event_store=other,
        outbox_enabled=False,
        dead_letter_mode="log",
        _timings=_timings(),
    )

    @log_app.event_handler
    async def logged(event: OrderPlaced) -> None:
        pass

    await log_app.start()
    await asyncio.wait_for(other.subscribed.wait(), 1.0)
    other.subscriptions[0].queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(log_app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)
    assert processors_of(log_app).statuses()[0].dead_letters == 0, "0 outside collection"
    assert processors_of(log_app).statuses()[0].dead_letter_mode == "log"


def test_the_hub_bounds_of_a_status_are_kept() -> None:
    long_position = "9" * 129
    long_segment = "s" * 129
    view = StatusView(
        name="mail",
        subscription="s" * 300,
        source="n" * 300,
        event_store="a" * 100,
        capabilities=frozenset("capability-" + str(n) for n in range(20)),
        state=hub_pb2.PROCESSOR_RUNNING,
        segments=[
            SegmentStatus(segment="0", position=long_position, committed=None, head="0", lag=-5),
            SegmentStatus(segment=long_segment, position="0", head="0", lag=0),
        ],
        replay_until={"0": long_position},
        last_error="é" * 400,
        event_types=["orders." + "T" * 400],
        operation_id="o" * 100,
    )
    status = build_status(view)
    (segment,) = status.segments  # the long segment name is dropped
    assert segment.segment == "0"
    assert (segment.position, segment.committed, segment.head, segment.replay_until) == ("", "", "0", "")
    assert segment.lag == 0, "a negative lag reads as 0 (unknown is -1 only)"
    assert len(status.capabilities) == 16
    assert status.event_store == "a" * 64
    assert status.operation_id == "o" * 64
    assert len(status.last_error.encode()) <= 512
    assert status.subscription == "s" * 256
    assert status.source == "n" * 256
    assert len(status.event_types[0].encode()) <= 256
    assert status.event_types[0].startswith("orders.")
