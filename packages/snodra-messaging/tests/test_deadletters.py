"""The dead letters of the event processors in mode ``collection``: parking, sequencing and the other modes.

Check: L3-DEAD-LETTERS (collection).

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", Dead letters) and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Dead letters).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from eventhelpers import (
    OrderPlaced,
    ScriptedEventStore,
    ScriptedSubscription,
    assigned,
    dead_letter,
    delivery,
)
from fakehub import FakeHub
from helpers import SCALED, eventually, new_app, processors_of
from snodra import (
    App,
    Capability,
    ConfigurationError,
    DeadLetter,
    DeadLetterReason,
    MemoryStore,
    MessageStore,
    MessageStoreUnsupported,
)
from snodra._eventstore import EventHeaders, OutgoingEvent
from snodra._hubpb import hub_pb2
from snodra._timings import Timings


def _fast(**changes: object) -> Timings:
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


async def _letters(store: MessageStore, processor: str = "default") -> list[DeadLetter]:
    return await store.dead_letters.list(processor, 100)


async def test_an_exhausted_handler_parks_a_handler_failed_letter_at_its_position(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=2,
        _timings=_fast(),
        _clock=lambda: 1_793_700_000_000,
    )
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("the projection store is down")

    sub = await _started(hub, app, events)
    sub.deliver(delivery(segment="3", position="8", key="order-42"))
    await _acked(sub)

    assert calls == 2, "every attempt ran"
    (letter,) = await _letters(store)
    assert letter.id == "default:snodra.events:3:8"  # <processor>:<source>:<segment>:<position>
    assert (letter.processor, letter.source, letter.segment, letter.position) == (
        "default",
        "snodra.events",
        "3",
        "8",
    )
    assert (letter.key, letter.event_id, letter.event_type) == ("order-42", "e-1", "orders.OrderPlaced")
    assert letter.headers[EventHeaders.CORRELATION_ID] == "c-1"
    assert letter.payload == b'{"orderId":"o-1"}'
    assert letter.reason is DeadLetterReason.HANDLER_FAILED
    assert (letter.error.type_name, letter.error.message) == ("RuntimeError", "the projection store is down")
    assert (letter.attempts, letter.blocked_by, letter.replay) == (2, None, False)
    assert (letter.parked_at_ms, letter.last_attempt_at_ms) == (1_793_700_000_000, 1_793_700_000_000)
    assert letter.expire_at_ms == 1_793_700_000_000 + 30 * 24 * 60 * 60 * 1000
    status = processors_of(app).statuses()[0]
    assert (status.failed_total, status.last_error, status.last_error_at_ms) == (
        1,
        "RuntimeError: the projection store is down",
        1_793_700_000_000,
    )
    await eventually(lambda: processors_of(app).statuses()[0].dead_letters == 1)


async def test_a_key_with_a_parked_letter_parks_its_later_records_sequenced_while_other_keys_flow(
    hub: FakeHub,
) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, consumer_max_attempts=1, _timings=_fast())
    calls: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        calls.append(event.order_id)
        if event.order_id == "bad":
            raise RuntimeError("bad")

    sub = await _started(hub, app, events)
    sub.deliver(delivery(event_id="e-1", position="0", key="k1", payload=b'{"orderId":"bad"}'))
    await _acked(sub, 1)
    # The same key waits behind the letter without running the handlers.
    sub.deliver(delivery(event_id="e-2", position="1", key="k1", payload=b'{"orderId":"later"}'))
    await _acked(sub, 2)
    # Another key flows.
    sub.deliver(delivery(event_id="e-3", position="2", key="k2", payload=b'{"orderId":"other"}'))
    await _acked(sub, 3)
    assert calls == ["bad", "other"], "the sequenced record ran no handler"

    letters = await _letters(store)
    head, follower = sorted(letters, key=lambda row: row.position)
    assert (head.position, head.reason) == ("0", DeadLetterReason.HANDLER_FAILED)
    assert (follower.position, follower.reason, follower.blocked_by) == ("1", DeadLetterReason.SEQUENCED, head.id)
    assert follower.error.type_name == "Sequenced"
    assert follower.attempts == 0
    assert sub.acks == [("0", "0"), ("0", "1"), ("0", "2")]


async def test_a_record_whose_own_letter_exists_is_acknowledged_without_running_its_handler(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, consumer_max_attempts=1, _timings=_fast())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    sub = await _started(hub, app, events)
    sub.deliver(delivery(position="5", key="k1"))
    await _acked(sub, 1)
    assert calls == 1
    # The instance died after parking and before the acknowledgement: the record comes again, and its own letter
    # makes it a duplicate.
    sub.deliver(delivery(position="5", key="k1"))
    await _acked(sub, 2)
    assert calls == 1
    assert len(await _letters(store)) == 1


async def test_invalid_records_are_parked_without_blocking_their_key(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, consumer_max_attempts=1, _timings=_fast())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    sub = await _started(hub, app, events)
    sub.deliver(delivery(type_=None, event_id=None, position="0", key="k1"))
    await _acked(sub, 1)
    assert "k1" not in processors_of(app).consumer("default")._parked_keys, "an INVALID letter blocks no key"
    sub.deliver(delivery(payload=b"[]", event_id="e-2", position="1", key="k1"))
    await _acked(sub, 2)
    # The same key flows: INVALID never blocks.
    sub.deliver(delivery(event_id="e-3", position="2", key="k1"))
    await _acked(sub, 3)
    assert calls == 1

    letters = sorted(await _letters(store), key=lambda row: row.position)
    assert [row.reason for row in letters] == [DeadLetterReason.INVALID, DeadLetterReason.INVALID]
    assert [row.attempts for row in letters] == [0, 0]
    assert [row.error.type_name for row in letters] == ["InvalidMessage", "InvalidMessage"]
    assert letters[0].event_type is None
    assert letters[1].event_type == "orders.OrderPlaced"
    assert all(row.blocked_by is None for row in letters)
    assert processors_of(app).statuses()[0].failed_total == 0, "an INVALID record did not exhaust an attempt"


async def test_mode_event_store_publishes_to_source_dlt_with_the_six_headers(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    events.capabilities = frozenset({Capability.REPLAY, Capability.NATIVE_DEAD_LETTER})
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
        dead_letter_mode="event-store",
        _timings=_fast(),
    )
    published: list[OutgoingEvent] = []

    async def publish(event: OutgoingEvent) -> None:
        published.append(event)

    events.publish = publish  # type: ignore[method-assign]

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise RuntimeError("boom")

    sub = await _started(hub, app, events)
    sub.deliver(delivery(segment="2", position="7", key="k-1"))
    await _acked(sub)
    assert await store.dead_letters.count() == 0, "nothing in the message store in this mode"
    (event,) = published
    assert event.source == "snodra.events.dlt"
    assert event.id == "e-1"
    assert event.type_name == "orders.OrderPlaced"
    assert event.key == "k-1"
    assert event.payload == b'{"orderId":"o-1"}'
    headers = event.headers
    assert headers["snodra-dlt-processor"] == "default"
    assert headers["snodra-dlt-reason"] == "HANDLER_FAILED"
    assert headers["snodra-dlt-error"].startswith("RuntimeError: boom")
    assert headers["snodra-dlt-original-topic"] == "snodra.events"
    assert headers["snodra-dlt-original-partition"] == "2"
    assert headers["snodra-dlt-original-offset"] == "7"
    assert headers[EventHeaders.ID] == "e-1"


async def test_mode_log_skips_and_parks_nothing(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
        dead_letter_mode="log",
        _timings=_fast(),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise RuntimeError("boom")

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    assert await store.dead_letters.count() == 0
    status = processors_of(app).statuses()[0]
    assert (status.dead_letter_mode, status.dead_letters, status.failed_total) == ("log", 0, 1)


async def test_a_park_that_fails_keeps_the_record_unacknowledged_and_parks_it_once(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, consumer_max_attempts=1, _timings=_fast())
    failures = 1
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    class _FlakyDeadLetters:
        async def park(self, letter):  # type: ignore[no-untyped-def]
            nonlocal failures
            if failures > 0:
                failures -= 1
                raise RuntimeError("the store is down")
            return await real.park(letter)

        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

    real = store.dead_letters
    store.dead_letters = _FlakyDeadLetters()  # type: ignore[assignment]

    sub = await _started(hub, app, events)
    sub.deliver(delivery(position="4", key="k1"))
    # Nothing is acknowledged while the sink fails; the redelivery goes straight to the sink, without the handler.
    await eventually(lambda: failures == 0)
    await eventually(lambda: sub.redeliveries == [("0", "4")])
    assert sub.acks == []
    sub.deliver(delivery(position="4", key="k1"))
    await _acked(sub)
    assert calls == 1
    assert len(await real.list("default", 10)) == 1


async def test_a_store_without_dead_letters_acknowledges_the_failed_record_as_lost(hub: FakeHub) -> None:
    store = MemoryStore()
    real = store.dead_letters

    class _UnsupportedPark:
        async def park(self, letter):  # type: ignore[no-untyped-def]
            raise MessageStoreUnsupported("dead letters in this store (level 3)")

        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

    store.dead_letters = _UnsupportedPark()  # type: ignore[assignment]
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, consumer_max_attempts=1, _timings=_fast())
    calls = 0

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    sub = await _started(hub, app, events)
    sub.deliver(delivery(position="4", key="k1"))
    await _acked(sub)
    assert calls == 1
    assert sub.redeliveries == [], "an unsupported dead-letter store does not redeliver"
    assert await real.count() == 0
    status = processors_of(app).statuses()[0]
    assert status.failed_total == 1
    assert "dead letters" in status.last_error


async def test_mode_event_store_needs_the_native_dead_letter_capability(hub: FakeHub) -> None:
    events = ScriptedEventStore()
    assert not events.supports(Capability.NATIVE_DEAD_LETTER)
    app = new_app(
        hub,
        store=MemoryStore(),
        event_store=events,
        outbox_enabled=False,
        dead_letter_mode="event-store",
        _timings=_fast(),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    with pytest.raises(ConfigurationError, match="native-dead-letter"):
        await app.start()


async def test_the_dlt_error_header_is_cut_to_2000_code_points(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    events.capabilities = frozenset({Capability.REPLAY, Capability.NATIVE_DEAD_LETTER})
    published: list[OutgoingEvent] = []

    async def publish(event: OutgoingEvent) -> None:
        published.append(event)

    events.publish = publish  # type: ignore[method-assign]
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
        dead_letter_mode="event-store",
        _timings=_fast(),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise RuntimeError("é" * 3000)

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await _acked(sub)
    (event,) = published
    header = event.headers["snodra-dlt-error"]
    assert header.startswith("RuntimeError: ")
    assert len(header) == 2000, "code points, not UTF-8 bytes"


async def test_a_dlt_publish_is_bounded_and_leaves_the_record_unacknowledged(hub: FakeHub) -> None:
    store = MemoryStore()
    events = ScriptedEventStore()
    events.capabilities = frozenset({Capability.REPLAY, Capability.NATIVE_DEAD_LETTER})
    gate = asyncio.Event()

    async def publish(event: OutgoingEvent) -> None:
        await gate.wait()

    events.publish = publish  # type: ignore[method-assign]
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
        dead_letter_mode="event-store",
        _timings=_fast(dead_letter_publish_timeout=0.1),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise RuntimeError("boom")

    sub = await _started(hub, app, events)
    sub.deliver(delivery())
    await eventually(lambda: sub.redeliveries == [("0", "0")], within=1.5)
    assert sub.acks == [], "the publish did not finish: the record is not acknowledged"
    gate.set()
    await app.shutdown(timeout=1.0)


async def test_a_failing_parked_key_check_redelivers_in_place_without_resubscribing(hub: FakeHub) -> None:
    store = MemoryStore()
    real = store.dead_letters
    await real.park(dead_letter("0", key="k1"))
    failures = 1

    class _FlakyHead:
        async def head(self, processor: str, key: str) -> str | None:
            nonlocal failures
            if failures > 0:
                failures -= 1
                raise RuntimeError("the store is briefly down")
            return await real.head(processor, key)

        def __getattr__(self, name: str) -> object:
            return getattr(real, name)

    store.dead_letters = _FlakyHead()  # type: ignore[assignment]
    events = ScriptedEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, consumer_max_attempts=1, _timings=_fast())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise AssertionError("the sequenced record must not run its handler")

    await app.start()
    await asyncio.wait_for(events.subscribed.wait(), 1.0)
    sub = events.subscriptions[0]
    sub.queue.put_nowait(assigned("0"))
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING)

    sub.deliver(delivery(event_id="e-2", position="1", key="k1", payload=b'{"orderId":"o-2"}'))
    await eventually(lambda: sub.redeliveries == [("0", "1")])
    assert len(events.subscriptions) == 1, "the subscription was kept"
    assert sub.closes == 0
    assert sub.acks == []

    sub.deliver(delivery(event_id="e-2", position="1", key="k1", payload=b'{"orderId":"o-2"}'))
    await eventually(lambda: len(sub.acks) == 1)
    letters = sorted(await real.list("default", 10), key=lambda row: row.position)
    assert [(row.position, row.reason) for row in letters] == [
        ("0", DeadLetterReason.HANDLER_FAILED),
        ("1", DeadLetterReason.SEQUENCED),
    ]
