"""Replay: the range boundaries, the handlers marked for no replay, the letter flag and the status.

Checks: L3-REPLAY.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", "The target of a reset") and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Replay).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace

from eventhelpers import OrderPlaced, delivery
from fakehub import FakeHub
from helpers import SCALED, eventually, new_app, processors_of
from snodra import App, Earliest, MemoryEventStore, MemoryStore
from snodra._eventstore import ReplayRange, SegmentStatus
from snodra._hubpb import hub_pb2
from snodra._replay import in_replay, open_until, passed_ranges
from snodra._reset import ResetDone, ResetOk
from snodra._store import DeadLetter, ProcessedMarker
from snodra._timings import Timings


def _timings() -> Timings:
    return replace(
        SCALED,
        event_handler_backoff=0.01,
        event_resubscribe_min=0.01,
        event_resubscribe_max=0.02,
        event_resubscribe_stable=0.05,
        processor_control_poll=0.05,
        dead_letter_sample=0.05,
        status_interval=300.0,
    )


def _compare(a: str, b: str) -> int:
    return (int(a) > int(b)) - (int(a) < int(b))


async def _app(hub: FakeHub, *, attempts: int = 5) -> tuple[App, MemoryStore, MemoryEventStore]:
    store = MemoryStore()
    events = MemoryEventStore(segments=1)
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        consumer_max_attempts=attempts,
        _timings=_timings(),
    )
    return app, store, events


async def _running(app: App) -> None:
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING, within=3.0)


async def _event_id(store: MemoryStore) -> str:
    await eventually(lambda: bool(store._data.outbox.rows))
    return next(iter(store._data.outbox.rows.values())).entry.id


async def _await_marker(store: MemoryStore, event_id: str) -> None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while not await store.processed.exists(f"processor:default:{event_id}"):
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"no marker for {event_id}")
        await asyncio.sleep(0.005)


async def _await_letters(store: MemoryStore, count: int) -> list[DeadLetter]:
    deadline = asyncio.get_running_loop().time() + 3.0
    while True:
        letters = await store.dead_letters.list("default", 10)
        if len(letters) >= count:
            return letters
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"{count} dead letter(s) within 3 s, got {len(letters)}")
        await asyncio.sleep(0.005)


def test_a_range_is_start_inclusive_and_until_exclusive() -> None:
    ranges = [ReplayRange("s", "0", "2", "5")]
    for position, inside in (("1", False), ("2", True), ("4", True), ("5", False), ("6", False)):
        assert in_replay(delivery(position=position), ranges, _compare) is inside, position
    assert in_replay(delivery(position="3", segment="1"), ranges, _compare) is False, "another segment"
    assert in_replay(delivery(position="3"), [ReplayRange("s", "0", "5", "")], _compare) is False, "an open end"


def test_open_until_reports_the_furthest_end_a_segment_has_not_passed() -> None:
    ranges = [
        ReplayRange("s", "0", "0", "3"),
        ReplayRange("s", "0", "2", "7"),
        ReplayRange("s", "1", "0", "4"),
    ]
    segments = [
        SegmentStatus("0", position="1", committed="1", head="9", lag=8),
        SegmentStatus("1", position="4", committed="4", head="4", lag=0),
    ]
    until, replaying = open_until(ranges, segments, _compare)
    assert until == {"0": "7"}
    assert replaying is True
    assert passed_ranges(ranges, segments, _compare) == [ReplayRange("s", "1", "0", "4")]
    closed = [SegmentStatus("0", position="8", committed="8", head="8", lag=0)]
    assert open_until(ranges, closed, _compare) == ({}, False)


async def test_a_replay_skips_the_handlers_marked_for_no_replay(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    calls: list[str] = []

    @app.event_handler
    async def every_time(event: OrderPlaced) -> None:
        calls.append("every")

    @app.event_handler(replay=False)
    async def once(event: OrderPlaced) -> None:
        calls.append("once")

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    await eventually(lambda: calls == ["every", "once"])
    event_id = await _event_id(store)
    await _await_marker(store, event_id)

    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    assert isinstance(await processors.reset.execute("default", "op-1", Earliest()), ResetDone)
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await eventually(lambda: len(calls) == 3, within=3.0)
    assert calls == ["every", "once", "every"], "the replay skipped the replay=False handler"
    await _await_marker(store, event_id)
    assert await store.processed.exists(f"processor:default:{event_id}"), "the marker is written with no handler"


async def test_a_failing_replay_parks_a_letter_marked_as_a_replay(hub: FakeHub) -> None:
    app, store, _ = await _app(hub, attempts=1)
    attempt = 0
    fail = False

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal attempt
        attempt += 1
        if fail and attempt >= 2:
            raise RuntimeError("the replay cannot take it")

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    await eventually(lambda: attempt == 1)
    event_id = await _event_id(store)
    await _await_marker(store, event_id)

    fail = True
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    assert isinstance(await processors.reset.execute("default", "op-1", Earliest()), ResetDone)
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    (letter,) = await _await_letters(store, 1)
    assert letter.replay is True, "a letter parked during a replay is retried as one"
    assert letter.reason.value == "HANDLER_FAILED"

    fail = False
    attempt = 0
    outcome = await processors.dead_letters.retry(letter.id)
    assert (outcome.result, attempt) == ("Retried", 1)
    assert await store.dead_letters.get(letter.id) is None
    assert await store.processed.exists(f"processor:default:{event_id}"), "the retry recorded the marker"


async def test_a_marker_without_a_position_is_never_deleted_by_a_reset(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    async with store.transaction():
        assert await store.processed.record(
            ProcessedMarker(
                id="saga:payment:e-1", type_name="orders.PlaceOrder", processed_at_ms=1, processor="default"
            )
        )
    await app.start()
    await _running(app)
    processors = processors_of(app)
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    event_id = await _event_id(store)
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    done = await processors.reset.execute("default", "op-1", Earliest())
    assert isinstance(done, ResetDone)
    assert await store.processed.exists("saga:payment:e-1"), "a marker without a position is never deleted"
    assert not await store.processed.exists(f"processor:default:{event_id}")


async def test_the_status_reports_the_open_range_until_it_closes(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    calls = 0
    replaying = False
    entered = asyncio.Event()
    gate = asyncio.Event()

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        if replaying:
            entered.set()
            await gate.wait()

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await app.publish(OrderPlaced(order_id="o-1"), key="o-1")
    await eventually(lambda: calls == 1)
    replaying = True
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    assert isinstance(await processors.reset.execute("default", "op-1", Earliest()), ResetDone)
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await asyncio.wait_for(entered.wait(), 3.0)
    status = processors.statuses()[0]
    assert status.replaying is True
    assert status.state == hub_pb2.PROCESSOR_RUNNING
    assert status.segments[0].replay_until, "the segment's open end"
    gate.set()
    await eventually(lambda: calls == 2)
    await eventually(lambda: processors.statuses()[0].replaying is False, within=3.0)
    assert processors.statuses()[0].segments[0].replay_until == ""
    await _await_no_replay(store)


async def _await_no_replay(store: MemoryStore) -> None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while (await store.control.get("default")).replay:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("the closed range was not removed from the control document")
        await asyncio.sleep(0.005)
