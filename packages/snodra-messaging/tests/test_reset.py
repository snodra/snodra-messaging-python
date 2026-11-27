"""The leased reset: prepare, execute, reset handlers, finish, expiry and the refusals.

Checks: L3-RESET.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control") and
https://snodra.damoreira.com/docs/server/processor-control/.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import replace

from eventhelpers import Clock, OrderPlaced
from fakehub import FakeHub
from helpers import SCALED, acknowledged, eventually, new_app, processors_of
from snodra import (
    App,
    Capability,
    Earliest,
    EventStore,
    Latest,
    MemoryEventStore,
    MemoryStore,
    Positions,
    ProcessorControlState,
    ProcessorControlStore,
    ResetContext,
    Timestamp,
    Unsupported,
)
from snodra._eventstore import (
    EventStoreAdmin,
    EventStoreSubscription,
    OutgoingEvent,
    ReplayRange,
    ResetTarget,
    SubscriptionSpec,
)
from snodra._hubpb import hub_pb2
from snodra._processors import Processors
from snodra._reset import ResetDone, ResetOk, ResetRefused, merged, replaying
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
        "status_interval": 300.0,
    }
    values.update(changes)
    return replace(SCALED, **values)  # type: ignore[arg-type]


async def _app(
    hub: FakeHub,
    *,
    clock: Callable[[], int] | None = None,
    event_clock: Callable[[], int] | None = None,
    segments: int = 2,
    instance: str = "i-1",
    timings: Timings | None = None,
) -> tuple[App, MemoryStore, MemoryEventStore]:
    store = MemoryStore()
    events = MemoryEventStore(segments=segments, clock=event_clock)
    app = new_app(
        hub,
        instance=instance,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        _timings=_timings() if timings is None else timings,
        _clock=clock,
    )
    return app, store, events


async def _running(app: App) -> None:
    await eventually(lambda: processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_RUNNING, within=3.0)


async def _publish(app: App, *ids: str) -> None:
    for order_id in ids:
        await app.publish(OrderPlaced(order_id=order_id), key=order_id)


async def _handle_until(seen: list[str], count: int) -> None:
    await eventually(lambda: len(seen) >= count, within=3.0)


async def test_prepare_takes_the_lock_leaves_the_subscription_and_a_late_renewal_is_refused(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    reset = processors.reset
    prepared = await reset.prepare("default", "op-1", 30_000)
    assert isinstance(prepared, ResetOk), prepared
    state = await store.control.get("default")
    assert state.operation is not None
    assert state.operation.id == "op-1"
    assert processors.consumer("default").running is False
    status = processors.statuses()[0]
    assert (status.state, status.operation_id) == (hub_pb2.PROCESSOR_STOPPED, "op-1")

    locked = await reset.prepare("default", "op-2", 30_000)
    assert isinstance(locked, ResetRefused)
    assert locked.code == "LOCKED"
    assert "op-1" in locked.message
    renewed = await reset.prepare("default", "op-1", 30_000)
    assert isinstance(renewed, ResetOk)
    assert renewed.detail == "lease renewed"
    assert await reset.prepare("default", "op-1", 999) == ResetRefused(
        "INVALID", "a prepare needs an operation id and a lease of at least 1000 ms"
    )

    finished = await reset.finish("default", "op-1")
    assert isinstance(finished, ResetOk)
    assert (await store.control.get("default")).operation is None
    await _running(app)
    late = await reset.prepare("default", "op-1", 30_000)
    assert isinstance(late, ResetRefused)
    assert (late.code, "already finished" in late.message) == ("NOT_PREPARED", True)
    assert isinstance(await reset.finish("default", "op-1"), ResetOk)  # idempotent
    await _publish(app, "o-1")
    await _handle_until(seen, 1)


async def test_execute_to_earliest_replays_the_history_deletes_the_markers_and_closes_the_range(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    reset = processors.reset
    await _publish(app, "o-1", "o-2", "o-3")
    await _handle_until(seen, 3)
    ids = [row.entry.id for row in store._data.outbox.rows.values()]
    markers = {f"processor:default:{event_id}" for event_id in ids}
    for marker in markers:
        assert await store.processed.exists(marker)

    assert isinstance(await reset.prepare("default", "op-1", 30_000), ResetOk)
    done = await reset.execute("default", "op-1", Earliest())
    assert isinstance(done, ResetDone), done
    assert done.markers_deleted == 3
    assert done.reset_handlers == 0
    assert done.ranges, "earliest replays the acknowledged history"
    assert all(replay_range.until for replay_range in done.ranges)
    for marker in markers:
        assert not await store.processed.exists(marker), f"the reset deleted {marker}"
    document = await store.control.get("default")
    assert document.replay, "the replay ranges are stored"
    assert processors.consumer("default").replay_ranges == document.replay

    assert isinstance(await reset.finish("default", "op-1"), ResetOk)
    await _handle_until(seen, 6)
    assert seen == ["o-1", "o-2", "o-3", "o-1", "o-2", "o-3"], "each acknowledged event replayed once, in order"
    # The range closes once the committed positions reach its end, and the poll removes it.
    await eventually(lambda: processors.consumer("default").replay_ranges == (), within=3.0)
    await eventually(lambda: processors.statuses()[0].replaying is False)
    assert (await store.control.get("default")).replay == ()
    # A record outside the range is handled normally.
    await _publish(app, "o-4")
    await _handle_until(seen, 7)
    assert seen[-1] == "o-4"


async def test_a_reset_handler_runs_before_any_marker_or_position_moves(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    markers_seen: list[bool] = []
    replay_seen: list[tuple[ReplayRange, ...]] = []
    contexts: list[ResetContext] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    @app.reset_handler
    async def before(context: ResetContext) -> None:
        assert isinstance(context, ResetContext)
        contexts.append(context)
        assert await store.processed.exists(f"processor:default:{event_id}")
        replay_seen.append((await store.control.get("default")).replay)
        markers_seen.append(True)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)

    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    done = await processors.reset.execute("default", "op-1", Earliest())
    assert isinstance(done, ResetDone), done
    assert done.reset_handlers == 1
    assert markers_seen == [True], "the marker existed when the handler ran"
    assert replay_seen == [()], "no replay range was written when the handler ran"
    assert contexts
    assert (contexts[0].processor, contexts[0].operation_id) == ("default", "op-1")
    assert contexts[0].target == Earliest()
    assert not await store.processed.exists(f"processor:default:{event_id}")


async def _await_marker(store: MemoryStore, event_id: str) -> None:
    deadline = asyncio.get_running_loop().time() + 3.0
    while not await store.processed.exists(f"processor:default:{event_id}"):
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"no marker processor:default:{event_id} within 3 s")
        await asyncio.sleep(0.005)


async def test_a_failing_reset_handler_changes_nothing_and_says_so(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    @app.reset_handler("default")
    async def cannot_clear(context: ResetContext) -> None:
        raise RuntimeError("the projection cannot be cleared")

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)

    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    refused = await processors.reset.execute("default", "op-1", Earliest())
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "RESET_HANDLER_FAILED"
    assert "cannot_clear failed: RuntimeError: the projection cannot be cleared" in refused.message
    assert "reset handlers ran (1), no markers deleted, positions not moved: retry the reset" in refused.message
    assert await store.processed.exists(f"processor:default:{event_id}"), "the marker did not move"
    assert (await store.control.get("default")).replay == ()
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await _running(app)


async def test_execute_refuses_while_another_instance_reads_the_subscription(hub: FakeHub) -> None:
    app, store, events = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)

    other = await events.subscribe(SubscriptionSpec("orders-service.default", "snodra.events"))
    try:
        refused = await processors.reset.execute("default", "op-1", Earliest())
        assert isinstance(refused, ResetRefused)
        assert refused.code == "SUBSCRIBERS_ACTIVE"
        assert "still has 1 subscriber" in refused.message
        assert await store.processed.exists(f"processor:default:{event_id}"), "nothing moved"
        assert (await store.control.get("default")).replay == ()
    finally:
        await other.close()
    assert isinstance(await processors.reset.execute("default", "op-1", Earliest()), ResetDone)
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


async def test_latest_replays_nothing_and_moves_the_positions(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1", "o-2")
    await _handle_until(seen, 2)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    done = await processors.reset.execute("default", "op-1", Latest())
    assert isinstance(done, ResetDone)
    assert (done.ranges, done.markers_deleted, done.reset_handlers) == ((), 0, 0)
    assert (await store.control.get("default")).replay == ()
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await _publish(app, "o-3")
    await _handle_until(seen, 3)
    assert seen == ["o-1", "o-2", "o-3"], "latest replays nothing"


async def test_a_passed_lease_releases_the_lock_and_consumes_again_by_itself(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(hub, clock=clock)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 1_000), ResetOk)
    assert processors.consumer("default").running is False
    clock.advance(1.5)
    await eventually(lambda: processors.consumer("default").running, within=2.0)
    await _running(app)
    assert processors.statuses()[0].operation_id == ""
    assert (await store.control.get("default")).operation is None
    await _publish(app, "o-1")
    await _handle_until(seen, 1)


async def test_a_renewal_whose_lock_is_gone_is_not_prepared(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    await store.control.release("default", "op-1", NOW)  # another instance's finish
    late = await processors.reset.prepare("default", "op-1", 30_000)
    assert isinstance(late, ResetRefused)
    assert (late.code, "lock lost" in late.message) == ("NOT_PREPARED", True)
    assert processors.reset.held("default") is None


class _NoReplay:
    """A level-2 event store: no replay capability, no admin."""

    name = "no-replay"
    capabilities = frozenset({Capability.LAG})

    def __init__(self, inner: MemoryEventStore) -> None:
        self._inner = inner

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    async def publish(self, event: OutgoingEvent) -> None:
        await self._inner.publish(event)

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        return await self._inner.subscribe(spec)

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        raise Unsupported(Capability.REPLAY)

    def compare(self, a: str, b: str) -> int:
        return self._inner.compare(a, b)


async def test_a_store_without_replay_refuses_prepare_and_execute_as_unsupported(hub: FakeHub) -> None:
    store = MemoryStore()
    events: EventStore = _NoReplay(MemoryEventStore())
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False, _timings=_timings())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    refused = await processors.reset.prepare("default", "op-1", 30_000)
    assert isinstance(refused, ResetRefused)
    assert refused.code == "UNSUPPORTED"
    assert "REPLAY" in refused.message.upper()
    refused = await processors.reset.execute("default", "op-1", Earliest())
    assert isinstance(refused, ResetRefused)
    assert refused.code == "UNSUPPORTED"
    assert (await store.control.get("default")).operation is None, "nothing was locked"


async def test_a_paused_processor_stays_paused_after_the_reset(hub: FakeHub) -> None:
    app, _, _ = await _app(hub)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await _handle_until(seen, 1)
    await processors.pause("default")
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    assert isinstance(await processors.reset.execute("default", "op-1", Earliest()), ResetDone)
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await eventually(lambda: processors.consumer("default").running)
    assert processors.consumer("default").paused is True
    assert processors.statuses()[0].state == hub_pb2.PROCESSOR_PAUSED
    assert processors.consumer("default").replay_ranges, "the replay waits for the resume"
    await processors.resume("default")
    await _handle_until(seen, 2)


async def test_the_reset_control_answers_on_the_hub_stream(hub: FakeHub) -> None:
    app, _, _ = await _app(hub)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    await _publish(app, "o-1")
    await _handle_until(seen, 1)
    s = await acknowledged(hub)

    def request(action: str, **fields: object) -> hub_pb2.ControlRequest:
        return hub_pb2.ControlRequest(
            control_id=f"ctl-{action}",
            processor="default",
            **{action: fields or {}},
        )

    await s.send(hub_pb2.ServerMessage(control=request("reset_prepare", operation_id="op-1", lease_ms=30_000)))
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result")) == ("ctl-reset_prepare", "ok")
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="ctl-execute",
                processor="default",
                reset_execute=hub_pb2.ResetExecute(operation_id="op-1", target=hub_pb2.ResetTarget(earliest=True)),
            )
        )
    )
    reply = await s.recv_control_reply()
    assert reply.WhichOneof("result") == "reset"
    assert reply.reset.markers_deleted == 1
    assert len(reply.reset.ranges) >= 1
    assert reply.reset.ranges[0].segment
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="ctl-finish", processor="default", reset_finish=hub_pb2.ResetFinish(operation_id="op-1")
            )
        )
    )
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result")) == ("ctl-finish", "ok")
    await _handle_until(seen, 2)
    assert seen == ["o-1", "o-1"]


def test_reset_ranges_extend_an_open_range_and_merge_by_segment() -> None:
    from snodra._eventstore import ReplayRange

    def compare(a: str, b: str) -> int:
        return (int(a) > int(b)) - (int(a) < int(b))

    moves = [
        ReplayRange("s", "1", "4", "9"),
        ReplayRange("s", "0", "0", "2"),
        ReplayRange("s", "2", "3", "3"),
    ]
    open_ranges = [ReplayRange("s", "1", "0", "6"), ReplayRange("s", "3", "1", "2")]
    ranges = replaying(moves, Earliest(), open_ranges, compare)
    assert [(r.segment, r.start, r.until) for r in ranges] == [("0", "0", "2"), ("1", "4", "9")]
    assert replaying(moves, Latest(), open_ranges, compare) == ()
    merged_ranges = merged(open_ranges, ranges, compare)
    assert [(r.segment, r.start, r.until) for r in merged_ranges] == [
        ("0", "0", "2"),
        ("1", "4", "9"),
        ("3", "1", "2"),
    ]


async def test_prepare_finishes_the_record_in_hand_before_leaving(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    entered = asyncio.Event()
    gate = asyncio.Event()
    handled: list[str] = []

    @app.event_handler
    async def slow(event: OrderPlaced) -> None:
        entered.set()
        await gate.wait()
        handled.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await asyncio.wait_for(entered.wait(), 3.0)
    prepare = asyncio.create_task(processors.reset.prepare("default", "op-1", 30_000))
    await asyncio.sleep(0)
    assert not prepare.done(), "prepare waits for the record in hand before leaving"
    gate.set()
    reply = await asyncio.wait_for(prepare, 3.0)
    assert isinstance(reply, ResetOk)
    assert handled == ["o-1"], "the record in hand finished and was acknowledged"
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert await store.processed.exists(f"processor:default:{event_id}"), "its transaction committed before the leave"


async def test_a_positions_target_that_names_a_segment_twice_is_invalid(hub: FakeHub) -> None:
    app, _, _ = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    refused = await processors.reset.execute("default", "op-1", Positions((("0", "0"), ("0", "1"))))
    assert isinstance(refused, ResetRefused)
    assert (refused.code, "lists no segment or names one twice" in refused.message) == ("INVALID", True)
    refused = await processors.reset.execute("default", "op-1", Positions(()))
    assert isinstance(refused, ResetRefused)
    assert refused.code == "INVALID"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


async def test_a_renewal_and_a_dead_letter_list_are_answered_beside_a_running_execute(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)
    entered = asyncio.Event()
    gate = asyncio.Event()
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    @app.reset_handler
    async def slow(context: ResetContext) -> None:
        entered.set()
        await gate.wait()

    await app.start()
    await _running(app)
    await _publish(app, "o-1")
    await _handle_until(seen, 1)
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)

    s = await acknowledged(hub)

    def control(control_id: str, action: str, **fields: object) -> hub_pb2.ServerMessage:
        return hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(control_id=control_id, processor="default", **{action: fields or {}})
        )

    await s.send(control("p-1", "reset_prepare", operation_id="op-1", lease_ms=30_000))
    assert (await s.recv_control_reply()).WhichOneof("result") == "ok"
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="x-1",
                processor="default",
                reset_execute=hub_pb2.ResetExecute(operation_id="op-1", target=hub_pb2.ResetTarget(earliest=True)),
            )
        )
    )
    await asyncio.wait_for(entered.wait(), 3.0)
    await s.send(control("p-2", "reset_prepare", operation_id="op-1", lease_ms=30_000))
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="l-1", processor="default", dead_letter_list=hub_pb2.DeadLetterList(limit=10)
            )
        )
    )
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="r-1",
                processor="default",
                dead_letter_retry=hub_pb2.DeadLetterRetry(letter_id="default:snodra.events:0:0"),
            )
        )
    )
    replies = {reply.control_id: reply for reply in [await s.recv_control_reply(within=2.0) for _ in range(3)]}
    assert (replies["p-2"].WhichOneof("result"), replies["p-2"].ok.detail) == ("ok", "lease renewed")
    assert replies["l-1"].WhichOneof("result") == "dead_letters"
    assert replies["l-1"].dead_letters.total == 0
    assert (replies["r-1"].WhichOneof("result"), replies["r-1"].refused.code) == (
        "refused",
        hub_pb2.CONTROL_REFUSAL_LOCKED,
    )

    gate.set()
    execute = await s.recv_control_reply(within=3.0)
    assert (execute.control_id, execute.WhichOneof("result")) == ("x-1", "reset")
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="f-1", processor="default", reset_finish=hub_pb2.ResetFinish(operation_id="op-1")
            )
        )
    )
    assert (await s.recv_control_reply()).WhichOneof("result") == "ok"


async def _await_state(
    store: MemoryStore,
    processor: str,
    predicate: Callable[[ProcessorControlState], bool],
    within: float = 3.0,
) -> ProcessorControlState:
    deadline = asyncio.get_running_loop().time() + within
    while True:
        state = await store.control.get(processor)
        if predicate(state):
            return state
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"the control state of {processor} did not match within {within} s: {state}")
        await asyncio.sleep(0.005)


class _GetFailing:
    """A control store whose reads fail while ``fail`` is set."""

    def __init__(self, inner: ProcessorControlStore) -> None:
        self._inner = inner
        self.fail = False

    async def get(self, processor: str) -> ProcessorControlState:
        if self.fail:
            raise RuntimeError("the control store is down")
        return await self._inner.get(processor)

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


class _DelayedExpire:
    """A control store whose expire reports after the caller releases it, so a prepare can land meanwhile."""

    def __init__(self, inner: ProcessorControlStore) -> None:
        self._inner = inner
        self.expiring = asyncio.Event()
        self.go = asyncio.Event()

    async def expire(self, processor: str, operation: str, now_ms: int) -> bool:
        expired = await self._inner.expire(processor, operation, now_ms)
        if expired:
            self.expiring.set()
            await self.go.wait()
        return expired

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


async def test_finish_after_a_takeover_is_not_prepared_and_names_the_new_holder(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(hub, clock=clock, timings=_timings(processor_control_poll=5.0))

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    assert await store.control.release("default", "op-1", NOW)
    assert (await store.control.lock("default", "op-2", NOW + 30_000, NOW)).taken
    refused = await processors.reset.finish("default", "op-1")
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "NOT_PREPARED"
    assert "op-2" in refused.message
    await eventually(lambda: processors.statuses()[0].operation_id == "op-2")
    assert processors.consumer("default").running is False
    assert isinstance(await processors.reset.finish("default", "op-2"), ResetOk)
    await _running(app)


async def test_finish_whose_lock_cannot_be_read_says_it_resumes_once_read_back(hub: FakeHub) -> None:
    store = MemoryStore()
    control = _GetFailing(store.control)
    store.control = control  # type: ignore[assignment]
    events = MemoryEventStore(segments=2)
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        _timings=_timings(),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    control.fail = True
    released = await processors.reset.finish("default", "op-1")
    assert isinstance(released, ResetOk), released
    assert released.detail == "released; consuming once the lock is read back"
    assert processors.consumer("default").running is False
    control.fail = False
    await eventually(lambda: processors.consumer("default").running, within=3.0)
    await _running(app)


async def test_a_stale_poll_does_not_undo_a_prepare_that_landed_meanwhile(hub: FakeHub) -> None:
    clock = Clock(NOW)
    inner = MemoryStore()
    control = _DelayedExpire(inner.control)
    inner.control = control  # type: ignore[assignment]
    events = MemoryEventStore(segments=2)
    app = new_app(
        hub,
        store=inner,
        event_store=events,
        outbox_poll_interval=0.01,
        _timings=_timings(processor_control_poll=5.0),
        _clock=lambda: clock.now_ms,
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 1_000), ResetOk)
    clock.advance(1.5)  # op-1's lease passed: the poll expires it
    poll = asyncio.create_task(processors._read_and_apply(initial=False))
    await asyncio.wait_for(control.expiring.wait(), 3.0)
    # A prepare takes op-2 while the poll still waits on the expired op-1's expiry.
    assert isinstance(await processors.reset.prepare("default", "op-2", 30_000), ResetOk)
    control.go.set()
    await asyncio.wait_for(poll, 3.0)
    assert processors.reset.held("default") == "op-2", "the stale poll dropped the new operation"
    assert processors.consumer("default").running is False, "the stale poll started the consumer under op-2"
    await eventually(lambda: processors.statuses()[0].operation_id == "op-2")
    assert processors.statuses()[0].state == hub_pb2.PROCESSOR_STOPPED
    assert isinstance(await processors.reset.finish("default", "op-2"), ResetOk)
    await _running(app)


async def test_an_execute_that_outlives_its_limit_loses_the_lease_before_the_markers(hub: FakeHub) -> None:
    app, store, _ = await _app(hub, timings=_timings(reset_execute_limit=0.05))
    entered = asyncio.Event()
    gate = asyncio.Event()

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    @app.reset_handler
    async def slow(context: ResetContext) -> None:
        entered.set()
        await gate.wait()

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 1_000), ResetOk)
    execute = asyncio.create_task(processors.reset.execute("default", "op-1", Earliest()))
    await asyncio.wait_for(entered.wait(), 3.0)
    await _await_polls(processors, 3)  # past the 50 ms execute limit
    assert processors.consumer("default").running is False, "a poll restarted the consumer while the execute ran"
    gate.set()
    refused = await asyncio.wait_for(execute, 3.0)
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "FAILED"
    assert "ran longer than" in refused.message
    assert "reset handlers ran (1)" in refused.message
    assert "positions not moved" in refused.message
    assert await store.processed.exists(f"processor:default:{event_id}"), "the marker moved"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


async def test_the_execute_extends_its_lease_while_it_runs(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(
        hub, clock=clock, timings=_timings(reset_lease_extension=0.01, processor_control_poll=5.0)
    )
    entered = asyncio.Event()
    gate = asyncio.Event()

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    @app.reset_handler
    async def slow(context: ResetContext) -> None:
        entered.set()
        await gate.wait()

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 1_000), ResetOk)
    initial = (await store.control.get("default")).operation
    assert initial is not None
    clock.advance(0.5)
    execute = asyncio.create_task(processors.reset.execute("default", "op-1", Earliest()))
    await asyncio.wait_for(entered.wait(), 3.0)
    await _await_state(
        store,
        "default",
        lambda state: state.operation is not None and state.operation.lease_until_ms > initial.lease_until_ms,
    )
    gate.set()
    assert isinstance(await asyncio.wait_for(execute, 3.0), ResetDone)
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


async def test_a_lease_lost_after_the_reset_handlers_stops_before_the_markers(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(hub, clock=clock, timings=_timings())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    @app.reset_handler
    async def lose(context: ResetContext) -> None:
        await store.control.release("default", context.operation_id, clock.now_ms)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    refused = await processors.reset.execute("default", "op-1", Earliest())
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "FAILED"
    assert "lease lost" in refused.message
    assert "reset handlers ran (1)" in refused.message
    assert "positions not moved" in refused.message
    assert await store.processed.exists(f"processor:default:{event_id}"), "a lost lease deleted the marker"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


async def test_a_timestamp_reset_replays_only_from_its_time(hub: FakeHub) -> None:
    clock = Clock(NOW)
    event_clock = Clock(NOW)
    app, store, _ = await _app(hub, clock=clock, event_clock=event_clock, segments=1)
    seen: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        seen.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await _handle_until(seen, 1)
    event_clock.advance(1.0)
    await _publish(app, "o-2")
    await _handle_until(seen, 2)
    ids = [row.entry.id for row in store._data.outbox.rows.values()]
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    done = await processors.reset.execute("default", "op-1", Timestamp(NOW + 1_000))
    assert isinstance(done, ResetDone), done
    assert [(r.segment, r.start, r.until) for r in done.ranges] == [("0", "1", "2")]
    assert done.markers_deleted == 1
    assert not await store.processed.exists(f"processor:default:{ids[1]}")
    assert await store.processed.exists(f"processor:default:{ids[0]}"), "before the time: its marker stays"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await _handle_until(seen, 3)
    assert seen == ["o-1", "o-2", "o-2"], "only the record at or after the time replayed"


async def test_a_named_processor_reset_leaves_the_default_processor_alone(hub: FakeHub) -> None:
    app, store, _ = await _app(hub, segments=1)
    seen: list[str] = []

    @app.event_handler
    async def in_default(event: OrderPlaced) -> None:
        seen.append("default")

    @app.event_handler(processor="mail")
    async def in_mail(event: OrderPlaced) -> None:
        seen.append("mail")

    await app.start()
    await _publish(app, "o-1")
    await eventually(lambda: len(seen) == 2)
    processors = processors_of(app)
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    default_marker = await store.processed.exists(f"processor:default:{event_id}")
    assert default_marker
    assert isinstance(await processors.reset.prepare("mail", "op-1", 30_000), ResetOk)
    done = await processors.reset.execute("mail", "op-1", Earliest())
    assert isinstance(done, ResetDone), done
    assert await store.processed.exists(f"processor:default:{event_id}"), "the default processor's marker was deleted"
    assert not await store.processed.exists(f"processor:mail:{event_id}")
    assert isinstance(await processors.reset.finish("mail", "op-1"), ResetOk)
    await eventually(lambda: len(seen) == 3, within=3.0)
    assert sorted(seen) == ["default", "mail", "mail"], "only the mail processor replayed"


async def test_a_prepare_that_cannot_finish_the_record_cancels_it_and_the_record_comes_again(hub: FakeHub) -> None:
    app, store, _ = await _app(hub, timings=_timings(handler_drain=0.05))
    entered = asyncio.Event()
    gate = asyncio.Event()
    block = True
    handled: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal block
        entered.set()
        if block:
            await gate.wait()
        handled.append(event.order_id)

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await asyncio.wait_for(entered.wait(), 3.0)
    reply = await asyncio.wait_for(processors.reset.prepare("default", "op-1", 30_000), 3.0)
    assert isinstance(reply, ResetOk)
    assert handled == [], "the record in hand was cancelled before its handler finished"
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    assert not await store.processed.exists(f"processor:default:{event_id}"), "a cancelled record left its marker"
    block = False
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await eventually(lambda: handled == ["o-1"], within=3.0)


async def test_two_instances_share_a_reset_and_the_non_executor_applies_it_paused(hub: FakeHub) -> None:
    store = MemoryStore()
    events = MemoryEventStore(segments=1)
    normal: list[str] = []
    no_replay: list[str] = []
    timings = _timings(processor_control_poll=5.0)

    def build(instance: str, *, with_no_replay: bool) -> App:
        app = new_app(
            hub,
            instance=instance,
            store=store,
            event_store=events,
            outbox_poll_interval=0.01,
            _timings=timings,
        )

        @app.event_handler
        async def every(event: OrderPlaced) -> None:
            normal.append(f"{instance}:{event.order_id}")

        if with_no_replay:

            @app.event_handler(replay=False)
            async def once(event: OrderPlaced) -> None:
                no_replay.append(f"{instance}:{event.order_id}")

        return app

    app2 = build("i-2", with_no_replay=True)
    app1 = build("i-1", with_no_replay=False)
    await app2.start()
    await _running(app2)
    await app1.start()
    await _running(app1)
    await _publish(app2, "o-1", "o-2")
    await eventually(lambda: len(normal) == 2, within=3.0)
    assert sorted(normal) == ["i-2:o-1", "i-2:o-2"], "the non-executor holds the single segment"
    assert no_replay == ["i-2:o-1", "i-2:o-2"]

    reset2 = processors_of(app2).reset
    reset1 = processors_of(app1).reset
    assert isinstance(await reset2.prepare("default", "op-1", 30_000), ResetOk)
    assert isinstance(await reset1.prepare("default", "op-1", 30_000), ResetOk)
    done = await reset1.execute("default", "op-1", Earliest())
    assert isinstance(done, ResetDone), done
    assert done.markers_deleted == 2
    await app1.shutdown(timeout=2.0)
    await store.control.set_paused("default", True, "test", 1_793_700_000_000)
    assert isinstance(await reset2.finish("default", "op-1"), ResetOk)
    processors2 = processors_of(app2)
    await eventually(
        lambda: (
            processors2.statuses()[0].state == hub_pb2.PROCESSOR_PAUSED and processors2.statuses()[0].operation_id == ""
        ),
        within=3.0,
    )
    assert processors2.consumer("default").paused is True
    assert processors2.consumer("default").replay_ranges, "the persisted replay ranges reached the non-executor"
    assert len(normal) == 2, "nothing replayed before the resume"
    await processors2.resume("default")
    await eventually(lambda: len(normal) == 4, within=3.0)
    assert no_replay == ["i-2:o-1", "i-2:o-2"], "the replay skipped the replay=False handler"


async def test_finish_releases_an_expired_document_entry(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(hub, clock=clock)
    await store.control.lock("default", "op-1", NOW + 1_000, NOW)
    clock.advance(2.0)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert processors.reset.held("default") is None
    assert (await store.control.get("default")).operation is not None, "the expired entry is still stored"
    reply = await processors.reset.finish("default", "op-1")
    assert isinstance(reply, ResetOk)
    assert reply.detail == "no operation holds the lock"
    assert (await store.control.get("default")).operation is None, "the expired entry was released"
    await _running(app)


async def test_a_prepare_trims_the_operation_id(hub: FakeHub) -> None:
    app, store, _ = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "  op-1  ", 30_000), ResetOk)
    state = await store.control.get("default")
    assert state.operation is not None
    assert state.operation.id == "op-1"
    assert processors.reset.held("default") == "op-1"
    assert isinstance(await processors.reset.finish("default", " op-1 "), ResetOk)


async def test_a_renewal_stops_a_consumer_that_started_meanwhile(hub: FakeHub) -> None:
    app, _, _ = await _app(hub)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    await processors.set_running("default", True)  # a stale start, as a poll that read the old document
    assert processors.consumer("default").running is True
    renewed = await processors.reset.prepare("default", "op-1", 30_000)
    assert isinstance(renewed, ResetOk)
    await eventually(lambda: processors.consumer("default").running is False)
    assert processors.statuses()[0].operation_id == "op-1"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


class _FlipMembersAdmin:
    """A reset side whose first members check is empty and whose second names a reader that rejoined."""

    def __init__(self, inner: EventStoreAdmin) -> None:
        self._inner = inner
        self.calls = 0
        self.applied = 0

    async def plan(self, target: ResetTarget) -> list[ReplayRange]:
        return await self._inner.plan(target)

    async def members(self) -> list[str]:
        self.calls += 1
        if self.calls >= 2:
            return ["a-reader"]
        return list(await self._inner.members())

    async def apply(self, ranges: Sequence[ReplayRange]) -> None:
        self.applied += 1
        await self._inner.apply(ranges)


class _FlipMembersStore:
    def __init__(self, inner: MemoryEventStore) -> None:
        self._inner = inner
        self.admin_instance: _FlipMembersAdmin | None = None

    @property
    def name(self) -> str:
        return self._inner.name

    @property
    def capabilities(self) -> frozenset[Capability]:
        return self._inner.capabilities

    def supports(self, capability: Capability) -> bool:
        return self._inner.supports(capability)

    async def publish(self, event: OutgoingEvent) -> None:
        await self._inner.publish(event)

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        return await self._inner.subscribe(spec)

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        self.admin_instance = _FlipMembersAdmin(self._inner.admin(spec))
        return self.admin_instance

    def compare(self, a: str, b: str) -> int:
        return self._inner.compare(a, b)


async def test_a_reader_that_rejoins_between_the_plan_and_the_move_stops_the_reset(hub: FakeHub) -> None:
    store = MemoryStore()
    events = _FlipMembersStore(MemoryEventStore(segments=1))
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        _timings=_timings(),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await eventually(lambda: bool(store._data.outbox.rows))
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    refused = await processors.reset.execute("default", "op-1", Earliest())
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "SUBSCRIBERS_ACTIVE"
    assert "rejoined" in refused.message or "a-reader" in refused.message
    assert "positions not moved" in refused.message
    assert events.admin_instance is not None
    assert events.admin_instance.applied == 0, "the positions moved while a reader was back"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)


async def _await_polls(processors: Processors, count: int, within: float = 3.0) -> None:
    """Waits for ``count`` more control polls, never for a fixed time."""
    target = processors._polls + count
    await eventually(lambda: processors._polls >= target, within=within)


class _TakeoverOnRelease:
    """A control store where a new operation takes the lock between a finish's read and its release."""

    def __init__(self, inner: ProcessorControlStore) -> None:
        self._inner = inner

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        await self._inner.lock(processor, "op-2", now_ms + 30_000, now_ms)
        return await self._inner.release(processor, operation, now_ms)

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


async def test_a_finish_without_the_lock_sees_a_takeover_before_saying_nothing_holds_it(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(hub, clock=clock, timings=_timings(processor_control_poll=5.0))
    await store.control.lock("default", "op-1", NOW + 1_000, NOW)
    control = _TakeoverOnRelease(store.control)
    store.control = control  # type: ignore[assignment]

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await eventually(lambda: processors_of(app).statuses()[0].operation_id == "op-1")
    assert processors_of(app).consumer("default").running is False
    clock.advance(2.0)
    refused = await processors_of(app).reset.finish("default", "op-1")
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "NOT_PREPARED"
    assert "op-2" in refused.message
    await eventually(lambda: processors_of(app).statuses()[0].operation_id == "op-2")
    assert processors_of(app).consumer("default").running is False
    assert isinstance(await processors_of(app).reset.finish("default", "op-2"), ResetOk)


class _TakeoverAfterRelease:
    """A control store where a new operation takes the lock right after a successful release."""

    def __init__(self, inner: ProcessorControlStore) -> None:
        self._inner = inner
        self.enabled = False

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        released = await self._inner.release(processor, operation, now_ms)
        if released and self.enabled:
            self.enabled = False
            await self._inner.lock(processor, "op-2", now_ms + 30_000, now_ms)
        return released

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


async def test_a_release_followed_by_a_remote_takeover_does_not_claim_consuming(hub: FakeHub) -> None:
    app, store, _ = await _app(hub, timings=_timings(processor_control_poll=5.0))
    control = _TakeoverAfterRelease(store.control)
    store.control = control  # type: ignore[assignment]

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await _running(app)
    processors = processors_of(app)
    assert isinstance(await processors.reset.prepare("default", "op-1", 30_000), ResetOk)
    control.enabled = True
    released = await processors.reset.finish("default", "op-1")
    assert isinstance(released, ResetOk), released
    assert released.detail == "released; consuming once the lock is read back"
    assert processors.consumer("default").running is False
    await eventually(lambda: processors.statuses()[0].operation_id == "op-2")
    assert processors.statuses()[0].state == hub_pb2.PROCESSOR_STOPPED
    assert isinstance(await processors.reset.finish("default", "op-2"), ResetOk)


class _ExtendFailing:
    """A control store whose lease extensions fail while ``fail`` is set."""

    def __init__(self, inner: ProcessorControlStore) -> None:
        self._inner = inner
        self.fail = False

    async def extend(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> bool:
        if self.fail:
            return False
        return await self._inner.extend(processor, operation, lease_until_ms, now_ms)

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


async def test_a_lease_that_passes_during_an_execute_keeps_the_consumer_out(hub: FakeHub) -> None:
    clock = Clock(NOW)
    app, store, _ = await _app(
        hub, clock=clock, timings=_timings(reset_lease_extension=5.0, processor_control_poll=0.05)
    )
    control = _ExtendFailing(store.control)
    store.control = control  # type: ignore[assignment]
    entered = asyncio.Event()
    gate = asyncio.Event()
    handled: list[str] = []

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        handled.append(event.order_id)

    @app.reset_handler
    async def slow(context: ResetContext) -> None:
        entered.set()
        await gate.wait()

    await app.start()
    await _running(app)
    processors = processors_of(app)
    await _publish(app, "o-1")
    await _handle_until(handled, 1)
    event_id = next(iter(store._data.outbox.rows.values())).entry.id
    await _await_marker(store, event_id)
    assert isinstance(await processors.reset.prepare("default", "op-1", 1_000), ResetOk)
    control.fail = True
    execute = asyncio.create_task(processors.reset.execute("default", "op-1", Earliest()))
    await asyncio.wait_for(entered.wait(), 3.0)
    clock.advance(2.0)  # the persisted lease passes while the reset handler still runs
    await _await_polls(processors, 3)
    status = processors.statuses()[0]
    assert (status.state, status.operation_id) == (hub_pb2.PROCESSOR_STOPPED, "op-1")
    assert processors.consumer("default").running is False
    await _publish(app, "o-2")
    await _await_polls(processors, 2)
    assert handled == ["o-1"], "a record was read under a lost reset lock"
    gate.set()
    refused = await asyncio.wait_for(execute, 3.0)
    assert isinstance(refused, ResetRefused), refused
    assert refused.code == "FAILED"
    assert "lease lost" in refused.message
    assert await store.processed.exists(f"processor:default:{event_id}"), "a lost lease deleted the marker"
    assert isinstance(await processors.reset.finish("default", "op-1"), ResetOk)
    await _handle_until(handled, 2)
    assert handled == ["o-1", "o-2"]
