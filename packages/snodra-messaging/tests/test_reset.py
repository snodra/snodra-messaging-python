"""The leased reset: prepare, execute, reset handlers, finish, expiry and the refusals.

Checks: L3-RESET.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control") and
https://snodra.damoreira.com/docs/server/processor-control/.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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
    ResetContext,
    Unsupported,
)
from snodra._eventstore import (
    EventStoreAdmin,
    EventStoreSubscription,
    OutgoingEvent,
    ReplayRange,
    SubscriptionSpec,
)
from snodra._hubpb import hub_pb2
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
    hub: FakeHub, *, clock: Callable[[], int] | None = None, segments: int = 2
) -> tuple[App, MemoryStore, MemoryEventStore]:
    store = MemoryStore()
    events = MemoryEventStore(segments=segments)
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_poll_interval=0.01,
        _timings=_timings(),
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
