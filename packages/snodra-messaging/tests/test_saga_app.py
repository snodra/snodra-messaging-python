"""The app side of the saga stage: a saga-only service consumes events, and the stage runs before the ack."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest
from helpers import new_app
from snodra import (
    EventHeaders,
    MemoryEventStore,
    OutgoingEvent,
    SagaCommandFailed,
    SagaContext,
    message_type,
    saga_command_failed,
    saga_start,
)
from snodra._store_memory import MemoryStore


@message_type("test.AppStarted")
@dataclass(frozen=True)
class AppStarted:
    order_id: str


class AppSaga:
    seen: int = 0

    @saga_start("order_id")
    async def started(self, event: AppStarted, ctx: SagaContext) -> None:
        self.seen += 1


async def _event(source: str, event_id: str, order_id: str) -> OutgoingEvent:
    return OutgoingEvent(
        source=source,
        id=event_id,
        type_name="test.AppStarted",
        key=order_id,
        headers={
            EventHeaders.ID: event_id,
            EventHeaders.TYPE: "test.AppStarted",
            EventHeaders.CORRELATION_ID: "c-1",
            EventHeaders.SOURCE: "test",
        },
        payload=b'{"orderId":"' + order_id.encode() + b'"}',
    )


async def test_a_saga_only_service_consumes_events_and_records_its_state(hub: Any) -> None:
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.AppSaga")(AppSaga)
    await app.start()
    try:
        await events.publish(await _event("snodra.events", "evt-1", "o-1"))
        for _ in range(200):
            if await store.sagas.sagas.count_live_by_type():
                break
            await asyncio.sleep(0.01)
        assert await store.sagas.sagas.count_live_by_type() == {"test.AppSaga": 1}
    finally:
        await app.shutdown()


async def test_a_saga_without_the_stores_is_refused_at_start(hub: Any) -> None:
    app = new_app(hub)
    app.saga("test.AppSaga")(AppSaga)
    with pytest.raises(Exception, match="message store"):
        await app.start()


@message_type("test.Charge")
@dataclass(frozen=True)
class Charge:
    order_id: str


@message_type("test.Ask")
@dataclass(frozen=True)
class Ask:
    value: str


class SendingSaga:
    @saga_start("order_id")
    async def started(self, event: AppStarted, ctx: SagaContext) -> None:
        ctx.send(Charge(event.order_id), routing_key=event.order_id)


async def test_the_app_relay_sends_a_saga_command_after_the_handling_with_its_key(hub: Any) -> None:
    from snodra import current_message

    seen: list[tuple[str, str]] = []
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.SendingSaga")(SendingSaga)

    @app.command_handler
    async def charge(command: Charge) -> None:
        info = current_message()
        assert info is not None
        seen.append((command.order_id, info.headers.get("idempotency-key", "")))

    await app.start()
    try:
        await events.publish(await _event("snodra.events", "evt-1", "o-1"))
        for _ in range(200):
            if seen:
                break
            await asyncio.sleep(0.01)
        assert len(seen) == 1
        assert seen[0][0] == "o-1"
        assert seen[0][1].startswith(seen[0][1].rsplit(":", 1)[0] + ":")
    finally:
        await app.shutdown()


_blocking_entered: asyncio.Event | None = None
_blocking_release: asyncio.Event | None = None


class BlockingSaga:
    @saga_start("order_id")
    async def started(self, event: AppStarted, ctx: SagaContext) -> None:
        assert _blocking_entered is not None
        assert _blocking_release is not None
        _blocking_entered.set()
        await _blocking_release.wait()


async def test_a_blocking_saga_handler_does_not_stall_an_in_process_command(hub: Any) -> None:
    global _blocking_entered, _blocking_release
    _blocking_entered = asyncio.Event()
    _blocking_release = asyncio.Event()
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.BlockingSaga")(BlockingSaga)

    @app.command_handler
    async def ask(command: Ask) -> str:
        return command.value

    await app.start()
    try:
        await events.publish(await _event("snodra.events", "evt-1", "o-1"))
        assert _blocking_entered is not None
        assert _blocking_release is not None
        await asyncio.wait_for(_blocking_entered.wait(), 2.0)
        assert await asyncio.wait_for(app.send(Ask("pong"), str), 2.0) == "pong"
        _blocking_release.set()
    finally:
        if _blocking_release is not None:
            _blocking_release.set()
        await app.shutdown()


class FailingSaga:
    notices: int = 0

    @saga_start("order_id")
    async def started(self, event: AppStarted, ctx: SagaContext) -> None:
        ctx.send(Charge(event.order_id))

    @saga_command_failed
    async def failed(self, event: SagaCommandFailed) -> None:
        self.notices += 1


async def test_a_permanent_command_failure_sends_a_local_notice(hub: Any) -> None:
    from snodra import PermissionDenied

    ordinary: list[str] = []
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.FailingSaga")(FailingSaga)

    @app.command_handler
    async def charge(command: Charge) -> None:
        raise PermissionDenied("no caller")

    @app.event_handler
    async def notice(event: SagaCommandFailed) -> None:
        ordinary.append(type(event).__name__)

    await app.start()
    try:
        await events.publish(await _event("snodra.events", "evt-1", "o-1"))
        for _ in range(200):
            if ordinary:
                break
            await asyncio.sleep(0.01)
        assert ordinary == ["SagaCommandFailed"]
        live = await store.sagas.sagas.find_live("test.FailingSaga", "order_id", "o-1")
        assert len(live) == 1
        assert '"notices":1' in live[0].state, "the saga itself took the notice"
        assert (await store.sagas.dispatch.next_pending(live[0].id)) is None
    finally:
        await app.shutdown()


async def _saga_id(store: MemoryStore) -> str:
    for _ in range(200):
        live = await store.sagas.sagas.find_live("test.FailingSaga", "order_id", "o-1")
        if live:
            return live[0].id
        await asyncio.sleep(0.01)
    raise AssertionError("the saga did not start")


async def test_the_saga_stage_runs_after_stage_one_failed(hub: Any) -> None:
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
        dead_letter_mode="log",
    )
    app.saga("test.AppSaga")(AppSaga)

    @app.event_handler
    async def broken(event: AppStarted) -> None:
        raise RuntimeError("stage one failed")

    await app.start()
    try:
        await events.publish(await _event("snodra.events", "evt-1", "o-1"))
        for _ in range(200):
            if await store.sagas.sagas.count_live_by_type():
                break
            await asyncio.sleep(0.01)
        assert await store.sagas.sagas.count_live_by_type() == {"test.AppSaga": 1}
    finally:
        await app.shutdown()


async def test_app_send_and_publish_refuse_inside_a_saga_handler(hub: Any) -> None:
    from snodra import ConfigurationError
    from snodra._saga import saga_handling

    app = new_app(hub)
    with saga_handling():
        with pytest.raises(ConfigurationError, match="SagaContext"):
            await app.send(Ask("x"))
        with pytest.raises(ConfigurationError, match="SagaContext"):
            await app.publish(AppStarted("o-1"))


@message_type("test.Blocked")
@dataclass(frozen=True)
class Blocked:
    order_id: str


async def test_a_saga_sees_a_record_that_is_parked_sequenced(hub: Any) -> None:
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(
        hub,
        store=store,
        event_store=events,
        outbox_enabled=False,
        consumer_max_attempts=1,
    )
    app.saga("test.AppSaga")(AppSaga)

    @app.event_handler
    async def blocked(event: Blocked) -> None:
        raise RuntimeError("park this key")

    @app.event_handler
    async def started(event: AppStarted) -> None:
        pass

    await app.start()
    try:
        await events.publish(
            OutgoingEvent(
                source="snodra.events",
                id="blocked-1",
                type_name="test.Blocked",
                key="k1",
                headers={EventHeaders.ID: "blocked-1", EventHeaders.TYPE: "test.Blocked"},
                payload=b'{"orderId":"o-1"}',
            )
        )
        for _ in range(200):
            if await store.dead_letters.count() == 1:
                break
            await asyncio.sleep(0.01)
        assert await store.dead_letters.count() == 1
        await events.publish(
            OutgoingEvent(
                source="snodra.events",
                id="saga-1",
                type_name="test.AppStarted",
                key="k1",
                headers={
                    EventHeaders.ID: "saga-1",
                    EventHeaders.TYPE: "test.AppStarted",
                    EventHeaders.CORRELATION_ID: "c-1",
                },
                payload=b'{"orderId":"o-1"}',
            )
        )
        for _ in range(200):
            if await store.sagas.sagas.count_live_by_type():
                break
            await asyncio.sleep(0.01)
        assert await store.sagas.sagas.count_live_by_type() == {"test.AppSaga": 1}
        assert await store.dead_letters.count() == 2, "the saga event is still parked behind the key"
    finally:
        await app.shutdown()


async def test_a_saga_only_processor_lists_the_saga_event_types_in_its_status(hub: Any) -> None:
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.AppSaga")(AppSaga)
    await app.start()
    try:
        processors = app._processors
        assert processors is not None
        status = processors.statuses()[0]
        assert "test.AppStarted" in status.event_types
    finally:
        await app.shutdown()


async def test_a_replay_does_not_run_the_saga_stage(hub: Any) -> None:
    from snodra import Delivery

    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.AppSaga")(AppSaga)
    await app.start()
    try:
        processors = app._processors
        assert processors is not None
        consumer = processors.consumer("default")
        delivery = Delivery(
            segment="0",
            position="0",
            key="o-1",
            headers={"id": "evt-1", "type": "test.AppStarted"},
            payload=b'{"orderId":"o-1"}',
            timestamp_ms=0,
        )
        await consumer._saga_stage(delivery, "test.AppStarted", "evt-1", True)
        assert await store.sagas.sagas.count_live_by_type() == {}
    finally:
        await app.shutdown()


async def test_a_duplicate_stage_one_marker_does_not_repeat_the_saga(hub: Any) -> None:
    calls: list[str] = []
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.AppSaga")(AppSaga)

    @app.event_handler
    async def started(event: AppStarted) -> None:
        calls.append(event.order_id)

    await app.start()
    try:
        first = await _event("snodra.events", "dup-1", "o-1")
        second = await _event("snodra.events", "dup-1", "o-1")
        await events.publish(first)
        await events.publish(second)
        for _ in range(200):
            if await store.sagas.sagas.count_live_by_type():
                break
            await asyncio.sleep(0.01)
        assert calls == ["o-1"], "stage 1 ran once"
        assert await store.sagas.sagas.count_live_by_type() == {"test.AppSaga": 1}
    finally:
        await app.shutdown()


async def test_the_record_is_acknowledged_only_after_the_saga_stage(hub: Any) -> None:
    global _blocking_entered, _blocking_release
    _blocking_entered = asyncio.Event()
    _blocking_release = asyncio.Event()
    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.BlockingSaga")(BlockingSaga)
    await app.start()
    try:
        await events.publish(await _event("snodra.events", "ack-1", "o-1"))
        assert _blocking_entered is not None
        assert _blocking_release is not None
        await asyncio.wait_for(_blocking_entered.wait(), 2.0)
        processors = app._processors
        assert processors is not None
        consumer = processors.consumer("default")
        assert consumer._subscription is not None
        status = consumer._subscription.segments()[0]
        assert status.committed is None, "stage 2 still runs: the record is not acknowledged"
        _blocking_release.set()
        for _ in range(200):
            assert consumer._subscription is not None
            status = consumer._subscription.segments()[0]
            if status.committed == "1":
                break
            await asyncio.sleep(0.01)
        assert status.committed == "1"
    finally:
        if _blocking_release is not None:
            _blocking_release.set()
        await app.shutdown()
    from helpers import assert_no_library_tasks

    assert_no_library_tasks()


async def test_a_hub_command_carries_the_saga_headers_and_no_authorization(hub: Any) -> None:
    from helpers import acknowledged
    from snodra._hubpb import hub_pb2

    store = MemoryStore()
    events = MemoryEventStore()
    app = new_app(hub, store=store, event_store=events, outbox_enabled=False)
    app.saga("test.SendingSaga")(SendingSaga)
    await app.start()
    try:
        stream = await acknowledged(hub)
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        event = await _event("snodra.events", "hub-1", "o-1")
        event.headers[EventHeaders.TRACEPARENT] = traceparent
        await events.publish(event)
        request = await stream.recv_request()
        assert request.type == "test.Charge"
        assert request.routing_key == "o-1"
        assert request.headers["idempotency-key"].endswith(":0")
        assert request.headers["saga-id"] in request.headers["idempotency-key"]
        assert request.headers["correlationId"] == "c-1"
        assert request.headers["traceparent"] == traceparent
        assert "authorization" not in request.headers
        await stream.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=request.id, payload=b"{}")))
        for _ in range(200):
            if await store.sagas.dispatch.next_pending(next(iter(store._data.sagas.rows))) is None:
                break
            await asyncio.sleep(0.01)
    finally:
        await app.shutdown()
