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


_blocking_entered = asyncio.Event()
_blocking_release = asyncio.Event()


class BlockingSaga:
    @saga_start("order_id")
    async def started(self, event: AppStarted, ctx: SagaContext) -> None:
        _blocking_entered.set()
        await _blocking_release.wait()


async def test_a_blocking_saga_handler_does_not_stall_an_in_process_command(hub: Any) -> None:
    _blocking_entered.clear()
    _blocking_release.clear()
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
        await asyncio.wait_for(_blocking_entered.wait(), 2.0)
        assert await asyncio.wait_for(app.send(Ask("pong"), str), 2.0) == "pong"
        _blocking_release.set()
    finally:
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
