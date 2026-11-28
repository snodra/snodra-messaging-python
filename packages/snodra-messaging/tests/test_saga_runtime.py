"""The saga model and stage 2: definitions, matching, state, dispatch records, conflicts and parking."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any

import pytest
from snodra import (
    SagaCommandFailed,
    SagaContext,
    SagaError,
    message_type,
    saga_command_failed,
    saga_deadline,
    saga_event,
    saga_start,
)
from snodra._saga import SagaModel
from snodra._saga_runtime import SagaRuntime
from snodra._store import DispatchKind
from snodra._store_memory import MemoryStore
from snodra._timings import DEFAULT_TIMINGS


@message_type("test.OrderPlaced")
@dataclass(frozen=True)
class OrderPlaced:
    order_id: str


@message_type("test.Paid")
@dataclass(frozen=True)
class Paid:
    order_id: str
    ref: str


@message_type("test.Charge")
@dataclass(frozen=True)
class Charge:
    order_id: str


@message_type("test.Done")
@dataclass(frozen=True)
class Done:
    order_id: str


@message_type("test.Finish")
@dataclass(frozen=True)
class Finish:
    order_id: str


class PaymentSaga:
    order_id: str = ""
    ref: str = ""
    calls: int = 0

    @saga_start("order_id")
    async def placed(self, event: OrderPlaced, ctx: SagaContext) -> None:
        self.order_id = event.order_id
        self.ref = f"pay-{event.order_id}"
        self.calls += 1
        ctx.associate_with("ref", self.ref)
        ctx.send(Charge(event.order_id), routing_key=event.order_id)
        ctx.publish(Done(event.order_id), key=event.order_id)
        ctx.schedule(60, "timeout")

    @saga_event("ref", end=True)
    async def paid(self, event: Paid, ctx: SagaContext) -> None:
        self.calls += 1

    @saga_deadline("timeout", end=True)
    async def timed_out(self, ctx: SagaContext) -> None:
        self.calls += 1

    @saga_command_failed
    async def failed(self, event: SagaCommandFailed, ctx: SagaContext) -> None:
        self.calls += 1


class BrokenSaga:
    @saga_start("order_id")
    async def placed(self, event: OrderPlaced, ctx: SagaContext) -> None:
        raise RuntimeError("boom")


def _model(*classes: type) -> SagaModel:
    model = SagaModel()
    for cls in classes:
        model.add(cls)
    return model


def _timings() -> Any:
    return replace(
        DEFAULT_TIMINGS,
        saga_stage_max_attempts=5,
        saga_stage_initial_backoff=0.001,
        saga_stage_max_backoff=0.002,
    )


def _headers(event_id: str = "evt-1", **more: str) -> dict[str, str]:
    return {"type": "test.OrderPlaced", "id": event_id, "correlationId": "c-1", **more}


async def _runtime(*classes: type) -> tuple[SagaRuntime, MemoryStore, SagaModel]:
    store = MemoryStore()
    model = _model(*classes)
    return SagaRuntime(model, store, __import__("snodra").JsonCodec(), timings=_timings()), store, model


async def test_a_start_handler_creates_the_instance_and_records_its_effects() -> None:
    runtime, store, model = await _runtime(PaymentSaga)
    payload = b'{"orderId":"o-1"}'
    assert await runtime.process("test.OrderPlaced", payload, _headers(), "evt-1")
    live = await store.sagas.sagas.find_live("test.PaymentSaga", "order_id", "o-1", tx=store.plain())
    # The default type name is the class's fully qualified name, not its short name.
    assert live == []
    definition = model.definitions()[0]
    assert definition is not None
    live = await store.sagas.sagas.find_live(definition.name, "order_id", "o-1", tx=store.plain())
    assert len(live) == 1
    saga = live[0]
    assert saga.version == 0
    assert "pay-o-1" in saga.state
    assert ("ref", "pay-o-1") in [(a.key, a.value) for a in saga.associations]
    entry = await store.sagas.dispatch.next_pending(saga.id)
    assert entry is not None
    assert entry.kind is DispatchKind.COMMAND
    assert entry.seq == 0
    assert entry.id == f"{saga.id}:0"
    assert entry.headers["correlation-id"] == "c-1"
    assert entry.routing_key == "o-1"
    assert (await store.sagas.deadlines.count_overdue_by_name(0)) == {}


async def test_a_redelivered_event_is_skipped_by_its_marker() -> None:
    runtime, store, _model = await _runtime(PaymentSaga)
    payload = b'{"orderId":"o-1"}'
    assert await runtime.process("test.OrderPlaced", payload, _headers(), "evt-1")
    count = await store.sagas.sagas.count_live_by_type()
    assert list(count.values()) == [1]
    await runtime.process("test.OrderPlaced", payload, _headers(), "evt-1")
    assert await store.sagas.sagas.count_live_by_type() == count
    assert await store.sagas.dispatch.pending() == 2


async def test_an_existing_instance_gets_the_event_by_association_and_ends() -> None:
    runtime, store, model = await _runtime(PaymentSaga)
    definition = model.definitions()[0]
    assert definition is not None
    await runtime.process("test.OrderPlaced", b'{"orderId":"o-1"}', _headers(), "evt-1")
    saga = (await store.sagas.sagas.find_live(definition.name, "order_id", "o-1", tx=store.plain()))[0]
    assert await runtime.process(
        "test.Paid",
        b'{"orderId":"o-1","ref":"pay-o-1"}',
        _headers("evt-2", type="test.Paid"),
        "evt-2",
    )
    stored = await store.sagas.sagas.find_by_id(saga.id, tx=store.plain())
    assert stored is not None
    assert stored.ended is True
    assert stored.version == 1
    assert stored.start_key is None
    assert await store.sagas.deadlines.count_overdue_by_name(0) == {}, "ending cancels the deadlines"


async def test_the_notice_reaches_the_instance_by_id() -> None:
    runtime, store, model = await _runtime(PaymentSaga)
    definition = model.definitions()[0]
    assert definition is not None
    await runtime.process("test.OrderPlaced", b'{"orderId":"o-1"}', _headers(), "evt-1")
    saga = (await store.sagas.sagas.find_live(definition.name, "order_id", "o-1", tx=store.plain()))[0]
    notice = SagaCommandFailed(
        saga_id=saga.id,
        command_type="test.Charge",
        idempotency_key=f"{saga.id}:0",
        reason="NO_HANDLER",
        message="nobody",
    )
    assert await runtime.process_notice(notice, {"correlation-id": "c-1"})
    stored = await store.sagas.sagas.find_by_id(saga.id, tx=store.plain())
    assert stored is not None
    assert stored.version == 1


async def test_a_failing_handler_is_parked_with_the_marker_after_two_attempts() -> None:
    runtime, store, model = await _runtime(BrokenSaga)
    assert await runtime.process("test.OrderPlaced", b'{"orderId":"o-1"}', _headers(), "evt-1")
    failures = await store.sagas.failures.list(tx=store.plain())
    assert len(failures) == 1
    assert failures[0].event_id == "evt-1"
    assert failures[0].attempts >= 2
    assert failures[0].event_type == "test.OrderPlaced"
    assert await store.sagas.markers.exists(f"saga:{model.definitions()[0].name}:evt-1", tx=store.plain())


async def test_a_definition_error_names_the_saga_and_the_handler() -> None:
    class NoStart:
        @saga_event("order_id")
        async def on(self, event: OrderPlaced) -> None:
            pass

    with pytest.raises(SagaError, match="NoStart"):
        _model(NoStart)

    class BadAssociation:
        @saga_start("missing")
        async def on(self, event: OrderPlaced) -> None:
            pass

    with pytest.raises(SagaError, match=r"BadAssociation\.on"):
        _model(BadAssociation)

    class BadState:
        value: bytes = b""

        @saga_start("order_id")
        async def on(self, event: OrderPlaced) -> None:
            pass

    # bytes are valid JSON (base64) in the codec, so the state round-trips; a non-serialisable field does not.
    class WorseState:
        value: object = object()

        @saga_start("order_id")
        async def on(self, event: OrderPlaced) -> None:
            pass

    with pytest.raises(SagaError, match="WorseState"):
        _model(WorseState)


async def test_a_second_runtime_continues_the_saga_on_the_same_store() -> None:
    store = MemoryStore()
    first_model = _model(PaymentSaga)
    second_model = _model(PaymentSaga)
    codec = __import__("snodra").JsonCodec()
    first = SagaRuntime(first_model, store, codec, timings=_timings())
    second = SagaRuntime(second_model, store, codec, timings=_timings())
    assert await first.process("test.OrderPlaced", b'{"orderId":"o-1"}', _headers(), "evt-1")
    definition = first_model.definitions()[0]
    saga = (await store.sagas.sagas.find_live(definition.name, "order_id", "o-1", tx=store.plain()))[0]
    assert await second.process("test.Paid", b'{"orderId":"o-1","ref":"pay-o-1"}', _headers("evt-2"), "evt-2")
    stored = await store.sagas.sagas.find_by_id(saga.id, tx=store.plain())
    assert stored is not None
    assert stored.ended is True


async def test_two_concurrent_deliveries_of_one_event_apply_it_once() -> None:
    runtime, store, model = await _runtime(PaymentSaga)
    payload = b'{"orderId":"o-1"}'
    await asyncio.gather(
        runtime.process("test.OrderPlaced", payload, _headers(), "evt-1"),
        runtime.process("test.OrderPlaced", payload, _headers(), "evt-1"),
    )
    assert await store.sagas.sagas.count_live_by_type() == {model.definitions()[0].name: 1}
    assert await store.sagas.dispatch.pending() == 2, "one command and one event, not two of each"
