"""Postponed annotations in handler signatures.

With ``from __future__ import annotations`` every annotation is a string. A saga handler written as
``ctx: snodra.SagaContext`` must register exactly like one that imported ``SagaContext`` directly, and the other
handler decorators must resolve a module-qualified message or reset context the same way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import ClassVar

import helpers
import pytest
import snodra
from fakehub import FakeHub
from helpers import new_app
from snodra._saga import SagaModel
from snodra._saga_runtime import SagaRuntime
from snodra._store_memory import MemoryStore
from snodra._timings import DEFAULT_TIMINGS


@snodra.message_type("test.PostponedStarted")
@dataclass(frozen=True)
class Started:
    order_id: str


@snodra.message_type("test.PostponedCommand")
@dataclass(frozen=True)
class Command:
    order_id: str


@snodra.message_type("test.PostponedEvent")
@dataclass(frozen=True)
class Happened:
    order_id: str


@snodra.message_type("test.PostponedCharge")
@dataclass(frozen=True)
class PostponedCharge:
    order_id: str


class PostponedSaga:
    kind: ClassVar[str] = "postponed"
    order_id: str = ""
    calls: int = 0
    deadline_calls: int = 0

    @snodra.saga_start("order_id")
    async def started(self, event: Started, ctx: snodra.SagaContext) -> None:
        self.order_id = event.order_id
        self.calls += 1
        ctx.send(Command(event.order_id), routing_key=event.order_id)
        ctx.schedule(1, "timeout")

    @snodra.saga_deadline("timeout")
    async def timed_out(self, ctx: snodra.SagaContext) -> None:
        self.deadline_calls += 1
        ctx.associate_with("timeout", "fired")


async def test_a_saga_accepts_postponed_snodra_context_annotations() -> None:
    from snodra._deadline import DeadlineScheduler

    clock = [1_000]
    store = MemoryStore(clock=lambda: clock[0])
    model = SagaModel()
    model.add(PostponedSaga, "test.PostponedSaga")
    timings = replace(
        DEFAULT_TIMINGS,
        saga_stage_max_attempts=5,
        saga_stage_initial_backoff=0.001,
        saga_stage_max_backoff=0.002,
    )
    runtime = SagaRuntime(model, store, snodra.JsonCodec(), clock=lambda: clock[0], timings=timings)

    await runtime.process(
        "test.PostponedStarted",
        b'{"orderId":"o-1"}',
        {"type": "test.PostponedStarted", "id": "evt-1", "correlationId": "c-1"},
        "evt-1",
    )

    live = await store.sagas.sagas.find_live("test.PostponedSaga", "order_id", "o-1")
    assert len(live) == 1
    state = json.loads(live[0].state)
    assert state["order_id"] == "o-1"
    assert state["calls"] == 1
    assert "kind" not in state, "a ClassVar is not part of the stored state"
    entry = await store.sagas.dispatch.next_pending(live[0].id)
    assert entry is not None
    assert entry.type_name == "test.PostponedCommand"

    scheduler = DeadlineScheduler(
        deadlines=store.sagas.deadlines, sagas=runtime, clock=lambda: clock[0], timings=timings
    )
    clock[0] = 2_000
    assert await scheduler.run_once()
    live = await store.sagas.sagas.find_live("test.PostponedSaga", "order_id", "o-1")
    assert len(live) == 1
    assert json.loads(live[0].state)["deadline_calls"] == 1
    assert ("timeout", "fired") in [(a.key, a.value) for a in live[0].associations], (
        "the context-only deadline handler got the context"
    )


async def test_an_idempotent_command_handler_accepts_postponed_annotations(hub: FakeHub) -> None:
    store = MemoryStore()
    app = new_app(hub, store=store, outbox_enabled=False)
    calls: list[str] = []

    @app.idempotent_command_handler
    async def charge(command: PostponedCharge) -> None:
        calls.append(command.order_id)

    await app.start()
    try:
        await app.send(
            PostponedCharge("o-1"),
            headers={"idempotency-key": "postponed-1"},
        )
        assert calls == ["o-1"]
    finally:
        await app.shutdown()


async def test_a_command_query_event_and_reset_handler_accept_postponed_annotations(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), outbox_enabled=False)

    @app.command_handler
    async def command(message: helpers.PlaceOrder) -> None:
        pass

    @app.query_handler
    async def query(message: helpers.GetOrder) -> None:
        pass

    @app.event_handler
    async def event(message: Happened) -> None:
        pass

    @app.reset_handler("mail")
    async def reset(ctx: snodra.ResetContext) -> None:
        pass


async def test_a_saga_still_accepts_a_directly_imported_context() -> None:
    from snodra import SagaContext

    class DirectSaga:
        order_id: str = ""

        @snodra.saga_start("order_id")
        async def started(self, event: Started, ctx: SagaContext) -> None:
            self.order_id = event.order_id

    model = SagaModel()
    assert model.add(DirectSaga, "test.DirectSaga") is DirectSaga


def test_a_saga_handler_rejects_a_parameter_that_is_not_a_context() -> None:
    class WrongSaga:
        @snodra.saga_start("order_id")
        async def started(self, event: Started, ctx: str) -> None:
            pass

    with pytest.raises(snodra.SagaError, match="must be a SagaContext"):
        SagaModel().add(WrongSaga, "test.WrongSaga")
