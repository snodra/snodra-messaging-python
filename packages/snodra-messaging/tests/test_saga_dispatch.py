"""The saga dispatch relay: order, outbox/command entries, retries, permanent failure and the notice."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any

from snodra import (
    ErrorCode,
    HubError,
    NeverSent,
    SagaCommandFailed,
    SagaContext,
    message_type,
    saga_start,
)
from snodra._saga import SagaModel
from snodra._saga_dispatch import SagaDispatchRelay
from snodra._saga_runtime import SagaRuntime
from snodra._store import DispatchKind, SagaDispatchDocument
from snodra._store_memory import MemoryStore
from snodra._timings import DEFAULT_TIMINGS


@message_type("test.Start")
@dataclass(frozen=True)
class Start:
    order_id: str


class Sender:
    local = None
    has_configured_hubs = True

    def __init__(self, outcomes: list[BaseException | None] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[tuple[str, dict[str, str], str | None]] = []

    async def call_payload(
        self,
        type_: str,
        payload: bytes,
        kind: str = "command",
        *,
        routing_key: str | None = None,
        headers: dict[str, str] | None = None,
        retry: object = None,
    ) -> None:
        self.calls.append((type_, dict(headers or {}), routing_key))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if outcome is not None:
                raise outcome


class RelaySaga:
    calls: int = 0

    @saga_start("order_id")
    async def started(self, event: Start, ctx: SagaContext) -> None:
        self.calls += 1


def _timings() -> Any:
    return replace(
        DEFAULT_TIMINGS,
        saga_dispatch_poll=0.001,
        saga_dispatch_initial_backoff=0.01,
        saga_dispatch_max_backoff=0.02,
        saga_dispatch_max_attempts=3,
        saga_dispatch_send_timeout=0.01,
        saga_notice_max_attempts=3,
    )


def _entry(
    saga_id: str, seq: int, kind: DispatchKind, *, type_name: str = "test.Charge", payload: str = '{"n":1}'
) -> SagaDispatchDocument:
    return SagaDispatchDocument(
        id=f"{saga_id}:{seq}",
        saga_id=saga_id,
        seq=seq,
        kind=kind,
        type_name=type_name,
        payload=payload,
        headers={"correlation-id": "c-1"},
        routing_key="o-1" if kind is DispatchKind.COMMAND else None,
        created_at_ms=1_000,
    )


async def _relay(sender: Sender, clock: Any = None, notice: Any = None) -> tuple[SagaDispatchRelay, MemoryStore]:
    store = MemoryStore(clock=clock) if clock is not None else MemoryStore()
    relay = SagaDispatchRelay(
        store=store,
        sender=sender,  # type: ignore[arg-type]
        codec=__import__("snodra").JsonCodec(),
        notice=notice or (lambda notice, headers: asyncio.sleep(0, result=True)),
        clock=clock or (lambda: 1_000),
        timings=_timings(),
        owner="relay-a",
    )
    return relay, store


async def test_commands_go_in_order_with_their_key_and_headers_and_the_event_goes_to_the_outbox() -> None:
    sender = Sender()
    relay, store = await _relay(sender)
    saga_id = "saga-1"
    command = _entry(saga_id, 0, DispatchKind.COMMAND)
    event = _entry(saga_id, 1, DispatchKind.EVENT, type_name="test.Done", payload='{"done":true}')
    async with store.transaction():
        await store.sagas.dispatch.record([event, command])
    await relay.run_once()
    assert [call[0] for call in sender.calls] == ["test.Charge"], "the event is not a command"
    _, headers, routing = sender.calls[0]
    assert headers["idempotency-key"] == command.id
    assert headers["saga-id"] == saga_id
    assert headers["correlationId"] == "c-1"
    assert routing == "o-1"
    assert await store.sagas.dispatch.next_pending(saga_id) is None
    outbox = await store.outbox.claim("relay-check", 60.0, 10)
    assert len(outbox) == 1
    assert outbox[0].id != event.id, "the outbox entry has an id of its own"
    assert outbox[0].type_name == "test.Done"
    assert outbox[0].key is None
    assert outbox[0].headers["correlationId"] == "c-1"


async def test_a_transient_failure_delays_the_saga_and_keeps_later_entries_waiting() -> None:
    clock = [1_000]
    sender = Sender([NeverSent("no hub")])
    relay, store = await _relay(sender, clock=lambda: clock[0])
    saga_id = "saga-2"
    command = _entry(saga_id, 0, DispatchKind.COMMAND)
    later = _entry(saga_id, 1, DispatchKind.COMMAND, type_name="test.Later")
    async with store.transaction():
        await store.sagas.dispatch.record([command, later])
    await relay.run_once()
    entry = await store.sagas.dispatch.next_pending(saga_id)
    assert entry is not None
    assert entry.attempts == 1
    assert entry.next_attempt_at_ms is not None
    assert entry.next_attempt_at_ms > clock[0]
    assert sender.calls == [("test.Charge", sender.calls[0][1], "o-1")]
    clock[0] = entry.next_attempt_at_ms
    await relay.run_once()
    assert [call[0] for call in sender.calls] == ["test.Charge", "test.Charge", "test.Later"]


async def test_a_permanent_failure_records_its_notice_and_the_later_entry_is_still_sent() -> None:
    sender = Sender([HubError(ErrorCode.PERMISSION_DENIED, "denied")])
    seen: list[SagaCommandFailed] = []

    async def notice(failure: SagaCommandFailed, headers: dict[str, str]) -> bool:
        seen.append(failure)
        return True

    relay, store = await _relay(sender, notice=notice)
    saga_id = "saga-3"
    command = _entry(saga_id, 0, DispatchKind.COMMAND)
    later = _entry(saga_id, 1, DispatchKind.COMMAND, type_name="test.Later")
    async with store.transaction():
        await store.sagas.dispatch.record([command, later])
    await relay.run_once()
    assert [call[0] for call in sender.calls] == ["test.Charge", "test.Later"]
    assert len(seen) == 1
    assert (seen[0].saga_id, seen[0].command_type, seen[0].idempotency_key, seen[0].reason) == (
        saga_id,
        "test.Charge",
        command.id,
        "DENIED",
    )
    entry = await store.sagas.dispatch.next_pending(saga_id)
    assert entry is None


async def test_a_retryable_handler_error_is_transient_and_an_unknown_outcome_is_permanent() -> None:
    retryable = HubError(ErrorCode.HANDLER_ERROR, "later", b'{"snodra":"retryable"}')
    sender = Sender([retryable])
    relay, store = await _relay(sender)
    saga_id = "saga-4"
    entry = _entry(saga_id, 0, DispatchKind.COMMAND)
    async with store.transaction():
        await store.sagas.dispatch.record([entry])
    await relay.run_once()
    pending = await store.sagas.dispatch.next_pending(saga_id)
    assert pending is not None
    assert pending.attempts == 1

    unknown = HubError(ErrorCode.HANDLER_ERROR, "unknown", b'{"snodra":"unknown-outcome"}')
    sender2 = Sender([unknown])
    relay2, store2 = await _relay(sender2)
    entry2 = _entry(saga_id, 0, DispatchKind.COMMAND)
    async with store2.transaction():
        await store2.sagas.dispatch.record([entry2])
    await relay2.run_once()
    assert await store2.sagas.dispatch.next_pending(saga_id) is None


async def test_a_deadline_fires_once_after_it_is_due_and_a_cancelled_one_never_fires() -> None:
    from snodra._deadline import DeadlineScheduler
    from snodra._saga import saga_deadline
    from snodra._store import DeadlineDocument, DeadlineStatus

    class DeadlineSaga:
        fired: int = 0

        @saga_start("order_id")
        async def started(self, event: Start, ctx: SagaContext) -> None:
            ctx.schedule(60, "timeout", {"why": "late"})

        @saga_deadline("timeout", end=True)
        async def timeout(self, payload: dict[str, str], ctx: SagaContext) -> None:
            self.fired += 1

    clock = [1_000]
    store = MemoryStore(clock=lambda: clock[0])
    model = SagaModel()
    model.add(DeadlineSaga)
    runtime = SagaRuntime(model, store, __import__("snodra").JsonCodec(), clock=lambda: clock[0], timings=_timings())
    await runtime.process("test.Start", b'{"orderId":"o-1"}', {"id": "e-1", "type": "test.Start"}, "e-1")
    definition = model.definitions()[0]
    saga = (await store.sagas.sagas.find_live(definition.name, "order_id", "o-1", tx=store.plain()))[0]
    scheduled = await store.sagas.deadlines.claim_due("early", 1_000, 2_000)
    assert scheduled is None, "not due yet"
    scheduler = DeadlineScheduler(
        deadlines=store.sagas.deadlines, sagas=runtime, clock=lambda: clock[0], timings=_timings()
    )
    clock[0] = 61_000
    assert await scheduler.run_once()
    stored = await store.sagas.sagas.find_by_id(saga.id, tx=store.plain())
    assert stored is not None
    assert stored.ended is True
    assert stored.version == 1
    assert await store.sagas.deadlines.claim_due("late", 61_000, 62_000) is None, "fired"

    # A cancelled deadline never fires, and an ended saga's deadline is cancelled.
    other = DeadlineDocument(
        id="dead-2",
        saga_type=definition.name,
        saga_id=saga.id,
        name="timeout",
        payload=None,
        payload_type=None,
        due_at_ms=62_000,
        status=DeadlineStatus.SCHEDULED,
        claimed_by=None,
        claimed_until_ms=None,
        finished_at_ms=None,
        correlation_id=None,
    )
    async with store.transaction():
        await store.sagas.deadlines.insert(other)
        await store.sagas.deadlines.cancel(saga.id, other.id, 61_000)
    clock[0] = 70_000
    assert await scheduler.run_once() is False


class StubbornSender(Sender):
    """A local send that ignores cancellation; the relay must abandon it as a transient failure."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()

    async def call_payload(
        self,
        type_: str,
        payload: bytes,
        kind: str = "command",
        *,
        routing_key: str | None = None,
        headers: dict[str, str] | None = None,
        retry: object = None,
    ) -> None:
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            await self.release.wait()


async def test_a_send_that_ignores_cancellation_is_abandoned_transiently() -> None:
    sender = StubbornSender()
    relay, store = await _relay(sender)
    saga_id = "saga-stubborn"
    entry = _entry(saga_id, 0, DispatchKind.COMMAND)
    async with store.transaction():
        await store.sagas.dispatch.record([entry])
    await relay.run_once()
    pending = await store.sagas.dispatch.next_pending(saga_id)
    assert pending is not None
    assert pending.attempts == 1, "an abandoned send is a transient attempt, never permanent"
    sender.release.set()
    await asyncio.sleep(0)


class HublessSender(Sender):
    """No configured hubs and no local handler."""

    has_configured_hubs = False


async def test_a_hubless_command_with_no_local_handler_is_no_handler_at_once() -> None:
    sender = HublessSender()
    relay, store = await _relay(sender)
    entry = _entry("saga-hubless", 0, DispatchKind.COMMAND)
    async with store.transaction():
        await store.sagas.dispatch.record([entry])
    await relay.run_once()
    assert await store.sagas.dispatch.next_pending("saga-hubless") is None
    failed = await store.sagas.dispatch.next_pending("saga-hubless")
    assert failed is None
    # The notice is pending; there is no second attempt and no retry time on the command.
    assert await store.sagas.dispatch.pending() == 0


async def test_transient_failures_end_as_exhausted_with_a_notice() -> None:
    clock = [1_000]
    sender = Sender([NeverSent("down"), NeverSent("down"), NeverSent("down")])
    relay, store = await _relay(sender, clock=lambda: clock[0])
    entry = _entry("saga-exhausted", 0, DispatchKind.COMMAND)
    async with store.transaction():
        await store.sagas.dispatch.record([entry])
    for _ in range(3):
        await relay.run_once()
        pending = await store.sagas.dispatch.next_pending("saga-exhausted")
        if pending is None:
            break
        assert pending.next_attempt_at_ms is not None
        clock[0] = pending.next_attempt_at_ms
    assert await store.sagas.dispatch.next_pending("saga-exhausted") is None
    assert await store.sagas.dispatch.pending() == 0


async def test_a_notice_that_cannot_be_delivered_is_retried_then_cleared() -> None:
    clock = [1_000]
    sender = Sender([HubError(ErrorCode.PERMISSION_DENIED, "denied")])
    attempts: list[int] = []

    async def notice(failure: SagaCommandFailed, headers: dict[str, str]) -> bool:
        attempts.append(1)
        return len(attempts) >= 2

    relay, store = await _relay(sender, clock=lambda: clock[0], notice=notice)
    entry = _entry("saga-notice", 0, DispatchKind.COMMAND)
    async with store.transaction():
        await store.sagas.dispatch.record([entry])
    await relay.run_once()
    assert len(attempts) == 1, "the first delivery failed and was retried"
    # Find the notice's next time and advance to it.
    row = next(iter(store._data.dispatch.rows.values()))
    assert row.notice_next_at_ms is not None
    clock[0] = row.notice_next_at_ms
    await relay.run_once()
    assert len(attempts) == 2
    cleared = next(iter(store._data.dispatch.rows.values()))
    assert not cleared.notice_pending
