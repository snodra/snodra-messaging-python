"""The idempotent external-call adapter, its two markers and the startup checks around it.

The record is the message store's ``external_calls``: the first insert wins, a repeat after success returns the first
reply, a permanent failure replays, a lapsed call is taken over or given up as unknown, and every settlement is
conditional on the owner. The adapter's own writes never join the caller's transaction.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from typing import Any, cast

import pytest
from fakehub import FakeHub, forward
from helpers import SCALED, acknowledged, new_app, next_reply
from snodra import (
    App,
    ConfigurationError,
    ErrorCode,
    EventHeaders,
    ExternalCall,
    ExternalCallStatus,
    HandlerError,
    HubError,
    MemoryEventStore,
    MemoryStore,
    MessageStoreUnsupported,
    OutgoingEvent,
    RegistrationClosed,
    Retryable,
    SagaContext,
    UnknownOutcome,
    is_retryable,
    is_unknown_outcome,
    message_type,
    saga_start,
)
from snodra._hubpb import hub_pb2
from snodra._store import SagaStores


@message_type("test.ChargeCard")
@dataclass(frozen=True)
class Charge:
    order_id: str
    mode: str = ""


@message_type("test.ChargeReceipt")
@dataclass(frozen=True)
class Receipt:
    order_id: str
    ticket: int


@message_type("test.OrderStarted")
@dataclass(frozen=True)
class OrderStarted:
    order_id: str


def _app(hub: FakeHub, store: MemoryStore | None = None, **settings: Any) -> App:
    return new_app(hub, store=MemoryStore() if store is None else store, outbox_enabled=False, **settings)


async def _send(app: App, key: str | None = "job-1", *, mode: str = "") -> Receipt:
    headers = None if key is None else {"idempotency-key": key}
    return await app.send(Charge("c-1", mode), Receipt, headers=headers)


async def test_external_call_idempotency_returns_first_result(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        first = await _send(app, "job-1")
        second = await _send(app, "job-1")
        assert first == second == Receipt("c-1", 1)
        assert bodies == 1
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.SUCCEEDED
        assert (record.attempts, record.owner, record.lease_until_ms) == (1, None, None)
        assert record.reply is not None
        assert json.loads(record.reply) == {"orderId": "c-1", "ticket": 1}
        assert (await _send(app, "job-2")).ticket == 2
        assert bodies == 2
    finally:
        await app.shutdown()


async def test_the_first_external_call_wins(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    entered = asyncio.Event()
    release = asyncio.Event()
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        entered.set()
        await release.wait()
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        first = asyncio.create_task(_send(app, "job-1"))
        await asyncio.wait_for(entered.wait(), 2.0)
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.IN_PROGRESS
        assert record.owner is not None
        assert record.lease_until_ms is not None
        with pytest.raises(HubError) as refused:
            await _send(app, "job-1")
        assert is_retryable(refused.value)
        assert refused.value.details == b'{"snodra":"retryable"}'
        release.set()
        assert (await first).ticket == 1
        assert bodies == 1
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.SUCCEEDED
    finally:
        release.set()
        await app.shutdown()


async def test_a_failed_external_call_replays_the_stored_error(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        raise RuntimeError("card declined")

    await app.start()
    try:
        with pytest.raises(HubError) as first:
            await _send(app, "job-1")
        assert first.value.code is ErrorCode.HANDLER_ERROR
        assert "card declined" in first.value.message
        with pytest.raises(HubError) as second:
            await _send(app, "job-1")
        assert second.value.message == first.value.message
        assert bodies == 1
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.FAILED
        assert record.error is not None
        stored = json.loads(record.error)
        assert stored["code"] == "HANDLER_ERROR"
        assert "card declined" in stored["message"]
    finally:
        await app.shutdown()


async def test_a_retryable_external_call_runs_again(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        if bodies == 1:
            raise Retryable("partner busy")
        return Receipt(command.order_id, 7)

    await app.start()
    try:
        with pytest.raises(HubError) as first:
            await _send(app, "job-1")
        assert is_retryable(first.value)
        assert first.value.details == b'{"snodra":"retryable"}'
        assert await store.external.find("job-1") is None, "a retryable failure resets the record"
        assert (await _send(app, "job-1")).ticket == 7
        assert bodies == 2
    finally:
        await app.shutdown()


async def test_an_unknown_external_outcome_stays_unknown(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        raise UnknownOutcome("connection reset after the request")

    await app.start()
    try:
        with pytest.raises(HubError) as first:
            await _send(app, "job-1")
        assert is_unknown_outcome(first.value)
        assert first.value.details == b'{"snodra":"unknown-outcome"}'
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.UNKNOWN
        with pytest.raises(HubError) as second:
            await _send(app, "job-1")
        assert is_unknown_outcome(second.value)
        assert bodies == 1
    finally:
        await app.shutdown()


async def test_a_lapsed_call_is_given_up_as_unknown(hub: FakeHub) -> None:
    store = MemoryStore()
    t0 = 1_000
    now = [t0 + 120_000]
    await store.external.insert(
        ExternalCall(
            key="job-1",
            status=ExternalCallStatus.IN_PROGRESS,
            attempts=1,
            updated_at_ms=t0,
            lease_until_ms=t0 + 60_000,
            owner="old-call",
        )
    )
    app = _app(hub, store, _clock=lambda: now[0])
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        with pytest.raises(HubError) as refused:
            await _send(app, "job-1")
        assert is_unknown_outcome(refused.value)
        assert bodies == 0
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.UNKNOWN
    finally:
        await app.shutdown()


async def test_a_lapsed_call_is_taken_over_when_the_partner_is_idempotent(hub: FakeHub) -> None:
    store = MemoryStore()
    t0 = 1_000
    now = [t0 + 120_000]
    await store.external.insert(
        ExternalCall(
            key="job-1",
            status=ExternalCallStatus.IN_PROGRESS,
            attempts=1,
            updated_at_ms=t0,
            lease_until_ms=t0 + 60_000,
            owner="old-call",
        )
    )
    app = _app(hub, store, _clock=lambda: now[0])
    bodies = 0

    @app.idempotent_command_handler(external_idempotent=True)
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        receipt = await _send(app, "job-1")
        assert receipt.ticket == 1
        assert bodies == 1
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.SUCCEEDED
        assert record.attempts == 2
    finally:
        await app.shutdown()


async def test_a_missing_idempotency_key_fails_before_the_record_and_body(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        with pytest.raises(HubError) as refused:
            await _send(app, None)
        assert "idempotency-key" in refused.value.message
        assert refused.value.details == b""
        assert bodies == 0
        assert not store._data.external.rows
        with pytest.raises(HubError):
            await _send(app, "   ")
        assert bodies == 0
    finally:
        await app.shutdown()


async def test_an_external_call_record_survives_the_callers_rollback(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        first: Receipt | None = None

        async def roll_back() -> None:
            nonlocal first
            async with store.transaction():
                first = await _send(app, "job-1")
                raise RuntimeError("roll back")

        with pytest.raises(RuntimeError, match="roll back"):
            await roll_back()
        assert first is not None
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.SUCCEEDED
        assert await _send(app, "job-1") == first
        assert bodies == 1
    finally:
        await app.shutdown()


class _RacingCalls:
    """A stale read: the record is settled by another call before the adapter acts on what it read."""

    def __init__(self, inner: Any, key: str, reply: str, now: int) -> None:
        self._inner = inner
        self._key = key
        self._reply = reply
        self._now = now
        self._raced = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def find(self, key: str) -> ExternalCall | None:
        record: ExternalCall | None = await self._inner.find(key)
        if record is not None and not self._raced:
            self._raced = True
            assert await self._inner.succeed(self._key, record.owner or "", self._reply, self._now)
        return record


@pytest.mark.parametrize("partner", [False, True])
async def test_a_lapsed_call_is_not_settled_over_a_changed_record(hub: FakeHub, partner: bool) -> None:
    store = MemoryStore()
    t0 = 1_000
    now = t0 + 120_000
    await store.external.insert(
        ExternalCall(
            key="job-1",
            status=ExternalCallStatus.IN_PROGRESS,
            attempts=1,
            updated_at_ms=t0,
            lease_until_ms=t0 + 60_000,
            owner="old-call",
        )
    )
    store.external = _RacingCalls(store.external, "job-1", '{"orderId":"c-1","ticket":42}', now)
    app = _app(hub, store, _clock=lambda: now)
    bodies = 0

    @app.idempotent_command_handler(external_idempotent=partner)
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        assert (await _send(app, "job-1")).ticket == 42
        assert bodies == 0
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.SUCCEEDED
    finally:
        await app.shutdown()


class _LosingSucceedCalls:
    """The body returns but its SUCCEEDED write cannot be recorded."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def succeed(self, key: str, owner: str, reply: str, now_ms: int) -> bool:
        return False


@pytest.mark.parametrize("partner", [False, True])
async def test_an_unrecorded_external_success_is_unknown_or_retryable(hub: FakeHub, partner: bool) -> None:
    store = MemoryStore()
    base = store.external
    store.external = _LosingSucceedCalls(base)
    app = _app(hub, store)
    bodies = 0

    @app.idempotent_command_handler(external_idempotent=partner)
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        with pytest.raises(HubError) as refused:
            await _send(app, "job-1")
        if partner:
            assert is_retryable(refused.value)
            record = await base.find("job-1")
            assert record is not None
            assert record.status is ExternalCallStatus.IN_PROGRESS
        else:
            assert is_unknown_outcome(refused.value)
            record = await base.find("job-1")
            assert record is not None
            assert record.status is ExternalCallStatus.UNKNOWN
        assert bodies == 1
    finally:
        await app.shutdown()


async def test_the_two_markers_are_written_in_process(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        if command.mode == "retryable":
            raise Retryable("partner busy")
        raise UnknownOutcome("connection reset")

    await app.start()
    try:
        with pytest.raises(HubError) as retryable:
            await _send(app, "marker-r", mode="retryable")
        assert retryable.value.code is ErrorCode.HANDLER_ERROR
        assert retryable.value.details == b'{"snodra":"retryable"}'
        assert is_retryable(retryable.value)
        assert not is_unknown_outcome(retryable.value)
        with pytest.raises(HubError) as unknown:
            await _send(app, "marker-u", mode="unknown")
        assert unknown.value.code is ErrorCode.HANDLER_ERROR
        assert unknown.value.details == b'{"snodra":"unknown-outcome"}'
        assert is_unknown_outcome(unknown.value)
        assert not is_retryable(unknown.value)
    finally:
        await app.shutdown()


async def test_the_two_markers_are_written_through_the_hub(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        if command.mode == "retryable":
            raise Retryable("partner busy")
        raise UnknownOutcome("connection reset")

    await app.start()
    try:
        stream = await acknowledged(hub)
        for index, (mode, marker) in enumerate(
            [
                ("retryable", b'{"snodra":"retryable"}'),
                ("unknown", b'{"snodra":"unknown-outcome"}'),
            ]
        ):
            request = hub_pb2.Request(
                id=f"r-{index}",
                kind=hub_pb2.COMMAND,
                type="test.ChargeCard",
                payload=json.dumps({"orderId": "c-1", "mode": mode}).encode(),
                headers={"idempotency-key": f"hub-{index}"},
            )
            await stream.send(forward(request))
            reply = await next_reply(stream)
            assert reply.WhichOneof("result") == "error"
            assert reply.error.code == int(ErrorCode.HANDLER_ERROR)
            assert reply.error.details == marker
    finally:
        await app.shutdown()


async def test_the_two_markers_are_read_from_a_hub(hub: FakeHub) -> None:
    app = _app(hub)
    await app.start()
    try:
        stream = await acknowledged(hub)
        for index, (marker, check) in enumerate(
            [
                (b'{"snodra":"retryable"}', is_retryable),
                (b'{"snodra":"unknown-outcome"}', is_unknown_outcome),
            ]
        ):
            sending = asyncio.create_task(_send(app, f"read-{index}"))
            request = await stream.recv_request()
            await stream.send(
                hub_pb2.ServerMessage(
                    reply=hub_pb2.Reply(
                        request_id=request.id,
                        error=hub_pb2.Error(
                            request_id=request.id,
                            code=int(ErrorCode.HANDLER_ERROR),  # type: ignore[arg-type]
                            message="busy",
                            details=marker,
                        ),
                    )
                )
            )
            with pytest.raises(HubError) as refused:
                await sending
            assert refused.value.details == marker
            assert check(refused.value)
    finally:
        await app.shutdown()


def test_markers_are_read_with_whitespace_trimmed_and_through_causes() -> None:
    retryable = HubError(ErrorCode.HANDLER_ERROR, "busy", b' {"snodra":"retryable"}\n')
    unknown = HubError(ErrorCode.HANDLER_ERROR, "lost", b'\t{"snodra":"unknown-outcome"} ')
    assert is_retryable(retryable)
    assert not is_unknown_outcome(retryable)
    assert is_unknown_outcome(unknown)
    assert not is_retryable(unknown)
    assert is_retryable(HandlerError("busy", details={"snodra": "retryable"}))
    assert not is_retryable(RuntimeError("retryable"))
    nested = RuntimeError("outer")
    nested.__cause__ = Retryable("busy")
    assert is_retryable(nested)
    assert not is_unknown_outcome(nested)


async def test_an_adapter_panic_is_an_unknown_outcome(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    bodies = 0

    class Panic(BaseException):
        pass

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        raise Panic("adapter exploded")

    await app.start()
    try:
        with pytest.raises(HubError) as refused:
            await _send(app, "job-1")
        assert is_unknown_outcome(refused.value)
        assert refused.value.details == b'{"snodra":"unknown-outcome"}'
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.UNKNOWN
        assert bodies == 1
    finally:
        await app.shutdown()


class _FailingCalls:
    """One external-call operation fails, before the body."""

    def __init__(self, inner: Any, op: str, error: BaseException) -> None:
        self._inner = inner
        self._op = op
        self._error = error

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def insert(self, call: ExternalCall) -> bool:
        if self._op == "insert":
            raise self._error
        return cast("bool", await self._inner.insert(call))

    async def find(self, key: str) -> ExternalCall | None:
        if self._op == "find":
            raise self._error
        return cast("ExternalCall | None", await self._inner.find(key))

    async def take_over(self, seen: ExternalCall, owner: str, lease_until_ms: int, now_ms: int) -> ExternalCall | None:
        if self._op == "takeover":
            raise self._error
        return cast("ExternalCall | None", await self._inner.take_over(seen, owner, lease_until_ms, now_ms))

    async def expire(self, seen: ExternalCall, now_ms: int) -> bool:
        if self._op == "expire":
            raise self._error
        return cast("bool", await self._inner.expire(seen, now_ms))


@pytest.mark.parametrize(
    ("op", "seed", "partner"),
    [("insert", False, False), ("find", True, False), ("takeover", True, True), ("expire", True, False)],
)
async def test_store_failures_before_the_body_are_plain_handler_errors(
    hub: FakeHub, op: str, seed: bool, partner: bool
) -> None:
    from snodra._saga_failure import classify

    store = MemoryStore()
    t0 = 1_000
    now = t0 + 120_000
    if seed:
        await store.external.insert(
            ExternalCall(
                key="job-1",
                status=ExternalCallStatus.IN_PROGRESS,
                attempts=1,
                updated_at_ms=t0,
                lease_until_ms=t0 + 60_000,
                owner="old-call",
            )
        )
    store.external = _FailingCalls(store.external, op, RuntimeError("store unavailable"))
    app = _app(hub, store, _clock=lambda: now)
    bodies = 0

    @app.idempotent_command_handler(external_idempotent=partner)
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    # No start: the startup probe reads the port too, and this failure is the call's.
    with pytest.raises(HubError) as refused:
        await _send(app, "job-1")
    assert refused.value.code is ErrorCode.HANDLER_ERROR
    assert refused.value.message == "store unavailable"
    assert refused.value.details == b""
    assert not is_retryable(refused.value)
    assert not is_unknown_outcome(refused.value)
    assert bodies == 0
    classification = classify(refused.value)
    assert not classification.transient
    assert classification.reason == "FAILED"


async def test_problems_with_the_external_call_port_are_refused_at_start(hub: FakeHub) -> None:
    no_store = new_app(hub, outbox_enabled=False)

    @no_store.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        return Receipt(command.order_id, 1)

    with pytest.raises(ConfigurationError, match="message store"):
        await no_store.start()

    store = MemoryStore()
    store.external = None  # type: ignore[assignment]
    app = _app(hub, store)

    @app.idempotent_command_handler
    async def charge_again(command: Charge) -> Receipt:
        return Receipt(command.order_id, 1)

    with pytest.raises(ConfigurationError, match="external calls"):
        await app.start()

    unsupported = MemoryStore()

    class _Unsupported:
        async def find(self, key: str) -> ExternalCall | None:
            raise MessageStoreUnsupported("external calls")

    unsupported.external = _Unsupported()  # type: ignore[assignment]
    third = _app(hub, unsupported)

    @third.idempotent_command_handler
    async def charge_third(command: Charge) -> Receipt:
        return Receipt(command.order_id, 1)

    with pytest.raises(ConfigurationError, match="external calls"):
        await third.start()


async def test_an_idempotent_only_service_starts_without_an_event_store(hub: FakeHub) -> None:
    store = MemoryStore()
    app = new_app(hub, store=store, outbox_enabled=False)
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    await app.start()
    try:
        assert await _send(app, "job-1") == Receipt("c-1", 1)
    finally:
        await app.shutdown()


async def test_idempotent_command_registration(hub: FakeHub) -> None:
    app = _app(hub)

    with pytest.raises(TypeError, match="external_idempotent"):
        app.command_handler(external_idempotent=True)  # type: ignore[call-overload]

    with pytest.raises(TypeError, match="external_idempotent"):
        app.idempotent_command_handler(external_idempotent="yes")  # type: ignore[call-overload]

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        return Receipt(command.order_id, 1)

    with pytest.raises(TypeError, match="already has the command handler"):
        app.idempotent_command_handler(charge)

    await app.start()
    try:

        async def late(command: Charge) -> Receipt:
            return Receipt(command.order_id, 2)

        with pytest.raises(RegistrationClosed):
            app.idempotent_command_handler(late)
    finally:
        await app.shutdown()


class _LosingMark:
    """The first mark-dispatched write is lost, as a crash between the send and the mark would."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._lost = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def mark_dispatched(self, entry_id: str, now_ms: int, *, tx: Any = None) -> None:
        if not self._lost:
            self._lost = True
            raise RuntimeError("the mark was lost")
        await self._inner.mark_dispatched(entry_id, now_ms, tx=tx)


class _ResendSaga:
    @saga_start("order_id")
    async def started(self, event: OrderStarted, ctx: SagaContext) -> None:
        ctx.send(Charge(event.order_id))


async def _started_event(order_id: str) -> OutgoingEvent:
    return OutgoingEvent(
        source="snodra.events",
        id=f"start-{order_id}",
        type_name="test.OrderStarted",
        key=order_id,
        headers={
            EventHeaders.ID: f"start-{order_id}",
            EventHeaders.TYPE: "test.OrderStarted",
            EventHeaders.CORRELATION_ID: "c-1",
            EventHeaders.SOURCE: "test",
        },
        payload=b'{"orderId":"' + order_id.encode() + b'"}',
    )


async def test_a_saga_command_resent_after_a_lost_mark_runs_the_adapter_once(hub: FakeHub) -> None:
    base = MemoryStore()
    sagas = base.sagas
    base.sagas = SagaStores(
        sagas=sagas.sagas,
        dispatch=_LosingMark(sagas.dispatch),
        deadlines=sagas.deadlines,
        failures=sagas.failures,
        markers=sagas.markers,
    )
    events = MemoryEventStore()
    app = new_app(
        hub,
        store=base,
        event_store=events,
        outbox_enabled=False,
        _timings=replace(SCALED, saga_dispatch_poll=300.0),
    )
    bodies = 0

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        nonlocal bodies
        bodies += 1
        return Receipt(command.order_id, bodies)

    app.saga("test.ResendSaga")(_ResendSaga)
    await app.start()
    relay = app._saga_dispatch
    assert relay is not None
    relay.stop()  # the test drives the passes itself
    try:
        await events.publish(await _started_event("o-1"))
        live = []
        for _ in range(200):
            live = await base.sagas.sagas.find_live("test.ResendSaga", "order_id", "o-1")
            if live:
                break
            await asyncio.sleep(0.01)
        assert len(live) == 1
        await relay.run_once()
        assert bodies == 1
        pending = await base.sagas.dispatch.next_pending(live[0].id)
        assert pending is not None, "the entry is still pending after the lost mark"
        await relay.run_once()
        assert bodies == 1, "the same idempotency key does not run the adapter again"
        assert await base.sagas.dispatch.next_pending(live[0].id) is None
        record = await base.external.find(pending.id)
        assert record is not None
        assert record.status is ExternalCallStatus.SUCCEEDED
    finally:
        await app.shutdown()


async def test_a_marker_nested_in_another_error_is_still_written_and_settles(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        if command.mode == "retryable":
            raise RuntimeError("outer: busy") from Retryable("busy")
        raise RuntimeError("outer: lost") from UnknownOutcome("connection reset")

    await app.start()
    try:
        with pytest.raises(HubError) as retryable:
            await _send(app, "nested-r", mode="retryable")
        assert retryable.value.details == b'{"snodra":"retryable"}'
        assert is_retryable(retryable.value)
        assert await store.external.find("nested-r") is None, "the nested retryable marker reset the record"
        with pytest.raises(HubError) as unknown:
            await _send(app, "nested-u", mode="unknown")
        assert unknown.value.details == b'{"snodra":"unknown-outcome"}'
        assert is_unknown_outcome(unknown.value)
        record = await store.external.find("nested-u")
        assert record is not None
        assert record.status is ExternalCallStatus.UNKNOWN
    finally:
        await app.shutdown()


async def test_a_cancelled_body_leaves_the_record_in_progress_for_its_lease(hub: FakeHub) -> None:
    store = MemoryStore()
    app = _app(hub, store)
    entered = asyncio.Event()

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        entered.set()
        await asyncio.Event().wait()
        return Receipt(command.order_id, 1)

    await app.start()
    try:
        running = asyncio.create_task(_send(app, "job-1"))
        await asyncio.wait_for(entered.wait(), 2.0)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        record = await store.external.find("job-1")
        assert record is not None
        assert record.status is ExternalCallStatus.IN_PROGRESS
        with pytest.raises(HubError) as refused:
            await _send(app, "job-1")
        assert is_retryable(refused.value)
    finally:
        await app.shutdown()


class _ProbePort:
    def __init__(self, inner: Any, steps: list[str]) -> None:
        self._inner = inner
        self._steps = steps

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def find(self, key: str) -> ExternalCall | None:
        self._steps.append("probe")
        return cast("ExternalCall | None", await self._inner.find(key))


class _LifecycleStore(MemoryStore):
    """A message store that records the order of its schema preparation and the external-call probe."""

    def __init__(self) -> None:
        super().__init__()
        self.steps: list[str] = []
        self.external = _ProbePort(self.external, self.steps)

    def use_clock(self, clock: Any) -> None:
        self.steps.append("clock")
        super().use_clock(clock)

    async def init_schema(self) -> None:
        self.steps.append("schema")

    async def check_schema(self) -> None:
        self.steps.append("check")

    async def background(self) -> None:
        await asyncio.Event().wait()


async def test_the_external_call_port_is_probed_after_the_schema_is_prepared(hub: FakeHub) -> None:
    store = _LifecycleStore()
    app = _app(hub, store)

    @app.idempotent_command_handler
    async def charge(command: Charge) -> Receipt:
        return Receipt(command.order_id, 1)

    await app.start()
    try:
        assert store.steps == ["clock", "schema", "probe"]
    finally:
        await app.shutdown()
