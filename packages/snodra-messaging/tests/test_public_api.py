"""The add-on ports are public: a third-party add-on imports them from ``snodra``, never from a private module."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from contextvars import Token
from types import TracebackType

import snodra

PUBLIC_NAMES = (
    # Event store.
    "Assigned",
    "Capability",
    "Delivery",
    "Earliest",
    "EventHeaders",
    "EventStore",
    "EventStoreAdmin",
    "EventStoreSubscription",
    "Failed",
    "FIRST_ASSIGNMENT_WITHIN",
    "Invalid",
    "Latest",
    "LeaseLost",
    "MAX_POSITION_BYTES",
    "MAX_SEGMENT_BYTES",
    "NotOwner",
    "OutgoingEvent",
    "Positions",
    "ProcessedMarker",
    "Rejected",
    "ReplayRange",
    "ResetTarget",
    "Revoked",
    "SegmentStatus",
    "StoreError",
    "SubscribersActive",
    "SubscriptionEvent",
    "SubscriptionSpec",
    "Timestamp",
    "Unsupported",
    "dead_letter_source",
    "segment_order",
    # Message store.
    "AppendListener",
    "Association",
    "Conflict",
    "ControlOperation",
    "DeadLetter",
    "DeadLetterError",
    "DeadLetterReason",
    "DeadLetterStore",
    "DeadLetterSummary",
    "DeadlineDocument",
    "DeadlineStatus",
    "DeadlineStore",
    "DispatchFailure",
    "DispatchKind",
    "ExternalCall",
    "ExternalCallStatus",
    "ExternalCallStore",
    "FailureReason",
    "FailureStore",
    "InvalidMessage",
    "Locked",
    "MAX_ERROR_MESSAGE_CHARS",
    "MAX_PAYLOAD_DEPTH",
    "MessageStore",
    "MessageStoreError",
    "MessageStoreFailed",
    "MessageStoreUnsupported",
    "OutboxEntry",
    "OutboxStore",
    "Parked",
    "ProcessedStore",
    "ProcessorControlState",
    "ProcessorControlStore",
    "RESET_KIND",
    "SagaCommandFailed",
    "SagaContext",
    "SagaDispatchDocument",
    "SagaDispatchStore",
    "SagaDocument",
    "SagaError",
    "SagaFailureDocument",
    "SagaMarkerStore",
    "SagaRepository",
    "SagaStores",
    "StoreLifecycle",
    "TransactionScope",
    "Transactions",
    "Tx",
    "bind_transaction",
    "check_outbox_payload",
    "current_transaction",
    "letter_id",
    "now_ms",
    "unbind_transaction",
)


def test_every_port_name_is_public() -> None:
    for name in PUBLIC_NAMES:
        assert name in snodra.__all__, f"{name} is not in snodra.__all__"
        assert getattr(snodra, name) is not None, f"snodra.{name} is missing"


def test_the_two_store_error_families_do_not_collide() -> None:
    event_base: type[object] = snodra.StoreError
    message_base: type[object] = snodra.MessageStoreError
    assert event_base is not message_base
    event_failed: type[object] = snodra.Failed
    message_failed: type[object] = snodra.MessageStoreFailed
    assert event_failed is not message_failed
    event_unsupported: type[object] = snodra.Unsupported
    message_unsupported: type[object] = snodra.MessageStoreUnsupported
    assert event_unsupported is not message_unsupported
    for error in (snodra.Invalid, snodra.SubscribersActive, snodra.NotOwner, snodra.LeaseLost, snodra.Rejected):
        assert issubclass(error, snodra.StoreError)
    assert issubclass(snodra.Failed, snodra.StoreError)
    assert issubclass(snodra.Unsupported, snodra.StoreError)
    for message_error in (snodra.Conflict, snodra.InvalidMessage):
        assert issubclass(message_error, snodra.MessageStoreError)
    assert issubclass(snodra.MessageStoreFailed, snodra.MessageStoreError)
    assert issubclass(snodra.MessageStoreUnsupported, snodra.MessageStoreError)
    # The level-1 subscription query keeps its name; the event store's subscription is distinct.
    level_one_subscription: type[object] = snodra.Subscription
    event_subscription: type[object] = snodra.EventStoreSubscription
    assert level_one_subscription is not event_subscription


class _StubEventSubscription:
    """A typed event-store subscription, checked against the protocol by mypy."""

    async def next(self) -> snodra.SubscriptionEvent:
        raise NotImplementedError

    async def ack(self, segment: str, position: str) -> None:
        raise NotImplementedError

    async def redeliver(self, segment: str, position: str) -> None:
        raise NotImplementedError

    def pause(self) -> None:
        raise NotImplementedError

    def resume(self) -> None:
        raise NotImplementedError

    def segments(self) -> list[snodra.SegmentStatus]:
        raise NotImplementedError

    def take_error(self) -> str | None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class _StubEventAdmin:
    async def members(self) -> list[str]:
        raise NotImplementedError

    async def plan(self, target: snodra.ResetTarget) -> list[snodra.ReplayRange]:
        raise NotImplementedError

    async def apply(self, ranges: Sequence[snodra.ReplayRange]) -> None:
        raise NotImplementedError


class _StubEventStore:
    """A typed event store, checked against the protocol by mypy."""

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def capabilities(self) -> frozenset[snodra.Capability]:
        raise NotImplementedError

    def supports(self, capability: snodra.Capability) -> bool:
        raise NotImplementedError

    async def publish(self, event: snodra.OutgoingEvent) -> None:
        raise NotImplementedError

    async def subscribe(self, spec: snodra.SubscriptionSpec) -> snodra.EventStoreSubscription:
        raise NotImplementedError

    def admin(self, spec: snodra.SubscriptionSpec) -> snodra.EventStoreAdmin:
        raise NotImplementedError

    def compare(self, a: str, b: str) -> int:
        raise NotImplementedError


class _StubMessageStore:
    """A typed message store, checked against the protocol by mypy."""

    @property
    def name(self) -> str:
        raise NotImplementedError

    def transaction(self) -> AbstractAsyncContextManager[snodra.Tx]:
        raise NotImplementedError

    def use_clock(self, clock: Callable[[], int]) -> None:
        raise NotImplementedError

    async def begin(self) -> snodra.Tx:
        raise NotImplementedError

    def plain(self) -> snodra.Tx:
        raise NotImplementedError

    async def commit(self, tx: snodra.Tx) -> None:
        raise NotImplementedError

    async def rollback(self, tx: snodra.Tx) -> None:
        raise NotImplementedError

    @property
    def outbox(self) -> snodra.OutboxStore:
        raise NotImplementedError

    @property
    def processed(self) -> snodra.ProcessedStore:
        raise NotImplementedError

    @property
    def dead_letters(self) -> snodra.DeadLetterStore:
        raise NotImplementedError

    @property
    def control(self) -> snodra.ProcessorControlStore:
        raise NotImplementedError

    @property
    def external(self) -> snodra.ExternalCallStore:
        raise NotImplementedError

    @property
    def sagas(self) -> snodra.SagaStores:
        raise NotImplementedError


class _StubTransactions:
    """A typed ``Transactions`` port, checked by mypy."""

    def __init__(self) -> None:
        self.commits: list[snodra.Tx] = []
        self.rollbacks: list[snodra.Tx] = []

    async def begin(self) -> snodra.Tx:
        return snodra.Tx(object())

    def plain(self) -> snodra.Tx:
        return snodra.Tx.none()

    async def commit(self, tx: snodra.Tx) -> None:
        self.commits.append(tx)

    async def rollback(self, tx: snodra.Tx) -> None:
        self.rollbacks.append(tx)


class _StubTransactionManager:
    """An add-on's own transaction context manager: it binds on entry and unbinds in a ``finally``, and joins a
    transaction already bound (as ``MessageStore.transaction`` requires)."""

    def __init__(self, store: _StubTransactions) -> None:
        self._store = store
        self._tx: snodra.Tx | None = None
        self._token: Token[snodra.Tx | None] | None = None
        self._joined = False

    async def __aenter__(self) -> snodra.Tx:
        bound = snodra.current_transaction()
        if bound is not None:
            self._joined = True
            return bound
        tx = await self._store.begin()
        self._tx = tx
        self._token = snodra.bind_transaction(tx)
        return tx

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._joined:
            return
        tx, self._tx = self._tx, None
        token, self._token = self._token, None
        try:
            if tx is not None:
                if exc_type is None:
                    await self._store.commit(tx)
                else:
                    await self._store.rollback(tx)
        finally:
            if token is not None:
                snodra.unbind_transaction(token)


class _StubLifecycle:
    """A typed ``StoreLifecycle``, checked by mypy."""

    def use_clock(self, clock: Callable[[], int]) -> None:
        raise NotImplementedError

    async def init_schema(self) -> None:
        raise NotImplementedError

    async def check_schema(self) -> None:
        raise NotImplementedError

    async def background(self) -> None:
        raise NotImplementedError


def _accept_event_store(store: snodra.EventStore) -> None:
    pass


def _accept_message_store(store: snodra.MessageStore) -> None:
    pass


def test_a_typed_stub_add_on_implements_the_ports() -> None:
    # mypy checks the assignments: the stubs satisfy the public protocols with the public types only.
    _accept_event_store(_StubEventStore())
    _accept_message_store(_StubMessageStore())
    subscription: snodra.EventStoreSubscription = _StubEventSubscription()
    admin: snodra.EventStoreAdmin = _StubEventAdmin()
    lifecycle: snodra.StoreLifecycle = _StubLifecycle()
    assert subscription is not None
    assert admin is not None
    assert lifecycle is not None


async def test_the_public_transaction_helpers_bind_and_unbind() -> None:
    store = _StubTransactions()
    manager: AbstractAsyncContextManager[snodra.Tx] = _StubTransactionManager(store)
    assert snodra.current_transaction() is None
    async with manager as tx:
        assert snodra.current_transaction() is tx
        async with _StubTransactionManager(store) as nested:
            assert nested is tx  # the nested manager joined, as store.transaction() must
        assert snodra.current_transaction() is tx
    assert snodra.current_transaction() is None
    assert store.commits == [tx]
