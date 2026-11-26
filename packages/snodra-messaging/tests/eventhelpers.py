"""Stores, subscriptions and deliveries of the event tests."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from snodra import message_type
from snodra._eventstore import (
    Assigned,
    Capability,
    Delivery,
    EventHeaders,
    EventStoreAdmin,
    EventStoreSubscription,
    OutgoingEvent,
    ReplayRange,
    ResetTarget,
    SegmentStatus,
    SubscriptionEvent,
    SubscriptionSpec,
)
from snodra._store import OutboxEntry, OutboxStore, ProcessedMarker, ProcessedStore, Tx


@message_type("orders.OrderPlaced")
@dataclass
class OrderPlaced:
    order_id: str
    total: int = 0


@message_type("orders.OrderShipped")
@dataclass
class OrderShipped:
    order_id: str


@message_type("orders.PlaceOrder")
@dataclass
class PlaceOrder:
    order_id: str
    total: int = 0


class Clock:
    """The application's clock, advanced by a test."""

    def __init__(self, now_ms: int = 1_793_700_000_000) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms

    def advance(self, seconds: float) -> None:
        self.now_ms += int(seconds * 1000)


class RecordingEventStore:
    """An event store that records what the relay publishes, and can fail or block a publish.

    ``failures`` is the script of the next publishes: ``None`` accepts, an exception is raised instead; once it is
    empty every publish is accepted. ``block`` holds publishes until it is set; ``on_publish`` runs before either.
    """

    name = "recording"
    capabilities: frozenset[Capability] = frozenset()

    def __init__(self) -> None:
        self.events: list[OutgoingEvent] = []
        self.failures: deque[BaseException | None] = deque()
        self.block: asyncio.Event | None = None
        self.on_publish: Callable[[OutgoingEvent], None] | None = None
        self.published = asyncio.Event()
        self.attempts = 0

    async def publish(self, event: OutgoingEvent) -> None:
        self.attempts += 1
        if self.on_publish is not None:
            self.on_publish(event)
        if self.failures:
            failure = self.failures.popleft()
            if failure is not None:
                raise failure
        self.events.append(event)
        self.published.set()
        if self.block is not None:
            await self.block.wait()

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        raise NotImplementedError("the recording event store is a publisher only")

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        raise NotImplementedError("the recording event store has no admin")

    def compare(self, a: str, b: str) -> int:
        return (a > b) - (a < b)

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities


@dataclass
class ScriptedSubscription:
    """A subscription a test drives: deliveries from ``queue``, every acknowledgement and close recorded."""

    queue: asyncio.Queue[SubscriptionEvent | BaseException] = field(default_factory=asyncio.Queue)
    acks: list[tuple[str, str]] = field(default_factory=list)
    closes: int = 0
    on_ack: Callable[[ScriptedSubscription, str, str], Any] | None = None
    _paused: bool = False

    async def next(self) -> SubscriptionEvent:
        event = await self.queue.get()
        if isinstance(event, BaseException):
            raise event
        return event

    async def ack(self, segment: str, position: str) -> None:
        self.acks.append((segment, position))
        if self.on_ack is not None:
            outcome = self.on_ack(self, segment, position)
            if inspect.isawaitable(outcome):
                await outcome

    async def redeliver(self, segment: str, position: str) -> None:
        pass

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def segments(self) -> list[SegmentStatus]:
        return []

    def take_error(self) -> str | None:
        return None

    async def close(self) -> None:
        self.closes += 1

    def deliver(self, delivery: Delivery) -> None:
        self.queue.put_nowait(delivery)

    def fail(self, error: BaseException) -> None:
        """The next ``next`` raises ``error``, as a broken subscription does."""
        self.queue.put_nowait(error)


class ScriptedEventStore:
    """An event store whose subscriptions the test scripts; ``subscribe_failures`` fail the next subscribes."""

    name = "scripted"
    capabilities = frozenset({Capability.REPLAY})

    def __init__(self) -> None:
        self.subscriptions: list[ScriptedSubscription] = []
        self.specs: list[SubscriptionSpec] = []
        self.subscribe_times: list[float] = []
        self.subscribe_failures: deque[BaseException] = deque()
        self.subscribed = asyncio.Event()

    async def publish(self, event: OutgoingEvent) -> None:
        raise NotImplementedError("the scripted event store publishes nothing")

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        self.subscribe_times.append(time.monotonic())
        if self.subscribe_failures:
            raise self.subscribe_failures.popleft()
        subscription = ScriptedSubscription()
        self.specs.append(spec)
        self.subscriptions.append(subscription)
        self.subscribed.set()
        return subscription

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        raise NotImplementedError("the scripted event store has no admin")

    def compare(self, a: str, b: str) -> int:
        return (a > b) - (a < b)

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities


def delivery(
    type_: str | None = "orders.OrderPlaced",
    event_id: str | None = "e-1",
    payload: bytes = b'{"orderId":"o-1"}',
    segment: str = "0",
    position: str = "0",
    *,
    key: str | None = None,
    headers: dict[str, str | None] | None = None,
) -> Delivery:
    """One delivery; ``headers`` overrides the default event headers, a ``None`` value removes one."""
    all_headers: dict[str, str | None] = {
        EventHeaders.TYPE: type_,
        EventHeaders.ID: event_id,
        EventHeaders.CORRELATION_ID: "c-1",
        EventHeaders.USER_ID: "u-1",
    }
    all_headers.update(headers or {})
    return Delivery(
        segment=segment,
        position=position,
        key=key,
        headers={name: value for name, value in all_headers.items() if value is not None},
        payload=payload,
        timestamp_ms=0,
    )


def outbox_entry(
    number: int,
    *,
    type_: str = "orders.Placed",
    payload: bytes = b"{}",
    key: str | None = None,
    created_at_ms: int = 1_793_700_000_000,
    seq: int | None = None,
    headers: dict[str, str] | None = None,
) -> OutboxEntry:
    """An outbox entry a test appends directly."""
    return OutboxEntry(
        id=f"e-{number}",
        type_name=type_,
        key=key,
        headers={"correlationId": f"c-{number}", "userId": ""} if headers is None else headers,
        payload=payload,
        created_at_ms=created_at_ms,
        seq=number if seq is None else seq,
    )


class SlowOutbox:
    """An outbox whose claim blocks, off the loop as an add-on's blocking driver must, before the inner one answers."""

    def __init__(self, inner: OutboxStore, delay: float) -> None:
        self._inner = inner
        self._delay = delay

    async def append(self, entry: OutboxEntry, *, tx: Tx | None = None) -> None:
        await asyncio.to_thread(_sleep, self._delay)
        await self._inner.append(entry, tx=tx)

    async def append_now(self, entry: OutboxEntry) -> None:
        await asyncio.to_thread(_sleep, self._delay)
        await self._inner.append_now(entry)

    async def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        await asyncio.to_thread(_sleep, self._delay)
        return await self._inner.claim(owner, lease, limit)

    async def mark_sent(self, ids: Sequence[str]) -> None:
        await self._inner.mark_sent(ids)

    async def mark_attempt_failed(self, entry_id: str, error: str) -> None:
        await self._inner.mark_attempt_failed(entry_id, error)

    async def mark_failed(self, entry_id: str, error: str) -> None:
        await self._inner.mark_failed(entry_id, error)

    async def purge(self, sent_before_ms: int) -> int:
        return await self._inner.purge(sent_before_ms)

    async def pending(self) -> int:
        return await self._inner.pending()

    def on_append(self, wake: Callable[[], None]) -> Any:
        return self._inner.on_append(wake)

    async def oldest_pending(self) -> int | None:
        return await self._inner.oldest_pending()


class SlowProcessed:
    """The processed store, recording a marker only after a blocking read off the loop."""

    def __init__(self, inner: ProcessedStore, delay: float) -> None:
        self._inner = inner
        self._delay = delay

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        await asyncio.to_thread(_sleep, self._delay)
        return await self._inner.record(marker, tx=tx)

    async def exists(self, marker_id: str) -> bool:
        return await self._inner.exists(marker_id)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        return await self._inner.delete_markers(processor, ranges)

    async def purge(self, before_ms: int) -> int:
        return await self._inner.purge(before_ms)


class RecordingProcessed:
    """The processed store, keeping every marker it records."""

    def __init__(self, inner: ProcessedStore) -> None:
        self._inner = inner
        self.markers: list[ProcessedMarker] = []

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        self.markers.append(marker)
        return await self._inner.record(marker, tx=tx)

    async def exists(self, marker_id: str) -> bool:
        return await self._inner.exists(marker_id)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        return await self._inner.delete_markers(processor, ranges)

    async def purge(self, before_ms: int) -> int:
        return await self._inner.purge(before_ms)


def _sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)


class FailingProcessed:
    """The processed store, failing its next ``record`` calls with a store error that may be retried."""

    def __init__(self, inner: ProcessedStore, failures: int) -> None:
        self._inner = inner
        self._failures = failures
        self.calls = 0

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        self.calls += 1
        if self._failures > 0:
            self._failures -= 1
            raise RuntimeError("the store is down")
        return await self._inner.record(marker, tx=tx)

    async def exists(self, marker_id: str) -> bool:
        return await self._inner.exists(marker_id)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        return await self._inner.delete_markers(processor, ranges)

    async def purge(self, before_ms: int) -> int:
        return await self._inner.purge(before_ms)


def unused_admin(spec: SubscriptionSpec) -> EventStoreAdmin:
    raise AssertionError("the event store's admin is not used")


def unused_reset(target: ResetTarget) -> list[ReplayRange]:
    raise AssertionError("the event store's admin is not used")


def assigned(*segments: str) -> Assigned:
    return Assigned(segments)
