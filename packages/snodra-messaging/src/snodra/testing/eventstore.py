"""The event-store conformance kit: the checks every add-on passes, a third party's included.

An add-on's own test hands the kit a source it prepared, a way to make the add-on (one call per service instance) and
a message store (for the at-least-once check)::

    from snodra.testing import run_event_store_conformance

    async def test_my_event_store_passes_conformance() -> None:
        message_store = MyMessageStore()  # any message store, its schema in place
        await run_event_store_conformance("conformance.events", lambda: MyEventStore(), message_store)

- **The source** has at least two segments (the competing-instances check spreads them over two instances), and when
  the event store declares ``native-dead-letter`` its dead-letter source exists too. The kit only appends to them, and
  must be their only publisher while it runs.
- **Each call of ``open_store``** is one service instance: a store reading the same event store (an in-process store
  returns the same one each time).
- The checks run one after the other, each on subscription names of its own; a new name starts at the earliest
  position, so each check reads what the earlier ones appended and looks only at its own events (by their ``id``
  header). Every check is public, so a failing one can be run by itself on an :class:`EventStoreKit`.
- The kit waits at most a minute for what the event store does in the background (an assignment, a delivery), and a
  publish, subscribe, close or admin call that does not answer fails the check naming the step.

The check ids are the ones of the reference kit, one to one; ``checks.txt`` lists them. This module holds the helpers
and the runner; :mod:`snodra.testing.checks` holds the checks themselves.

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#the-conformance-kit.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Protocol

from snodra._eventstore import (
    MAX_POSITION_BYTES,
    MAX_SEGMENT_BYTES,
    Assigned,
    Delivery,
    EventHeaders,
    EventStore,
    EventStoreSubscription,
    OutgoingEvent,
    ProcessedMarker,
    Revoked,
    StoreError,
    SubscriptionEvent,
    SubscriptionSpec,
)

WAIT: Final = 60.0
"""The longest a check waits for what the event store does in the background, in seconds."""

QUIET: Final = 3.0
"""How long a check watches a subscription that must deliver nothing, in seconds."""

SLICE: Final = 0.1
"""One turn of reading a subscription, in seconds."""

JOIN_WINDOW: Final = 6.0
"""How long a check waits for the assignment change a joining instance may cause, in seconds."""

EVENT_TYPE: Final = "conformance.Event"
"""The type of the kit's events."""


def now_ms() -> int:
    """The current time in epoch milliseconds."""
    return time.time_ns() // 1_000_000


def fresh(what: str) -> str:
    """A fresh name: ``<what>-<random hex>``."""
    return f"{what}-{uuid.uuid4().hex}"


def id_of(delivery: Delivery) -> str:
    """The ``id`` header of a delivery."""
    return delivery.headers.get(EventHeaders.ID, "")


def ids(sent: Sequence[OutgoingEvent]) -> list[str]:
    """The ids of ``sent``."""
    return [event.id for event in sent]


class ProcessedEvents(Protocol):
    """The processed-events port the at-least-once check records its marker through."""

    async def record(self, marker: ProcessedMarker) -> bool:
        """Records ``marker``; false when a marker with its id was recorded already."""
        ...


class MarkerStore(Protocol):
    """The message store the kit needs: its processed-events port (and nothing else)."""

    @property
    def processed(self) -> ProcessedEvents: ...


@dataclass(frozen=True, slots=True)
class EventStoreKit:
    """What every check runs on."""

    source: str
    """The prepared source (module docstring)."""
    open_store: Callable[[], EventStore]
    """One service instance per call."""
    store: MarkerStore
    """The message store of the at-least-once check."""

    def instance(self) -> EventStore:
        """One more service instance."""
        return self.open_store()

    def spec(self) -> SubscriptionSpec:
        """A subscription name nothing has used, on the prepared source."""
        return SubscriptionSpec(name=f"conformance.{uuid.uuid4().hex}", source=self.source)


class Reader:
    """One subscription and what it gave: its assignment and its deliveries, in order.

    Every delivery is acknowledged as it comes but the held ones.
    """

    __slots__ = (
        "assigned",
        "changes",
        "changing",
        "first",
        "held",
        "hold_everything",
        "name",
        "revoked",
        "seen",
        "subscription",
    )

    def __init__(
        self,
        name: str,
        subscription: EventStoreSubscription,
        *,
        hold: Iterable[str] = (),
        hold_everything: bool = False,
    ) -> None:
        self.name = name
        self.subscription: EventStoreSubscription | None = subscription
        self.assigned: set[str] = set()
        self.seen: list[Delivery] = []
        self.held: set[str] = set(hold)
        self.hold_everything = hold_everything
        self.revoked = 0
        self.changes = 0
        self.first: str | None = None
        """The kind of the first event the subscription gave."""
        self.changing = False
        """A ``Revoked`` came and no ``Assigned`` after it yet."""

    @classmethod
    async def join(
        cls,
        events: EventStore,
        spec: SubscriptionSpec,
        *,
        hold: Iterable[str] = (),
        hold_everything: bool = False,
    ) -> Reader:
        """Subscribes ``spec`` and returns the reader; nothing is read until :meth:`pump`."""
        subscription = await bounded(f"subscribe {spec.name}", events.subscribe(spec))
        return cls(spec.name, subscription, hold=hold, hold_everything=hold_everything)

    def _subscription(self) -> EventStoreSubscription:
        if self.subscription is None:
            raise AssertionError(f"{self.name}: closed")
        return self.subscription

    async def pump(self, window: float) -> int:
        """Takes what the subscription gives for ``window`` seconds; returns how many events it gave."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + window
        taken = 0
        while (left := deadline - loop.time()) > 0:
            try:
                event = await asyncio.wait_for(self._subscription().next(), left)
            except TimeoutError:
                return taken
            taken += 1
            await self._take(event)
        return taken

    async def _take(self, event: SubscriptionEvent) -> None:
        if self.first is None:
            if isinstance(event, Assigned):
                self.first = "Assigned"
            elif isinstance(event, Revoked):
                self.first = "Revoked"
            else:
                self.first = "Delivery"
        if isinstance(event, Assigned):
            self.changes += 1
            self.changing = False
            for segment in event.segments:
                assert segment, f"{self.name}: an empty segment name"
                assert len(segment.encode()) <= MAX_SEGMENT_BYTES, (
                    f"{self.name}: a segment name of 1 to {MAX_SEGMENT_BYTES} bytes: {segment!r}"
                )
                self.assigned.add(segment)
        elif isinstance(event, Revoked):
            self.changes += 1
            self.revoked += 1
            self.changing = True
            self.assigned.difference_update(event.segments)
        else:
            assert event.segment in self.assigned, (
                f"{self.name}: a delivery of segment {event.segment} it does not hold ({sorted(self.assigned)})"
            )
            assert event.position, f"{self.name}: an empty position"
            assert len(event.position.encode()) <= MAX_POSITION_BYTES, (
                f"{self.name}: a position of 1 to {MAX_POSITION_BYTES} bytes: {event.position!r}"
            )
            if not self.hold_everything and id_of(event) not in self.held:
                await self.ack(event)
            self.seen.append(event)

    async def until(self, what: str, done: Callable[[Reader], bool]) -> None:
        """Reads until ``done``, at most :data:`WAIT` seconds."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + WAIT
        while not done(self):
            assert loop.time() < deadline, (
                f"{self.name}: {what} within {WAIT:g} s (assigned {sorted(self.assigned)}, {len(self.seen)} deliveries)"
            )
            await self.pump(SLICE)

    def pause(self) -> None:
        """Stops delivering; the assignment goes on."""
        self._subscription().pause()

    def resume(self) -> None:
        """Delivers again from where the pause stopped."""
        self._subscription().resume()

    async def ack(self, delivery: Delivery) -> None:
        await bounded(
            f"{self.name}: ack {delivery.segment} {delivery.position}",
            self._subscription().ack(delivery.segment, delivery.position),
        )

    async def redeliver(self, delivery: Delivery) -> None:
        await bounded(
            f"{self.name}: redeliver {delivery.segment} {delivery.position}",
            self._subscription().redeliver(delivery.segment, delivery.position),
        )

    def has(self, event_id: str) -> bool:
        """Whether the event ``event_id`` was delivered."""
        return any(id_of(delivery) == event_id for delivery in self.seen)

    def has_all(self, wanted: Sequence[str]) -> bool:
        """Whether every event of ``wanted`` was delivered."""
        return all(self.has(event_id) for event_id in wanted)

    def mine(self, wanted: Sequence[str]) -> list[Delivery]:
        """The deliveries of the events of ``wanted``, in delivery order."""
        return [delivery for delivery in self.seen if id_of(delivery) in wanted]

    def ids_in_order(self, wanted: Sequence[str]) -> list[str]:
        """The ids of the deliveries of ``wanted``, in delivery order."""
        return [id_of(delivery) for delivery in self.mine(wanted)]

    def first_delivery(self, event_id: str) -> Delivery:
        """The first delivery of the event ``event_id``."""
        for delivery in self.seen:
            if id_of(delivery) == event_id:
                return delivery
        raise AssertionError(f"{self.name}: {event_id} delivered")

    def known_lag(self, required: Sequence[str]) -> int | None:
        """The lag of the assigned segments whose lag is known, summed; ``None`` when none is known or one of
        ``required`` is not."""
        segments = self._subscription().segments()
        known = {status.segment: status.lag for status in segments}
        if any(known.get(segment) is None for segment in required):
            return None
        lags = [lag for lag in known.values() if lag is not None]
        return sum(lags) if lags else None

    async def close(self) -> None:
        """Closes the subscription; a second call does nothing."""
        if self.subscription is not None:
            subscription, self.subscription = self.subscription, None
            await bounded(f"close {self.name}", subscription.close())


async def bounded[T](step: str, call: Awaitable[T]) -> T:
    """``call``'s outcome; raises :class:`AssertionError` naming ``step`` when it does not answer within
    :data:`WAIT` seconds."""
    try:
        return await asyncio.wait_for(call, WAIT)
    except TimeoutError as error:
        raise AssertionError(f"{step}: no answer within {WAIT:g} s") from error


async def all_until(readers: Sequence[Reader], what: str, done: Callable[[Sequence[Reader]], bool]) -> None:
    """Reads all of ``readers`` until ``done``, at most :data:`WAIT` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT
    while not done(readers):
        assert loop.time() < deadline, (
            f"{what} within {WAIT:g} s (assigned {[sorted(reader.assigned) for reader in readers]}, "
            f"revocations {[reader.revoked for reader in readers]})"
        )
        await asyncio.gather(*(reader.pump(SLICE) for reader in readers))


async def both_until(a: Reader, b: Reader, what: str, done: Callable[[Reader, Reader], bool]) -> None:
    """Reads both until ``done``, at most :data:`WAIT` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + WAIT
    while not done(a, b):
        assert loop.time() < deadline, (
            f"{what} within {WAIT:g} s (assigned {sorted(a.assigned)} and {sorted(b.assigned)}, "
            f"{len(a.seen)} and {len(b.seen)} deliveries)"
        )
        await asyncio.gather(a.pump(SLICE), b.pump(SLICE))


def event(
    kit: EventStoreKit,
    key: str | None,
    *,
    type_name: str = EVENT_TYPE,
    extra: Mapping[str, str] | None = None,
) -> OutgoingEvent:
    """An event of the kit to ``kit.source``: the seven headers, a fresh id, a JSON object payload."""
    event_id = str(uuid.uuid4())
    trace = uuid.uuid4().hex
    span = uuid.uuid4().hex[:16]
    headers = {
        EventHeaders.TYPE: type_name,
        EventHeaders.ID: event_id,
        EventHeaders.CORRELATION_ID: fresh("correlation"),
        EventHeaders.TIMESTAMP: rfc3339(now_ms()),
        EventHeaders.SOURCE: "conformance",
        EventHeaders.USER_ID: "conformance-user",
        EventHeaders.TRACEPARENT: f"00-{trace}-{span}-01",
    }
    if extra is not None:
        headers.update(extra)
    return OutgoingEvent(
        source=kit.source,
        id=event_id,
        type_name=type_name,
        key=key,
        headers=headers,
        payload=f'{{"id":"{event_id}","amount":42}}'.encode(),
    )


def rfc3339(millis: int) -> str:
    """``millis`` as the ``timestamp`` header writes it: UTC, with milliseconds."""
    seconds, fraction = divmod(millis, 1000)
    stamp = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{fraction:03d}Z"


async def publish_all(events: EventStore, sent: Sequence[OutgoingEvent]) -> None:
    """Publishes ``sent`` in order."""
    for outgoing in sent:
        try:
            await bounded(f"publish {outgoing.id}", events.publish(outgoing))
        except StoreError as error:
            raise AssertionError(f"publish {outgoing.id}: {error}") from error


async def run_event_store_conformance(
    source: str,
    open_store: Callable[[], EventStore],
    store: MarkerStore,
) -> None:
    """Runs every check by name; the first failing check raises :class:`AssertionError` naming itself and the event
    store. See the module docstring for ``source``, ``open_store`` and ``store``."""
    kit = EventStoreKit(source, open_store, store)
    name = open_store().name
    from snodra.testing.checks import EVENT_STORE_CHECKS

    for check in EVENT_STORE_CHECKS:
        try:
            await check(kit)
        except Exception as error:
            raise AssertionError(
                f"event store conformance check `{check.__name__}` failed on event store `{name}`: {error}"
            ) from error
