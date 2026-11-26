"""The event-store ports: where a service's events are kept and read by its event processors.

Every event store provides the contract below, which :mod:`snodra.testing` checks:

- **Publish**: the outbox relay hands it one :class:`OutgoingEvent`; :meth:`EventStore.publish` returns once the
  event store accepted it.
- **One subscription per processor**: every :attr:`SubscriptionSpec.name` receives every event of its source,
  whatever other processors and services do.
- **Competing instances**: the subscriptions of one name share its segments; adding one spreads them.
- **Order per key**: events with the same key go to one segment and reach a processor in publish order.
- **At-least-once with acknowledgement**: the core acknowledges after the handler and the processed-event record
  committed; what was not acknowledged comes again, and the message store's processed events skip it.
- **Pause and resume**: a subscription stops delivering without losing its place.
- **The headers**: the seven :class:`EventHeaders` travel as the event store's native metadata.

Segments and positions are opaque strings that only the event store reads, at most :data:`MAX_SEGMENT_BYTES` and
:data:`MAX_POSITION_BYTES` bytes; :meth:`EventStore.compare` orders two positions of one segment and is the only way
the core orders them. Payloads are JSON bytes, carried as given. A new subscription name starts at the earliest
position of every segment.

This library declares no event browser and no add-on declares ``browse`` at these levels; the interface stops at
:meth:`EventStore.admin`, and an add-on that needs a browser is out of scope here.

The ports, their values and their errors are re-exported from :mod:`snodra` for add-ons: import ``EventStore``,
``EventStoreSubscription``, ``Assigned``, ``Rejected`` and the rest from ``snodra``, never from this private module.
``StoreError``, ``Failed`` and ``Unsupported`` are this module's; the message store's errors of those names are
``MessageStoreError``, ``MessageStoreFailed`` and ``MessageStoreUnsupported`` in :mod:`snodra`.

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Protocol

from snodra._errors import SnodraError

MAX_SEGMENT_BYTES: Final = 128
"""The most bytes of a segment's name (a longer one is dropped by the hub)."""

MAX_POSITION_BYTES: Final = 128
"""The most bytes of a position (a longer one reads as unknown on the hub)."""

FIRST_ASSIGNMENT_WITHIN: Final = 10.0
"""The longest a new subscription takes to yield its first event, an :class:`Assigned`; in seconds."""


class EventHeaders:
    """The headers every event carries, as the event store's native metadata."""

    TYPE: Final = "type"
    """The event's type name."""

    ID: Final = "id"
    """The event's id."""

    CORRELATION_ID: Final = "correlationId"
    """The correlation id of the message that caused the event."""

    TIMESTAMP: Final = "timestamp"
    """When the event was recorded, RFC 3339 in UTC with milliseconds."""

    SOURCE: Final = "source"
    """The publishing service's name (not the event store's source)."""

    USER_ID: Final = "userId"
    """The caller's user id; empty when none."""

    TRACEPARENT: Final = "traceparent"
    """The W3C trace context of the message that caused the event."""

    ALL: Final = (TYPE, ID, CORRELATION_ID, TIMESTAMP, SOURCE, USER_ID, TRACEPARENT)
    """The seven headers."""


def dead_letter_source(source: str) -> str:
    """The dead-letter source of ``source`` (``<source>.dlt``): where the dead-letter mode ``event-store`` sends what
    a processor cannot handle, on an event store declaring :attr:`Capability.NATIVE_DEAD_LETTER`."""
    return f"{source}.dlt"


def segment_order(a: str, b: str) -> int:
    """The order segments are listed in (the hub's), as a comparator: negative when ``a`` comes first.

    Names of ASCII digits only go first, by their value (any length, no overflow; equal values by their text), then
    every other name by its UTF-8 bytes.
    """
    first = _digits(a)
    second = _digits(b)
    if first is not None and second is not None:
        if len(first) != len(second):
            return -1 if len(first) < len(second) else 1
        if first != second:
            return -1 if first < second else 1
        return _compare(a.encode(), b.encode())
    if first is not None:
        return -1
    if second is not None:
        return 1
    return _compare(a.encode("utf-8", "surrogatepass"), b.encode("utf-8", "surrogatepass"))


def _digits(name: str) -> str | None:
    """``name`` without its leading zeros when it is only ASCII digits (and not empty), else ``None``."""
    if name and all("0" <= char <= "9" for char in name):
        return name.lstrip("0")
    return None


def _compare(a: bytes, b: bytes) -> int:
    return (a > b) - (a < b)


class Capability(StrEnum):
    """What an event store may offer beyond the required contract; the hub and the dashboard offer only what an
    event store declares."""

    REPLAY = "replay"
    """Reset to earliest, latest, a time or explicit positions: reset handlers and replay ranges."""

    BROWSE = "browse"
    """The event store can be read by an event browser; no add-on of this library declares it."""

    LAG = "lag"
    """How far a subscription is behind, per segment (:attr:`SegmentStatus.lag`)."""

    NATIVE_DEAD_LETTER = "native-dead-letter"
    """A dead-letter source (:func:`dead_letter_source`): the dead-letter mode ``event-store``."""

    @classmethod
    def all(cls) -> tuple[Capability, ...]:
        """Every capability, in protocol order."""
        return tuple(cls)


class StoreError(SnodraError):
    """The base of every failure of a store port."""


class Invalid(StoreError):
    """A bad argument: an unknown segment, a position the segment does not have."""

    def __init__(self, why: str) -> None:
        super().__init__(f"invalid: {why}")
        self.why = why


class SubscribersActive(StoreError):
    """Another instance still reads the subscription, so its positions cannot move
    (``CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE``)."""

    def __init__(self, members: Sequence[str]) -> None:
        self.members = tuple(members)
        super().__init__(f"the subscription has active subscribers: {', '.join(self.members)}")


class Unsupported(StoreError):
    """The store lacks the feature (``CONTROL_REFUSAL_UNSUPPORTED``); a port not yet at its level reports this."""

    def __init__(self, feature: Capability | str) -> None:
        self.feature = feature
        super().__init__(f"unsupported: {feature}")


class NotOwner(StoreError):
    """The record or the lease belongs to another caller."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"not the owner: {why}")


class LeaseLost(StoreError):
    """The caller's lease lapsed before it acted; another caller may own the record now."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"the lease was lost: {why}")


class Rejected(StoreError):
    """The event store refused the event for good (too large, not authorized, a destination it cannot have):
    publishing it again cannot succeed, so the outbox relay gives it up at once."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"rejected by the event store: {why}")


class Failed(StoreError):
    """The event store failed or cannot be reached; trying again may succeed."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"event store: {why}")


@dataclass(frozen=True, slots=True)
class OutgoingEvent:
    """One event the outbox relay hands the event store (:meth:`EventStore.publish`).

    ``source`` is the source it goes to: the service's events source, or its dead-letter source
    (:func:`dead_letter_source`). ``id`` and ``type_name`` are also in headers :attr:`EventHeaders.ID` and
    :attr:`EventHeaders.TYPE`. ``key`` is the order key: events with one key reach a processor in publish order;
    ``None`` spreads them as the event store does. ``headers`` are carried as given, as the event store's native
    metadata: the seven :class:`EventHeaders` and any other (a dead letter's ``snodra-dlt-*``). Headers are UTF-8
    text, one value per name. ``payload`` is JSON bytes, carried as given.
    """

    source: str
    id: str
    type_name: str
    key: str | None
    headers: dict[str, str]
    payload: bytes


@dataclass(frozen=True, slots=True)
class Delivery:
    """One event a subscription delivers.

    ``segment`` and ``position`` are opaque strings of at most :data:`MAX_SEGMENT_BYTES` and
    :data:`MAX_POSITION_BYTES` bytes that only the event store reads (decimal numbers for the first-wave stores); the
    core orders two positions of one segment only through :meth:`EventStore.compare`.
    """

    segment: str
    position: str
    key: str | None
    headers: dict[str, str]
    payload: bytes
    timestamp_ms: int
    """When the event store appended it, epoch milliseconds (its own clock)."""


@dataclass(frozen=True, slots=True)
class Assigned:
    """These segments became this subscription's (in addition to the ones it has). A new subscription's first event,
    empty when it holds nothing."""

    segments: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Revoked:
    """These segments are this subscription's no longer; what it delivered of them and did not acknowledge comes again
    to whichever subscription gets them. Always followed by an :class:`Assigned` (possibly empty), which ends the
    change."""

    segments: tuple[str, ...] = ()


type SubscriptionEvent = Assigned | Revoked | Delivery
"""What :meth:`EventStoreSubscription.next` gives: an assignment change or a delivery."""


@dataclass(frozen=True, slots=True)
class SubscriptionSpec:
    """A processor's subscription: every subscription of one ``name`` shares the events of ``source`` (competing
    instances), and each name receives every event (one per processor)."""

    name: str
    """``<service>.<processor>``: the name the event store knows the subscription by."""

    source: str
    """The source read."""


@dataclass(frozen=True, slots=True)
class SegmentStatus:
    """One segment assigned to a subscription, as :meth:`EventStoreSubscription.segments` reports it (without the
    replay range, which is the core's). Positions as in :class:`Delivery`; ``None`` when unknown."""

    segment: str
    position: str | None = None
    """The next position this subscription delivers."""
    committed: str | None = None
    """The subscription's committed position (the one after its last acknowledgement)."""
    head: str | None = None
    """The position of the next event appended to the segment."""
    lag: int | None = None
    """How many events lie between ``committed`` and ``head`` (between ``position`` and ``head`` when nothing is
    committed): what is not acknowledged yet. ``None`` when unknown, always ``None`` without
    :attr:`Capability.LAG`."""


@dataclass(frozen=True, slots=True)
class ProcessedMarker:
    """The idempotency marker of a handled event: ``id`` is ``processor:<name>:<eventId>``, ``saga:<type>:<eventId>``
    and so on. A stage-1 marker of a delivered event also carries its ``processor``, ``source``, ``segment`` and
    ``position``, so a reset deletes exactly the markers of its replay range; every other marker leaves them ``None``.
    """

    id: str
    type_name: str
    processed_at_ms: int
    processor: str | None = None
    source: str | None = None
    segment: str | None = None
    position: str | None = None


@dataclass(frozen=True, slots=True)
class ResetTarget:
    """Where a reset moves a subscription (:attr:`Capability.REPLAY`)."""


@dataclass(frozen=True, slots=True)
class Earliest(ResetTarget):
    """Every segment's first position: everything the event store still has is a replay."""


@dataclass(frozen=True, slots=True)
class Latest(ResetTarget):
    """Every segment's head: nothing is replayed."""


@dataclass(frozen=True, slots=True)
class Timestamp(ResetTarget):
    """The first position at or after ``timestamp_ms`` (epoch milliseconds, the event store's append time) per
    segment, else its head."""

    timestamp_ms: int


@dataclass(frozen=True, slots=True)
class Positions(ResetTarget):
    """These segments to these positions; a segment not listed keeps its committed position."""

    positions: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class ReplayRange:
    """Events of ``segment`` of ``source`` at a position ``start <= position < until`` are a replay.

    What :meth:`EventStoreAdmin.plan` gives is one per segment the target addresses: ``start`` is the position it
    moves to, ``until`` the subscription's committed position before (empty when it had none). Only a range with
    ``until`` set and ``start`` before it (by :meth:`EventStore.compare`) replays anything; the others are moves
    without a replay.
    """

    source: str
    segment: str
    start: str
    until: str


class EventStoreSubscription(Protocol):
    """One instance's subscription of a processor (:meth:`EventStore.subscribe`): the segments the event store
    assigns it and their events, one at a time.

    - **Driven by** :meth:`next`. Nothing is delivered until it is called, and the core keeps calling it while paused:
      that keeps the subscription's membership alive and reports assignment changes.
    - **Order per segment.** A segment's events come in their order, each after the :class:`Assigned` naming its
      segment and before any :class:`Revoked` of it.
    - **The first event is an** :class:`Assigned` (an empty one when the subscription holds no segment), within
      :data:`FIRST_ASSIGNMENT_WITHIN` seconds of joining: an instance that joined and holds nothing is a member,
      idle. An empty first ``Assigned`` may be followed by one naming segments; an ``Assigned`` naming segments is
      never followed by a late empty "first" one.
    - **Every** :class:`Revoked` is followed by an :class:`Assigned` (an empty one when nothing new comes): the
      assignment change is over only then.
    - **At-least-once.** An event is acknowledged with :meth:`ack` after its handling committed; one not acknowledged
      comes again to the next subscription of the same name that gets its segment (after a :meth:`close` or a crash),
      or to this one after :meth:`redeliver`.
    - **A failure ends it.** An error from :meth:`next` means the subscription is broken: the caller closes it and
      subscribes again after a backoff.
    """

    async def next(self) -> SubscriptionEvent:
        """The next assignment change or delivery; waits for one.

        Cancel-safe: dropping the await before it completes loses nothing (the core races it with its shutdown and
        its timers). An error ends the subscription.
        """
        ...

    async def ack(self, segment: str, position: str) -> None:
        """Acknowledges the delivery at ``position`` of ``segment`` and every earlier one of that segment: a later
        subscription of this name resumes after it. May reach the event store after the call returns; :meth:`close`
        flushes it."""
        ...

    async def redeliver(self, segment: str, position: str) -> None:
        """Hands the delivery at ``position`` of ``segment`` out again, without leaving the subscription: :meth:`next`
        delivers it and that segment's later events again. Other segments are unaffected."""
        ...

    def pause(self) -> None:
        """Stops delivering without losing the place: :meth:`next` gives assignment changes only, and a segment
        assigned while paused is paused at once. Idempotent; called before the first :meth:`next`, nothing is ever
        delivered until :meth:`resume`."""
        ...

    def resume(self) -> None:
        """Delivers again from where the pause stopped. Idempotent."""
        ...

    def segments(self) -> list[SegmentStatus]:
        """The segments assigned now, in :func:`segment_order`, with their positions and lag as last known; never a
        blocking call."""
        ...

    def take_error(self) -> str | None:
        """The latest trouble the subscription recovered from by itself since the last call, as text for the
        processor's status; ``None`` when there was none. Never blocking."""
        ...

    async def close(self) -> None:
        """Leaves the subscription after flushing the acknowledgements: its segments go to the other subscriptions of
        the name."""
        ...


Subscription = EventStoreSubscription
"""The Rust trait's name for :class:`EventStoreSubscription`."""


class EventStoreAdmin(Protocol):
    """The reset side of one subscription (:meth:`EventStore.admin`). Every call does its event-store work off the
    event loop: implementations that block run it on a thread."""

    async def members(self) -> list[str]:
        """The members reading the subscription now (the event store's ids for them); empty when none does."""
        ...

    async def plan(self, target: ResetTarget) -> list[ReplayRange]:
        """Where ``target`` moves each segment, from the event store's current positions; nothing moves yet. Sorted by
        segment in :func:`segment_order`. A target position the segment does not have (a segment the source lacks, a
        position past the head) is :class:`Invalid`."""
        ...

    async def apply(self, ranges: Sequence[ReplayRange]) -> None:
        """Moves the committed position of each range's segment to its ``start`` (``until`` is not read), all or
        nothing; refused with :class:`SubscribersActive` while an instance reads the subscription."""
        ...


SubscriptionAdmin = EventStoreAdmin
"""The Rust trait's name for :class:`EventStoreAdmin`."""


class EventStore(Protocol):
    """An event-store add-on: see the module docstring for the contract it owes."""

    @property
    def name(self) -> str:
        """The add-on's name (``kafka``, ``rabbitmq-streams``, ``memory`` or a third party's), reported to the hub as
        the processors' event store."""
        ...

    @property
    def capabilities(self) -> frozenset[Capability]:
        """What it offers beyond the required contract."""
        ...

    def supports(self, capability: Capability) -> bool:
        """Whether it declares ``capability``."""
        ...

    async def publish(self, event: OutgoingEvent) -> None:
        """Appends ``event`` to ``event.source``; returns once the event store accepted it. Two publishes of one key,
        the second called after the first returned, are delivered in that order. Raises :class:`Rejected` when the
        event store will never take this event (the relay gives it up at once), :class:`Failed` when another try may
        succeed."""
        ...

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        """Joins the subscription ``spec`` as one more instance. The returned subscription is the caller's to close;
        an add-on whose subscribe is cancelled after it built one closes it itself."""
        ...

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        """The reset side of the subscription ``spec``, for a reset run while no instance reads it; raises
        :class:`Unsupported` without :attr:`Capability.REPLAY`."""
        ...

    def compare(self, a: str, b: str) -> int:
        """The order of two positions ``a`` and ``b`` of one segment: negative when ``a`` comes first, zero when they
        are the same, positive otherwise."""
        ...
