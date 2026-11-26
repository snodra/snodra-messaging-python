"""The in-memory event store: the whole event-store contract inside one process, for tests, local runs and
single-process demos, with no infrastructure.

- **One log per source**, split into a fixed number of segments (:class:`MemoryEventStore`'s ``segments``, four by
  default) named ``0`` to ``n - 1``; an event's key goes to segment ``FNV-1a 64(UTF-8 bytes) mod n`` (events without
  a key go round the segments from ``0``), so events of one key keep their publish order. Positions are the decimal
  indexes of a segment's events, from ``0``. Member ids are ``memory-<n>``.
- **A subscription per name**, shared by every :meth:`~snodra._eventstore.EventStore.subscribe` of that name in the
  process (competing instances): its segments are spread over its members, and a segment moves to another member only
  after the one holding it saw it revoked, was closed or was dropped. The committed positions are kept per name in
  memory.
- **Capabilities** ``replay`` and ``lag``; not ``browse`` (this library has no event browser) and not
  ``native-dead-letter`` (the dead letters stay in the message store, mode ``collection``).
- **Not durable**: it keeps every event it is given until the process ends, and loses them all then. Bounding what it
  holds is the caller's concern (a test, a demo).

One instance serves every service instance of a test: hand the same store to each subscription to make them compete.
Every method runs on one event loop; a subscription's :meth:`pause`, :meth:`resume` and :meth:`segments` may be called
from any coroutine of that loop.

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from bisect import bisect_left
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from functools import cmp_to_key
from typing import Final

from snodra._eventstore import (
    Assigned,
    Capability,
    Delivery,
    Earliest,
    EventStoreAdmin,
    EventStoreSubscription,
    Failed,
    Invalid,
    Latest,
    OutgoingEvent,
    Positions,
    ReplayRange,
    ResetTarget,
    Revoked,
    SegmentStatus,
    SubscribersActive,
    SubscriptionEvent,
    SubscriptionSpec,
    Timestamp,
    segment_order,
)

NAME: Final = "memory"
"""The event store's name, reported to the hub as the processors' event store."""

DEFAULT_SEGMENTS: Final = 4
"""Segments per source unless configured otherwise."""

MAX_SEGMENTS: Final = 1024
"""The most segments per source."""

_CAPABILITIES: Final[frozenset[Capability]] = frozenset({Capability.REPLAY, Capability.LAG})


@dataclass(slots=True)
class _Record:
    """One appended event, kept as published (bar the copies the store takes and gives)."""

    key: str | None
    headers: dict[str, str]
    payload: bytes
    timestamp_ms: int


type _Log = list[list[_Record]]


@dataclass(slots=True)
class _Group:
    """A subscription name's committed positions, its members (in joining order) and which member holds each segment."""

    committed: dict[int, int] = field(default_factory=dict)
    members: list[int] = field(default_factory=list)
    holders: dict[int, int] = field(default_factory=dict)

    def target(self, segment: int) -> int | None:
        """The member a segment goes to: the members share the segments round robin."""
        return self.members[segment % len(self.members)] if self.members else None

    def leave(self, member: int) -> None:
        """``member`` leaves: its segments are free for the others."""
        self.members = [other for other in self.members if other != member]
        self.holders = {segment: holder for segment, holder in self.holders.items() if holder != member}


class _Notifier:
    """Wakes the subscriptions waiting for a change of the store, without losing a wake between a check and a wait."""

    def __init__(self) -> None:
        self.version = 0
        self._waiters: set[asyncio.Future[None]] = set()

    def bump(self) -> None:
        """Tells every waiter that something it may wait for happened."""
        self.version += 1
        waiters, self._waiters = self._waiters, set()
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait(self, version: int) -> None:
        """Waits until :meth:`bump` ran after ``version`` was read; cancel-safe."""
        while self.version == version:
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.add(waiter)
            try:
                await waiter
            finally:
                self._waiters.discard(waiter)


class _Shared:
    """What every subscription of one store shares."""

    def __init__(self, segments: int) -> None:
        self.segments = segments
        self.sources: dict[str, _Log] = {}
        self.groups: dict[SubscriptionSpec, _Group] = {}
        self.next_member = 0
        self.spread = 0
        self.notifier = _Notifier()

    def source(self, name: str) -> _Log:
        """The log of ``name``, created empty on first use."""
        log = self.sources.get(name)
        if log is None:
            log = [[] for _ in range(self.segments)]
            self.sources[name] = log
        return log

    def records(self, source: str, segment: int) -> list[_Record]:
        """The events of ``segment`` of ``source``, oldest first; empty for a source nothing was published to."""
        log = self.sources.get(source)
        return [] if log is None else log[segment]

    def head(self, source: str, segment: int) -> int:
        """The position of the next event appended to ``segment``."""
        return len(self.records(source, segment))

    def segment(self, name: str) -> int:
        """The index of the segment named ``name`` (a canonical decimal from ``0`` to ``segments - 1``)."""
        index = _canonical(name)
        if index is None or index >= self.segments:
            raise Invalid(f"no segment {name!r} (the segments are 0 to {self.segments - 1})")
        return index


def _canonical(text: str) -> int | None:
    """``text`` as a decimal number written canonically (no sign, no leading zero), else ``None``."""
    if not text or not all("0" <= char <= "9" for char in text):
        return None
    if len(text) > 1 and text.startswith("0"):
        return None
    return int(text)


def _position(text: str) -> int:
    """A position as the store reads it: a canonical decimal event index."""
    index = _canonical(text)
    if index is None:
        raise Invalid(f"{text!r} is not a position of the in-memory event store")
    return index


def _fnv1a64(data: bytes) -> int:
    """FNV-1a 64 of ``data``, unsigned."""
    value = 0xCBF29CE484222325
    for byte in data:
        value ^= byte
        value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


def segment_of(key: str, segments: int) -> int:
    """The segment of ``key`` among ``segments``: FNV-1a 64 of its UTF-8 bytes, unsigned, modulo ``segments``."""
    return _fnv1a64(key.encode()) % segments


def _now_ms() -> int:
    """The wall clock in epoch milliseconds."""
    return time.time_ns() // 1_000_000


class MemoryEventStore:
    """The in-memory event store: see the module docstring.

    ``segments`` is the number of segments every source has, 1 to :data:`MAX_SEGMENTS`
    (:data:`DEFAULT_SEGMENTS` by default): how many instances of one service can share a processor's events.
    ``clock`` is the append time in epoch milliseconds (the wall clock by default), for tests that need known times.
    """

    def __init__(
        self,
        segments: int = DEFAULT_SEGMENTS,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if not 1 <= segments <= MAX_SEGMENTS:
            raise Invalid(f"the in-memory event store takes 1 to {MAX_SEGMENTS} segments, not {segments}")
        self._shared = _Shared(segments)
        self._clock = _now_ms if clock is None else clock

    @property
    def name(self) -> str:
        return NAME

    @property
    def capabilities(self) -> frozenset[Capability]:
        return _CAPABILITIES

    def supports(self, capability: Capability) -> bool:
        return capability in _CAPABILITIES

    async def publish(self, event: OutgoingEvent) -> None:
        """Appends at once: a keyed event to its key's segment, an unkeyed one to the next segment in turn."""
        shared = self._shared
        if event.key is None:
            index = shared.spread
            shared.spread = (shared.spread + 1) % shared.segments
        else:
            index = segment_of(event.key, shared.segments)
        segment = shared.source(event.source)[index]
        now = self._clock()
        # TODO(spec-gap): SG-15
        # A segment's append times never go back, so a time-targeted reset has one meaning even if the clock does not.
        timestamp_ms = max(segment[-1].timestamp_ms, now) if segment else now
        segment.append(_Record(event.key, dict(event.headers), event.payload, timestamp_ms))
        shared.notifier.bump()

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        """Joins at once as one more instance of ``spec.name``."""
        shared = self._shared
        shared.next_member += 1
        member = shared.next_member
        group = shared.groups.get(spec)
        if group is None:
            group = _Group()
            shared.groups[spec] = group
        group.members.append(member)
        shared.notifier.bump()
        return _MemorySubscription(shared, spec, member)

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        """The reset side of ``spec``: this store declares ``replay``, so it is always there."""
        return _MemoryAdmin(self._shared, spec)

    def compare(self, a: str, b: str) -> int:
        """Positions by value (they are decimal indexes); anything else is :class:`Invalid`."""
        first = _position(a)
        second = _position(b)
        return (first > second) - (first < second)


class _MemorySubscription:
    """One instance's subscription of the in-memory event store."""

    def __init__(self, shared: _Shared, spec: SubscriptionSpec, member: int) -> None:
        self._shared = shared
        self._spec = spec
        self._member = member
        self._cursors: dict[int, int] = {}
        self._paused = False
        self._turn = 0
        self._left = False
        self._changing = False
        self._announced = False

    async def next(self) -> SubscriptionEvent:
        while True:
            version = self._shared.notifier.version
            event = self._step()
            if event is not None:
                return event
            await self._shared.notifier.wait(version)

    def _step(self) -> SubscriptionEvent | None:
        """What :meth:`next` gives now, if anything; never waits, so a cancelled :meth:`next` loses nothing."""
        if self._left:
            raise Failed(f"the subscription {self._spec.name!r} is closed")
        shared = self._shared
        group = shared.groups.get(self._spec)
        if group is None:
            return None
        lost = sorted(segment for segment in self._cursors if group.target(segment) != self._member)
        if lost:
            for segment in lost:
                del self._cursors[segment]
                if group.holders.get(segment) == self._member:
                    del group.holders[segment]
            shared.notifier.bump()  # the next holder may take them now
            self._changing = True
            return Revoked(tuple(str(segment) for segment in lost))
        gained = [
            segment
            for segment in range(shared.segments)
            if group.target(segment) == self._member
            and segment not in self._cursors
            and group.holders.get(segment) is None
        ]
        if gained:
            for segment in gained:
                group.holders[segment] = self._member
                self._cursors[segment] = group.committed.get(segment, 0)
            self._changing = False
            self._announced = True
            return Assigned(tuple(str(segment) for segment in gained))
        if self._changing or not self._announced:
            # Every revocation is closed by an assignment, an empty one when nothing new came; and a new subscription
            # that holds nothing says so (a member, idle).
            self._changing = False
            self._announced = True
            return Assigned(())
        if self._paused or not self._cursors:
            return None
        log = shared.sources.get(self._spec.source)
        if log is None:
            return None
        held = sorted(self._cursors)
        for index in range(len(held)):
            segment = held[(self._turn + index) % len(held)]
            at = self._cursors[segment]
            if at < len(log[segment]):
                record = log[segment][at]
                self._cursors[segment] = at + 1
                self._turn = (self._turn + index + 1) % len(held)
                return Delivery(
                    segment=str(segment),
                    position=str(at),
                    key=record.key,
                    headers=dict(record.headers),
                    payload=record.payload,
                    timestamp_ms=record.timestamp_ms,
                )
        return None

    async def ack(self, segment: str, position: str) -> None:
        index = self._shared.segment(segment)
        at = _position(position)
        group = self._shared.groups.get(self._spec)
        if group is None or index not in self._cursors:
            return  # given up: its next holder starts from what was acknowledged before
        committed = group.committed.get(index, 0)
        if at + 1 > committed:
            group.committed[index] = at + 1

    async def redeliver(self, segment: str, position: str) -> None:
        index = self._shared.segment(segment)
        at = _position(position)
        cursor = self._cursors.get(index)
        # TODO(spec-gap): SG-14
        if cursor is None or at >= cursor:
            return
        self._cursors[index] = at
        self._shared.notifier.bump()

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False
        self._shared.notifier.bump()

    def segments(self) -> list[SegmentStatus]:
        shared = self._shared
        group = shared.groups.get(self._spec)
        statuses: list[SegmentStatus] = []
        for segment in self._cursors:
            head = shared.head(self._spec.source, segment)
            position = self._cursors[segment]
            committed = None if group is None else group.committed.get(segment)
            lag = head - (committed if committed is not None else position)
            statuses.append(
                SegmentStatus(
                    segment=str(segment),
                    position=str(position),
                    committed=None if committed is None else str(committed),
                    head=str(head),
                    lag=max(0, lag),
                )
            )
        statuses.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return statuses

    def take_error(self) -> str | None:
        return None

    async def close(self) -> None:
        self._leave()

    def _leave(self) -> None:
        if self._left:
            return
        self._left = True
        group = self._shared.groups.get(self._spec)
        if group is not None:
            group.leave(self._member)
        self._cursors.clear()
        self._shared.notifier.bump()

    def __del__(self) -> None:
        """A subscription dropped without a close (a task that ended or failed) leaves too."""
        with contextlib.suppress(Exception):  # the interpreter may be shutting its modules down
            self._leave()


class _MemoryAdmin:
    """The reset side of one subscription name: every call answers at once."""

    def __init__(self, shared: _Shared, spec: SubscriptionSpec) -> None:
        self._shared = shared
        self._spec = spec

    async def members(self) -> list[str]:
        group = self._shared.groups.get(self._spec)
        return [] if group is None else [f"memory-{member}" for member in group.members]

    async def plan(self, target: ResetTarget) -> list[ReplayRange]:
        shared = self._shared
        spec = self._spec
        group = shared.groups.get(spec)
        committed = {} if group is None else group.committed

        def range_for(segment: int, start: int) -> ReplayRange:
            until = committed.get(segment)
            return ReplayRange(
                source=spec.source,
                segment=str(segment),
                start=str(start),
                until="" if until is None else str(until),
            )

        ranges: list[ReplayRange]
        if isinstance(target, Earliest):
            ranges = [range_for(segment, 0) for segment in range(shared.segments)]
        elif isinstance(target, Latest):
            ranges = [range_for(segment, shared.head(spec.source, segment)) for segment in range(shared.segments)]
        elif isinstance(target, Timestamp):
            ranges = [
                range_for(segment, bisect_left(_times(shared.records(spec.source, segment)), target.timestamp_ms))
                for segment in range(shared.segments)
            ]
        elif isinstance(target, Positions):
            ranges = []
            for segment, position in target.positions:
                index = shared.segment(segment)
                start = _position(position)
                head = shared.head(spec.source, index)
                if start > head:
                    raise Invalid(f"position {start} is past the head {head} of segment {index}")
                ranges.append(range_for(index, start))
        else:
            raise Invalid(f"unknown reset target {target!r}")
        ranges.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return ranges

    async def apply(self, ranges: Sequence[ReplayRange]) -> None:
        shared = self._shared
        spec = self._spec
        moves: list[tuple[int, int]] = []
        for replay_range in ranges:
            if replay_range.source != spec.source:
                raise Invalid(f"a range of source {replay_range.source!r}, not of {spec.source!r}")
            index = shared.segment(replay_range.segment)
            start = _position(replay_range.start)
            head = shared.head(spec.source, index)
            if start > head:
                raise Invalid(f"position {start} is past the head {head} of segment {index}")
            moves.append((index, start))
        group = shared.groups.get(spec)
        if group is not None and group.members:
            raise SubscribersActive([f"memory-{member}" for member in group.members])
        if group is None:
            group = _Group()
            shared.groups[spec] = group
        for index, start in moves:
            group.committed[index] = start
        shared.notifier.bump()


def _times(records: Sequence[_Record]) -> list[int]:
    """The append times of ``records``; a segment's are nondecreasing (see :meth:`MemoryEventStore.publish`)."""
    return [record.timestamp_ms for record in records]
