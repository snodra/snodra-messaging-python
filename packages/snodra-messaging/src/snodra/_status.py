"""The event processors' status: one ``ProcessorStatus`` per started processor, built from memory.

Every instance reports its processors on every ``InstanceStatus`` it sends
(https://snodra.damoreira.com/docs/library-authors/protocol/, "Processor status"). The status is built from what the
processor holds in memory and what its subscription reports, so building it never waits on a store, a consumer or a
thread: ``segments()`` is the subscription's own non-blocking read.

The hub bounds what it keeps of a status (``hub.proto``): strings are cut at a UTF-8 boundary, a segment named in more
than :data:`~snodra._eventstore.MAX_SEGMENT_BYTES` bytes is dropped and a longer position reads as unknown (empty),
since a cut position would name another record.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final

from snodra._eventstore import MAX_POSITION_BYTES, MAX_SEGMENT_BYTES, SegmentStatus
from snodra._hubpb import hub_pb2

FAILING_WINDOW_MS: Final = 60_000
"""A processor reports ``PROCESSOR_FAILING`` this long after a record exhausted its attempts or its reader reported an
error."""

MAX_LAST_ERROR_BYTES: Final = 512
"""``ProcessorStatus.last_error`` is cut to this many UTF-8 bytes."""

MAX_NAME_BYTES: Final = 64
"""``ProcessorStatus.event_store`` and each capability are cut to this many UTF-8 bytes."""

MAX_SUBSCRIPTION_BYTES: Final = 256
"""``ProcessorStatus.subscription`` and ``ProcessorStatus.source`` are cut to this many UTF-8 bytes."""

MAX_EVENT_TYPE_BYTES: Final = 256
"""An event type in ``ProcessorStatus.event_types`` is cut to this many UTF-8 bytes."""

MAX_CAPABILITIES: Final = 16
"""The most capabilities a status lists."""

MAX_TEXT_BYTES: Final = 512
"""A control answer's texts are cut to this many UTF-8 bytes (``ControlOk.detail``, ``ControlRefused.message``,
``DeadLetterSummary.error_message``)."""


def utf8_cut(text: str, max_bytes: int) -> str:
    """``text`` cut to at most ``max_bytes`` UTF-8 bytes, never inside one character."""
    if len(text.encode("utf-8", "replace")) <= max_bytes:
        return text
    kept: list[str] = []
    used = 0
    for char in text:
        size = len(char.encode("utf-8", "replace"))
        if used + size > max_bytes:
            break
        kept.append(char)
        used += size
    return "".join(kept)


def bounded_position(position: str | None) -> str:
    """A position as the status carries it: ``""`` when unknown or longer than :data:`MAX_POSITION_BYTES` (never cut: a
    cut position is wrong)."""
    if position is None or len(position.encode("utf-8", "replace")) > MAX_POSITION_BYTES:
        return ""
    return position


def state_of(
    *,
    running: bool,
    paused: bool,
    failing_since_ms: int,
    now_ms: int,
    assigned: bool,
    rebalancing: bool,
) -> hub_pb2.ProcessorState:
    """The ``ProcessorState`` of a processor, by the rules of ``hub.proto``.

    ``STOPPED`` while nothing reads (before a reset's lock cleared it, or after shutdown), ``PAUSED`` while a pause
    holds it (also when a failure is recent), ``FAILING`` for :data:`FAILING_WINDOW_MS` after an exhausted record or a
    reader error, ``STARTING`` before the first assignment or during a reassignment, else ``RUNNING`` (idle included).
    """
    if not running:
        return hub_pb2.PROCESSOR_STOPPED
    if paused:
        return hub_pb2.PROCESSOR_PAUSED
    if failing_since_ms and now_ms - failing_since_ms < FAILING_WINDOW_MS:
        return hub_pb2.PROCESSOR_FAILING
    if not assigned or rebalancing:
        return hub_pb2.PROCESSOR_STARTING
    return hub_pb2.PROCESSOR_RUNNING


@dataclass(frozen=True, slots=True)
class StatusView:
    """What a processor reports, read from memory: the fields of one ``ProcessorStatus``."""

    name: str
    subscription: str
    source: str
    event_store: str
    capabilities: frozenset[str]
    state: hub_pb2.ProcessorState
    segments: Sequence[SegmentStatus] = ()
    replay_until: Mapping[str, str] = field(default_factory=dict)
    processed_total: int = 0
    failed_total: int = 0
    dead_letters: int = 0
    last_error: str = ""
    last_error_at_ms: int = 0
    event_types: Sequence[str] = ()
    dead_letter_mode: str = "collection"
    replaying: bool = False
    operation_id: str = ""


def build_status(view: StatusView) -> hub_pb2.ProcessorStatus:
    """One ``ProcessorStatus``, its strings and segments bounded as ``hub.proto`` says."""
    status = hub_pb2.ProcessorStatus(
        name=utf8_cut(view.name, MAX_NAME_BYTES),
        subscription=utf8_cut(view.subscription, MAX_SUBSCRIPTION_BYTES),
        source=utf8_cut(view.source, MAX_SUBSCRIPTION_BYTES),
        state=view.state,
        processed_total=view.processed_total,
        failed_total=view.failed_total,
        dead_letters=view.dead_letters,
        last_error=utf8_cut(view.last_error, MAX_LAST_ERROR_BYTES),
        last_error_at_ms=view.last_error_at_ms,
        event_types=[utf8_cut(name, MAX_EVENT_TYPE_BYTES) for name in view.event_types],
        dead_letter_mode=view.dead_letter_mode,
        replaying=view.replaying,
        operation_id=utf8_cut(view.operation_id, MAX_NAME_BYTES),
        event_store=utf8_cut(view.event_store, MAX_NAME_BYTES),
        capabilities=[utf8_cut(name, MAX_NAME_BYTES) for name in sorted(view.capabilities)[:MAX_CAPABILITIES]],
    )
    for segment in view.segments:
        if len(segment.segment.encode("utf-8", "replace")) > MAX_SEGMENT_BYTES:
            continue
        status.segments.append(
            hub_pb2.SegmentStatus(
                segment=segment.segment,
                position=bounded_position(segment.position),
                committed=bounded_position(segment.committed),
                head=bounded_position(segment.head),
                lag=-1 if segment.lag is None else max(0, segment.lag),
                replay_until=bounded_position(view.replay_until.get(segment.segment)),
            )
        )
    return status
