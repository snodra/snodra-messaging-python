"""Replay ranges: a record inside one runs the processor's handlers again, without the ones marked for no replay.

A reset stores the ranges it leaves in ``processor_control``; every instance applies them to its consumer. A record of
``segment`` at a position ``start <= position < until`` (by :meth:`~snodra._eventstore.EventStore.compare`) is a replay:
stage 1 runs without the handlers registered with ``replay=False`` (their marker is still written), and a failure parks
a letter marked as a replay so a later retry skips the same handlers. Sagas never replay (they run in ``default`` only,
in a later level). The range closes once the subscription's committed position (its delivered position while nothing is
committed) reaches ``until``; the instance that sees it removes the range from the control document.

A marker without a position is never deleted by a reset (nothing says where it came from), so a record whose marker
survived a reset is a duplicate and does not replay; markers of the range were deleted by the execute.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", "The target of a reset") and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Replay).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from snodra._eventstore import Delivery, ReplayRange, SegmentStatus


def in_replay(delivery: Delivery, ranges: Sequence[ReplayRange], compare: Callable[[str, str], int]) -> bool:
    """Whether ``delivery`` is inside one of ``ranges`` (``start <= position < until`` by ``compare``)."""
    for replay_range in ranges:
        if replay_range.segment != delivery.segment or not replay_range.until:
            continue
        try:
            start_before = compare(replay_range.start, delivery.position) <= 0
            if start_before and compare(delivery.position, replay_range.until) < 0:
                return True
        except Exception:
            continue  # a position the store cannot order is not inside the range
    return False


def passed(at: str | None, until: str, compare: Callable[[str, str], int]) -> bool:
    """Whether position ``at`` (``None`` unknown) is at or past ``until`` by ``compare``."""
    if at is None or not until:
        return False
    try:
        return compare(at, until) >= 0
    except Exception:
        return False


def open_until(
    ranges: Sequence[ReplayRange],
    segments: Sequence[SegmentStatus],
    compare: Callable[[str, str], int],
) -> tuple[dict[str, str], bool]:
    """Each segment's open replay end (the furthest range end its progress has not passed), and whether any is open."""
    until: dict[str, str] = {}
    replaying = False
    for segment in segments:
        progress = segment.committed if segment.committed is not None else segment.position
        ends = [
            replay_range.until
            for replay_range in ranges
            if replay_range.segment == segment.segment and not passed(progress, replay_range.until, compare)
        ]
        if not ends:
            continue
        furthest = ends[0]
        for end in ends[1:]:
            try:
                if compare(end, furthest) > 0:
                    furthest = end
            except Exception:
                continue
        until[segment.segment] = furthest
        replaying = True
    return until, replaying


def passed_ranges(
    ranges: Sequence[ReplayRange],
    segments: Sequence[SegmentStatus],
    compare: Callable[[str, str], int],
) -> list[ReplayRange]:
    """The ranges each of the assigned ``segments`` has consumed past (its committed or delivered position reached
    ``until``)."""
    by_segment = {segment.segment: segment for segment in segments}
    gone: list[ReplayRange] = []
    for replay_range in ranges:
        segment = by_segment.get(replay_range.segment)
        if segment is None:
            continue
        progress = segment.committed if segment.committed is not None else segment.position
        if passed(progress, replay_range.until, compare):
            gone.append(replay_range)
    return gone
