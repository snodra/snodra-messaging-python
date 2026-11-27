"""The leased reset state machine: prepare, execute and finish, and the replay ranges a reset leaves.

A reset is a leased operation, not a call: the hub prepares every instance that runs the processor, the instance with
the freshest status executes, and every prepared instance is finished afterwards (or aborted). The state lives in
``processor_control`` so a late message finds it, and the operation id is the lease's name.

- **Prepare** takes the lock for ``operation_id`` until ``lease_ms`` from now and leaves the subscription. The same
  operation prepares again as a renewal (it never leaves twice). A second operation is ``LOCKED``; an operation that
  already ended on this instance — or a renewal whose lock is gone — is ``NOT_PREPARED``, never a new prepare.
- **Execute** plans where the target moves each segment with the event store, refuses while an instance still reads the
  subscription, runs the processor's reset handlers when anything replays, deletes the processor's stage-1 markers in
  the ranges, stores the ranges, then moves the positions. The handlers run before any marker or position changes; a
  refusal after they ran says what did change. The executor renews its own lease while it works; one that outlives
  :data:`EXECUTE_LIMIT` stops before its markers.
- **Finish** releases the lock (also when nothing is held) and consumes again, still paused if it was. A lease that
  passes without a renewal is released by the instance that held it and it consumes again by itself; an instance that
  starts under a held lock does not join.

The replay itself lives in :mod:`snodra._replay`: a range ``[start, until)`` makes the processor run its handlers for
the records again, except those marked ``replay=False``; the range closes when the committed position reaches
``until`` and is then removed from the control document.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", "The target of a reset") and
https://snodra.damoreira.com/docs/server/processor-control/.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import cmp_to_key
from typing import TYPE_CHECKING, Final

from snodra._eventstore import (
    Capability,
    EventStore,
    Failed,
    Invalid,
    Latest,
    Positions,
    Rejected,
    ReplayRange,
    ResetTarget,
    StoreError,
    SubscribersActive,
    Unsupported,
    segment_order,
)
from snodra._store import Locked, MessageStore, ProcessorControlState

if TYPE_CHECKING:
    from snodra._events import EventRegistry
    from snodra._processors import Processors

log = logging.getLogger(__name__)

MIN_LEASE_MS: Final = 1000
"""The shortest lease a prepare may ask for: a shorter one would lapse before anything happened."""

EXECUTE_LIMIT: Final = 120.0
"""The longest an execute runs before it stops its own lease (the hub's reset timeout), in seconds."""

LEASE_EXTENSION: Final = 10.0
"""An executing reset renews its lease at least this often (and at most a third of the lease)."""

FINISHED_KEPT: Final = 60.0
"""An operation that ended on this instance is remembered at least this long (twice its lease when longer)."""

FINISHED_KEPT_MAX: Final = 24 * 60 * 60.0
"""And at most this long (a lease of years would overflow the clock)."""


@dataclass(frozen=True, slots=True)
class ResetContext:
    """What a reset handler is told: the processor, the reset's operation id, its target and the ranges it replays
    (never empty)."""

    processor: str
    operation_id: str
    target: ResetTarget
    ranges: tuple[ReplayRange, ...]


@dataclass(frozen=True, slots=True)
class ResetOk:
    """A prepare, a renewal or a finish that did what it says; ``detail`` is free text for the operator."""

    detail: str


@dataclass(frozen=True, slots=True)
class ResetRefused:
    """The instance would not or could not act: ``code`` is one of the ``CONTROL_REFUSAL_*`` suffixes (``LOCKED``,
    ``NOT_PREPARED``, ...), ``message`` says what did or did not change."""

    code: str
    message: str


@dataclass(frozen=True, slots=True)
class ResetDone:
    """An execute that moved the subscription: the ranges that replay (empty for ``latest``), the markers deleted and
    the reset handlers that ran."""

    ranges: tuple[ReplayRange, ...]
    markers_deleted: int
    reset_handlers: int


type ResetReply = ResetOk | ResetRefused | ResetDone
"""What one reset action answered."""


def replaying(
    moves: Sequence[ReplayRange],
    target: ResetTarget,
    open_ranges: Sequence[ReplayRange],
    compare: Callable[[str, str], int],
) -> tuple[ReplayRange, ...]:
    """The moves that replay: ``[start, until)`` with ``until`` set and ``start`` before it, never for ``latest``; a
    segment that still has an open range gets ``[start, max(until, open until))`` (its records past ``until`` were not
    replayed yet and their markers are gone). In segment order."""
    if isinstance(target, Latest):
        return ()
    result: list[ReplayRange] = []
    for move in moves:
        if not move.until or compare(move.start, move.until) >= 0:
            continue
        until = move.until
        for existing in open_ranges:
            same = existing.source == move.source and existing.segment == move.segment
            if same and compare(existing.until, until) > 0:
                until = existing.until
        result.append(ReplayRange(move.source, move.segment, move.start, until))
    result.sort(key=cmp_to_key(lambda a, b: _range_order(a, b, compare)))
    return tuple(result)


def merged(
    open_ranges: Sequence[ReplayRange],
    new: Sequence[ReplayRange],
    compare: Callable[[str, str], int],
) -> tuple[ReplayRange, ...]:
    """The ranges after the reset: the open ones without the segments that got a new one, then ``new``; sorted by
    source, segment (in segment order) and ``start``."""
    kept = [
        existing
        for existing in open_ranges
        if not any(item.source == existing.source and item.segment == existing.segment for item in new)
    ]
    result = [*kept, *new]
    result.sort(key=cmp_to_key(lambda a, b: _range_order(a, b, compare)))
    return tuple(result)


def _range_order(a: ReplayRange, b: ReplayRange, compare: Callable[[str, str], int]) -> int:
    if a.source != b.source:
        return (a.source > b.source) - (a.source < b.source)
    order = segment_order(a.segment, b.segment)
    return order if order != 0 else compare(a.start, b.start)


class ResetMachine:
    """The reset state machine of this instance's processors, driven by the control plane and the control poll."""

    def __init__(
        self,
        processors: Processors,
        store: MessageStore,
        events: EventStore,
        registry: EventRegistry,
        clock: Callable[[], int],
        *,
        monotonic: Callable[[], float] = time.monotonic,
        execute_limit: float = EXECUTE_LIMIT,
        lease_extension: float = LEASE_EXTENSION,
        finished_kept: float = FINISHED_KEPT,
    ) -> None:
        self._processors = processors
        self._store = store
        self._events = events
        self._registry = registry
        self._clock = clock
        self._monotonic = monotonic
        self._execute_limit = execute_limit
        self._lease_extension = lease_extension
        self._finished_kept = finished_kept
        self._held: dict[str, tuple[str, int]] = {}  # processor -> (operation id, lease ms)
        self._executing: dict[str, str] = {}
        self._ended: dict[tuple[str, str], float] = {}  # (processor, operation) -> monotonic deadline

    # ---- state the manager and the control plane read ---------------------------------------------------------

    def held(self, processor: str) -> str | None:
        """The operation this instance holds ``processor``'s lock for, or ``None``."""
        held = self._held.get(processor)
        return None if held is None else held[0]

    def executing(self, processor: str) -> str | None:
        """The operation whose execute runs here now, or ``None``."""
        return self._executing.get(processor)

    def hold(self, processor: str, operation: str, lease_ms: int) -> None:
        """Marks ``operation`` held by this instance (a test or a restored state)."""
        self._held[processor] = (operation, lease_ms)

    def unhold(self, processor: str) -> None:
        """Forgets the operation this instance held, remembering that it ended."""
        held = self._held.pop(processor, None)
        if held is not None:
            self._remember_ended(processor, held[0], held[1])

    def ended_recently(self, processor: str, operation: str) -> bool:
        """Whether ``operation`` ended on this instance recently (a late renewal is ``NOT_PREPARED``)."""
        deadline = self._ended.get((processor, operation))
        if deadline is None:
            return False
        if self._monotonic() >= deadline:
            del self._ended[(processor, operation)]
            return False
        return True

    def _remember_ended(self, processor: str, operation: str, lease_ms: int) -> None:
        now = self._monotonic()
        for key, deadline in list(self._ended.items()):
            if deadline <= now:
                del self._ended[key]
        kept = min(max(self._finished_kept, lease_ms * 2 / 1000), FINISHED_KEPT_MAX)
        self._ended[(processor, operation)] = now + kept

    async def converge_held(self, processor: str, document: ProcessorControlState, now_ms: int) -> str | None:
        """Applies the document to what this instance holds: a lock no longer named ended; an expired one is cleared
        (unless its execute runs here) and noted as ended. Returns the operation held now, or ``None``."""
        held = self._held.get(processor)
        if held is None:
            return None
        operation, lease_ms = held
        current = document.operation
        if current is None or current.id != operation:
            # Another instance finished it, or a new operation took the lock: this one ended.
            log.warning(
                "The reset operation %s of processor %s is no longer in its control document: releasing it locally",
                operation,
                processor,
            )
            self._held.pop(processor, None)
            self._remember_ended(processor, operation, lease_ms)
            return None
        if current.holds_at(now_ms) or self._executing.get(processor) == operation:
            return operation
        expired = await self._store.control.expire(processor, operation, now_ms)
        if self._held.get(processor) != held:
            # A prepare landed while the expiry was checked: the new operation owns the lock now, and this poll must
            # neither drop it nor start the consumer under it.
            return self.held(processor)
        if expired:
            log.warning(
                "The lease of reset operation %s on processor %s passed without a renewal: released it, "
                "consuming again",
                operation,
                processor,
            )
            self._held.pop(processor, None)
            self._remember_ended(processor, operation, lease_ms)
            return None
        return operation  # renewed meanwhile: the next poll decides

    # ---- prepare ----------------------------------------------------------------------------------------------

    async def prepare(self, processor: str, operation: str, lease_ms: int) -> ResetReply:
        """Takes the processor's lock for ``operation`` and leaves its subscription (a renewal of the held one)."""
        operation = operation.strip()
        if not processor or not self._processors.runs(processor):
            return ResetRefused("UNKNOWN_PROCESSOR", f"processor {processor} does not run on this instance")
        if not operation.strip() or lease_ms < MIN_LEASE_MS:
            return ResetRefused("INVALID", f"a prepare needs an operation id and a lease of at least {MIN_LEASE_MS} ms")
        if not self._events.supports(Capability.REPLAY):
            return self._unsupported(processor)
        if self.ended_recently(processor, operation):
            return ResetRefused("NOT_PREPARED", f"operation {operation} already finished on this instance")
        self._processors.bump(processor)
        now = self._clock()
        held = self._held.get(processor)
        if held is not None and held[0] == operation:
            # A renewal: the lock must still be this operation's; a late renewal never takes a free lock again.
            if await self._store.control.extend(processor, operation, now + lease_ms, now):
                self._held[processor] = (operation, lease_ms)
                self._processors.consumer(processor).operation_id = operation
                await self._processors.set_running(processor, False, why=f"reset operation {operation} holds its lock")
                return ResetOk("lease renewed")
            self._held.pop(processor, None)
            self._remember_ended(processor, operation, held[1])
            return ResetRefused(
                "NOT_PREPARED", f"lock lost: operation {operation} no longer holds the lock of processor {processor}"
            )
        lock = await self._store.control.lock(processor, operation, now + lease_ms, now)
        if isinstance(lock, Locked) and not lock.taken:
            holder = lock.holder
            if holder is None:
                return ResetRefused("LOCKED", f"another operation holds the lock of processor {processor}")
            return ResetRefused(
                "LOCKED",
                f"operation {holder.id} holds the lock of processor {processor} until {holder.lease_until_ms} ms",
            )
        self._held[processor] = (operation, lease_ms)
        self._processors.bump(processor)
        self._processors.consumer(processor).operation_id = operation
        await self._processors.set_running(processor, False, why=f"reset operation {operation} holds its lock")
        await self._processors.converge(processor, initial=False)
        log.info(
            "Processor %s prepared for reset operation %s: left the subscription %s for %d ms",
            processor,
            operation,
            self._processors.consumer(processor).subscription.name,
            lease_ms,
        )
        return ResetOk(f"stopped, left the subscription {self._processors.consumer(processor).subscription.name}")

    # ---- execute ----------------------------------------------------------------------------------------------

    async def execute(self, processor: str, operation: str, target: ResetTarget) -> ResetReply:
        """Plans and applies the reset of ``operation``, its reset handlers first and its positions last."""
        operation = operation.strip()
        if not processor or not self._processors.runs(processor):
            return ResetRefused("UNKNOWN_PROCESSOR", f"processor {processor} does not run on this instance")
        if not operation.strip():
            return ResetRefused("INVALID", "an execute needs an operation id")
        if not self._events.supports(Capability.REPLAY):
            return self._unsupported(processor)
        held = self._held.get(processor)
        if held is None or held[0] != operation:
            return await self._not_prepared(processor, operation)
        self._processors.bump(processor)
        listed = target.positions if isinstance(target, Positions) else ()
        if isinstance(target, Positions) and (not listed or len({segment for segment, _ in listed}) != len(listed)):
            return ResetRefused("INVALID", "the positions target lists no segment or names one twice")
        lease_ms = held[1]
        self._executing[processor] = operation
        until = self._monotonic() + self._execute_limit
        extension = asyncio.create_task(
            self._extend_lease(processor, operation, lease_ms, until),
            name=f"snodra:reset-lease:{processor}",
        )
        try:
            return await self._reset(processor, operation, target, lease_ms, until)
        finally:
            extension.cancel()
            await asyncio.gather(extension, return_exceptions=True)
            if self._monotonic() < until:
                try:
                    await self._store.control.extend(processor, operation, self._clock() + lease_ms, self._clock())
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    log.debug("Could not renew the lease of reset operation %s for its finish: %s", operation, error)
            self._executing.pop(processor, None)

    async def _extend_lease(self, processor: str, operation: str, lease_ms: int, until: float) -> None:
        """Renews the executor's lease every :data:`LEASE_EXTENSION` seconds (at most a third of the lease)."""
        period = min(self._lease_extension, max(lease_ms / 3000, 0.01))
        while True:
            await asyncio.sleep(period)
            if self._monotonic() >= until:
                return
            try:
                await self._store.control.extend(processor, operation, self._clock() + lease_ms, self._clock())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.debug("Could not extend the lease of reset operation %s: %s", operation, error)

    async def _reset(
        self,
        processor: str,
        operation: str,
        target: ResetTarget,
        lease_ms: int,
        until: float,
    ) -> ResetReply:
        consumer = self._processors.consumer(processor)
        spec = consumer.subscription
        compare = self._events.compare
        try:
            admin = self._events.admin(spec)
        except Unsupported as error:
            return self._unsupported(processor, error)
        except StoreError as error:
            return self._store_refused(error, processor)
        try:
            plan = await admin.plan(target)
            members = await admin.members()
        except Unsupported as error:
            return self._unsupported(processor, error)
        except StoreError as error:
            return self._store_refused(error, processor)
        except Exception as error:
            return ResetRefused("FAILED", f"{type(error).__name__}: {error}")
        if members:
            return ResetRefused(
                "SUBSCRIBERS_ACTIVE",
                f"subscription {spec.name} still has {len(members)} subscriber(s): {', '.join(members)}",
            )
        document = await self._store.control.get(processor)
        ranges = replaying(plan, target, document.replay, compare)
        handlers_run = 0
        changed = _Changed()
        if ranges:
            context = ResetContext(processor, operation, target, ranges)
            for handler in self._registry.reset_handlers(processor):
                handlers_run += 1
                changed.handlers = handlers_run
                try:
                    if handler.wants_context:
                        await handler.fn(context)
                    else:
                        await handler.fn()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    log.warning(
                        "Reset handler %s of processor %s failed, the reset %s stops before the markers: %s: %s",
                        handler.label,
                        processor,
                        operation,
                        type(error).__name__,
                        error,
                    )
                    return changed.told(
                        ResetRefused(
                            "RESET_HANDLER_FAILED",
                            f"{handler.label} failed: {type(error).__name__}: {error}",
                        )
                    )
        lost = await self._lease_lost(processor, operation, lease_ms, until)
        if lost is not None:
            return changed.told(ResetRefused("FAILED", lost))
        try:
            if ranges:
                changed.markers = await self._store.processed.delete_markers(processor, ranges)
            replay = merged(document.replay, ranges, compare)
            if ranges:
                await self._store.control.set_replay(processor, replay, self._clock())
                changed.ranges = True
            lost = await self._lease_lost(processor, operation, lease_ms, until)
            if lost is not None:
                return changed.told(ResetRefused("FAILED", lost))
            try:
                # A reader may have rejoined since the plan: check again immediately before the positions move.
                members = await admin.members()
                if members:
                    return changed.told(
                        ResetRefused(
                            "SUBSCRIBERS_ACTIVE",
                            f"subscription {spec.name} still has {len(members)} subscriber(s): {', '.join(members)}",
                        )
                    )
                await admin.apply(plan)
            except Unsupported as error:
                return changed.told(self._unsupported(processor, error))
            except StoreError as error:
                return changed.told(self._store_refused(error, processor))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning("Reset %s of processor %s failed after its reset handlers: %s", operation, processor, error)
            return changed.told(ResetRefused("FAILED", f"{type(error).__name__}: {error}"))
        consumer.set_replay(replay)
        self._processors.bump(processor)
        log.info(
            "Processor %s reset to %s by operation %s: %d markers deleted, %d reset handlers, %d replaying range(s)",
            processor,
            target,
            operation,
            changed.markers,
            handlers_run,
            len(ranges),
        )
        return ResetDone(ranges, changed.markers, handlers_run)

    async def _lease_lost(self, processor: str, operation: str, lease_ms: int, until: float) -> str | None:
        """``None`` when the lease was extended now and the execute is within its limit; else why it was lost."""
        if self._monotonic() - until >= 0:
            return f"lease lost: the reset ran longer than {self._execute_limit:g} s"
        try:
            renewed = await self._store.control.extend(processor, operation, self._clock() + lease_ms, self._clock())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return f"lease lost: cannot extend it: {type(error).__name__}: {error}"
        if not renewed:
            return f"lease lost: operation {operation} no longer holds the lock of processor {processor}"
        return None

    async def _not_prepared(self, processor: str, operation: str) -> ResetRefused:
        document = await self._store.control.get(processor)
        current = document.operation
        holder = ""
        if current is not None and current.holds_at(self._clock()):
            holder = f"; operation {current.id} holds the lock"
        return ResetRefused("NOT_PREPARED", f"operation {operation} is not prepared on this instance{holder}")

    def _unsupported(self, processor: str, error: Unsupported | None = None) -> ResetRefused:
        capability = Capability.REPLAY.value if error is None else str(error.feature)
        return ResetRefused(
            "UNSUPPORTED",
            f"the event store {self._events.name} lacks the capability {capability}: "
            f"processor {processor} cannot be reset",
        )

    def _store_refused(self, error: StoreError, processor: str) -> ResetRefused:
        if isinstance(error, Invalid):
            return ResetRefused("INVALID", str(error))
        if isinstance(error, SubscribersActive):
            return ResetRefused(
                "SUBSCRIBERS_ACTIVE",
                f"the subscription has active subscribers: {', '.join(error.members)}",
            )
        if isinstance(error, Unsupported):
            return self._unsupported(processor, error)
        if isinstance(error, Rejected | Failed):
            log.warning("The reset of a processor: the event store failed: %s", error)
            return ResetRefused("FAILED", str(error))
        return ResetRefused("FAILED", f"{type(error).__name__}: {error}")

    # ---- finish -----------------------------------------------------------------------------------------------

    async def finish(self, processor: str, operation: str) -> ResetReply:
        """Releases ``operation``'s lock and consumes again (still paused if the document says so). A finish for an
        operation this instance held that another operation took over is ``NOT_PREPARED``, never "consuming again"."""
        operation = operation.strip()
        if not processor or not self._processors.runs(processor):
            return ResetRefused("UNKNOWN_PROCESSOR", f"processor {processor} does not run on this instance")
        if not operation:
            return ResetRefused("INVALID", "a finish needs an operation id")
        self._processors.bump(processor)
        held = self._held.get(processor)
        lease_ms = 0 if held is None or held[0] != operation else held[1]
        self._remember_ended(processor, operation, lease_ms)
        if held is not None and held[0] == operation:
            released = await self._store.control.release(processor, operation, self._clock())
            if self._held.get(processor) == held:
                self._held.pop(processor, None)
            if not released:
                takeover = await self._takeover(processor, operation)
                if takeover is not None:
                    return takeover
            applied = await self._processors.converge(processor, initial=False)
            log.info("Processor %s finished reset operation %s: released", processor, operation)
            return ResetOk("released, consuming again" if applied else "released; consuming once the lock is read back")
        document = await self._store.control.get(processor)
        current = document.operation
        now = self._clock()
        if current is None or not current.holds_at(now):
            if current is not None and current.id == operation:
                # The document kept an expired entry of this operation: release it, as the base does.
                released = await self._store.control.release(processor, operation, now)
                if not released:
                    takeover = await self._takeover(processor, operation)
                    if takeover is not None:
                        return takeover
            # A new operation may have taken the lock between the read and the release: check again before saying
            # that nothing holds it.
            takeover = await self._takeover(processor, operation)
            if takeover is not None:
                return takeover
            await self._processors.converge(processor, initial=False)
            return ResetOk("no operation holds the lock")
        if current.id == operation:
            released = await self._store.control.release(processor, operation, self._clock())
            if not released:
                takeover = await self._takeover(processor, operation)
                if takeover is not None:
                    return takeover
            applied = await self._processors.converge(processor, initial=False)
            return ResetOk("released, consuming again" if applied else "released; consuming once the lock is read back")
        return ResetRefused(
            "NOT_PREPARED",
            f"operation {current.id} holds the lock of processor {processor}, not {operation}",
        )

    async def _takeover(self, processor: str, operation: str) -> ResetRefused | None:
        """The refusal naming the operation that took the lock when this finish's release found another one; ``None``
        when nothing (unexpired) holds it."""
        document = await self._store.control.get(processor)
        current = document.operation
        if current is None or not current.holds_at(self._clock()) or current.id == operation:
            return None
        await self._processors.converge(processor, initial=False)
        return ResetRefused(
            "NOT_PREPARED",
            f"operation {current.id} holds the lock of processor {processor}, not {operation}",
        )


class _Changed:
    """What an execute changed before it stopped, for a refusal that must not say "nothing changed"."""

    def __init__(self) -> None:
        self.handlers = 0
        self.markers = 0
        self.ranges = False

    def told(self, refusal: ResetRefused) -> ResetRefused:
        if self.handlers == 0 and self.markers == 0 and not self.ranges:
            return refusal
        deleted = "no markers deleted" if self.markers == 0 else f"{self.markers} markers deleted"
        written = ", replay ranges written" if self.ranges else ""
        message = (
            f"{refusal.message}; reset handlers ran ({self.handlers}), {deleted}{written}, "
            "positions not moved: retry the reset"
        )
        return ResetRefused(refusal.code, message[:512])
