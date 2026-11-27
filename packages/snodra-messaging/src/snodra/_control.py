"""The hub's processor control: each ``ControlRequest`` acted on off the read loop, one per processor at a time.

The hub asks an instance to pause or resume a processor, to work its dead letters, or to run a reset (prepare, execute
or finish, the leased state machine in :mod:`snodra._reset`). Every request is answered with exactly one
``ControlReply`` carrying the request's ``control_id`` on the stream the request arrived on; the hub gives up after
10 s (120 s for a reset execute) and drops a late reply. Handling never runs on the read loop:
:meth:`ControlPlane.handle` only puts the request on its processor's queue and returns, so heartbeats and request
dispatch keep flowing while an action waits on the message store. One worker per processor takes the actions one at a
time, in arrival order, and every action is logged at INFO; a renewal of the operation an execute holds and a
dead-letter list are answered beside the execute, and a dead-letter change during one is refused LOCKED at once.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from snodra._deadletters import MAX_LIST, RetryAllOutcome, RetryOutcome
from snodra._eventstore import Earliest, Latest, Positions, ResetTarget, Timestamp
from snodra._hubpb import hub_pb2
from snodra._reset import MIN_LEASE_MS, ResetDone, ResetOk, ResetRefused
from snodra._status import MAX_TEXT_BYTES, utf8_cut
from snodra._store import DeadLetter

if TYPE_CHECKING:
    from snodra._processors import Processors

log = logging.getLogger(__name__)

Put = Callable[[hub_pb2.ClientMessage], bool]
"""Writes a frame on the stream a control request arrived on; false when the stream is gone."""


def _ok(control_id: str, detail: str) -> hub_pb2.ControlReply:
    return hub_pb2.ControlReply(
        control_id=control_id,
        ok=hub_pb2.ControlOk(detail=utf8_cut(detail, MAX_TEXT_BYTES)),
    )


def _refused(control_id: str, code: hub_pb2.ControlRefusal, message: str) -> hub_pb2.ControlReply:
    return hub_pb2.ControlReply(
        control_id=control_id,
        refused=hub_pb2.ControlRefused(code=code, message=utf8_cut(message, MAX_TEXT_BYTES)),
    )


def _summary(letter: DeadLetter) -> hub_pb2.DeadLetterSummary:
    return hub_pb2.DeadLetterSummary(
        id=letter.id,
        processor=letter.processor,
        source=letter.source,
        segment=letter.segment,
        position=letter.position,
        key="" if letter.key is None else letter.key,
        event_id="" if letter.event_id is None else letter.event_id,
        event_type="" if letter.event_type is None else letter.event_type,
        reason=letter.reason.value,
        error_type=letter.error.type_name,
        error_message=utf8_cut(letter.error.message, MAX_TEXT_BYTES),
        blocked_by="" if letter.blocked_by is None else letter.blocked_by,
        attempts=letter.attempts,
        parked_at_ms=letter.parked_at_ms,
        last_attempt_at_ms=letter.last_attempt_at_ms,
    )


class ControlPlane:
    """The control requests of this instance's processors."""

    def __init__(self, owner: Processors | None) -> None:
        self._owner = owner
        self._queues: dict[str, asyncio.Queue[tuple[hub_pb2.ControlRequest, Put]]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._side_tasks: set[asyncio.Task[None]] = set()

    def attach(self, owner: Processors | None) -> None:
        """Takes the processors this instance runs and starts one worker per processor."""
        self._owner = owner
        for name in () if owner is None else owner.names:
            queue: asyncio.Queue[tuple[hub_pb2.ControlRequest, Put]] = asyncio.Queue()
            self._queues[name] = queue
            self._tasks.append(asyncio.create_task(self._serve(name, queue), name=f"snodra:control:{name}"))

    def tasks(self) -> list[asyncio.Task[None]]:
        """The workers and the requests answered beside them, for the shutdown to cancel and await."""
        return [*self._tasks, *self._side_tasks]

    def handle(self, request: hub_pb2.ControlRequest, put: Put) -> None:
        """Hands a request to its processor's worker and returns at once (the work never runs on the read loop)."""
        processor = request.processor
        if request.WhichOneof("action") == "reset_prepare" and request.reset_prepare.lease_ms < MIN_LEASE_MS:
            # The lease is checked before the processor, as the base does: a too-short lease is INVALID even for a
            # processor that does not run here. A blank operation id is checked by the machine (UNKNOWN_PROCESSOR for
            # an unknown processor first, then INVALID).
            reply = _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_INVALID,
                f"a prepare needs an operation id and a lease of at least {MIN_LEASE_MS} ms",
            )
            log.info(
                "Control %s %s on processor %s: refused INVALID (malformed prepare)",
                request.control_id,
                _action_of(request),
                processor,
            )
            put(hub_pb2.ClientMessage(control_reply=reply))
            return
        queue = self._queues.get(processor)
        if queue is None:
            reply = _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_UNKNOWN_PROCESSOR,
                f"processor {request.processor} does not run on this instance",
            )
            log.info(
                "Control %s %s on processor %s: refused UNKNOWN_PROCESSOR",
                request.control_id,
                _action_of(request),
                request.processor,
            )
            put(hub_pb2.ClientMessage(control_reply=reply))
            return
        action = request.WhichOneof("action")
        executing = self._owner.reset.executing(processor) if self._owner is not None else None
        renewal = (
            action == "reset_prepare"
            and self._owner is not None
            and bool(request.reset_prepare.operation_id)
            and self._owner.reset.held(processor) == request.reset_prepare.operation_id
        )
        if renewal or (executing is not None and action == "dead_letter_list"):
            # A renewal of the operation held here and a dead-letter list are answered beside an execute, never behind
            # it: the execute may run for a minute (the hub renews every 10 s and waits at most 10 s for an answer).
            task = asyncio.create_task(self._answer(processor, request, put), name=f"snodra:control:{processor}:beside")
            self._side_tasks.add(task)
            task.add_done_callback(self._side_tasks.discard)
            return
        if executing is not None and action in (
            "dead_letter_retry",
            "dead_letter_retry_all",
            "dead_letter_discard",
        ):
            # A change beside the execute would race its reset handlers and marker deletion: refused at once.
            reply = _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_LOCKED,
                f"processor {processor} is executing reset operation {executing}; try again when it finished",
            )
            log.info(
                "Control %s %s on processor %s: refused LOCKED (executing %s)",
                request.control_id,
                _action_of(request),
                processor,
                executing,
            )
            put(hub_pb2.ClientMessage(control_reply=reply))
            return
        queue.put_nowait((request, put))

    async def _serve(self, processor: str, queue: asyncio.Queue[tuple[hub_pb2.ControlRequest, Put]]) -> None:
        """Takes the processor's requests one at a time, in arrival order, and answers each exactly once."""
        while True:
            request, put = await queue.get()
            await self._answer(processor, request, put)

    async def _answer(self, processor: str, request: hub_pb2.ControlRequest, put: Put) -> None:
        """One action's answer, logged; a failure is a FAILED refusal, and exactly one reply goes out."""
        try:
            reply = await self._act(processor, request)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning("Control %s on processor %s failed: %s", request.control_id, processor, error)
            reply = _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_FAILED,
                f"{type(error).__name__}: {error}",
            )
        log.info(
            "Control %s %s on processor %s: %s",
            request.control_id,
            _action_of(request),
            processor,
            _describe(reply),
        )
        put(hub_pb2.ClientMessage(control_reply=reply))

    async def _act(self, processor: str, request: hub_pb2.ControlRequest) -> hub_pb2.ControlReply:
        """One action; every failure becomes its refusal."""
        owner = self._owner
        assert owner is not None  # a queue exists only for a processor of an attached owner
        action = request.WhichOneof("action")
        try:
            if action == "pause":
                await owner.set_paused(processor, True, request.control_id)
                return _ok(request.control_id, "paused")
            if action == "resume":
                await owner.set_paused(processor, False, request.control_id)
                return _ok(request.control_id, "resumed")
            if action == "dead_letter_list":
                return await self._list(processor, request)
            if action == "dead_letter_retry":
                return await self._retry(processor, request)
            if action == "dead_letter_retry_all":
                return await self._retry_all(processor, request)
            if action == "dead_letter_discard":
                return await self._discard(processor, request)
            if action == "reset_prepare":
                prepare = request.reset_prepare
                return _reset_reply(
                    request.control_id,
                    await owner.reset.prepare(processor, prepare.operation_id, prepare.lease_ms),
                )
            if action == "reset_execute":
                execute = request.reset_execute
                target = _target_of(execute.target)
                if target is None:
                    return _refused(request.control_id, hub_pb2.CONTROL_REFUSAL_INVALID, "the reset has no target")
                return _reset_reply(
                    request.control_id,
                    await owner.reset.execute(processor, execute.operation_id, target),
                )
            if action == "reset_finish":
                finish = request.reset_finish
                return _reset_reply(
                    request.control_id,
                    await owner.reset.finish(processor, finish.operation_id),
                )
            return _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_INVALID,
                "the control request has no action this instance knows",
            )
        except ValueError as error:
            return _refused(request.control_id, hub_pb2.CONTROL_REFUSAL_INVALID, str(error))
        except KeyError as error:
            return _refused(request.control_id, hub_pb2.CONTROL_REFUSAL_UNKNOWN_PROCESSOR, str(error))

    async def _list(self, processor: str, request: hub_pb2.ControlRequest) -> hub_pb2.ControlReply:
        limit = request.dead_letter_list.limit
        if not 1 <= limit <= MAX_LIST:
            return _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_INVALID,
                f"limit must be 1 to {MAX_LIST}, was {limit}",
            )
        owner = self._owner
        assert owner is not None
        letters = await owner.dead_letters.list(processor, limit)
        total = await owner.dead_letters.count(processor)
        page = hub_pb2.DeadLetterPage(total=total)
        for letter in letters:
            page.letters.append(_summary(letter))
        return hub_pb2.ControlReply(control_id=request.control_id, dead_letters=page)

    async def _retry(self, processor: str, request: hub_pb2.ControlRequest) -> hub_pb2.ControlReply:
        letter_id = request.dead_letter_retry.letter_id
        if not letter_id.strip():
            return _refused(request.control_id, hub_pb2.CONTROL_REFUSAL_INVALID, "no letter id")
        owner = self._owner
        assert owner is not None
        try:
            found = await owner.dead_letters.get(letter_id)
        except KeyError:
            found = None  # a letter of a processor this instance does not run
        if found is None or found.processor != processor:
            outcome = RetryOutcome("NotFound")
        else:
            outcome = await owner.dead_letters.retry(letter_id)
        return hub_pb2.ControlReply(
            control_id=request.control_id,
            retry=hub_pb2.DeadLetterRetryOutcome(
                result=outcome.result,
                detail=utf8_cut(outcome.detail, MAX_TEXT_BYTES),
            ),
        )

    async def _retry_all(self, processor: str, request: hub_pb2.ControlRequest) -> hub_pb2.ControlReply:
        owner = self._owner
        assert owner is not None
        outcome: RetryAllOutcome = await owner.dead_letters.retry_all(processor)
        return hub_pb2.ControlReply(
            control_id=request.control_id,
            retry_all=hub_pb2.DeadLetterRetryAllOutcome(
                retried=outcome.retried,
                failed=outcome.failed,
                blocked=outcome.blocked,
                skipped=outcome.skipped,
            ),
        )

    async def _discard(self, processor: str, request: hub_pb2.ControlRequest) -> hub_pb2.ControlReply:
        letter_id = request.dead_letter_discard.letter_id
        if not letter_id.strip():
            return _refused(request.control_id, hub_pb2.CONTROL_REFUSAL_INVALID, "no letter id")
        owner = self._owner
        assert owner is not None
        try:
            found = await owner.dead_letters.get(letter_id)
        except KeyError:
            found = None  # a letter of a processor this instance does not run
        if found is None or found.processor != processor:
            return _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_NOT_FOUND,
                f"no dead letter {letter_id} in processor {processor}",
            )
        if not await owner.dead_letters.discard(letter_id):
            return _refused(
                request.control_id,
                hub_pb2.CONTROL_REFUSAL_NOT_FOUND,
                f"no dead letter {letter_id} in processor {processor}",
            )
        return _ok(request.control_id, "discarded")


_REFUSAL_CODES: dict[str, hub_pb2.ControlRefusal] = {
    "UNKNOWN_PROCESSOR": hub_pb2.CONTROL_REFUSAL_UNKNOWN_PROCESSOR,
    "LOCKED": hub_pb2.CONTROL_REFUSAL_LOCKED,
    "NOT_PREPARED": hub_pb2.CONTROL_REFUSAL_NOT_PREPARED,
    "SUBSCRIBERS_ACTIVE": hub_pb2.CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE,
    "RESET_HANDLER_FAILED": hub_pb2.CONTROL_REFUSAL_RESET_HANDLER_FAILED,
    "INVALID": hub_pb2.CONTROL_REFUSAL_INVALID,
    "FAILED": hub_pb2.CONTROL_REFUSAL_FAILED,
    "NOT_FOUND": hub_pb2.CONTROL_REFUSAL_NOT_FOUND,
    "UNSUPPORTED": hub_pb2.CONTROL_REFUSAL_UNSUPPORTED,
}
"""The reset machine's refusal suffixes, as ``hub.proto`` numbers them."""


def _reset_reply(control_id: str, reply: ResetOk | ResetRefused | ResetDone) -> hub_pb2.ControlReply:
    """One reset action's answer, as ``ControlReply`` carries it."""
    if isinstance(reply, ResetOk):
        return _ok(control_id, reply.detail)
    if isinstance(reply, ResetRefused):
        return _refused(control_id, _REFUSAL_CODES[reply.code], reply.message)
    outcome = hub_pb2.ResetOutcome(
        markers_deleted=reply.markers_deleted,
        reset_handlers=reply.reset_handlers,
    )
    for replay_range in reply.ranges:
        wire = hub_pb2.ReplayRange(segment=replay_range.segment, until=replay_range.until)
        setattr(wire, "from", replay_range.start)
        outcome.ranges.append(wire)
    return hub_pb2.ControlReply(control_id=control_id, reset=outcome)


def _target_of(target: hub_pb2.ResetTarget) -> ResetTarget | None:
    """The reset target a ``ResetTarget`` names, or ``None`` when it names none."""
    match target.WhichOneof("to"):
        case "earliest":
            return Earliest()
        case "latest":
            return Latest()
        case "timestamp_ms":
            return Timestamp(target.timestamp_ms)
        case "positions":
            positions = tuple((item.segment, item.position) for item in target.positions.positions)
            return Positions(positions)
        case _:
            return None


def _action_of(request: hub_pb2.ControlRequest) -> str:
    return request.WhichOneof("action") or "none"


def _describe(reply: hub_pb2.ControlReply) -> str:
    result = reply.WhichOneof("result")
    if result == "ok":
        return f"ok ({reply.ok.detail})"
    if result == "refused":
        code = hub_pb2.ControlRefusal.Name(reply.refused.code).removeprefix("CONTROL_REFUSAL_")
        return f"refused {code}: {reply.refused.message}"
    if result == "reset":
        return (
            f"reset ({len(reply.reset.ranges)} ranges, {reply.reset.markers_deleted} markers deleted, "
            f"{reply.reset.reset_handlers} reset handlers)"
        )
    if result == "dead_letters":
        return f"{len(reply.dead_letters.letters)} of {reply.dead_letters.total} letters"
    if result == "retry":
        return reply.retry.result
    if result == "retry_all":
        return (
            f"{reply.retry_all.retried} retried, {reply.retry_all.failed} failed, "
            f"{reply.retry_all.blocked} blocked, {reply.retry_all.skipped} skipped"
        )
    return result or "none"
