"""The hub's processor control: each ``ControlRequest`` acted on off the read loop, one per processor at a time.

The hub asks an instance to pause or resume a processor or to work its dead letters (and, once the reset state machine
lands, to reset or replay it). Every request is answered with exactly one ``ControlReply`` carrying the request's
``control_id`` on the stream the request arrived on; the hub gives up after 10 s and drops a late reply. Handling never
runs on the read loop: :meth:`ControlPlane.handle` only puts the request on its processor's queue and returns, so
heartbeats and request dispatch keep flowing while an action waits on the message store. One worker per processor takes
the actions one at a time, in arrival order, and every action is logged at INFO.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from snodra._deadletters import MAX_LIST, RetryAllOutcome, RetryOutcome
from snodra._hubpb import hub_pb2
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

    def attach(self, owner: Processors | None) -> None:
        """Takes the processors this instance runs and starts one worker per processor."""
        self._owner = owner
        for name in () if owner is None else owner.names:
            queue: asyncio.Queue[tuple[hub_pb2.ControlRequest, Put]] = asyncio.Queue()
            self._queues[name] = queue
            self._tasks.append(asyncio.create_task(self._serve(name, queue), name=f"snodra:control:{name}"))

    def tasks(self) -> list[asyncio.Task[None]]:
        """The workers, for the shutdown to cancel and await."""
        return list(self._tasks)

    def handle(self, request: hub_pb2.ControlRequest, put: Put) -> None:
        """Hands a request to its processor's worker and returns at once (the work never runs on the read loop)."""
        queue = self._queues.get(request.processor)
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
        queue.put_nowait((request, put))

    async def _serve(self, processor: str, queue: asyncio.Queue[tuple[hub_pb2.ControlRequest, Put]]) -> None:
        """Takes the processor's requests one at a time, in arrival order, and answers each exactly once."""
        while True:
            request, put = await queue.get()
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
            if action in ("reset_prepare", "reset_execute", "reset_finish"):
                # TODO(L3-5): the leased reset state machine (prepare/execute/finish, replay ranges). Every reset
                # action must bump its processor's version in Processors, like pause and resume do, so a poll that
                # read an older document cannot undo it.
                return _refused(
                    request.control_id,
                    hub_pb2.CONTROL_REFUSAL_UNSUPPORTED,
                    "the reset control is not implemented in this library yet",
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
