"""The saga dispatch relay: sends the entries a saga recorded, one saga at a time and in order.

A relay in every instance claims the dispatch lease of a saga, reads its first pending entry, sends it, and only then
moves to the next. A command is marked sent after its reply; an event is appended to the ordinary outbox and marked
sent in one transaction. A transient failure delays that saga only; a permanent one records the failure and a pending
notice, and the later entries are still sent. The relay also delivers those notices locally.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#dispatching-what-a-saga-sends.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import random
import uuid
from collections.abc import Awaitable, Callable, Mapping
from typing import Final, Literal

from snodra import _traceparent
from snodra._backoff import jitter
from snodra._codec import Codec
from snodra._events import is_json_object
from snodra._eventstore import EventHeaders
from snodra._saga import SagaCommandFailed
from snodra._saga_failure import classify
from snodra._send import Sender
from snodra._store import (
    DispatchFailure,
    DispatchKind,
    FailureReason,
    MessageStore,
    OutboxEntry,
    SagaDispatchDocument,
    now_ms,
)
from snodra._timings import Timings

log = logging.getLogger(__name__)

NOTICE_LEASE: Final = 60.0
"""How long one relay reserves a failure notice it claimed."""

Outcome = Literal["sent", "transient", "permanent"]


class SagaDispatchRelay:
    """One instance's relay for the saga-side outbox and the failure notices."""

    def __init__(
        self,
        *,
        store: MessageStore,
        sender: Sender,
        codec: Codec,
        notice: Callable[[SagaCommandFailed, Mapping[str, str]], Awaitable[bool]],
        clock: Callable[[], int] = now_ms,
        timings: Timings,
        owner: str | None = None,
    ) -> None:
        self._store = store
        self._sender = sender
        self._codec = codec
        self._notice = notice
        self._clock = clock
        self._timings = timings
        self._owner = f"saga-relay-{uuid.uuid4()}" if owner is None else owner
        self._outbox_seq = itertools.count((self._clock() << 20) + 1)
        self._in_flight: set[str] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._running = False
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def owner(self) -> str:
        return self._owner

    def start(self) -> asyncio.Task[None]:
        """Starts the polling loop and returns its task."""
        self._running = True
        self._loop_task = asyncio.create_task(self._loop(), name="snodra:saga-dispatch")
        return self._loop_task

    def stop(self) -> None:
        self._running = False
        for task in list(self._tasks):
            task.cancel()

    async def run_once(self, *, wait: bool = True) -> None:
        """One polling pass: starts a worker for each due saga, then delivers due notices.

        ``wait`` awaits the workers (the tests and a one-shot drain); the app's polling loop leaves them in flight so a
        slow send holds only its own saga and the next pass can start other sagas.
        """
        now = self._clock()
        due = await self._store.sagas.dispatch.due_sagas(now, tuple(self._in_flight), self._timings.saga_dispatch_batch)
        for saga_id in due:
            if saga_id in self._in_flight:
                continue
            self._in_flight.add(saga_id)
            task = asyncio.create_task(self._run_saga(saga_id), name=f"snodra:saga-dispatch:{saga_id}")
            self._tasks.add(task)
            task.add_done_callback(lambda finished: self._finished(finished))
        if wait and self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        await self._drain_notices()

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        with contextlib.suppress(Exception):
            saga_id = task.get_name().rsplit(":", 1)[-1]
            self._in_flight.discard(saga_id)

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.run_once(wait=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("the saga dispatch pass failed; the next pass retries")
            await asyncio.sleep(self._timings.saga_dispatch_poll)

    async def _run_saga(self, saga_id: str) -> None:
        now = self._clock()
        lease = self._timings.saga_dispatch_lease
        if not await self._store.sagas.dispatch.claim_lease(saga_id, self._owner, now, now + int(lease * 1000)):
            return
        try:
            while True:
                now = self._clock()
                entry = await self._store.sagas.dispatch.next_pending(saga_id)
                if entry is None:
                    return
                if entry.next_attempt_at_ms is not None and entry.next_attempt_at_ms > now:
                    return
                outcome = await self._dispatch_entry(entry, now)
                if outcome == "transient":
                    return
                # A sent or permanent entry moves the saga on; the lease is renewed before the next one.
                if not await self._store.sagas.dispatch.renew_lease(
                    saga_id, self._owner, self._clock() + int(lease * 1000)
                ):
                    return
        finally:
            with contextlib.suppress(Exception):
                await self._store.sagas.dispatch.release_lease(saga_id, self._owner)

    async def _dispatch_entry(self, entry: SagaDispatchDocument, now: int) -> Outcome:
        if entry.kind is DispatchKind.EVENT:
            return await self._dispatch_event(entry, now)
        return await self._dispatch_command(entry, now)

    async def _dispatch_event(self, entry: SagaDispatchDocument, now: int) -> Outcome:
        if not is_json_object(entry.payload.encode()):
            await self._store.sagas.dispatch.mark_failed(
                entry.id,
                DispatchFailure(
                    attempts=None,
                    reason=FailureReason.INVALID,
                    message=f"saga event {entry.id} ({entry.type_name}) is not a JSON object",
                    failed_at_ms=now,
                    notice_at_ms=None,
                ),
            )
            log.error("Saga event %s of type %s is not a JSON object: given up as INVALID", entry.id, entry.type_name)
            return "permanent"
        headers = _event_headers(entry)
        outbox = OutboxEntry(
            id=entry.id,
            type_name=entry.type_name,
            key=entry.routing_key,
            headers=headers,
            payload=entry.payload.encode(),
            created_at_ms=entry.created_at_ms,
            seq=next(self._outbox_seq),
        )
        try:
            async with self._store.transaction():
                await self._store.outbox.append(outbox)
                await self._store.sagas.dispatch.mark_dispatched(entry.id, now)
        except Exception as error:
            # A payload the outbox refuses never becomes an entry: the same INVALID, no notice.
            from snodra._store import InvalidMessage

            if isinstance(error, InvalidMessage):
                await self._store.sagas.dispatch.mark_failed(
                    entry.id,
                    DispatchFailure(
                        attempts=None,
                        reason=FailureReason.INVALID,
                        message=str(error),
                        failed_at_ms=now,
                        notice_at_ms=None,
                    ),
                )
                log.error("Saga event %s was refused by the outbox: given up as INVALID: %s", entry.id, error)
                return "permanent"
            classification = classify(error)
            if classification.transient:
                return await self._transient(entry, now, classification.message)
            await self._permanent_event(entry, now, classification.reason, classification.message)
            return "permanent"
        return "sent"

    async def _dispatch_command(self, entry: SagaDispatchDocument, now: int) -> Outcome:
        sender_local = getattr(self._sender, "local", None)
        local = None if sender_local is None else sender_local.handler("command", entry.type_name)
        if local is None and not getattr(self._sender, "has_configured_hubs", True):
            await self._permanent(
                entry,
                now,
                "NO_HANDLER",
                f"No handler registered for '{entry.type_name}'",
                attempts=entry.attempts + 1,
            )
            return "permanent"
        headers = _command_headers(entry)
        try:
            await self._sender.call_payload(
                entry.type_name,
                entry.payload.encode(),
                "command",
                routing_key=entry.routing_key,
                headers=headers,
                retry=False,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            classification = classify(error)
            if classification.transient:
                return await self._transient(entry, now, classification.message)
            await self._permanent(entry, now, classification.reason, classification.message)
            return "permanent"
        await self._store.sagas.dispatch.mark_dispatched(entry.id, now)
        return "sent"

    async def _transient(self, entry: SagaDispatchDocument, now: int, message: str) -> Outcome:
        attempts = entry.attempts + 1
        max_attempts = self._timings.saga_dispatch_max_attempts
        if attempts >= max_attempts:
            await self._permanent(entry, now, FailureReason.EXHAUSTED.value, message, attempts=attempts)
            return "permanent"
        wait = _wait(self._timings, attempts + 1)
        await self._store.sagas.dispatch.schedule_retry(entry.id, attempts, now + int(wait * 1000))
        log.warning(
            "Saga dispatch %s failed transiently on attempt %d; retrying in %.2f s: %s",
            entry.id,
            attempts,
            wait,
            message,
        )
        return "transient"

    async def _permanent_event(self, entry: SagaDispatchDocument, now: int, reason: str, message: str) -> None:
        await self._store.sagas.dispatch.mark_failed(
            entry.id,
            DispatchFailure(
                attempts=entry.attempts + 1,
                reason=FailureReason(reason),
                message=message,
                failed_at_ms=now,
                notice_at_ms=None,
            ),
        )
        log.error("Saga event %s given up as %s: %s", entry.id, reason, message)

    async def _permanent(
        self,
        entry: SagaDispatchDocument,
        now: int,
        reason: str,
        message: str,
        *,
        attempts: int | None = None,
    ) -> None:
        await self._store.sagas.dispatch.mark_failed(
            entry.id,
            DispatchFailure(
                attempts=attempts if attempts is not None else entry.attempts + 1,
                reason=FailureReason(reason),
                message=message,
                failed_at_ms=now,
                notice_at_ms=now,
            ),
        )
        log.error("Saga command %s of type %s given up as %s: %s", entry.id, entry.type_name, reason, message)

    async def _drain_notices(self) -> None:
        now = self._clock()
        while True:
            claimed = await self._store.sagas.dispatch.claim_due_notice(now, now + int(NOTICE_LEASE * 1000))
            if claimed is None:
                return
            notice = SagaCommandFailed(
                saga_id=claimed.saga_id,
                command_type=claimed.type_name,
                idempotency_key=claimed.id,
                reason=(claimed.failure_reason.value if claimed.failure_reason is not None else "EXHAUSTED"),
                message=claimed.failure_message or "",
            )
            headers = {"correlation-id": claimed.headers.get("correlation-id", claimed.id)}
            try:
                taken = await self._notice(notice, headers)
            except asyncio.CancelledError:
                raise
            except Exception:
                taken = False
                log.exception("the failure notice %s could not be delivered", notice.notice_id)
            if taken:
                await self._store.sagas.dispatch.clear_notice(claimed.id)
                continue
            attempts = (claimed.notice_attempts or 0) + 1
            if attempts >= self._timings.saga_notice_max_attempts:
                await self._store.sagas.dispatch.clear_notice(claimed.id)
                log.error("Saga failure notice %s was given up after %d attempts", notice.notice_id, attempts)
                continue
            await self._store.sagas.dispatch.schedule_notice_retry(
                claimed.id, attempts, now + int(_wait(self._timings, attempts + 1) * 1000)
            )


def _wait(timings: Timings, attempt: int) -> float:
    base = min(
        timings.saga_dispatch_initial_backoff * 2 ** max(0, attempt - 2),
        timings.saga_dispatch_max_backoff,
    )
    return jitter(base, timings.saga_dispatch_max_backoff, random.random())


def _event_headers(entry: SagaDispatchDocument) -> dict[str, str]:
    headers = {EventHeaders.CORRELATION_ID: entry.headers.get("correlation-id") or entry.id}
    traceparent = entry.headers.get("traceparent")
    if traceparent is not None and _traceparent.valid(traceparent):
        headers[EventHeaders.TRACEPARENT] = traceparent
    return headers


def _command_headers(entry: SagaDispatchDocument) -> dict[str, str]:
    headers = {
        "idempotency-key": entry.id,
        "saga-id": entry.saga_id,
        "correlationId": entry.headers.get("correlation-id") or entry.id,
    }
    traceparent = entry.headers.get("traceparent")
    if traceparent is not None and _traceparent.valid(traceparent):
        headers["traceparent"] = traceparent
    return headers


__all__ = ["SagaDispatchRelay"]
