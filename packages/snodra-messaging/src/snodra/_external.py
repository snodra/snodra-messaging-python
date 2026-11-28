"""The idempotent external-call adapter: one record per ``idempotency-key``, and the two reserved markers.

An idempotent command handler is registered with :meth:`snodra.App.idempotent_command_handler`. Each call is recorded in
the message store's ``external_calls`` under the ``idempotency-key`` header before the body runs:

- the first insert wins; a repeat after a success returns the first reply, a repeat after a permanent failure replays
  the stored error, and a repeat after an unknown outcome reports it again;
- a call still ``IN_PROGRESS`` within its lease asks the caller to come back (the retryable marker); a lapsed one is
  given up as ``UNKNOWN``, or taken over and run again with ``external_idempotent=True`` when the partner de-duplicates
  by the key;
- every settlement is conditional on the owner this call wrote, so a call whose lease ran out never overwrites the
  record of the call that took over, gave up, or reset it;
- the record's writes are their own, outside the caller's transaction: a rolled-back handler never forgets a call that
  happened.

A body that raises :class:`snodra.Retryable` resets the record (the relay owns the backoff); one that raises
:class:`snodra.UnknownOutcome` leaves it ``UNKNOWN``; any other failure is stored ``FAILED`` with ``HANDLER_ERROR`` and
its text, and replayed. A panic (a ``BaseException`` that is not an ``Exception``) is settled ``UNKNOWN`` and answered
with the unknown-outcome marker: the external system may have acted before the body died (SG-48, matching Go's SG-54).
The adapter never retries a call itself.

The records never join the caller's transaction. The port's operations take no transaction and the official add-ons
commit each write on its own.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#external-calls.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Final

from snodra._context import current_message
from snodra._errors import HandlerError, Retryable, UnknownOutcome, is_retryable, is_unknown_outcome
from snodra._store import ExternalCall, ExternalCallStatus, ExternalCallStore, MessageStore, now_ms
from snodra._timings import Timings

if TYPE_CHECKING:
    from snodra._codec import Codec
    from snodra._registry import Handler

log = logging.getLogger(__name__)

IDEMPOTENCY_KEY_HEADER: Final = "idempotency-key"
"""The request header an idempotent command's record is keyed by (SG-47: the key is the header only)."""

MAX_BEGIN_ROUNDS: Final = 3
"""How often a call reads a record that keeps changing under it before it asks the caller to come back."""


class IdempotentAdapter:
    """Runs an idempotent command handler under its external-call record."""

    def __init__(
        self,
        *,
        store: MessageStore | None,
        codec: Codec,
        timings: Timings,
        clock: Callable[[], int] = now_ms,
    ) -> None:
        self._store = store
        self._codec = codec
        self._clock = clock
        self._timings = timings

    async def call(self, handler: Handler, message: object) -> bytes:
        """The handler's reply payload, from the body or from the record; raises its failure (markers included)."""
        calls = self._calls(handler)
        info = current_message()
        if info is None:
            raise HandlerError(f"the idempotent command {handler.label} runs outside a handler")
        key = info.headers.get(IDEMPOTENCY_KEY_HEADER, "")
        if not key.strip():
            # TODO(spec-gap): SG-47
            raise HandlerError(f"the idempotent command {handler.label} needs an {IDEMPOTENCY_KEY_HEADER} header")
        owner, replay = await self._begin(calls, handler, key, handler.partner_idempotent)
        if replay is not None:
            return replay
        try:
            result = await handler.fn(message)
        except asyncio.CancelledError:
            raise  # whether the external system was reached is unknown: the record stays IN_PROGRESS for its lease
        except BaseException as error:
            if isinstance(error, Exception):
                await self._settle_failure(calls, handler, key, owner, error)
                raise
            # TODO(spec-gap): SG-48
            # A panic: the external system may have acted before the body died, so this is not a known FAILED.
            await self._mark_unknown(calls, handler, key, owner)
            raise UnknownOutcome(f"the idempotent command {handler.label} panicked: {error}") from error
        try:
            reply = self._codec.encode(result)
        except Exception as error:
            raise await self._unrecorded(calls, handler, key, owner, error) from error
        try:
            recorded = await calls.succeed(key, owner, reply.decode(), self._now())
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise await self._unrecorded(calls, handler, key, owner, error) from error
        if not recorded:
            raise await self._lost(calls, handler, key, owner)
        return reply

    def _calls(self, handler: Handler) -> ExternalCallStore:
        port = None if self._store is None else self._store.external
        if port is None:
            raise HandlerError(f"the idempotent command {handler.label} needs the message store's external calls port")
        return port

    def _now(self) -> int:
        return int(self._clock())

    async def _begin(
        self, calls: ExternalCallStore, handler: Handler, key: str, partner_idempotent: bool
    ) -> tuple[str, bytes | None]:
        """The owner token this call wrote, or the stored reply; raises the record's answer."""
        try:
            return await self._begin_record(calls, handler, key, partner_idempotent)
        except (Retryable, UnknownOutcome, HandlerError):
            raise
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # A store failure before the body has no marker: it is a plain permanent HANDLER_ERROR, never a retry.
            raise HandlerError(_text(error)) from error

    async def _begin_record(
        self, calls: ExternalCallStore, handler: Handler, key: str, partner_idempotent: bool
    ) -> tuple[str, bytes | None]:
        lease_ms = int(self._timings.external_call_lease * 1000)
        for _ in range(MAX_BEGIN_ROUNDS):
            now = self._now()
            owner = str(uuid.uuid4())
            inserted = await calls.insert(
                ExternalCall(
                    key=key,
                    status=ExternalCallStatus.IN_PROGRESS,
                    attempts=1,
                    updated_at_ms=now,
                    lease_until_ms=now + lease_ms,
                    owner=owner,
                )
            )
            if inserted:
                return owner, None
            record = await calls.find(key)
            if record is None:
                continue  # reset or expired meanwhile: insert again
            if record.status is ExternalCallStatus.SUCCEEDED:
                return "", b"{}" if not record.reply else record.reply.encode()
            if record.status is ExternalCallStatus.FAILED:
                raise HandlerError(_stored_message(record))
            if record.status is ExternalCallStatus.UNKNOWN:
                raise UnknownOutcome(_unknown_message(key, handler))
            if record.status is not ExternalCallStatus.IN_PROGRESS:
                raise HandlerError(f"the external call record {key} has status {record.status}")
            if record.lease_until_ms is not None and record.lease_until_ms > now:
                raise Retryable(f"the call with key {key!r} is already in progress (command {handler.label})")
            if partner_idempotent:
                taken = await calls.take_over(record, owner, now + lease_ms, now)
                if taken is not None:
                    return owner, None
                continue  # another call changed the record: read it again
            if await calls.expire(record, now):
                raise UnknownOutcome(_unknown_message(key, handler))
            # The record changed since it was read: read it again rather than report an unknown outcome over it.
        raise Retryable(f"the call with key {key!r} kept changing under this call (command {handler.label})")

    async def _settle_failure(
        self, calls: ExternalCallStore, handler: Handler, key: str, owner: str, error: Exception
    ) -> None:
        """Stores the body's failure; its own errors are logged and never mask the body's."""
        if is_retryable(error):
            await self._best_effort(calls.reset(key, owner), "reset", key, handler)
        elif is_unknown_outcome(error):
            await self._mark_unknown(calls, handler, key, owner)
        else:
            stored = json.dumps({"code": "HANDLER_ERROR", "message": _describe(error)}, separators=(",", ":"))
            await self._best_effort(calls.fail(key, owner, stored, self._now()), "FAILED", key, handler)

    async def _mark_unknown(self, calls: ExternalCallStore, handler: Handler, key: str, owner: str) -> None:
        await self._best_effort(calls.mark_unknown(key, owner, self._now()), "UNKNOWN", key, handler)

    async def _best_effort(self, write: Awaitable[bool], what: str, key: str, handler: Handler) -> None:
        try:
            written = await write
            if not written:
                log.warning(
                    "The call with key %r (handler %s) no longer owns its record, so it is not %s",
                    key,
                    handler.label,
                    what,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning(
                "Could not record the %s of the call with key %r (handler %s)",
                what,
                key,
                handler.label,
                exc_info=True,
            )

    async def _unrecorded(
        self, calls: ExternalCallStore, handler: Handler, key: str, owner: str, cause: BaseException
    ) -> BaseException:
        """The body returned but its SUCCEEDED write cannot say so: unknown, or retryable for a partner."""
        log.error(
            "The call with key %r (handler %s) ran but its outcome could not be recorded: %s",
            key,
            handler.label,
            cause,
        )
        if handler.partner_idempotent:
            return Retryable(
                f"the call with key {key!r} (handler {handler.label}) ran but its outcome could not be recorded; "
                "a repeat is safe because the partner de-duplicates by the key"
            )
        await self._mark_unknown(calls, handler, key, owner)
        return UnknownOutcome(
            f"the call with key {key!r} (handler {handler.label}) ran but its outcome could not be recorded"
        )

    async def _lost(self, calls: ExternalCallStore, handler: Handler, key: str, owner: str) -> BaseException:
        """The body returned but the record is no longer this call's."""
        log.warning(
            "The call with key %r (handler %s) ran but no longer owns its record: its lease ran out",
            key,
            handler.label,
        )
        if handler.partner_idempotent:
            return Retryable(
                f"the call with key {key!r} (handler {handler.label}) ran after its lease ran out and another call "
                "owns its record now; a repeat is safe because the partner de-duplicates by the key"
            )
        await self._mark_unknown(calls, handler, key, owner)
        return UnknownOutcome(
            f"the call with key {key!r} (handler {handler.label}) ran after its lease ran out and another call owns "
            "its record now"
        )


def _stored_message(record: ExternalCall) -> str:
    """The message of a stored FAILED record, else a fallback naming the key."""
    if record.error:
        try:
            message = json.loads(record.error).get("message")
        except (ValueError, AttributeError):
            message = None
        if isinstance(message, str) and message:
            return message
    return f"the call with key {record.key!r} failed earlier"


def _unknown_message(key: str, handler: Handler) -> str:
    return (
        f"the outcome of the call with key {key!r} is unknown: an earlier call of handler {handler.label} did not "
        "finish and the external system may have acted"
    )


def _describe(error: BaseException) -> str:
    text = str(error)
    return type(error).__name__ if not text else f"{type(error).__name__}: {text}"


def _text(error: BaseException) -> str:
    return str(error).strip() or type(error).__name__


__all__ = ["IDEMPOTENCY_KEY_HEADER", "IdempotentAdapter"]
