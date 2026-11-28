"""How a saga command failure is classified, and how a failure notice is dispatched locally.

The relay owns the retry table: a transient failure is retried with the command's backoff, a permanent one records the
failure and its pending notice at once. The notice is the local event :class:`SagaCommandFailed`; it goes to the
service's ordinary handlers and to the saga that sent the command, never to the event store.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#retries-and-failures.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from snodra._codec import Codec
from snodra._errors import (
    DecodeError,
    ErrorCode,
    HandlerError,
    HubError,
    InvalidRequest,
    NeverSent,
    OutcomeUnknown,
    SnodraError,
)
from snodra._events import EventHeaders, EventRegistry, event_handling, event_message, processor_marker
from snodra._saga import COMMAND_FAILED_TYPE, SagaCommandFailed
from snodra._saga_runtime import SagaRuntime
from snodra._store import MessageStore, ProcessedMarker

log = logging.getLogger(__name__)

RETRYABLE: bytes = b'{"snodra":"retryable"}'
UNKNOWN_OUTCOME: bytes = b'{"snodra":"unknown-outcome"}'


@dataclass(frozen=True, slots=True)
class Classification:
    """What one failed command attempt means to the relay."""

    transient: bool
    reason: str
    message: str


def classify(error: BaseException) -> Classification:
    """The outcome table of the reference libraries for one failed command attempt."""
    message = _text(error)
    if isinstance(error, NeverSent | OutcomeUnknown):
        return Classification(True, "EXHAUSTED", message)
    if isinstance(error, HubError):
        if error.code in (ErrorCode.TIMEOUT, ErrorCode.HANDLER_DISCONNECTED, ErrorCode.RESOURCE_EXHAUSTED):
            return Classification(True, "EXHAUSTED", message)
        if error.code is ErrorCode.NO_HANDLER:
            # The local sender's NO_HANDLER is permanent; a hub's is a redeploy in progress. The relay marks a local
            # one itself before it reaches here; a HubError comes from a hub or a local handler and is retryable.
            return Classification(True, "EXHAUSTED", message)
        if error.code in (ErrorCode.UNAUTHENTICATED, ErrorCode.PERMISSION_DENIED):
            return Classification(False, "DENIED", message)
        if error.code is ErrorCode.INVALID_REQUEST:
            return Classification(False, "INVALID", message)
        if error.code is ErrorCode.HANDLER_ERROR:
            details = error.details.strip()
            if details == RETRYABLE:
                return Classification(True, "EXHAUSTED", message)
            if details == UNKNOWN_OUTCOME:
                return Classification(False, "UNKNOWN_OUTCOME", message)
            return Classification(False, "FAILED", message)
        if error.code is ErrorCode.CANCELLED:
            return Classification(False, "FAILED", message)
        return Classification(False, "FAILED", message)
    if isinstance(error, HandlerError):
        details = _details_bytes(error.details)
        if details == RETRYABLE:
            return Classification(True, "EXHAUSTED", message)
        if details == UNKNOWN_OUTCOME:
            return Classification(False, "UNKNOWN_OUTCOME", message)
        return Classification(False, "FAILED", message)
    if isinstance(error, InvalidRequest | DecodeError | ValueError):
        return Classification(False, "INVALID", message)
    if isinstance(error, SnodraError):
        # The sender's own storage or transport failed: a transient failure of the sender.
        return Classification(True, "EXHAUSTED", message)
    return Classification(True, "EXHAUSTED", message)


def _text(error: BaseException) -> str:
    text = str(error).strip() or type(error).__name__
    return text[:2000]


def _details_bytes(details: object) -> bytes:
    if details is None:
        return b""
    if isinstance(details, bytes):
        return details.strip()
    try:
        return json.dumps(details, separators=(",", ":")).encode()
    except Exception:
        return b""


class NoticeDispatcher:
    """Runs a failure notice through the ordinary handlers and the saga runtime, once per notice id."""

    def __init__(
        self,
        *,
        store: MessageStore,
        registry: EventRegistry,
        codec: Codec,
        sagas: SagaRuntime,
        clock: Callable[[], int],
    ) -> None:
        self._store = store
        self._registry = registry
        self._codec = codec
        self._sagas = sagas
        self._clock = clock

    async def dispatch(self, notice: SagaCommandFailed, command_headers: Mapping[str, str]) -> bool:
        """Whether at least one ordinary handler or saga took the notice; duplicate delivery is harmless."""
        headers = dict(command_headers)
        headers[EventHeaders.ID] = notice.notice_id
        headers[EventHeaders.TYPE] = COMMAND_FAILED_TYPE
        headers[EventHeaders.CORRELATION_ID] = headers.get("correlation-id") or notice.notice_id
        handled = await self._ordinary(notice, headers)
        try:
            if await self._sagas.process_notice(notice, headers):
                handled = True
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("the saga failure notice %s could not reach the saga %s", notice.notice_id, notice.saga_id)
            raise
        return handled

    async def _ordinary(self, notice: SagaCommandFailed, headers: Mapping[str, str]) -> bool:
        handlers = self._registry.handlers("default", COMMAND_FAILED_TYPE)
        if not handlers:
            return False
        marker = processor_marker("default", notice.notice_id)
        info = event_message(notice.notice_id, COMMAND_FAILED_TYPE, headers)
        try:
            async with self._store.transaction():
                if not await self._store.processed.record(
                    ProcessedMarker(
                        id=marker,
                        type_name=COMMAND_FAILED_TYPE,
                        processed_at_ms=int(self._clock()),
                        processor="default",
                        source="",
                        segment="",
                        position="",
                    )
                ):
                    return True
                with event_handling(info, ""):
                    for handler in handlers:
                        await handler.fn(notice)
        except Exception:
            log.exception("an ordinary handler failed on the saga failure notice %s", notice.notice_id)
            raise
        return True


__all__ = ["Classification", "NoticeDispatcher", "classify"]
