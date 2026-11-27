"""The dead letters of the processors (mode ``collection``): list, retry, retry all and discard.

A record that exhausts its attempts is parked in the message store (``DeadLetterStore``) at
``<processor>:<source>:<segment>:<position>`` with the reason ``HANDLER_FAILED``; a record whose key has a parked letter
waits behind it as ``SEQUENCED`` (so the key's order holds), and a record that cannot be handled at all (no type header,
a payload that is not a JSON object) is parked ``INVALID``, which blocks nothing and is never retried. This module is
the admin side the control plane calls; the consumer parks (``snodra._consumer``).

A retry runs the processor's stage 1 once, under its stage-1 marker, and deletes the letter **in the same transaction**:
the handler's effects, the marker and the deletion commit together or not at all. A duplicate marker deletes the letter
and reports ``Duplicate``; a failure keeps the letter, counts the attempt and records the new error. Stage 2 (the sagas)
is not run again: it ran when the record was consumed.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", Dead letters) and
https://snodra.damoreira.com/docs/server/processor-control/.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass

from snodra._codec import Codec
from snodra._events import EventRegistry, event_handling, event_message, processor_marker
from snodra._eventstore import EventHeaders, ProcessedMarker
from snodra._status import clean_text
from snodra._store import (
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    DeadLetterSummary,
    MessageStore,
)

log = logging.getLogger(__name__)

COLLECTION: str = "collection"
EVENT_STORE: str = "event-store"
LOG: str = "log"

MAX_LIST: int = 100
"""The most letters a ``DeadLetterList`` may ask for (the hub clamps to this too)."""


@dataclass(frozen=True, slots=True)
class RetryOutcome:
    """What :meth:`DeadLetters.retry` did: ``Retried``, ``Duplicate``, ``Failed``, ``Blocked``, ``NotRetriable`` or
    ``NotFound``, with the failure's message or the blocking letter's id in ``detail``."""

    result: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class RetryAllOutcome:
    """What :meth:`DeadLetters.retry_all` did: letters retried (duplicates included), failed, blocked behind a failure,
    skipped (``INVALID`` or gone)."""

    retried: int
    failed: int
    blocked: int
    skipped: int


def _order(summary: DeadLetterSummary) -> tuple[int, str]:
    """The retry-all order of a letter's group: the earlier parked, then the id."""
    return (summary.parked_at_ms, summary.id)


class _Duplicate(Exception):
    """Raised inside the retry's transaction so a duplicate marker aborts it (the port's contract: the caller rolls
    back when ``record`` answers false)."""


class _Gone(Exception):
    """The letter was deleted (retried or discarded elsewhere) before its retry's transaction could delete it: nothing
    of the handling commits."""


class DeadLetters:
    """The dead letters of the processors this instance runs: the admin side of mode ``collection``."""

    def __init__(
        self,
        mode: str,
        store: MessageStore | None,
        registry: EventRegistry,
        codec: Codec,
        processors: set[str],
        clock: Callable[[], int],
    ) -> None:
        self._mode = mode
        self._store = store
        self._registry = registry
        self._codec = codec
        self._processors = processors
        self._clock = clock

    @property
    def mode(self) -> str:
        """Where an exhausted record goes: ``collection``, ``event-store`` or ``log``."""
        return self._mode

    def _available(self) -> MessageStore:
        if self._mode != COLLECTION or self._store is None:
            raise ValueError(
                f"dead letters are not kept in mode {self._mode} "
                "(dead_letter_mode, SNODRA_EVENTS_DEAD_LETTER_MODE): there is nothing to list, retry or discard"
            )
        return self._store

    def _require_runs(self, processor: str) -> None:
        if processor not in self._processors:
            raise KeyError(f"processor {processor} does not run on this instance")

    async def list(self, processor: str, limit: int = MAX_LIST) -> list[DeadLetter]:
        """The letters of ``processor``, oldest ``parked_at_ms`` first (then by id), at most ``limit`` (1 to
        :data:`MAX_LIST`)."""
        store = self._available()
        self._require_runs(processor)
        if not 1 <= limit <= MAX_LIST:
            raise ValueError(f"limit must be 1 to {MAX_LIST}, was {limit}")
        return await store.dead_letters.list(processor, limit)

    async def get(self, letter: str) -> DeadLetter | None:
        """The letter, or ``None``; a letter of a processor that does not run here is refused."""
        found = await self._available().dead_letters.get(letter)
        if found is None:
            return None
        self._require_runs(found.processor)
        return found

    async def count(self, processor: str) -> int:
        """How many letters ``processor`` has, over all instances."""
        store = self._available()
        self._require_runs(processor)
        return await store.dead_letters.count(processor)

    async def discard(self, letter: str) -> bool:
        """Deletes the letter; false when there was none."""
        store = self._available()
        found = await store.dead_letters.get(letter)
        if found is None:
            return False
        self._require_runs(found.processor)
        deleted = await store.dead_letters.delete(letter)
        if deleted:
            log.info("Discarded dead letter %s (%s of type %s)", letter, found.reason, found.event_type)
        return deleted

    async def retry(self, letter: str) -> RetryOutcome:
        """Retries one letter once; see the module docstring."""
        store = self._available()
        found = await store.dead_letters.get(letter)
        if found is None:
            return RetryOutcome("NotFound")
        self._require_runs(found.processor)
        return await self._retry(store, found)

    async def retry_all(self, processor: str) -> RetryAllOutcome:
        """Retries every letter of ``processor``: per key in position order, a key stopping at its first failure (its
        later letters count as blocked); then the keyless letters. ``INVALID`` letters and letters gone meanwhile are
        skipped."""
        store = self._available()
        self._require_runs(processor)
        summaries = await store.dead_letters.summaries(processor)
        retried = failed = blocked = 0
        skipped = sum(1 for summary in summaries if summary.reason is DeadLetterReason.INVALID)
        retriable = [summary for summary in summaries if summary.reason is not DeadLetterReason.INVALID]
        groups: list[list[DeadLetterSummary]] = []
        by_key: dict[str, list[DeadLetterSummary]] = {}
        for summary in retriable:
            if summary.key is not None:
                by_key.setdefault(summary.key, []).append(summary)
        groups.extend(sorted(by_key.values(), key=lambda group: _order(group[0])))
        groups.extend([summary] for summary in sorted((s for s in retriable if s.key is None), key=_order))
        for group in groups:
            stopped = False
            for summary in group:
                if stopped:
                    blocked += 1
                    continue
                found = await store.dead_letters.get(summary.id)
                if found is None:
                    skipped += 1
                    continue
                outcome = await self._retry(store, found)
                if outcome.result in ("Retried", "Duplicate"):
                    retried += 1
                elif outcome.result == "Failed":
                    failed += 1
                    stopped = True
                elif outcome.result == "Blocked":
                    blocked += 1
                    stopped = True
                else:
                    skipped += 1
        log.info(
            "Retried the dead letters of processor %s: %d retried, %d failed, %d blocked, %d skipped",
            processor,
            retried,
            failed,
            blocked,
            skipped,
        )
        return RetryAllOutcome(retried, failed, blocked, skipped)

    async def _retry(self, store: MessageStore, letter: DeadLetter) -> RetryOutcome:
        if letter.reason is DeadLetterReason.INVALID:
            return RetryOutcome("NotRetriable")
        if letter.key is not None:
            earlier = await store.dead_letters.earlier(letter.processor, letter.key, letter.position)
            if earlier is not None:
                return RetryOutcome("Blocked", earlier)
        type_name = letter.event_type
        if type_name is None or not type_name.strip():
            return RetryOutcome("NotRetriable")  # only INVALID letters carry no type
        handlers = self._registry.handlers(letter.processor, type_name)
        dedupe_id = letter.dedupe_id()
        if not handlers:
            error = DeadLetterError.new("NoHandler", f"no handler for {type_name} in processor {letter.processor}")
            return await self._failed(store, letter, error)
        # A letter parked while its processor replayed is retried as a replay: the handlers marked for no replay are
        # skipped (the marker is still written, so the letter is deleted with it).
        active = tuple(handler for handler in handlers if not letter.replay or handler.replay)
        info = event_message(dedupe_id, type_name, letter.headers)
        user_id = letter.headers.get(EventHeaders.USER_ID, "")
        marker = ProcessedMarker(
            id=processor_marker(letter.processor, dedupe_id),
            type_name=type_name,
            processed_at_ms=self._clock(),
            processor=letter.processor,
            source=letter.source,
            segment=letter.segment,
            position=letter.position,
        )
        try:
            with event_handling(info, user_id):
                async with store.transaction():
                    if not await store.processed.record(marker):
                        raise _Duplicate
                    for handler in active:
                        message: object = self._codec.decode(letter.payload, handler.message)
                        await handler.fn(message)
                    if not await store.dead_letters.delete_in(letter.id):
                        raise _Gone
        except _Duplicate:
            await store.dead_letters.delete(letter.id)
            log.info("Dead letter %s was already handled; deleted it", letter.id)
            return RetryOutcome("Duplicate")
        except _Gone:
            return RetryOutcome("NotFound")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            letter_error = DeadLetterError.new(type(error).__name__, clean_text(str(error)))
            return await self._failed(store, letter, letter_error)
        log.info("Retried dead letter %s", letter.id)
        return RetryOutcome("Retried")

    async def _failed(self, store: MessageStore, letter: DeadLetter, error: DeadLetterError) -> RetryOutcome:
        if not await store.dead_letters.record_failure(letter.id, error, self._clock()):
            return RetryOutcome("NotFound")
        log.warning("Retry of dead letter %s failed: %s: %s", letter.id, error.type_name, error.message)
        return RetryOutcome("Failed", error.message)
