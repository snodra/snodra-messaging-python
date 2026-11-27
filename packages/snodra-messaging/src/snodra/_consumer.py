"""The event consumer: one processor's subscription, read at least once with the stage-1 marker.

For every record the event store delivers:

- **A type no handler handles** is acknowledged and runs nothing.
- **Otherwise** the consumer records the stage-1 marker ``processor:<name>:<eventId>`` and runs the processor's handlers
  in one transaction of the message store; the marker and the handlers' writes (an event they publish included) commit
  together, and the record is acknowledged only after the commit. An existing marker makes the delivery a duplicate: the
  handlers are skipped and the record is acknowledged.
- **A failing handler** is retried with the level-1 scaled backoff; a record that exhausts its attempts is parked as a
  dead letter (mode ``collection``), published to the event store's dead-letter source (mode ``event-store``) or logged
  and skipped (mode ``log``), then acknowledged. A malformed record (no type header, a payload that is not a JSON
  object) is an ``INVALID`` letter, which never blocks its key; it too is acknowledged.
- **A parked key** (mode ``collection``): a record whose key has a letter already waiting is parked ``SEQUENCED``
  without running the handlers, so the key keeps its order; when its own letter exists already (parked before a crash),
  the delivery is a duplicate and is acknowledged.

A record is never acknowledged before its handling committed or its letter was written: a crash in between leaves the
record to the next subscription of the same name, whose marker skips what was handled already. A subscription that fails
is closed and joined again after a backoff (1 s doubling to 30 s), which starts over after a subscription delivered a
record or lived 30 s before it failed.

Pause and status are driven from :mod:`snodra._processors`: a pause pauses the subscription without leaving it, and the
status is built from the counters and the subscription's own non-blocking reads.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Processors) and
https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-rule.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from functools import cmp_to_key

from snodra._backoff import next_wait
from snodra._codec import Codec
from snodra._deadletters import COLLECTION, EVENT_STORE
from snodra._errors import DecodeError
from snodra._events import (
    EventHandler,
    EventRegistry,
    event_handling,
    event_message,
    is_json_object,
    processor_marker,
    processor_subscription,
)
from snodra._eventstore import (
    Assigned,
    Delivery,
    EventHeaders,
    EventStore,
    EventStoreSubscription,
    OutgoingEvent,
    ProcessedMarker,
    ReplayRange,
    Revoked,
    SegmentStatus,
    SubscriptionSpec,
    dead_letter_source,
    segment_order,
)
from snodra._hubpb import hub_pb2
from snodra._replay import in_replay, open_until, passed_ranges
from snodra._status import (
    MAX_DEAD_LETTER_ERROR_CHARS,
    StatusView,
    build_status,
    clean_text,
    code_point_cut,
    state_of,
    utf8_cut,
)
from snodra._store import (
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    DeadLetterStore,
    MessageStore,
    Parked,
    Unsupported,
    letter_id,
    now_ms,
)

log = logging.getLogger(__name__)

DLT_PROCESSOR = "snodra-dlt-processor"
DLT_REASON = "snodra-dlt-reason"
DLT_ERROR = "snodra-dlt-error"
DLT_ORIGINAL_TOPIC = "snodra-dlt-original-topic"
DLT_ORIGINAL_PARTITION = "snodra-dlt-original-partition"
DLT_ORIGINAL_OFFSET = "snodra-dlt-original-offset"
_DLT_HEADERS = frozenset(
    {DLT_PROCESSOR, DLT_REASON, DLT_ERROR, DLT_ORIGINAL_TOPIC, DLT_ORIGINAL_PARTITION, DLT_ORIGINAL_OFFSET}
)


def _cancelling() -> bool:
    """Whether the current task is being cancelled (a ``CancelledError`` from elsewhere is a failure)."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


class _Duplicate(Exception):
    """Raised inside the marker's transaction so a duplicate aborts it instead of committing (the port contract:
    the caller aborts when ``record`` answers false)."""


class _Undecodable(Exception):
    """The codec could not read the payload as one handler's message: another attempt cannot change it."""

    def __init__(self, error: DecodeError) -> None:
        super().__init__(str(error))
        self.error = error


class EventConsumer:
    """One processor's consumer: its subscription on the event store, its handlers and its status."""

    def __init__(
        self,
        *,
        service: str,
        processor: str,
        store: MessageStore,
        events: EventStore,
        registry: EventRegistry,
        source: str,
        codec: Codec,
        max_attempts: int,
        backoff: float,
        resubscribe_min: float,
        resubscribe_max: float,
        resubscribe_stable: float,
        dead_letter_mode: str = COLLECTION,
        dead_letter_retention_ms: int = 30 * 24 * 60 * 60 * 1000,
        dead_letter_publish_timeout: float = 30.0,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"the event handler attempts must be at least 1, got {max_attempts}")
        self._processor = processor
        self._store = store
        self._events = events
        self._registry = registry
        self._source = source
        self._codec = codec
        self._max_attempts = max_attempts
        self._backoff = backoff
        self._resubscribe_min = resubscribe_min
        self._resubscribe_max = resubscribe_max
        self._resubscribe_stable = resubscribe_stable
        self._mode = dead_letter_mode
        self._retention_ms = dead_letter_retention_ms
        self._publish_timeout = dead_letter_publish_timeout
        self._clock: Callable[[], int] = now_ms if clock is None else clock
        self._spec = SubscriptionSpec(
            name=processor_subscription(service, processor),
            source=source,
        )
        self._delivered = False
        self._letters: DeadLetterStore | None = store.dead_letters if dead_letter_mode == COLLECTION else None
        # The status: counters and the latest trouble, written here and read by status().
        self._running = False
        self._started = False
        self._assigned = False
        self._rebalancing = False
        self._paused = False
        self._failing_since_ms = 0
        self._last_error = ""
        self._last_error_at_ms = 0
        self._processed_total = 0
        self._failed_total = 0
        self._dead_letters = 0
        self._parked_since_sample = False
        self._operation_id = ""
        self._replay: tuple[ReplayRange, ...] = ()
        self._subscription: EventStoreSubscription | None = None
        self._stop_requested = False
        self._handling = False
        # The parking state of mode collection: the keys with a letter, the segments whose keys are unknown, and what a
        # redelivery still has to write (it does not run the handlers again).
        self._parked_keys: set[str] = set()
        self._keys_unknown: set[str] = set()
        self._pending: dict[tuple[str, str], Callable[[], Awaitable[None]]] = {}

    @property
    def processor(self) -> str:
        """The processor this consumer runs."""
        return self._processor

    @property
    def subscription(self) -> SubscriptionSpec:
        """The subscription this consumer joins."""
        return self._spec

    @property
    def running(self) -> bool:
        """Whether its reader task runs now."""
        return self._running

    @property
    def was_started(self) -> bool:
        """Whether it was started (also after it stopped: the status then says STOPPED)."""
        return self._started

    @property
    def paused(self) -> bool:
        """Whether consumption is paused (or starts paused)."""
        return self._paused

    @property
    def operation_id(self) -> str:
        """The reset operation holding this processor's lock, as the status reports it; empty when none."""
        return self._operation_id

    @operation_id.setter
    def operation_id(self, value: str) -> None:
        self._operation_id = value

    @property
    def parked_dirty(self) -> bool:
        """Whether a letter was parked since the last sample."""
        return self._parked_since_sample

    def start(self) -> asyncio.Task[None]:
        """Starts reading, paused when a pause is in force, and returns its task."""
        self._started = True
        self._running = True
        self._stop_requested = False
        return asyncio.create_task(self.run(), name=f"snodra:consumer:{self._processor}")

    @property
    def handling(self) -> bool:
        """Whether a record is in hand now (a stop waits for it before leaving the subscription)."""
        return self._handling

    def request_stop(self) -> None:
        """Asks the reader to finish the record in hand, acknowledge it and leave the subscription."""
        self._stop_requested = True

    def pause(self) -> None:
        """Pauses consumption: the subscription delivers nothing more without being left (a record handed out before
        the pause took effect is handed out again on the resume). Idempotent."""
        self._paused = True
        if self._subscription is not None:
            self._subscription.pause()

    def resume(self) -> None:
        """Resumes consumption after :meth:`pause`. Idempotent."""
        self._paused = False
        if self._subscription is not None:
            self._subscription.resume()

    def set_replay(self, ranges: Sequence[ReplayRange]) -> None:
        """Sets the open replay ranges (the reset's, from ``processor_control``); another source's are ignored."""
        self._replay = tuple(r for r in ranges if r.source == self._source)

    async def run(self) -> None:
        """Subscribes, reads until a subscription fails, and subscribes again after a backoff."""
        # TODO(spec-gap): SG-21
        wait = self._resubscribe_min
        try:
            while True:
                try:
                    subscription = await self._events.subscribe(self._spec)
                except asyncio.CancelledError as error:
                    if _cancelling():
                        raise
                    self._failing_text(f"subscription: {error}")
                    log.warning(
                        "the event processor %s could not subscribe to %s, retrying in %.2f s: %s",
                        self._processor,
                        self._spec.name,
                        wait,
                        error,
                    )
                    await asyncio.sleep(wait)
                    wait = next_wait(wait, self._resubscribe_min, self._resubscribe_max)
                    continue
                except Exception as error:
                    self._failing_text(f"subscription: {error}")
                    log.warning(
                        "the event processor %s could not subscribe to %s, retrying in %.2f s: %s",
                        self._processor,
                        self._spec.name,
                        wait,
                        error,
                    )
                    await asyncio.sleep(wait)
                    wait = next_wait(wait, self._resubscribe_min, self._resubscribe_max)
                    continue
                self._subscription = subscription
                if self._paused:
                    subscription.pause()
                self._delivered = False
                started = time.monotonic()
                trouble: BaseException | None = None
                stopped = False
                try:
                    stopped = await self._read(subscription)
                except asyncio.CancelledError as error:
                    if _cancelling():
                        raise
                    trouble = error
                except Exception as error:
                    trouble = error
                finally:
                    self._subscription = None
                    self._parked_keys.clear()
                    self._keys_unknown.clear()
                    self._pending.clear()
                    self._rebalancing = True
                    try:
                        await subscription.close()
                    except asyncio.CancelledError as error:
                        if _cancelling():
                            raise
                        log.warning("closing the subscription %s failed: %s", self._spec.name, error)
                    except Exception as error:
                        log.warning("closing the subscription %s failed: %s", self._spec.name, error)
                if stopped:
                    return
                if trouble is not None:
                    if isinstance(trouble, Exception):
                        self._failing(trouble)
                    else:
                        self._failing_text(f"subscription: {trouble}")
                if self._delivered or time.monotonic() - started >= self._resubscribe_stable:
                    wait = self._resubscribe_min
                if trouble is not None:
                    log.warning(
                        "the event processor %s stopped reading %s, subscribing again in %.2f s: %s",
                        self._processor,
                        self._spec.name,
                        wait,
                        trouble,
                    )
                await asyncio.sleep(wait)
                wait = next_wait(wait, self._resubscribe_min, self._resubscribe_max)
        finally:
            self._running = False

    async def _read(self, subscription: EventStoreSubscription) -> bool:
        """Reads until cancelled, the subscription fails, or a stop was asked for; whether it was a stop."""
        while True:
            event = await subscription.next()
            if isinstance(event, Assigned):
                log.debug("the event processor %s was assigned %s", self._processor, event.segments)
                await self._load_keys(event.segments)
                self._assigned = True
                self._rebalancing = False
                continue
            if isinstance(event, Revoked):
                log.debug("the event processor %s lost %s", self._processor, event.segments)
                self._rebalancing = True
                continue
            if self._paused:
                # Paused after the subscription handed it out: not handled, handed out again on the resume.
                await self._redeliver(subscription, event)
                continue
            self._delivered = True
            replay = in_replay(event, self._replay, self._events.compare)
            self._handling = True
            try:
                await self._handle(subscription, event, replay)
            finally:
                self._handling = False
            if self._stop_requested:
                log.debug("the event processor %s stops after the record in hand", self._processor)
                return True
            trouble = subscription.take_error()
            if trouble is not None:
                self._failing_text(trouble)

    async def _handle(self, subscription: EventStoreSubscription, delivery: Delivery, replay: bool) -> None:
        """Handles one delivery, then acknowledges it; an exhausted or invalid record goes to its dead letters."""
        # TODO(spec-gap): SG-22
        coords = (delivery.segment, delivery.position)
        if coords in self._pending:
            await self._deliver(subscription, delivery, self._pending[coords])
            return
        type_ = delivery.headers.get(EventHeaders.TYPE)
        if type_ is None or not type_.strip():
            await self._invalid(subscription, delivery, None, "record without type header", replay)
            return
        handlers = self._registry.handlers(self._processor, type_)
        if not handlers:
            await self._ack(subscription, delivery)
            return
        if not is_json_object(delivery.payload):
            await self._invalid(subscription, delivery, type_, "payload is not a JSON object", replay)
            return
        event_id = self._event_id(delivery)
        if await self._sequenced(subscription, delivery, type_, replay):
            return
        await self._attempt(subscription, delivery, type_, event_id, handlers, replay)

    async def _attempt(
        self,
        subscription: EventStoreSubscription,
        delivery: Delivery,
        type_: str,
        event_id: str,
        handlers: tuple[EventHandler, ...],
        replay: bool,
    ) -> None:
        """Runs stage 1 with its attempts and backoff; an exhausted record goes to the dead letters."""
        info = event_message(event_id, type_, delivery.headers)
        user_id = delivery.headers.get(EventHeaders.USER_ID, "")
        # TODO(spec-gap): SG-20
        failure: BaseException | None = None
        attempt = 0
        for attempt in range(1, self._max_attempts + 1):
            try:
                with event_handling(info, user_id):
                    duplicate = await self._stage_one(delivery, type_, event_id, handlers, replay)
            except asyncio.CancelledError as error:
                if _cancelling():
                    raise
                failure = error
            except _Undecodable as error:
                # TODO(spec-gap): SG-22
                # The payload does not fit the handler's message class: another attempt cannot change it.
                failure = error.error
                break
            except Exception as error:
                failure = error
            else:
                self._processed_total += 1
                if duplicate:
                    log.debug("skipping the duplicate %s event %s", type_, event_id)
                await self._ack(subscription, delivery)
                return
            if attempt < self._max_attempts:
                log.warning(
                    "the %s event %s failed on attempt %d, retrying: %s",
                    type_,
                    event_id,
                    attempt,
                    failure,
                )
                await asyncio.sleep(self._backoff)
                continue
            break
        await self._exhausted(subscription, delivery, type_, failure, attempt, replay)

    async def _exhausted(
        self,
        subscription: EventStoreSubscription,
        delivery: Delivery,
        type_: str,
        failure: BaseException | None,
        attempts: int,
        replay: bool,
    ) -> None:
        """A record failed every attempt: park it, publish it to the dead-letter source, or log and skip it."""
        self._failed_total += 1
        self._failing(failure) if isinstance(failure, Exception) else self._failing_text(str(failure))
        error = _error_of(failure)
        if self._mode == COLLECTION:
            letter = self._letter(delivery, DeadLetterReason.HANDLER_FAILED, error, attempts, replay=replay)
            log.error(
                "Event type=%s id=%s at %s-%s@%s failed %d attempt(s) in processor %s, parking it as %s: %s: %s",
                type_,
                delivery.headers.get(EventHeaders.ID, ""),
                self._source,
                delivery.segment,
                delivery.position,
                attempts,
                self._processor,
                letter.id,
                error.type_name,
                error.message,
            )
            await self._deliver(subscription, delivery, self._park_work(letter))
        elif self._mode == EVENT_STORE:
            log.error(
                "Event type=%s id=%s at %s-%s@%s failed %d attempt(s) in processor %s, sending it to %s: %s: %s",
                type_,
                delivery.headers.get(EventHeaders.ID, ""),
                self._source,
                delivery.segment,
                delivery.position,
                attempts,
                self._processor,
                dead_letter_source(self._source),
                error.type_name,
                error.message,
            )
            await self._deliver(
                subscription,
                delivery,
                self._publish_work(delivery, DeadLetterReason.HANDLER_FAILED, error),
            )
        else:
            log.error(
                "Giving up on event type=%s id=%s at %s-%s@%s after %d attempt(s) in processor %s: %s: %s",
                type_,
                delivery.headers.get(EventHeaders.ID, ""),
                self._source,
                delivery.segment,
                delivery.position,
                attempts,
                self._processor,
                error.type_name,
                error.message,
            )
            await self._ack(subscription, delivery)

    async def _invalid(
        self,
        subscription: EventStoreSubscription,
        delivery: Delivery,
        type_: str | None,
        why: str,
        replay: bool,
    ) -> None:
        """A record that cannot be handled: an INVALID letter (which blocks nothing), a publish, or a log and skip."""
        error = DeadLetterError.invalid(why)
        label = "?" if type_ is None else type_
        if self._mode == COLLECTION:
            letter = self._letter(delivery, DeadLetterReason.INVALID, error, 0, replay=replay)
            log.warning(
                "Record at %s-%s@%s of type %s is invalid (%s), parking it as %s",
                self._source,
                delivery.segment,
                delivery.position,
                label,
                why,
                letter.id,
            )
            await self._deliver(subscription, delivery, self._park_work(letter))
        elif self._mode == EVENT_STORE:
            log.warning(
                "Record at %s-%s@%s of type %s is invalid (%s), sending it to %s",
                self._source,
                delivery.segment,
                delivery.position,
                label,
                why,
                dead_letter_source(self._source),
            )
            await self._deliver(subscription, delivery, self._publish_work(delivery, DeadLetterReason.INVALID, error))
        else:
            log.warning(
                "Skipping record at %s-%s@%s of type %s: %s",
                self._source,
                delivery.segment,
                delivery.position,
                label,
                why,
            )
            await self._ack(subscription, delivery)

    async def _sequenced(
        self,
        subscription: EventStoreSubscription,
        delivery: Delivery,
        type_: str,
        replay: bool,
    ) -> bool:
        """Parks the record SEQUENCED when its key has a letter (or acknowledges a duplicate); whether it handled it."""
        letters = self._letters
        key = delivery.key
        if letters is None or key is None:
            return False
        segment = delivery.segment
        if segment in self._keys_unknown:
            await self._load_keys((segment,))
        if segment not in self._keys_unknown and key not in self._parked_keys:
            return False
        try:
            head = await letters.head(self._processor, key)
        except asyncio.CancelledError:
            raise
        except Unsupported as error:
            # TODO(spec-gap): SG-45
            self._failing_text(
                f"the message store {self._store.name} does not keep dead letters ({error}); "
                "handling the record without sequencing it"
            )
            log.error(
                "The message store %s does not keep dead letters (%s): handling the record %s-%s@%s of processor %s "
                "without sequencing it",
                self._store.name,
                error,
                self._source,
                segment,
                delivery.position,
                self._processor,
            )
            return False
        except Exception as error:
            self._failing(error)
            log.warning(
                "the event processor %s could not check the parked key %s of %s-%s@%s, redelivering it: %s",
                self._processor,
                key,
                self._source,
                segment,
                delivery.position,
                error,
            )
            await self._redeliver(subscription, delivery)
            await asyncio.sleep(self._backoff)
            return True
        if head is None:
            self._parked_keys.discard(key)  # retried, discarded or expired, here or on another instance
            return False
        own = letter_id(self._processor, self._source, segment, delivery.position)
        if head == own or await letters.get(own) is not None:
            log.info(
                "Record at %s-%s@%s is already parked in processor %s, acknowledging it",
                self._source,
                segment,
                delivery.position,
                self._processor,
            )
            await self._ack(subscription, delivery)
            return True
        letter = self._letter(
            delivery, DeadLetterReason.SEQUENCED, DeadLetterError.sequenced(head), 0, blocked_by=head, replay=replay
        )
        log.info(
            "Record at %s-%s@%s of type %s parked SEQUENCED in processor %s behind %s",
            self._source,
            segment,
            delivery.position,
            type_,
            self._processor,
            head,
        )
        await self._deliver(subscription, delivery, self._park_work(letter))
        return True

    def _park_work(self, letter: DeadLetter) -> Callable[[], Awaitable[None]]:
        """What writes ``letter`` and keeps what the status and the sequencing need when it succeeded."""

        async def work() -> None:
            letters = self._letters
            assert letters is not None
            try:
                result = await letters.park(letter)
            except Unsupported as error:
                # TODO(spec-gap): SG-45
                self._failing_text(
                    f"the message store {self._store.name} does not keep dead letters ({error}); "
                    "the record is acknowledged as lost"
                )
                log.error(
                    "The message store %s does not keep dead letters (%s): acknowledging the record %s of processor "
                    "%s as lost instead of redelivering it forever",
                    self._store.name,
                    error,
                    letter.id,
                    self._processor,
                )
                return
            if letter.key is not None and letter.reason is not DeadLetterReason.INVALID:
                self._parked_keys.add(letter.key)
            if result is Parked.NEW:
                self._parked_since_sample = True

        return work

    def _publish_work(
        self,
        delivery: Delivery,
        reason: DeadLetterReason,
        error: DeadLetterError,
    ) -> Callable[[], Awaitable[None]]:
        """What publishes ``delivery`` to ``<source>.dlt`` with its six ``snodra-dlt-*`` headers."""

        async def work() -> None:
            headers = {name: value for name, value in delivery.headers.items() if name not in _DLT_HEADERS}
            headers[DLT_PROCESSOR] = self._processor
            headers[DLT_REASON] = reason.value
            error_text = f"{error.type_name}: {error.message}"
            headers[DLT_ERROR] = code_point_cut(error_text, MAX_DEAD_LETTER_ERROR_CHARS)
            headers[DLT_ORIGINAL_TOPIC] = self._source
            headers[DLT_ORIGINAL_PARTITION] = delivery.segment
            headers[DLT_ORIGINAL_OFFSET] = delivery.position
            async with asyncio.timeout(self._publish_timeout):
                await self._events.publish(
                    OutgoingEvent(
                        source=dead_letter_source(self._source),
                        id=self._event_id(delivery),
                        type_name=delivery.headers.get(EventHeaders.TYPE, ""),
                        key=delivery.key,
                        headers=headers,
                        payload=delivery.payload,
                    )
                )

        return work

    async def _deliver(
        self,
        subscription: EventStoreSubscription,
        delivery: Delivery,
        work: Callable[[], Awaitable[None]],
    ) -> None:
        """Runs the sink work (park, publish) and acknowledges the record; on failure the record stays unacknow-ledged,
        its work is kept for the redelivery, and it is handed out again after the backoff."""
        coords = (delivery.segment, delivery.position)
        self._pending[coords] = work
        try:
            await work()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._failing_text(
                f"could not hand record {self._source}-{delivery.segment}@{delivery.position} to the dead letters "
                f"({self._mode}), not acknowledging it: {type(error).__name__}: {error}"
            )
            await self._redeliver(subscription, delivery)
            await asyncio.sleep(self._backoff)
            return
        self._pending.pop(coords, None)
        await self._ack(subscription, delivery)

    async def _stage_one(
        self,
        delivery: Delivery,
        type_: str,
        event_id: str,
        handlers: tuple[EventHandler, ...],
        replay: bool,
    ) -> bool:
        """Records the marker and runs the handlers in one transaction; whether the event was a duplicate."""
        marker = _stage_marker(self._processor, self._source, delivery, type_, event_id, self._clock())
        try:
            async with self._store.transaction():
                if not await self._store.processed.record(marker):
                    # The port contract: a duplicate aborts the caller's transaction. Raising inside the context makes
                    # every add-on's context manager roll back, and nothing of this attempt commits.
                    raise _Duplicate
                for handler in handlers:
                    if replay and not handler.replay:
                        continue
                    try:
                        message: object = self._codec.decode(delivery.payload, handler.message)
                    except DecodeError as error:
                        raise _Undecodable(error) from error
                    await handler.fn(message)
        except _Duplicate:
            return True
        return False

    def _letter(
        self,
        delivery: Delivery,
        reason: DeadLetterReason,
        error: DeadLetterError,
        attempts: int,
        blocked_by: str | None = None,
        replay: bool = False,
    ) -> DeadLetter:
        """The letter of ``delivery``: its id, key, headers, payload, the reason and the attempts."""
        now = self._clock()
        return DeadLetter(
            id=letter_id(self._processor, self._source, delivery.segment, delivery.position),
            processor=self._processor,
            source=self._source,
            segment=delivery.segment,
            position=delivery.position,
            key=delivery.key,
            event_id=_header_or_none(delivery.headers, EventHeaders.ID),
            event_type=_header_or_none(delivery.headers, EventHeaders.TYPE),
            headers=dict(delivery.headers),
            payload=bytes(delivery.payload),
            reason=reason,
            error=error,
            blocked_by=blocked_by,
            attempts=attempts,
            parked_at_ms=now,
            last_attempt_at_ms=now,
            expire_at_ms=now + self._retention_ms,
            replay=replay,
        )

    async def _load_keys(self, segments: Sequence[str]) -> None:
        """Loads the keys with a parked letter on the assigned segments; a failure makes every keyed record confirm
        against the store until a load succeeds."""
        letters = self._letters
        if letters is None or not segments:
            return
        try:
            self._parked_keys.update(await letters.parked_keys(self._processor, self._source, tuple(segments)))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._keys_unknown.update(segments)
            log.warning(
                "the event processor %s could not load the parked keys of %s: %s",
                self._processor,
                segments,
                error,
            )
        else:
            self._keys_unknown.difference_update(segments)

    async def _ack(self, subscription: EventStoreSubscription, delivery: Delivery) -> None:
        try:
            await subscription.ack(delivery.segment, delivery.position)
        except asyncio.CancelledError as error:
            if _cancelling():
                raise
            log.warning(
                "acknowledging %s/%s of %s failed; it will come again: %s",
                delivery.segment,
                delivery.position,
                self._spec.source,
                error,
            )
        except Exception as error:
            log.warning(
                "acknowledging %s/%s of %s failed; it will come again: %s",
                delivery.segment,
                delivery.position,
                self._spec.source,
                error,
            )

    async def _redeliver(self, subscription: EventStoreSubscription, delivery: Delivery) -> None:
        try:
            await subscription.redeliver(delivery.segment, delivery.position)
        except asyncio.CancelledError as error:
            if _cancelling():
                raise
            log.warning("redelivering %s/%s failed: %s", delivery.segment, delivery.position, error)
        except Exception as error:
            log.warning("redelivering %s/%s failed: %s", delivery.segment, delivery.position, error)

    def _event_id(self, delivery: Delivery) -> str:
        event_id = delivery.headers.get(EventHeaders.ID)
        if event_id is not None and event_id.strip():
            return event_id
        return f"{self._spec.source}-{delivery.segment}-{delivery.position}"

    # ---- status and sampling -------------------------------------------------------------------

    def status(self) -> hub_pb2.ProcessorStatus:
        """This processor's status, from memory only: never blocks."""
        if self._subscription is not None:
            trouble = self._subscription.take_error()
            if trouble is not None:
                self._failing_text(trouble)
        now = self._clock()
        segments: list[SegmentStatus] = []
        replay_until: dict[str, str] = {}
        if self._running and self._subscription is not None:
            try:
                segments = self._subscription.segments()
            except Exception:
                segments = []
            segments.sort(key=cmp_to_key(_segment_status_order))
            replay_until, replaying = open_until(self._replay, segments, self._events.compare)
        else:
            replaying = False
        return build_status(
            StatusView(
                name=self._processor,
                subscription=self._spec.name,
                source=self._spec.source,
                event_store=self._events.name,
                capabilities=frozenset(cap.value for cap in self._events.capabilities),
                state=state_of(
                    running=self._running,
                    paused=self._paused,
                    failing_since_ms=self._failing_since_ms,
                    now_ms=now,
                    assigned=self._assigned,
                    rebalancing=self._rebalancing,
                ),
                segments=segments,
                replay_until=replay_until,
                processed_total=self._processed_total,
                failed_total=self._failed_total,
                dead_letters=self._dead_letters if self._mode == COLLECTION else 0,
                last_error=self._last_error,
                last_error_at_ms=self._last_error_at_ms,
                event_types=self._registry.types(self._processor),
                dead_letter_mode=self._mode,
                replaying=replaying,
                operation_id=self._operation_id,
            )
        )

    @property
    def replay_ranges(self) -> tuple[ReplayRange, ...]:
        """The open replay ranges this consumer holds (those of its source)."""
        return self._replay

    def passed_ranges(self) -> list[ReplayRange]:
        """The open ranges this instance's subscription has consumed past, as its segments report now."""
        if self._subscription is None:
            return []
        try:
            segments = self._subscription.segments()
        except Exception:
            return []
        return passed_ranges(self._replay, segments, self._events.compare)

    async def sample_dead_letters(self, force: bool = False) -> None:
        """Reads this processor's letter count (mode ``collection`` only) for its status; blocking, run it off the
        processor's read task. With ``force`` it samples even when nothing was parked since the last call."""
        letters = self._letters
        if letters is None:
            return
        if not force and not self._parked_since_sample:
            return
        self._parked_since_sample = False
        try:
            self._dead_letters = await letters.count(self._processor)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._parked_since_sample = True
            raise

    def _failing(self, error: BaseException) -> None:
        self._failing_text(f"{type(error).__name__}: {error}")

    def _failing_text(self, text: str) -> None:
        """``text`` becomes ``last_error`` and the processor is FAILING for the failing window."""
        now = self._clock()
        self._last_error = utf8_cut(text, 512)
        self._last_error_at_ms = now
        self._failing_since_ms = now


def _segment_status_order(a: SegmentStatus, b: SegmentStatus) -> int:
    """Segments in the hub's order."""
    return segment_order(a.segment, b.segment)


def _stage_marker(
    processor: str, source: str, delivery: Delivery, type_: str, event_id: str, now_ms: int
) -> ProcessedMarker:
    """The stage-1 marker of ``delivery``, carrying the processor and the record's position."""
    return ProcessedMarker(
        id=processor_marker(processor, event_id),
        type_name=type_,
        processed_at_ms=now_ms,
        processor=processor,
        source=source,
        segment=delivery.segment,
        position=delivery.position,
    )


def _error_of(failure: BaseException | None) -> DeadLetterError:
    if failure is None:
        return DeadLetterError.new("Error", "the handler failed without an error")
    return DeadLetterError.new(type(failure).__name__, clean_text(str(failure)))


def _header_or_none(headers: Mapping[str, str], name: str) -> str | None:
    value = headers.get(name)
    return None if value is None or not value.strip() else value
