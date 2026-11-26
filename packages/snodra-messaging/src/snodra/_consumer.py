"""The event consumer: one processor's subscription, read at least once with the stage-1 marker.

For every record the event store delivers:

- **A type no handler handles** is acknowledged and runs nothing.
- **Otherwise** the consumer records the stage-1 marker ``processor:<name>:<eventId>`` and runs the processor's handlers
  in one transaction of the message store; the marker and the handlers' writes (an event they publish included) commit
  together, and the record is acknowledged only after the commit. An existing marker makes the delivery a duplicate: the
  handlers are skipped and the record is acknowledged.
- **A failing handler** is retried with the level-1 scaled backoff, then, at level 2, logged and skipped (level 3 parks
  it as a dead letter). A malformed record (no type header, a payload that is not a JSON object) is logged and
  acknowledged as well; retrying it cannot help.

A record is never acknowledged before its handling committed: a crash in between leaves the record to the next
subscription of the same name, whose marker skips what was handled already. A subscription that fails is closed and
joined again after a backoff (1 s doubling to 30 s), which starts over after a subscription delivered a record or lived
30 s before it failed.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-2-events (Consume) and
https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-rule.
"""

from __future__ import annotations

import asyncio
import logging
import time

from snodra._backoff import next_wait
from snodra._codec import Codec
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
    Revoked,
    SubscriptionSpec,
)
from snodra._store import MessageStore, ProcessedMarker

log = logging.getLogger(__name__)


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
    """One processor's consumer: its subscription on the event store and its handlers."""

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
        self._spec = SubscriptionSpec(
            name=processor_subscription(service, processor),
            source=source,
        )
        self._delivered = False

    @property
    def subscription(self) -> SubscriptionSpec:
        """The subscription this consumer joins."""
        return self._spec

    def start(self) -> asyncio.Task[None]:
        """Reads the subscription until the task is cancelled."""
        return asyncio.create_task(self.run(), name=f"snodra:consumer:{self._processor}")

    async def run(self) -> None:
        """Subscribes, reads until a subscription fails, and subscribes again after a backoff."""
        # TODO(spec-gap): SG-21
        wait = self._resubscribe_min
        while True:
            try:
                subscription = await self._events.subscribe(self._spec)
            except asyncio.CancelledError as error:
                if _cancelling():
                    raise
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
            self._delivered = False
            started = time.monotonic()
            trouble: BaseException | None = None
            try:
                await self._read(subscription)
            except asyncio.CancelledError as error:
                if _cancelling():
                    raise
                trouble = error
            except Exception as error:
                trouble = error
            finally:
                try:
                    await subscription.close()
                except asyncio.CancelledError as error:
                    if _cancelling():
                        raise
                    log.warning("closing the subscription %s failed: %s", self._spec.name, error)
                except Exception as error:
                    log.warning("closing the subscription %s failed: %s", self._spec.name, error)
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

    async def _read(self, subscription: EventStoreSubscription) -> None:
        """Reads until cancelled or the subscription fails."""
        while True:
            event = await subscription.next()
            if isinstance(event, Assigned):
                log.debug("the event processor %s was assigned %s", self._processor, event.segments)
                continue
            if isinstance(event, Revoked):
                log.debug("the event processor %s lost %s", self._processor, event.segments)
                continue
            self._delivered = True
            await self._handle(subscription, event)
            trouble = subscription.take_error()
            if trouble is not None:
                log.warning("the event subscription %s recovered from: %s", self._spec.name, trouble)

    async def _handle(self, subscription: EventStoreSubscription, delivery: Delivery) -> None:
        """Handles one delivery, then acknowledges it; a failure is logged and skipped after its attempts."""
        # TODO(spec-gap): SG-22
        type_ = delivery.headers.get(EventHeaders.TYPE)
        if type_ is None or not type_.strip():
            log.warning(
                "skipping a record without a type at %s/%s of %s",
                delivery.segment,
                delivery.position,
                self._spec.source,
            )
            await self._ack(subscription, delivery)
            return
        handlers = self._registry.handlers(self._processor, type_)
        if not handlers:
            await self._ack(subscription, delivery)
            return
        if not is_json_object(delivery.payload):
            log.warning(
                "skipping the %s event without a JSON object at %s/%s of %s",
                type_,
                delivery.segment,
                delivery.position,
                self._spec.source,
            )
            await self._ack(subscription, delivery)
            return
        event_id = self._event_id(delivery)
        info = event_message(event_id, type_, delivery.headers)
        user_id = delivery.headers.get(EventHeaders.USER_ID, "")
        # TODO(spec-gap): SG-20
        failure: BaseException | None = None
        attempt = 0
        for attempt in range(1, self._max_attempts + 1):
            try:
                with event_handling(info, user_id):
                    duplicate = await self._stage_one(delivery, type_, event_id, handlers)
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
        log.error(
            "skipping the %s event %s after %d attempt(s) in processor %s: %s",
            type_,
            event_id,
            attempt,
            self._processor,
            failure,
        )
        await self._ack(subscription, delivery)

    async def _stage_one(
        self,
        delivery: Delivery,
        type_: str,
        event_id: str,
        handlers: tuple[EventHandler, ...],
    ) -> bool:
        """Records the marker and runs the handlers in one transaction; whether the event was a duplicate."""
        marker = ProcessedMarker(
            id=processor_marker(self._processor, event_id),
            type_name=type_,
            processed_at_ms=time.time_ns() // 1_000_000,
            processor=self._processor,
            source=self._spec.source,
            segment=delivery.segment,
            position=delivery.position,
        )
        try:
            async with self._store.transaction():
                if not await self._store.processed.record(marker):
                    # The port contract: a duplicate aborts the caller's transaction. Raising inside the context makes
                    # every add-on's context manager roll back, and nothing of this attempt commits.
                    raise _Duplicate
                for handler in handlers:
                    try:
                        message: object = self._codec.decode(delivery.payload, handler.message)
                    except DecodeError as error:
                        raise _Undecodable(error) from error
                    await handler.fn(message)
        except _Duplicate:
            return True
        return False

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

    def _event_id(self, delivery: Delivery) -> str:
        event_id = delivery.headers.get(EventHeaders.ID)
        if event_id is not None and event_id.strip():
            return event_id
        return f"{self._spec.source}-{delivery.segment}-{delivery.position}"
