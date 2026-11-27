"""The service's event processors: one consumer per processor, their status and their pause.

:meth:`snodra.App.start` builds one :class:`EventConsumer` per processor with a handler (``default`` first, then the
named ones) and this manager starts them as their ``processor_control`` documents say: a processor whose document is
paused consumes nothing without leaving its subscription, one under an unexpired reset lock does not join at all, and a
document is read again every :data:`~snodra._timings.Timings.processor_control_poll` seconds, so a pause made through
another instance (or through the hub, which wrote that instance's document) reaches this one too.

The status (:meth:`statuses`) is built from memory and never waits: it calls each consumer's non-blocking status and
asks a single background task to refresh the dead-letter counts (every
:data:`~snodra._timings.Timings.dead_letter_sample` seconds, and after a park) so a store read never delays a hub frame.

Pause and resume are persisted before they take effect here: the document is the source of truth, and an instance that
missed a control request converges on its next poll.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor status", "Processor control").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from snodra._codec import Codec
from snodra._consumer import EventConsumer
from snodra._deadletters import COLLECTION, EVENT_STORE, DeadLetters
from snodra._errors import ConfigurationError
from snodra._events import EventRegistry
from snodra._eventstore import Capability, EventStore
from snodra._hubpb import hub_pb2
from snodra._reset import ResetMachine
from snodra._settings import Settings
from snodra._store import MessageStore, ProcessorControlState, Unsupported
from snodra._timings import Timings

log = logging.getLogger(__name__)

LOCAL: str = "local"
"""``paused_by`` of a pause made through this instance's own API (the control id of a hub request otherwise)."""


class Processors:
    """The event processors this instance runs, their consumers, status and control documents."""

    def __init__(
        self,
        *,
        service: str,
        store: MessageStore,
        events: EventStore,
        registry: EventRegistry,
        codec: Codec,
        settings: Settings,
        timings: Timings,
        clock: Callable[[], int],
    ) -> None:
        self._store = store
        self._clock = clock
        self._settings = settings
        self._timings = timings
        self._names: tuple[str, ...] = tuple(registry.processors())
        mode = settings.dead_letter_mode
        if mode == EVENT_STORE and not events.supports(Capability.NATIVE_DEAD_LETTER):
            raise ConfigurationError(
                "dead_letter_mode (SNODRA_EVENTS_DEAD_LETTER_MODE) is event-store, but the event store "
                f"{events.name} does not declare the capability {Capability.NATIVE_DEAD_LETTER.value}: configure "
                "collection or log, or an event store that keeps its own dead letters"
            )
        self._letters = DeadLetters(
            mode,
            store if mode == COLLECTION else None,
            registry,
            codec,
            set(self._names),
            clock,
        )
        self._consumers: dict[str, EventConsumer] = {
            name: EventConsumer(
                service=service,
                processor=name,
                store=store,
                events=events,
                registry=registry,
                source=settings.events_source,
                codec=codec,
                max_attempts=settings.consumer_max_attempts,
                backoff=timings.event_handler_backoff,
                resubscribe_min=timings.event_resubscribe_min,
                resubscribe_max=timings.event_resubscribe_max,
                resubscribe_stable=timings.event_resubscribe_stable,
                dead_letter_mode=mode,
                dead_letter_retention_ms=int(settings.dead_letter_retention * 1000),
                dead_letter_publish_timeout=timings.dead_letter_publish_timeout,
                clock=clock,
            )
            for name in self._names
        }
        self._versions: dict[str, int] = {name: 0 for name in self._names}
        self._unsupported: set[str] = set()  # processors whose message store has no processor_control
        self.reset = ResetMachine(self, store, events, registry, clock)
        self._consumer_tasks: dict[str, asyncio.Task[None]] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._poll_interval = timings.processor_control_poll
        self._sample_interval_ms = int(timings.dead_letter_sample * 1000)
        self._last_sample_ms = 0
        self._sample_wake = asyncio.Event()
        self._running = False
        self._polls = 0  # completed poll passes (the tests use it to wait for a poll without sleeping)

    @property
    def names(self) -> tuple[str, ...]:
        """The processors this instance runs, ``default`` first, then the named ones."""
        return self._names

    @property
    def dead_letters(self) -> DeadLetters:
        """The admin side of the dead letters (mode ``collection``)."""
        return self._letters

    def consumer(self, processor: str) -> EventConsumer:
        """The consumer of ``processor``."""
        return self._consumers[processor]

    def runs(self, processor: str) -> bool:
        """Whether this instance runs ``processor``."""
        return processor in self._consumers

    def bump(self, processor: str) -> None:
        """Counts a change made here: a control document read before it must not be applied over it."""
        self._versions[processor] += 1

    async def start(self) -> None:
        """Reads the control documents, starts the processors they let run, then polls and samples."""
        self._running = True
        try:
            await self._read_and_apply(initial=True)
        except Exception:
            log.warning(
                "could not start the event processors as their control documents say; a poll retries", exc_info=True
            )
        self._tasks.append(asyncio.create_task(self._poll(), name="snodra:processors:control"))
        self._tasks.append(asyncio.create_task(self._sample(), name="snodra:processors:sample"))

    def tasks(self) -> list[asyncio.Task[None]]:
        """The manager's task and every running consumer's."""
        return [*self._tasks, *self._consumer_tasks.values()]

    async def stop(self) -> None:
        """Stops polling and sampling; the app cancels :meth:`tasks` too."""
        self._running = False
        self._sample_wake.set()

    # ---- pause and resume ----------------------------------------------------------------------

    async def pause(self, processor: str) -> None:
        """Pauses ``processor`` on every instance (its document says so), paused by ``local``."""
        await self.set_paused(processor, True, LOCAL)

    async def resume(self, processor: str) -> None:
        """Resumes ``processor`` on every instance."""
        await self.set_paused(processor, False, LOCAL)

    async def set_paused(self, processor: str, paused: bool, by: str) -> None:
        """Persists the pause (``by`` a control id or ``local``) and applies it here now."""
        consumer = self._consumers[processor]
        # The write, the count and the consumer change run without an await between them: a poll that read the old
        # document before the write applies it before this change, which then wins; one that read after the write sees
        # the new document or skips on the count.
        await self._store.control.set_paused(processor, paused, by, self._clock())
        self._versions[processor] += 1
        if paused != consumer.paused:
            consumer.pause() if paused else consumer.resume()
            log.info("Processor %s %s (%s)", processor, "paused" if paused else "resumed", by)

    # ---- status --------------------------------------------------------------------------------

    def statuses(self) -> list[hub_pb2.ProcessorStatus]:
        """One ``ProcessorStatus`` per started processor, built from memory; never blocks."""
        views = [
            consumer.status() for consumer in self._consumers.values() if consumer.was_started or consumer.operation_id
        ]
        self._kick_sample()
        return views

    def _kick_sample(self) -> None:
        """Asks the sampler to refresh what needs the store, without waiting for it."""
        if not self._running:
            return
        now = self._clock()
        if now - self._last_sample_ms >= self._sample_interval_ms or any(
            consumer.parked_dirty for consumer in self._consumers.values()
        ):
            self._sample_wake.set()

    async def _sample(self) -> None:
        """Samples the dead-letter counts every ``dead_letter_sample`` seconds and after a park."""
        while True:
            await self._sample_wake.wait()
            self._sample_wake.clear()
            if not self._running:
                return
            now = self._clock()
            force = now - self._last_sample_ms >= self._sample_interval_ms
            for consumer in self._consumers.values():
                if not force and not consumer.parked_dirty:
                    continue
                try:
                    await consumer.sample_dead_letters(force)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    log.debug("could not count the dead letters of processor %s: %s", consumer.processor, error)
            if force:
                self._last_sample_ms = now

    # ---- the control documents -----------------------------------------------------------------

    async def _poll(self) -> None:
        """Reads the control documents every poll interval and applies what changed."""
        while True:
            await asyncio.sleep(self._poll_interval)
            if not self._running:
                return
            self._polls += 1
            try:
                await self._read_and_apply(initial=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning(
                    "could not apply the processor control documents; keeping the processors as they are", exc_info=True
                )

    async def _read_and_apply(self, *, initial: bool) -> None:
        """Reads every processor's document, then applies those no change of this instance overtook meanwhile.

        A message store that does not implement ``processor_control`` (``Unsupported``) leaves its processor running
        unpaused and is remembered so it is not asked again: a level-2 store keeps its processors, never a missing
        document silently stopping consumption.
        """
        versions = dict(self._versions)
        documents: dict[str, ProcessorControlState] = {}
        unsupported: list[str] = []
        try:
            for name in self._names:
                if name in self._unsupported:
                    continue
                try:
                    documents[name] = await self._store.control.get(name)
                except Unsupported:
                    # TODO(spec-gap): SG-44
                    unsupported.append(name)
        except asyncio.CancelledError:
            raise
        except Exception:
            if initial:
                log.warning(
                    "could not read the processor control documents: no event processor starts until a poll reads them"
                )
            else:
                log.warning("could not read the processor control documents, keeping the processors as they are")
            return
        for name in unsupported:
            self._unsupported.add(name)
            log.info(
                "Processor %s: the message store %s does not implement processor_control; it runs unpaused and is not "
                "polled for a document",
                name,
                self._store.name,
            )
            await self.set_running(name, True, initial=initial)
        for name in self._names:
            if name in self._unsupported or name not in documents:
                continue
            if self._versions[name] != versions[name]:
                continue  # read before a change made here: it would undo it
            try:
                await self._apply(name, documents[name], initial=initial)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("could not apply the control document of processor %s", name, exc_info=True)

    async def _apply(self, processor: str, document: ProcessorControlState, *, initial: bool) -> None:
        """Applies one document: the pause, the lock that keeps the processor out of its subscription, its ranges."""
        consumer = self._consumers[processor]
        now = self._clock()
        if document.paused != consumer.paused:
            consumer.pause() if document.paused else consumer.resume()
            log.info("Processor %s %s (processor_control)", processor, "paused" if document.paused else "resumed")
        held = await self.reset.converge_held(processor, document, now)
        operation = document.operation
        foreign = operation if operation is not None and operation.holds_at(now) and operation.id != held else None
        consumer.operation_id = held or ("" if foreign is None else foreign.id)
        consumer.set_replay(document.replay)
        await self.set_running(
            processor,
            held is None and foreign is None,
            initial=initial,
            why=None if foreign is None else f"reset operation {foreign.id} holds its lock",
        )
        await self._remove_passed(processor, now)

    async def set_running(self, processor: str, run: bool, *, initial: bool = False, why: str | None = None) -> None:
        """Starts or stops one consumer, as the documents say (a stopped one stays in no subscription)."""
        task = self._consumer_tasks.get(processor)
        alive = task is not None and not task.done()
        if run and not alive:
            self._consumer_tasks[processor] = self._consumers[processor].start()
        elif not run and alive and task is not None:
            log.info("Processor %s stops: %s", processor, why or "consuming is paused")
            consumer = self._consumers[processor]
            consumer.request_stop()
            if consumer.handling:
                # Finish the record in hand, acknowledge it and leave the subscription (the base's stop joins the
                # reader); a handler that outlives the drain is cancelled, its transaction rolled back.
                # TODO(spec-gap): SG-46
                try:
                    async with asyncio.timeout(self._timings.handler_drain):
                        await task
                except TimeoutError:
                    log.warning(
                        "Processor %s did not leave within %.2f s: cancelling its record",
                        processor,
                        self._timings.handler_drain,
                    )
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            else:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            self._consumer_tasks.pop(processor, None)
        elif not run and initial:
            # Started under a held lock: it never joins (and reports STOPPED with the operation).
            log.info(
                "Processor %s stays out of its subscription: operation %s holds its lock",
                processor,
                self._consumers[processor].operation_id,
            )

    async def converge(self, processor: str, *, initial: bool = False) -> None:
        """Reads one processor's control document again and applies it, unless a change here overtook the read."""
        before = self._versions[processor]
        try:
            document = await self._store.control.get(processor)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("could not read the processor control document of %s", processor)
            return
        if self._versions[processor] != before:
            return
        await self._apply(processor, document, initial=initial)

    async def _remove_passed(self, processor: str, now: int) -> None:
        """Removes the replay ranges this instance's subscription consumed past from its control document."""
        consumer = self._consumers[processor]
        for replay_range in consumer.passed_ranges():
            try:
                removed = await self._store.control.remove_range(processor, replay_range, now)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                log.warning("could not remove the replay range of processor %s: %s", processor, error)
                continue
            if removed:
                log.info(
                    "Processor %s replayed segment %s of %s up to %s",
                    processor,
                    replay_range.segment,
                    replay_range.source,
                    replay_range.until,
                )
            consumer.set_replay(tuple(item for item in consumer.replay_ranges if item != replay_range))
