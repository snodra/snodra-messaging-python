"""The outbox relay: pending outbox entries to the event store, in order, at least once.

Every instance runs one relay unless configured off. A poll claims the oldest pending entries under a lease held by this
relay, sends them one after the other, and marks each sent once the event store accepted it:

- **Order per key.** A retriable failure ends the batch, so the entry stays this relay's and the next claim retries it
  before the later ones. Two relays' batches are not ordered against each other, and a service that needs the order per
  key of what one instance publishes runs one relay (``SNODRA_OUTBOX_ENABLED`` off on the others).
- **Rejected against failed.** An event the event store rejects for good (``Rejected``, or ``Invalid``) is given up at
  once; any other failure counts an attempt and is tried again at the next poll, up to ``max_attempts``.
- **Lease.** A batch stops before its lease could run out under a send (:data:`OUTBOX_SEND_TIMEOUT` before its end):
  the next poll claims again, which renews the lease of what this relay still holds.
- **Crash-safe.** The relay publishes, then marks sent. A crash between the two leaves the entry pending; after the
  lease another instance claims it and publishes it again, and consumers skip the duplicate by its processed-event
  marker. Nothing is lost.
- **Woken or polling.** A committed append may wake the relay through :meth:`OutboxStore.on_append`; otherwise it polls
  every ``poll_interval``. A full batch is followed by the next poll at once.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-2-events (Relay) and
https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-rule.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from snodra._events import outgoing_event
from snodra._eventstore import EventStore, Failed, Invalid, Rejected
from snodra._ids import new_id
from snodra._store import OutboxStore, now_ms

log = logging.getLogger(__name__)


def _cancelling() -> bool:
    """Whether the current task is being cancelled (a ``CancelledError`` from elsewhere is a failed attempt)."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


OUTBOX_SEND_TIMEOUT: Final = 40.0
"""The longest one publish to the event store may take; one still unanswered then counts as a failed attempt.

The specification is silent on it (SG-19); this library follows the Rust reference (Kotlin waits 45 s) and refuses a
lease no longer than it.
"""

# TODO(spec-gap): SG-19


@dataclass(frozen=True, slots=True)
class OutboxSettings:
    """What a relay publishes, where and how (``Settings``' ``SNODRA_EVENTS_*`` and ``SNODRA_OUTBOX_*``)."""

    source: str
    """The event store's source the events go to."""
    service: str
    """The publishing service's name: every event's ``source`` header."""
    poll_interval: float
    """The longest wait between two claims."""
    batch_size: int
    """The most entries one claim returns."""
    max_attempts: int
    """Sends of one entry in all before it is given up."""
    lease: float
    """How long a claimed batch stays this relay's; longer than :data:`OUTBOX_SEND_TIMEOUT`."""


class OutboxRelay:
    """The relay of one service's outbox to its event store."""

    def __init__(
        self,
        outbox: OutboxStore,
        events: EventStore,
        settings: OutboxSettings,
        *,
        send_timeout: float = OUTBOX_SEND_TIMEOUT,
        clock: Callable[[], int] | None = None,
        owner: str | None = None,
    ) -> None:
        if settings.batch_size < 1:
            raise ValueError(f"the outbox batch size must be at least 1, got {settings.batch_size}")
        if settings.max_attempts < 1:
            raise ValueError(f"the outbox attempts must be at least 1, got {settings.max_attempts}")
        if settings.poll_interval <= 0:
            raise ValueError(f"the outbox poll interval must be positive, got {settings.poll_interval}")
        if send_timeout <= 0:
            raise ValueError(f"the outbox send timeout must be positive, got {send_timeout}")
        if settings.lease <= send_timeout:
            raise ValueError(f"the outbox lease ({settings.lease:g} s) must be longer than a send ({send_timeout:g} s)")
        self._outbox = outbox
        self._events = events
        self._settings = settings
        self._send_timeout = send_timeout
        self._clock = now_ms if clock is None else clock
        self._owner = new_id() if owner is None else owner

    @property
    def owner(self) -> str:
        """This relay's claim id."""
        return self._owner

    def start(self) -> asyncio.Task[None]:
        """Runs the relay until the task is cancelled."""
        return asyncio.create_task(self.run(), name="snodra:outbox")

    async def run(self) -> None:
        """Polls until cancelled; a committed append wakes it, so a poll may come sooner."""
        wake = asyncio.Event()
        loop = asyncio.get_running_loop()

        def woken() -> None:
            # Threadsafe: an add-on's driver may report the append from a worker thread.
            loop.call_soon_threadsafe(wake.set)

        listener = None
        try:
            try:
                listener = self._outbox.on_append(woken)
            except Exception as error:  # a store that cannot tell: the relay polls
                log.debug("the outbox cannot wake the relay, polling instead: %s", error)
            while True:
                try:
                    sent = await self.relay_once()
                except asyncio.CancelledError as error:
                    if _cancelling():
                        raise
                    log.warning("the outbox relay poll failed: %s", error)
                    sent = 0
                except Exception as error:
                    log.warning("the outbox relay poll failed: %s", error)
                    sent = 0
                if sent >= self._settings.batch_size:
                    continue
                try:
                    async with asyncio.timeout(self._settings.poll_interval):
                        await wake.wait()
                except TimeoutError:
                    pass
                finally:
                    wake.clear()
        finally:
            if listener is not None:
                listener.close()

    async def relay_once(self) -> int:
        """Claims one batch and sends it in order; how many were sent.

        A retriable failure counts the attempt and ends the batch; a rejected one gives the entry up and moves on; an
        entry failing ``max_attempts`` times is given up and skipped from then on. The batch stops before its lease
        could run out under the next send.
        """
        claimed_at = self._clock()
        batch = await self._outbox.claim(self._owner, self._settings.lease, self._settings.batch_size)
        stop_at = claimed_at + int((self._settings.lease - self._send_timeout) * 1000)
        sent = 0
        for entry in batch:
            if self._clock() >= stop_at:
                log.debug("the outbox batch stops before its lease could run out under a send")
                break
            event = outgoing_event(entry, source=self._settings.source, service=self._settings.service)
            failure: BaseException | None = None
            try:
                async with asyncio.timeout(self._send_timeout):
                    await self._events.publish(event)
            except asyncio.CancelledError as error:
                if _cancelling():
                    raise
                failure = error
            except TimeoutError:
                failure = Failed(f"no answer from the event store within {self._send_timeout:g} s")
            except Exception as error:
                failure = error
            if failure is None:
                await self._outbox.mark_sent([entry.id])
                sent += 1
                continue
            attempt = entry.attempts + 1
            if isinstance(failure, Rejected | Invalid) or attempt >= self._settings.max_attempts:
                await self._outbox.mark_failed(entry.id, _describe(failure))
                log.error(
                    "the outbox entry %s of type %s was given up after %d attempt(s): %s",
                    entry.id,
                    entry.type_name,
                    attempt,
                    _describe(failure),
                )
                continue
            await self._outbox.mark_attempt_failed(entry.id, _describe(failure))
            log.warning(
                "the outbox entry %s of type %s failed on attempt %d and will be retried: %s",
                entry.id,
                entry.type_name,
                attempt,
                _describe(failure),
            )
            break
        return sent


def _describe(failure: BaseException) -> str:
    return str(failure) or type(failure).__name__
