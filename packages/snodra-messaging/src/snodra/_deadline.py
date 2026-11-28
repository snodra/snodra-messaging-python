"""The deadline scheduler: one instance claims each due deadline under a lease and fires it.

A deadline is recorded and cancelled in the saga's transaction. This scheduler claims one due deadline at a time with
``deadlines.claim_due``; :class:`~snodra._saga_runtime.SagaRuntime.apply_deadline` loads the saga, runs the handler,
writes the state and marks the deadline fired in one transaction. A failed or crashed firing leaves the claim to
expire, so a deadline fires at least once, never before it is due, and never for a cancelled deadline or an ended saga.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#deadlines.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable

from snodra._saga_runtime import SagaRuntime
from snodra._store import DeadlineDocument, DeadlineStore, now_ms
from snodra._timings import Timings

log = logging.getLogger(__name__)


class DeadlineScheduler:
    """One instance's deadline scheduler."""

    def __init__(
        self,
        *,
        deadlines: DeadlineStore,
        sagas: SagaRuntime,
        clock: Callable[[], int] = now_ms,
        timings: Timings,
    ) -> None:
        self._deadlines = deadlines
        self._sagas = sagas
        self._clock = clock
        self._timings = timings
        self._owner = f"deadline-{uuid.uuid4()}"
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @property
    def owner(self) -> str:
        return self._owner

    def start(self) -> asyncio.Task[None]:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="snodra:saga-deadlines")
        return self._task

    def stop(self) -> None:
        self._running = False

    async def run_once(self) -> bool:
        """Claims and fires at most one due deadline; whether one was claimed."""
        now = self._clock()
        claimed = await self._deadlines.claim_due(self._owner, now, now + int(self._timings.deadline_lease * 1000))
        if claimed is None:
            return False
        await self._fire(claimed)
        return True

    async def _fire(self, deadline: DeadlineDocument) -> None:
        try:
            await self._sagas.apply_deadline(deadline)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.error(
                "deadline %s of saga %s failed; its claim expires and it fires again: %s",
                deadline.id,
                deadline.saga_id,
                error,
            )

    async def _loop(self) -> None:
        while self._running:
            try:
                fired = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("the deadline scheduler pass failed; the next pass retries")
                fired = False
            if not fired:
                await asyncio.sleep(self._timings.deadline_poll)


__all__ = ["DeadlineScheduler"]
