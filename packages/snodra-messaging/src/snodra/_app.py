"""The application: one service instance and its hub connections."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from snodra._connection import HubConnection, Rejection
from snodra._pool import Pool
from snodra._settings import Settings
from snodra._timings import DEFAULT_TIMINGS, Timings

log = logging.getLogger(__name__)


class App:
    """One service instance: its hub connections.

    ``start`` opens a connection per hub; ``shutdown`` leaves the hubs in order
    (https://snodra.damoreira.com/library-authors/protocol/, "Shutting down"). ``run`` does both around the process's
    life, and ``async with app:`` around a block.
    """

    def __init__(self, settings: Settings, *, _timings: Timings | None = None) -> None:
        self._settings = settings
        self._pool = Pool(settings, DEFAULT_TIMINGS if _timings is None else _timings)
        self._started = False
        self._shutting_down = False
        self._stop = asyncio.Event()  # a signal, or shutdown: run() stops waiting
        self._down = asyncio.Event()  # shutdown is over

    @property
    def instance_id(self) -> str:
        """The instance id this app registers (``Register.instance_id``)."""
        return self._settings.instance

    @property
    def rejection(self) -> Rejection | None:
        """The latest refusal of this instance's registration by any hub; never cleared."""
        return self._pool.rejection

    def hub_ids(self) -> list[str]:
        """``RegisterAck.hub_id`` of every acknowledged stream (its address when empty), sorted."""
        return self._pool.hub_ids()

    async def start(self) -> None:
        """Opens a connection per hub and returns at once; the connections register and reconnect on their own."""
        if self._shutting_down:
            raise RuntimeError("the app was shut down")
        if self._started:
            raise RuntimeError("the app is already started")
        self._started = True
        self._pool.start()

    async def ready(self, timeout: float | None = None) -> None:  # noqa: ASYNC109 (the API takes a timeout)
        """Returns once a hub has acknowledged this instance's registration; ``TimeoutError`` after ``timeout``."""
        async with asyncio.timeout(timeout):
            await self._pool.ready()

    async def run(self) -> None:
        """Starts, serves until SIGTERM or SIGINT (where the event loop takes signal handlers), a ``shutdown`` from
        elsewhere or the cancellation of the task, then shuts down (``shutdown(15.0)``). A cancelled ``run`` ends
        cancelled: the unexpected error that crashed a connection is then only logged (at error level, when it
        happened); after a signal it is raised, as by ``shutdown``."""
        await self.start()
        loop = asyncio.get_running_loop()
        previous: dict[signal.Signals, Any] = {}
        for sig in (signal.SIGTERM, signal.SIGINT):
            before = signal.getsignal(sig)
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError, ValueError):  # Windows, or not the main thread
                continue
            previous[sig] = before
        propagating = False
        try:
            await self._stop.wait()
        except BaseException:
            propagating = True
            raise
        finally:
            for sig, before in previous.items():
                loop.remove_signal_handler(sig)
                # The handler found is restored, the default one too; None is a handler installed outside Python,
                # which signal.signal cannot reinstall: the default stays then.
                if before is not None:
                    signal.signal(sig, before)
            await self._shutdown(15.0, raise_crash=not propagating)
            await self._down.wait()  # a shutdown that another task started

    async def shutdown(self, timeout: float = 15.0) -> None:  # noqa: ASYNC109 (the API takes a timeout)
        """Leaves the hubs in order and stops every connection, within ``timeout``: a ``Register`` with no types on
        every live stream (its ack awaited for at most 1 s), then each stream half-closed and its end awaited (at most
        2 s). A second call returns at once.

        The unexpected error that crashed a connection (a library bug, logged at error level when it happened) is
        raised here once everything is stopped, unless another exception or a cancellation is already propagating
        out of the shutdown: that one is never replaced."""
        await self._shutdown(timeout, raise_crash=True)

    async def _shutdown(self, budget: float, *, raise_crash: bool) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        self._stop.set()
        propagating = False
        try:
            if self._started:
                try:
                    async with asyncio.timeout(budget):
                        await self._pool.leave()
                except TimeoutError:
                    log.warning("Leaving the hubs took longer than %.2f s: the streams are cut", budget)
        except BaseException:
            propagating = True
            raise
        finally:
            try:
                await self._pool.close()
            except Exception:
                if raise_crash and not propagating:
                    raise
                log.debug("A connection crashed (logged when it happened); not raised over what propagates")
            finally:
                self._down.set()

    async def __aenter__(self) -> App:
        await self.start()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, *exc: object) -> None:
        """Shuts down (``shutdown(15.0)``); the unexpected error that crashed a connection is raised only when the
        block ended without an exception: the block's own exception is never replaced (the crash was logged at error
        level when it happened)."""
        await self._shutdown(15.0, raise_crash=exc_type is None)

    def _connections(self) -> list[HubConnection]:
        """The hub connections (for tests)."""
        return list(self._pool.connections)
