"""The application: one service instance and its hub connections."""

from __future__ import annotations

import asyncio
import logging
import signal

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
        elsewhere or the cancellation of the task, then shuts down (``shutdown(15.0)``)."""
        await self.start()
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError, ValueError):  # Windows, or not the main thread
                continue
            installed.append(sig)
        try:
            await self._stop.wait()
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)
            await self.shutdown(15.0)
            await self._down.wait()  # a shutdown that another task started

    async def shutdown(self, timeout: float = 15.0) -> None:  # noqa: ASYNC109 (the API takes a timeout)
        """Leaves the hubs in order and stops every connection, within ``timeout``: a ``Register`` with no types on
        every live stream (its ack awaited for at most 1 s), then each stream half-closed and its end awaited (at most
        2 s). A second call returns at once."""
        if self._shutting_down:
            return
        self._shutting_down = True
        self._stop.set()
        try:
            if self._started:
                try:
                    async with asyncio.timeout(timeout):
                        await self._pool.leave()
                except TimeoutError:
                    log.warning("Leaving the hubs took longer than %.2f s: the streams are cut", timeout)
        finally:
            await self._pool.close()
            self._down.set()

    async def __aenter__(self) -> App:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    def _connections(self) -> list[HubConnection]:
        """The hub connections (for tests)."""
        return list(self._pool.connections)
