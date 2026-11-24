"""The hubs of an instance: one connection per hub address, every one registering the identical ``Register``."""

from __future__ import annotations

import asyncio
import logging

from snodra._connection import HubConnection, Rejection, settle
from snodra._hubpb import hub_pb2
from snodra._meta import client_version
from snodra._settings import Settings, hub_targets
from snodra._timings import Timings

log = logging.getLogger(__name__)


def build_register(settings: Settings) -> hub_pb2.Register:
    """The ``Register`` every stream of the instance opens with."""
    return hub_pb2.Register(
        service_name=settings.service,
        instance_id=settings.instance,
        initial_permits=settings.max_concurrent,
        load_factor=settings.load_factor,
        tags=dict(settings.tags),
        client_version=client_version(),
    )


class Pool:
    """The connections of one instance and what they know together: the acknowledged hubs, the latest refusal."""

    def __init__(self, settings: Settings, timings: Timings) -> None:
        self._timings = timings
        register = build_register(settings)
        self.connections = [
            HubConnection(
                target,
                register,
                backoff=settings.reconnect_backoff,
                timings=timings,
                on_change=self._changed,
                on_rejection=self._rejected,
            )
            for target in hub_targets(settings.hubs)
        ]
        self.rejection: Rejection | None = None
        """The latest refusal by any hub; never cleared."""
        self._acknowledged = asyncio.Event()

    def start(self) -> None:
        for c in self.connections:
            c.start()

    def hub_ids(self) -> list[str]:
        return sorted(hub for c in self.connections if (hub := c.hub_id) is not None)

    async def ready(self) -> None:
        """Returns once a hub has acknowledged a stream."""
        while not self.hub_ids():
            self._acknowledged.clear()
            await self._acknowledged.wait()

    async def leave(self) -> None:
        """Leaves every hub in order: the empty ``Register`` (its acks awaited for ``deregister_wait`` at most), then
        the half-close (the hub's end awaited for ``close_wait`` at most). Nothing reconnects after it."""
        t = self._timings
        for c in self.connections:
            c.stop_reconnecting()
        acks = [f for c in self.connections if (f := c.deregister()) is not None]
        if acks:
            try:
                async with asyncio.timeout(t.deregister_wait):
                    for f in acks:
                        await f
            except TimeoutError:
                log.debug("Not every hub acknowledged the empty Register within %.2f s", t.deregister_wait)
        for c in self.connections:
            c.half_close()
        try:
            async with asyncio.timeout(t.close_wait):
                for c in self.connections:
                    await c.wait_ended()
        except TimeoutError:
            log.debug("Not every hub finished its stream within %.2f s of the half-close", t.close_wait)

    async def close(self) -> None:
        """Ends every connection and its tasks, all at once; waits for them even when cancelled meanwhile (then
        re-raised), and raises the error that crashed a connection, if one did."""
        tasks = [t for c in self.connections if (t := c.cancel()) is not None]
        await settle(tasks, log_errors=False)  # a crash is logged by its connection and raised below
        for c in self.connections:
            if (e := c.crash) is not None:
                raise e

    def _changed(self) -> None:
        if self.hub_ids():
            self._acknowledged.set()

    def _rejected(self, rejection: Rejection) -> None:
        self.rejection = rejection
