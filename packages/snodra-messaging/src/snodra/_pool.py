"""The hubs of an instance: one connection per hub address, every one registering the identical ``Register``, and the
rollover of a renewed client certificate across them."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Collection

from snodra._connection import _TYPE_LISTS, HubConnection, Rejection, _Stream, settle
from snodra._credentials import HubCredentials
from snodra._dispatch import Dispatcher
from snodra._hubpb import hub_pb2
from snodra._meta import client_version
from snodra._settings import Settings, hub_targets
from snodra._timings import Timings
from snodra._tls import Certificates

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

    def __init__(
        self,
        settings: Settings,
        timings: Timings,
        dispatcher: Dispatcher | None = None,
        on_answer: Callable[[_Stream, hub_pb2.ServerMessage], None] | None = None,
        credentials: HubCredentials | None = None,
        certificates: Certificates | None = None,
    ) -> None:
        self._timings = timings
        self._dispatcher = dispatcher
        self._certificates = certificates
        self._watch: asyncio.Task[None] | None = None
        register = build_register(settings)
        self._register = register
        self.connections = [
            HubConnection(
                target,
                register,
                backoff=settings.reconnect_backoff,
                timings=timings,
                on_change=self._changed,
                on_rejection=self._rejected,
                on_request=None if dispatcher is None else dispatcher.on_request,
                on_cancel=None if dispatcher is None else dispatcher.on_cancel,
                on_flow=None if dispatcher is None else dispatcher.on_flow,
                on_answer=on_answer,
                on_lost=None if dispatcher is None else dispatcher.on_stream_lost,
                credentials=credentials,
                tls=settings.tls,
                certificates=certificates,
            )
            for target in hub_targets(settings.hubs)
        ]
        self.rejection: Rejection | None = None
        """The latest refusal by any hub; never cleared."""
        self._acknowledged = asyncio.Event()
        self._change = asyncio.Event()  # set, and replaced, whenever the acknowledged hubs change
        self._turn = 0  # the round-robin turn of unkeyed requests, shared by every call
        self.leaving = False
        """The app's shutdown drained its handlers: nothing is sent any more."""

    def declare(
        self,
        command_types: list[str],
        query_types: list[str],
        shared_query_types: list[str],
        subscription_query_types: list[str],
    ) -> None:
        """The types every ``Register`` of every stream lists; before :meth:`start`."""
        r = self._register
        for field, types in (
            (r.command_types, command_types),
            (r.query_types, query_types),
            (r.shared_query_types, shared_query_types),
            (r.subscription_query_types, subscription_query_types),
        ):
            del field[:]
            field.extend(types)

    def start(self) -> None:
        for c in self.connections:
            c.start()
        if self._certificates is not None:
            self._watch = asyncio.create_task(
                self._certificates.watch(lambda: self.connections, self._roll, self._timings),
                name="snodra:certificates",
            )

    async def _roll(self, connection: HubConnection) -> bool:
        """Rolls connection over to the renewed client certificate: its stream is idle once no request it carried is
        still served here or waits for its answer."""
        dispatcher = self._dispatcher
        return await connection.roll_over(
            lambda s: not s.requests and (dispatcher is None or not dispatcher.serving(s))
        )

    async def _stop_watch(self) -> None:
        """No certificate rollover from now on (one under way stops where it is)."""
        task, self._watch = self._watch, None
        if task is not None and not task.done():
            task.cancel()
            await settle([task])

    def pick(self, routing_key: str = "", exclude: Collection[HubConnection] = ()) -> HubConnection | None:
        """The hub a request goes to now, among the live ones not in ``exclude`` (stopping connections are never
        live), a hub being rolled over only when no other is live: ``live[turn % len(live)]``, the turn shared by
        every call. None when there is none."""
        live = [c for c in self.connections if c not in exclude and c.live_stream() is not None]
        live = [c for c in live if not c.rolling] or live
        if not live:
            return None
        chosen = live[self._turn % len(live)]
        self._turn += 1
        return chosen

    def first_ack(self) -> hub_pb2.RegisterAck | None:
        """The ``RegisterAck`` of the first live hub (the hubs of a deployment are configured alike); None when no
        hub is live."""
        for c in self.connections:
            if (s := c.live_stream()) is not None:
                return s.ack
        return None

    def change_signal(self) -> asyncio.Event:
        """Set at the next change of the acknowledged hubs (a stream acknowledged or ended) or when the app starts
        shutting down. Taken before looking at the hubs, it misses no change after that look."""
        return self._change

    def hub_ids(self) -> list[str]:
        return sorted(hub for c in self.connections if (hub := c.hub_id) is not None)

    async def ready(self) -> None:
        """Returns once a hub has acknowledged a stream."""
        while not self.hub_ids():
            self._acknowledged.clear()
            await self._acknowledged.wait()

    async def leave(
        self,
        drain: Callable[[], Awaitable[None]] | None = None,
        end_subscriptions: Callable[[], object] | None = None,
    ) -> None:
        """Leaves every hub in order: the empty ``Register`` (its acks awaited for ``deregister_wait`` at most), then
        ``end_subscriptions`` (the subscriptions this instance served end, so subscribers hear at once), then the
        drain of the handlers in flight, then the end of sending and the half-close (the hub's end awaited for
        ``close_wait`` at most). Until the half-close the handlers in flight may still send, and a stream lost
        meanwhile is opened again with the empty ``Register``, so no new work arrives on it; nothing reconnects after
        the half-close."""
        t = self._timings
        await self._stop_watch()  # a rollover never fights the shutdown
        self._withdraw_types()  # every later stream registers with no types
        acks = [f for c in self.connections if (f := c.deregister()) is not None]
        if acks:
            try:
                async with asyncio.timeout(t.deregister_wait):
                    for f in acks:
                        await f
            except TimeoutError:
                log.debug("Not every hub acknowledged the empty Register within %.2f s", t.deregister_wait)
        if end_subscriptions is not None:
            end_subscriptions()
        if drain is not None:
            await drain()
        # The handlers drained may still have sent requests until here; from now on nothing is sent.
        self._stop_sending()
        for c in self.connections:
            c.stop_reconnecting()
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
        self._stop_sending()
        await self._stop_watch()
        tasks = [t for c in self.connections if (t := c.cancel()) is not None]
        await settle(tasks, log_errors=False)  # a crash is logged by its connection and raised below
        for c in self.connections:
            if (e := c.crash) is not None:
                raise e

    def _changed(self) -> None:
        if self.hub_ids():
            self._acknowledged.set()
        self._signal_change()

    def _withdraw_types(self) -> None:
        """The ``Register`` every stream opens with lists no types from now on."""
        for name in _TYPE_LISTS:
            self._register.ClearField(name)

    def _signal_change(self) -> None:
        self._change.set()
        self._change = asyncio.Event()

    def _stop_sending(self) -> None:
        """Nothing is sent from now on: the calls waiting for a hub or for room in a buffer are woken, a request still
        buffered fails as never sent and a written one as an unknown outcome ("hub connection stopped")."""
        if not self.leaving:
            self.leaving = True
            self._signal_change()
            for c in self.connections:
                c.stop_requests("hub connection stopped")

    def _rejected(self, rejection: Rejection) -> None:
        self.rejection = rejection
