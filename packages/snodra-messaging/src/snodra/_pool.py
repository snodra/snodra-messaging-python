"""The hubs of an instance: one stream per hub, every one registering the identical ``Register``, the routing of
requests over them, the split of the permits, the instance's status, the resolution of a ``dns:///`` target and the
rollover of a renewed client certificate across them.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Several hubs", "Instance status").
"""

from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Awaitable, Callable, Collection, Sequence
from typing import TYPE_CHECKING, Final

from snodra._connection import _TYPE_LISTS, HubConnection, Rejection, _Stream, settle
from snodra._credentials import HubCredentials
from snodra._dispatch import Dispatcher
from snodra._hubpb import hub_pb2
from snodra._meta import client_version
from snodra._rendezvous import rank
from snodra._settings import Settings
from snodra._targets import one_family, parse_targets, share_of
from snodra._timings import Timings
from snodra._tls import Certificates

if TYPE_CHECKING:
    from snodra._telemetry import Telemetry

log = logging.getLogger(__name__)

_DRAIN_POLL: Final = 0.05
"""How often a drained address looks whether anything of the instance is still in flight on it."""


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
    """The connections of one instance and what they know together: the acknowledged hubs, the hub ids they hold, the
    permit split, the latest refusal and the status every stream reports.

    One stream per hub id: the first target to acknowledge an id owns it while it is kept (live or reconnecting); a
    later target answering the same id is closed and parked, and connected again once no kept target owns that id.
    """

    def __init__(
        self,
        settings: Settings,
        timings: Timings,
        dispatcher: Dispatcher | None = None,
        on_answer: Callable[[_Stream, hub_pb2.ServerMessage], None] | None = None,
        credentials: HubCredentials | None = None,
        certificates: Certificates | None = None,
        telemetry: Telemetry | None = None,
        on_cert_presented: Callable[[str], None] | None = None,
        on_control: Callable[[hub_pb2.ControlRequest, Callable[[hub_pb2.ClientMessage], bool]], None] | None = None,
        processors: Callable[[], Sequence[hub_pb2.ProcessorStatus]] | None = None,
    ) -> None:
        self._timings = timings
        self._dispatcher = dispatcher
        self._credentials = credentials
        self._certificates = certificates
        self._telemetry = telemetry
        self._on_cert_presented = on_cert_presented
        self._on_answer = on_answer
        self._on_control = on_control
        self._processors = processors
        self._backoff = settings.reconnect_backoff
        self._tls = settings.tls
        self._max_concurrent = settings.max_concurrent
        self._register = build_register(settings)
        self._targets = parse_targets(settings.hubs)
        self._dns = next((t for t in self._targets if t.dns), None)
        self._active: list[HubConnection] = []  # kept and in the routing, split and status
        self._draining: dict[HubConnection, asyncio.Task[None]] = {}  # left the DNS answer: out, closed once idle
        self._parked: dict[str, str] = {}  # address -> the hub id it answered: a duplicate, closed until unowned
        self._retired: list[HubConnection] = []  # stopped (a duplicate, a drained address): their tasks are awaited
        self._owners: dict[str, HubConnection] = {}  # hub id -> the kept target that acknowledged it first
        self._resolved: list[str] = []  # the latest DNS answer applied
        self._turn = 0  # the round-robin turn of unkeyed requests, shared by every call
        self._watch: asyncio.Task[None] | None = None
        self._status_task: asyncio.Task[None] | None = None
        self._dns_task: asyncio.Task[None] | None = None
        self._acknowledged = asyncio.Event()
        self._change = asyncio.Event()  # set, and replaced, whenever the acknowledged hubs change
        self.rejection: Rejection | None = None
        """The latest refusal by any hub; never cleared."""
        self.leaving = False
        """The app's shutdown drained its handlers: nothing is sent any more."""
        self._departing = False
        """The app's shutdown began: no status report, no DNS answer and no revival any more."""

    @property
    def connections(self) -> list[HubConnection]:
        """The kept connections (live or reconnecting), the ones whose address left the DNS answer included; duplicates
        and stopped ones excluded."""
        return list(self._active) + list(self._draining)

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
        if self._dns is None:
            self._add([t.address for t in self._targets])
        else:
            self._dns_task = asyncio.create_task(self._resolve_loop(), name="snodra:hubs:dns")
        self._status_task = asyncio.create_task(self._report_loop(), name="snodra:hubs:status")
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

    # Membership: one stream per hub id, parked duplicates, the DNS answer.

    def _add(self, addresses: Sequence[str]) -> None:
        """A connection per address, all of them counted before any opens a stream (each then gets its share)."""
        created = [self._new_connection(address) for address in addresses]
        self._active.extend(created)
        for connection in created:
            connection.start()

    def _new_connection(self, address: str) -> HubConnection:
        dispatcher = self._dispatcher
        name = None if self._dns is None else self._dns.name
        return HubConnection(
            address,
            self._register,
            backoff=self._backoff,
            timings=self._timings,
            on_change=self._changed,
            on_rejection=self._rejected,
            on_request=None if dispatcher is None else dispatcher.on_request,
            on_cancel=None if dispatcher is None else dispatcher.on_cancel,
            on_flow=None if dispatcher is None else dispatcher.on_flow,
            on_answer=self._on_answer,
            on_lost=None if dispatcher is None else dispatcher.on_stream_lost,
            credentials=self._credentials,
            tls=self._tls,
            certificates=self._certificates,
            permits=self._share_for,
            server_name=name,
            telemetry=self._telemetry,
            on_cert_presented=self._on_cert_presented,
            on_control=None if self._on_control is None else self._control_frame,
        )

    def _control_frame(self, stream: _Stream, request: hub_pb2.ControlRequest) -> None:
        """A control request arrives: hand it to the control plane with the stream's own write, never the read loop."""
        assert self._on_control is not None
        self._on_control(request, stream.put)

    def _changed(self) -> None:
        """A stream was acknowledged or ended, or a rollover began or ended: dedupe, revive a parked address, raise the
        permit shares and wake the calls waiting for a hub; the status loop reports the change at once, once."""
        self._dedupe()
        if self.hub_ids():
            self._acknowledged.set()
        self._top_up()
        self._signal_change()

    def _dedupe(self) -> None:
        """One owner per hub id: the first target to acknowledge it keeps it while it is kept; a later one is parked."""
        for connection in list(self._active):
            hub = connection.hub_id
            if hub is None:
                continue
            owner = self._owners.get(hub)
            if owner is not None and owner is not connection and owner in self._active:
                self._park(connection, hub, owner)
                continue
            for other in [i for i, o in self._owners.items() if o is connection and i != hub]:
                del self._owners[other]  # it answered another id: it gives up the one it owned
            self._owners[hub] = connection
        for hub, owner in list(self._owners.items()):
            if owner not in self._active:
                del self._owners[hub]  # a drained or stopped owner keeps nothing
        self._revive()

    def _park(self, connection: HubConnection, hub: str, owner: HubConnection) -> None:
        """A target that answered a hub id another kept target owns: closed and parked, its Register having replaced the
        owner's session (the owner reconnects once)."""
        if connection in self._active:
            self._active.remove(connection)
        self._parked[connection.address] = hub
        self._retired.append(connection)
        log.warning(
            "Hub targets %s and %s answer the same hub id %s: one hub; keeping %s, closing %s",
            owner.address,
            connection.address,
            hub,
            owner.address,
            connection.address,
        )
        connection.stop_reconnecting()
        connection.half_close()

    def _revive(self) -> None:
        """A parked address whose hub id no kept target owns is connected again (when its address is still resolved)."""
        if self._departing or self.leaving:
            return
        for address, hub in list(self._parked.items()):
            if hub in self._owners:
                continue
            del self._parked[address]
            if address not in self._resolved and self._dns is not None:
                continue  # its address left the DNS answer: dropped
            log.info("The hub this address (%s) was parked behind is gone; connecting it again", address)
            self._add([address])

    def _hub_id(self, connection: HubConnection) -> str | None:
        """The hub id a connection holds: its current ack's (its address when empty), else, while it reconnects, the id
        it owned."""
        if (hub := connection.hub_id) is not None:
            return hub
        return next((i for i, o in self._owners.items() if o is connection), None)

    def _live(self) -> list[HubConnection]:
        """The kept targets whose stream is acknowledged and taking frames, in target order."""
        return [c for c in self._active if c.live_stream() is not None]

    def _routable(self) -> list[HubConnection]:
        """The live hubs a request may go to now: those not being rolled over, or, when every live hub is, all of
        them."""
        live = self._live()
        steady = [c for c in live if not c.rolling]
        return steady or live

    # Routing.

    def pick(self, routing_key: str = "", exclude: Collection[HubConnection] = ()) -> HubConnection | None:
        """The hub a request goes to now, among the live ones not in ``exclude``: the highest rendezvous score for a
        key, else ``candidates[turn % len(candidates)]`` over the live untried hubs, with one turn shared by every
        call. A hub being rolled over is used only when no other is live. None when there is none."""
        candidates = [c for c in self._routable() if c not in exclude]
        if not candidates:
            return None
        # TODO(spec-gap): SG-5
        if routing_key:
            by_id: dict[str, HubConnection] = {}
            for c in candidates:
                if (hub := self._hub_id(c)) is not None:
                    by_id[hub] = c
            if not by_id:  # pragma: no cover (a live stream is acknowledged)
                return None
            return by_id[rank(list(by_id), routing_key)[0]]
        chosen = candidates[self._turn % len(candidates)]
        self._turn += 1
        return chosen

    def first_ack(self) -> hub_pb2.RegisterAck | None:
        """The ``RegisterAck`` of the first live hub (the hubs of a deployment are configured alike); None when no
        hub is live."""
        for c in self._live():
            if (s := c.live_stream()) is not None:
                return s.ack
        return None

    def change_signal(self) -> asyncio.Event:
        """Set at the next change of the acknowledged hubs (a stream acknowledged or ended) or when the app starts
        shutting down. Taken before looking at the hubs, it misses no change after that look."""
        return self._change

    def hub_ids(self) -> list[str]:
        """``RegisterAck.hub_id`` of every hub this instance holds a live stream to (its address when empty), one per
        hub id, sorted."""
        return sorted({hub for c in self._live() if not c.rolling and (hub := self._hub_id(c)) is not None})

    async def ready(self) -> None:
        """Returns once a hub has acknowledged a stream."""
        while not self.hub_ids():
            self._acknowledged.clear()
            await self._acknowledged.wait()

    # The permit split.

    def _share_for(self, connection: HubConnection) -> int:
        """The ``initial_permits`` of a stream connection opens: ``ceil(max / N)``, itself included."""
        return share_of(self._max_concurrent, self._hub_count(connection))

    def _hub_count(self, including: HubConnection | None) -> int:
        """N of the permit split: the kept hubs live, still on their first connect attempt or being rolled over, and
        including (its own stream, even before it is kept); a parked one does not count, a first attempt that failed
        counts as gone."""
        n = 1 if including is not None else 0
        for c in self._active:
            if c is including:
                continue
            if c.rolling or c.first_attempt or c.live_stream() is not None:
                n += 1
        return max(1, n)

    def _top_up(self) -> None:
        """Every live stream is raised to the current share (permits are never revoked; a stream that offered that much
        gets nothing)."""
        hubs = self._hub_count(None)
        share = share_of(self._max_concurrent, hubs)
        for connection in self._active:
            if connection.rolling:
                continue
            if (added := connection.top_up(share)) > 0:
                log.debug(
                    "Hub %s: %d more permits (share %d of %d over %d hubs)",
                    connection.address,
                    added,
                    share,
                    self._max_concurrent,
                    hubs,
                )

    # Instance status.

    async def _report_loop(self) -> None:
        """An ``InstanceStatus`` on every stream at least every ``status_interval``; a change of the acknowledged hubs
        reports at once."""
        while True:
            changed = self.change_signal()
            try:
                async with asyncio.timeout(self._timings.status_interval):
                    await changed.wait()
            except TimeoutError:
                pass
            if self.leaving:
                return
            self._report_status()

    def _report_status(self) -> None:
        """One ``InstanceStatus`` on every live stream: the hub ids it holds a stream to, the handlers in flight over
        every hub, and one status per started event processor (none when this library runs none)."""
        if self._departing:
            return  # the shutdown: the empty Register, the subscriptions' ends and the half-close only
        live = self._live()
        if not live:
            return
        steady = [c for c in live if not c.rolling]
        reported = steady or live
        ids: list[str] = []
        for c in reported:
            if (hub := self._hub_id(c)) is not None and hub not in ids:
                ids.append(hub)
        handlers = 0 if self._dispatcher is None else self._dispatcher.handlers_in_flight
        processors: Sequence[hub_pb2.ProcessorStatus] = ()
        if self._processors is not None:
            try:
                processors = self._processors()
            except Exception:  # a status that cannot be built is no reason to stop reporting the instance
                log.exception("the processor status could not be built; reporting none this time")
        for c in reported:
            stream = c.live_stream()
            if stream is None:  # pragma: no cover (reported hubs are live)
                continue
            status = hub_pb2.InstanceStatus(connected_hub_ids=ids, handlers_in_flight=handlers, processors=processors)
            stream.put(hub_pb2.ClientMessage(status=status))

    # The DNS answer.

    async def _resolve_loop(self) -> None:
        assert self._dns is not None
        assert self._dns.name is not None
        while True:
            addresses = await self._resolve()
            if self.leaving:
                return
            if addresses:
                self._apply(addresses)
            elif not self._resolved:
                log.warning("The hub name %s resolved to no address; connecting nothing until it does", self._dns.name)
            else:
                log.warning("The hub name %s resolved to no address; keeping the current hubs", self._dns.name)
            await asyncio.sleep(self._timings.dns_refresh)

    async def _resolve(self) -> list[str]:
        """The addresses of the ``dns:///`` target, one family, in the resolver's order; [] for a failed answer."""
        dns = self._dns
        assert dns is not None
        assert dns.name is not None
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(dns.name, dns.port, type=socket.SOCK_STREAM)
        except OSError as e:
            log.warning("The hub name %s could not be resolved (%s); keeping the current hubs", dns.name, e)
            return []
        return one_family([str(info[4][0]) for info in infos], dns.port)

    def _apply(self, resolved: list[str]) -> None:
        """The latest DNS answer: a new address gets a connection, a vanished one leaves the routing at once (an empty
        ``Register``, its stream closed once nothing of the instance is in flight on it), one resolved again is kept."""
        if self.leaving:
            return
        self._resolved = list(resolved)
        for address in list(self._parked):
            if address not in self._resolved:
                del self._parked[address]  # a parked address that left the answer is dropped
        for connection in list(self._active):
            if connection.address not in self._resolved:
                self._start_drain(connection)
        for connection in list(self._draining):
            if connection.address in self._resolved:
                self._revive_draining(connection)
        known = {c.address for c in self._active} | set(self._draining)
        self._add([a for a in self._resolved if a not in known])
        self._changed()

    def _start_drain(self, connection: HubConnection) -> None:
        if connection in self._draining:
            return
        if connection in self._active:
            self._active.remove(connection)
        connection.withdraw()  # the hub stops routing to this instance there
        if connection._stream is not None:  # an unacknowledged stream withdraws the types too
            connection.reregister(empty=True)
        log.info("Hub address %s is no longer resolved; closing it once idle", connection.address)
        self._draining[connection] = asyncio.create_task(
            self._drain(connection), name=f"snodra:drain:{connection.address}"
        )

    async def _drain(self, connection: HubConnection) -> None:
        """A connection whose address left the DNS answer: waited on at most ``dns_drain_limit`` for nothing of the
        instance to be in flight on it, then half-closed and stopped."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timings.dns_drain_limit
        # No event tells when nothing is in flight any more: the poll is the drain's own.
        while not self._idle(connection) and loop.time() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(_DRAIN_POLL)
        if self._draining.get(connection) is not asyncio.current_task():
            return  # resolved again meanwhile
        connection.stop_reconnecting()
        connection.half_close()
        try:
            async with asyncio.timeout(self._timings.close_wait):
                await connection.wait_ended()
        except TimeoutError:
            log.debug("The hub stream to %s did not end after the half-close of a drained address", connection.address)
        self._draining.pop(connection, None)
        self._retired.append(connection)
        log.info("The hub connection to %s is closed (its address left the DNS answer)", connection.address)
        self._changed()

    def _revive_draining(self, connection: HubConnection) -> None:
        """A draining address that is back in the DNS answer: kept, its declared types registered again. A connection
        whose drain already stopped it opens no stream ever again: the address is connected afresh instead."""
        task = self._draining.pop(connection, None)
        if task is not None:
            task.cancel()
        if connection.stopping:
            self._retired.append(connection)  # its task is awaited at the close with the other stopped ones
            self._add([connection.address])
            log.info(
                "Hub address %s is resolved again; its drained connection had stopped, connecting it afresh",
                connection.address,
            )
            return
        connection.restore()
        self._active.append(connection)
        if connection.live_stream() is not None:
            connection.reregister(empty=False)
        log.info("Hub address %s is resolved again; keeping its connection", connection.address)

    def _idle(self, connection: HubConnection) -> bool:
        """Nothing of the instance is in flight on the connection: no request waits on its stream, and none of its
        requests is being served here."""
        stream = connection.live_stream()
        if stream is None:
            return True
        return not stream.requests and (self._dispatcher is None or not self._dispatcher.serving(stream))

    # Leaving.

    async def leave(
        self,
        drain: Callable[[], Awaitable[None]] | None = None,
        end_subscriptions: Callable[[], object] | None = None,
    ) -> None:
        """Leaves every hub in order: the empty ``Register`` (its acks awaited for ``deregister_wait`` at most), then
        ``end_subscriptions`` (the subscriptions this instance served end, so subscribers hear at once), then the drain
        of the handlers in flight, then the end of sending and the half-close (the hub's end awaited for ``close_wait``
        at most). Until the half-close the handlers in flight may still send, and a stream lost meanwhile is opened
        again with the empty ``Register``, so no new work arrives on it; nothing reconnects after the half-close."""
        t = self._timings
        self._departing = True  # no status goes out from here on: the empty Register, then the ends and the half-close
        await self._stop_tasks()  # no status, no DNS answer and no drain fights the shutdown
        await self._stop_watch()  # a rollover never fights the shutdown either
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
        self._departing = True
        await self._stop_tasks()
        self._stop_sending()
        await self._stop_watch()
        every = self.connections + self._retired
        tasks = [t for c in every if (t := c.cancel()) is not None]
        await settle(tasks, log_errors=False)  # a crash is logged by its connection and raised below
        for c in every:
            if (e := c.crash) is not None:
                raise e
        self._active.clear()
        self._draining.clear()
        self._retired.clear()

    async def _stop_tasks(self) -> None:
        """Ends the status, DNS and drain tasks still running. The draining connections stay in place: leaving the hubs
        half-closes them, and closing cancels them (a drain task that ends moves its own connection out)."""
        tasks = [t for t in (self._status_task, self._dns_task, *self._draining.values()) if t is not None]
        self._status_task = self._dns_task = None
        for task in tasks:
            if not task.done():
                task.cancel()
        await settle(tasks)

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
            for c in self.connections + self._retired:
                c.stop_requests("hub connection stopped")

    def _rejected(self, rejection: Rejection) -> None:
        self.rejection = rejection
