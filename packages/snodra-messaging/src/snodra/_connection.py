"""The connection to one hub: its ``Hub.Connect`` streams, one after another.

A connection runs the state machine of https://snodra.damoreira.com/docs/library-authors/writing-a-library/ ("A
connection to one hub") as one task, ``snodra:connection:<address>``. Each attempt takes the instance's credential (a
stream is never opened without it), opens a channel (TLS when configured, presenting the client certificate the files
hold now) and a stream, writes the ``Register`` and a ``Heartbeat``, and runs three tasks of its own until the stream
ends: the writer (the one task that writes to the stream), the reader, and the heartbeats (which also close a stream
that gets no ``RegisterAck`` within the handshake timeout, and a silent hub's, and follow a changed credential). Then
it waits its backoff and opens the next stream.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable, Collection, Coroutine
from dataclasses import dataclass
from typing import Any, Final, cast

import grpc
import grpc.aio

from snodra import _tls
from snodra._backoff import jitter, next_wait
from snodra._credentials import HubCredentials, NoToken
from snodra._hubpb import hub_pb2, hub_pb2_grpc
from snodra._permits import PermitLedger
from snodra._timings import DEFAULT_TIMINGS, Timings

log = logging.getLogger(__name__)

REFUSALS: Final = frozenset(
    {
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
    }
)
"""The statuses with which a hub refuses a registration: retried at the backoff cap and exposed as a
:class:`Rejection` (https://snodra.damoreira.com/docs/library-authors/protocol/, "Refusals")."""

_TYPE_LISTS: Final = ("command_types", "query_types", "shared_query_types", "subscription_query_types")

CLEAN_UP_BOUND: Final = 2.0
"""After a cancellation, how long a clean-up still waits for the tasks it ends before it gives up on them."""

_LEFT_BEHIND: Final[set[asyncio.Future[Any]]] = set()
"""The tasks a clean-up gave up waiting for, held until they end."""

_ROLL_POLL: Final = 0.02
"""How often a rollover looks whether the stream it leaves is idle, and whether the new one is registered."""

CERTIFICATE_EXPIRED: Final = "certificate expired"
"""The hub's reason when it ends a certificate session at the certificate's end: an ordinary reconnect."""

_Call = grpc.aio.StreamStreamCall[hub_pb2.ClientMessage, hub_pb2.ServerMessage]


@dataclass(frozen=True)
class Rejection:
    """A hub's refusal of this instance's registration."""

    hub: str
    """The hub: its id (``RegisterAck.hub_id`` of the last stream this connection got acknowledged, the address when
    that id is empty) once a stream to it was acknowledged, else the address dialled."""
    code: str
    """The gRPC status, for example ``"ALREADY_EXISTS"``."""
    message: str
    """The hub's reason, for example ``"type orders.PlaceOrder is owned by billing-service"``."""
    at: float
    """When it arrived (seconds since the epoch)."""


@dataclass(frozen=True)
class _End:
    """Why a stream ended."""

    code: grpc.StatusCode
    message: str


class _HalfClose:
    """The writer's last item: half-close the stream."""


_HALF_CLOSE: Final = _HalfClose()


@dataclass(frozen=True)
class Lost:
    """A request's stream ended before its answer: ``written`` once the writer had taken it (the hub may have it),
    else it never left the buffer."""

    written: bool
    why: str
    """Why the stream ended, for example ``hub stream closed: UNAVAILABLE (restart)``."""


class OutboundRequest:
    """A request a caller queued on a stream: buffered until the stream's writer takes it, written from then on (handed
    to the transport). Its ``answer`` is resolved once: with the hub's ``Reply`` or ``Error`` frame, or with
    :class:`Lost` when the stream ends first."""

    keeps_frames = False
    """A request whose frames keep coming after the first answer (a stream's parts): its entry stays registered for
    them once the hub's first frame is a part, so that a frame landing before its caller resumes is not dropped."""

    def __init__(self, message: hub_pb2.ClientMessage) -> None:
        self.message = message
        self.id: str = message.request.id
        self.stream: _Stream | None = None
        """The stream it was queued on."""
        self.written = False
        self.withdrawn = False
        """Its caller gave up while it was buffered: the writer skips it."""
        self.answer: asyncio.Future[hub_pb2.ServerMessage | Lost] = asyncio.get_running_loop().create_future()

    def resolve(self, outcome: hub_pb2.ServerMessage | Lost) -> None:
        if not self.answer.done():
            self.answer.set_result(outcome)

    def take(self, m: hub_pb2.ServerMessage) -> bool:
        """A frame from the hub for this request's id: a ``Reply`` or an ``Error`` answers it; False for any other
        frame, which is no answer to a unary request."""
        if m.WhichOneof("message") not in ("reply", "error"):
            return False
        self.resolve(m)
        return True


class _Stream:
    """One ``Hub.Connect`` stream: its outbound queue, its handshake and the silent-hub watch."""

    def __init__(
        self, register: hub_pb2.Register, interval: float, permit_batch: int = DEFAULT_TIMINGS.permit_batch
    ) -> None:
        self.register = register
        self.out: asyncio.Queue[hub_pb2.ClientMessage | _HalfClose | OutboundRequest] = asyncio.Queue()
        self.writable = True
        """False once half-closed or ended: nothing more is queued."""
        self.ack: hub_pb2.RegisterAck | None = None
        """The latest ``RegisterAck``; None until the handshake."""
        self.acked = asyncio.Event()
        self.interval = interval
        """The heartbeat period: the ack's from the handshake on."""
        self.ended = asyncio.Event()
        self._registers = 0
        self._acks = 0
        self._ack_waiters: list[tuple[int, asyncio.Future[None]]] = []
        # The silent-hub watch, armed by the ack: the last sign of life (loop time) and the heartbeats written since.
        self.last_echo = 0.0
        self.unanswered = 0
        self.permits = PermitLedger(register.initial_permits, batch=permit_batch)
        """The permits the hub holds for this stream, and those to give back on it."""
        self.requests: dict[str, OutboundRequest] = {}
        """The requests queued on the stream whose callers wait for their answer, by id."""
        self.buffered = 0
        """The requests in the outbound buffer, not yet taken by the writer."""
        self.status: _End | None = None
        """The status that ended the call, as it first landed (see :meth:`HubConnection._keep_status`)."""
        self.why = "hub connection stopped"
        """Why the stream ended (set when it ends): what a request written to it fails with."""
        self.presented: hub_pb2.Credentials | None = None
        """The credentials the hub holds for the stream: its ``Register``'s, then each ``ClientMessage.credentials``."""
        self.certificate: tuple[int, _tls.Pair] | None = None
        """The client certificate the stream presents, and its generation."""
        self.rolled = False
        """Half-closed by a certificate rollover: the next stream opens at once."""
        self.cut = asyncio.Event()
        """Set to end the stream from this side (its heartbeats return ``cut_reason``)."""
        self.cut_reason = ""
        self._room: list[asyncio.Future[None]] = []
        self._last_beat_ms = 0
        self._probes: list[tuple[int, asyncio.Future[None]]] = []

    def put(self, m: hub_pb2.ClientMessage) -> bool:
        """Queues m for the writer; False when the stream takes nothing more."""
        if not self.writable:
            return False
        self.out.put_nowait(m)
        return True

    def put_request(self, request: OutboundRequest) -> bool:
        """Queues a request for the writer; False when the stream takes nothing more. The caller keeps the buffer's
        bound (:meth:`room`)."""
        if not self.writable:
            return False
        request.stream = self
        self.requests[request.id] = request
        self.out.put_nowait(request)
        self.buffered += 1
        return True

    def room(self) -> asyncio.Future[None]:
        """Done when a buffered request left the buffer, or the stream ended or stops taking requests."""
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if not self.writable:
            waiter.set_result(None)
        else:
            self._room.append(waiter)
        return waiter

    def wake_all(self) -> None:
        """Wakes every call waiting for room."""
        waiters, self._room = self._room, []
        for w in waiters:
            if not w.done():
                w.set_result(None)

    def took(self, request: OutboundRequest) -> bool:
        """The writer took the request from the buffer: True when it is to be written (now it counts as written),
        False when its caller withdrew it."""
        if request.withdrawn:
            return False
        request.written = True
        self._left_buffer()
        return True

    def forget(self, request: OutboundRequest) -> None:
        """Its caller is done with the request: still buffered, it is withdrawn (never written)."""
        if self.requests.get(request.id) is request:
            del self.requests[request.id]
        if not request.written and not request.withdrawn:
            request.withdrawn = True
            self._left_buffer()

    def regain(self, request: OutboundRequest) -> None:
        """Its caller keeps waiting for frames after the request's first answer (a stream's parts): the frames of its
        id reach it again, and a stream that ends fails it like any other request of the stream."""
        request.stream = self
        self.requests[request.id] = request

    def stop_requests(self, why: str) -> None:
        """The connection stops: every waiting request ends now. One still buffered is withdrawn (never written,
        never sent); a written one has an unknown outcome (``why``)."""
        requests, self.requests = self.requests, {}
        for r in requests.values():
            if not r.written and not r.withdrawn:
                r.withdrawn = True
                self._left_buffer()
            r.resolve(Lost(r.written, why))
        self.wake_all()

    def _left_buffer(self) -> None:
        self.buffered -= 1
        while self._room:
            w = self._room.pop(0)
            if not w.done():
                w.set_result(None)
                return

    def write_register(self, *, empty: bool) -> asyncio.Future[None]:
        """The stream's ``Register`` (with no types when empty), then a heartbeat; the future is done at the echo of
        that heartbeat (the hub read the ``Register`` before it), or when the stream ends."""
        register = self.register
        if empty:
            register = hub_pb2.Register()
            register.CopyFrom(self.register)
            for name in _TYPE_LISTS:
                register.ClearField(name)
        if self.put(hub_pb2.ClientMessage(register=register)):
            self._registers += 1
        return self._probe(self.heartbeat())

    def heartbeat(self) -> int | None:
        """A heartbeat; from the ack on, one the stream took counts as unanswered until an echo. Its ``sent_at_ms``
        (each one later than the one before), None when the stream took nothing."""
        ms = max(time.time_ns() // 1_000_000, self._last_beat_ms + 1)
        if not self.put(hub_pb2.ClientMessage(heartbeat=hub_pb2.Heartbeat(sent_at_ms=ms))):
            return None
        self._last_beat_ms = ms
        if self.ack is not None:
            self.unanswered += 1
        return ms

    def _probe(self, ms: int | None) -> asyncio.Future[None]:
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if ms is None or self.ended.is_set():
            waiter.set_result(None)
        else:
            self._probes.append((ms, waiter))
        return waiter

    def half_close(self) -> None:
        """The writer half-closes the stream after the frames queued before; nothing more is queued."""
        if self.writable:
            self.writable = False
            self.out.put_nowait(_HALF_CLOSE)

    def acknowledged(self) -> asyncio.Future[None]:
        """Done once every ``Register`` written so far is acknowledged, or the stream ended."""
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self._acks >= self._registers or self.ended.is_set():
            waiter.set_result(None)
        else:
            self._ack_waiters.append((self._registers, waiter))
        return waiter

    def on_ack(self, ack: hub_pb2.RegisterAck, now: float) -> bool:
        """Takes a ``RegisterAck``; True for the stream's first (the handshake)."""
        first = self.ack is None
        self.ack = ack
        if ack.heartbeat_interval_ms > 0:
            self.interval = ack.heartbeat_interval_ms / 1000
        self._acks += 1
        self._release(lambda n: n <= self._acks)
        if first:
            # The ack is the first sign of life: its time is taken before the heartbeat timer restarts from it.
            self.last_echo = now
            self.unanswered = 0
            self.acked.set()
        return first

    def on_echo(self, now: float, sent_at_ms: int = 0) -> None:
        """A heartbeat from the hub proves it alive; before the ack it confirms nothing. It answers every heartbeat sent
        up to its ``sent_at_ms`` (the hub reads a stream in order)."""
        if self.ack is not None:
            self.last_echo = now
            self.unanswered = 0
        keep = []
        for ms, waiter in self._probes:
            if ms > sent_at_ms:
                keep.append((ms, waiter))
            elif not waiter.done():
                waiter.set_result(None)
        self._probes = keep

    def finish(self) -> None:
        self.writable = False
        self.ended.set()
        self._release(lambda n: True)
        probes, self._probes = self._probes, []
        for _, waiter in probes:
            if not waiter.done():
                waiter.set_result(None)
        # The requests still waiting for an answer: written ones have an unknown outcome, buffered ones never left.
        requests, self.requests = self.requests, {}
        for r in requests.values():
            r.resolve(Lost(r.written, self.why))
        self.wake_all()

    def _release(self, due: Callable[[int], bool]) -> None:
        keep: list[tuple[int, asyncio.Future[None]]] = []
        for n, waiter in self._ack_waiters:
            if not due(n):
                keep.append((n, waiter))
            elif not waiter.done():
                waiter.set_result(None)
        self._ack_waiters = keep


class HubConnection:
    """The connection to one hub address."""

    def __init__(
        self,
        address: str,
        register: hub_pb2.Register,
        *,
        backoff: tuple[float, float],
        timings: Timings,
        on_change: Callable[[], None],
        on_rejection: Callable[[Rejection], None],
        on_request: Callable[[_Stream, hub_pb2.Request], None] | None = None,
        on_cancel: Callable[[_Stream, hub_pb2.Cancel], None] | None = None,
        on_flow: Callable[[_Stream, hub_pb2.Flow], None] | None = None,
        on_answer: Callable[[_Stream, hub_pb2.ServerMessage], None] | None = None,
        on_lost: Callable[[_Stream], None] | None = None,
        credentials: HubCredentials | None = None,
        tls: _tls.TLSConfig | None = None,
        certificates: _tls.Certificates | None = None,
    ) -> None:
        self.address = address
        self._register = register
        self._backoff = backoff
        self._timings = timings
        self._on_change = on_change
        self._on_rejection = on_rejection
        self._on_request = on_request
        self._on_cancel = on_cancel
        self._on_flow = on_flow
        self._on_answer = on_answer
        self._on_lost = on_lost
        self._gate: asyncio.Event | None = None  # the writer waits on it before writing a frame (a test hook)
        self._options = [
            ("grpc.keepalive_time_ms", round(timings.keepalive_time * 1000)),
            ("grpc.keepalive_timeout_ms", round(timings.keepalive_timeout * 1000)),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        self._credentials = credentials
        self._tls = tls
        self._certificates = certificates
        if credentials is not None:
            credentials.listen(self._credentials_changed)
        self.rolling = False
        """Being rolled over to a renewed client certificate: requests go to another hub while one is live."""
        self._stream: _Stream | None = None
        self._last_hub_id: str | None = None  # from every RegisterAck, kept after its stream ended
        self._reached_ack = False  # the last attempt's stream got its RegisterAck
        self._last_stream: _Stream | None = None  # the last attempt's stream, None when it opened none
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._wake = asyncio.Event()

    @property
    def hub_id(self) -> str | None:
        """``RegisterAck.hub_id`` of the current stream (the address when it is empty); None before its ack."""
        s = self._stream
        if s is None or s.ack is None:
            return None
        return s.ack.hub_id or self.address

    def live_stream(self) -> _Stream | None:
        """The current stream when a request may be sent on it: acknowledged and taking frames (not half-closed);
        else None."""
        s = self._stream
        if s is None or s.ack is None or not s.writable:
            return None
        return s

    @property
    def presented(self) -> tuple[int, _tls.Pair] | None:
        """The client certificate the registered stream presents, and its generation; None without one."""
        s = self._stream
        if s is None or s.ack is None or not s.writable:
            return None
        return s.certificate

    def stop_requests(self, why: str) -> None:
        """Ends the requests waiting on the current stream (see :meth:`_Stream.stop_requests`)."""
        if self._stream is not None:
            self._stream.stop_requests(why)

    def _block_writer(self, on: bool) -> None:
        """A test hook: while on, the writer of the connection's streams writes nothing (a frame it took is held,
        not written)."""
        if on:
            if self._gate is None:
                self._gate = asyncio.Event()
        elif self._gate is not None:
            self._gate.set()
            self._gate = None

    @property
    def crash(self) -> BaseException | None:
        """The unexpected error that ended the connection's task, if one did."""
        task = self._task
        if task is None or not task.done() or task.cancelled():
            return None
        return task.exception()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"snodra:connection:{self.address}")
        self._task.add_done_callback(self._ended)

    def _ended(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (e := task.exception()) is not None:
            log.error(
                "The connection to hub %s stopped on an unexpected error and no longer reconnects",
                self.address,
                exc_info=e,
            )

    def reregister(self, *, empty: bool) -> None:
        """A later ``Register`` on the current stream (with no types when empty), then a heartbeat."""
        s = self._stream
        if s is not None:
            s.write_register(empty=empty)

    # Credentials.

    def _credentials_changed(self) -> None:
        s = self._stream
        if s is not None and s.ack is not None and self._credentials is not None:
            self._follow(s, self._credentials.latest())

    def _follow(self, s: _Stream, wanted: hub_pb2.Credentials | None) -> None:
        """Sends the registered stream s the credentials wanted, once, unless the hub holds them already."""
        if wanted is None or wanted == s.presented or not s.writable:
            return
        if s.put(hub_pb2.ClientMessage(credentials=wanted)):
            s.presented = wanted
            log.debug("Credentials refreshed on the hub stream of %s to %s", self._register.service_name, self.address)

    async def roll_over(self, idle: Callable[[_Stream], bool]) -> bool:
        """Moves the registered stream onto the client certificate the files hold now: a ``Register`` with no types
        and its heartbeat (the hub routes elsewhere), at most ``roll_idle_limit`` for the stream to be idle (the hub
        echoed that heartbeat, so no request it forwarded before is still on its way, and idle(stream) holds), the
        half-close, at most ``close_wait`` for the hub's end, then the next stream opens at once. True once a new
        stream is registered (or there was nothing to roll, or the connection stops), False when none is within
        ``roll_reconnect_limit``."""
        s = self._stream
        if s is None or s.ack is None or not s.writable or self._stopping:
            return True
        t = self._timings
        log.info(
            "Rolling the hub stream of %s (%s) to %s over to the renewed client certificate",
            self._register.service_name,
            self._register.instance_id,
            self.address,
        )
        self.rolling = True
        self._on_change()
        try:
            echoed = s.write_register(empty=True)
            echo_came = False
            try:
                async with asyncio.timeout(t.roll_idle_limit):
                    await echoed
                    echo_came = True
                    while not idle(s) and not s.ended.is_set():  # noqa: ASYNC110 (no event tells when it is idle)
                        await asyncio.sleep(_ROLL_POLL)
            except TimeoutError:
                if echo_came:
                    log.warning(
                        "Requests still in flight on the hub stream to %s after %.1f s: rolling it over anyway",
                        self.address,
                        t.roll_idle_limit,
                    )
                else:
                    log.warning(
                        "The hub stream to %s sent no echo of its heartbeat within %.1f s (the hub may be gone): "
                        "rolling it over anyway",
                        self.address,
                        t.roll_idle_limit,
                    )
            if self._stopping:
                return True
            if not s.ended.is_set():
                s.rolled = True
                s.half_close()
                try:
                    async with asyncio.timeout(t.close_wait):
                        await s.ended.wait()
                except TimeoutError:
                    s.cut_reason = "the hub did not end the stream after the half-close of a rollover"
                    s.cut.set()
                    try:
                        async with asyncio.timeout(CLEAN_UP_BOUND):
                            await s.ended.wait()
                    except TimeoutError:
                        # The stream is cut already: the roll goes on to the wait for its replacement.
                        log.debug("The rolled hub stream to %s did not end after its cut", self.address)
            try:
                async with asyncio.timeout(t.roll_reconnect_limit):
                    while not self._stopping:
                        n = self._stream
                        if n is not None and n is not s and n.ack is not None:
                            return True
                        await asyncio.sleep(_ROLL_POLL)
                    return True
            except TimeoutError:
                return False
        finally:
            self.rolling = False
            self._on_change()

    # Shutdown, in the order App.shutdown calls them.

    def stop_reconnecting(self) -> None:
        """No stream is opened after the current one."""
        self._stopping = True
        self._wake.set()

    def deregister(self) -> asyncio.Future[None] | None:
        """Writes the empty ``Register`` (and its heartbeat) on the live stream; the future is done once the hub
        acknowledged every ``Register`` of the stream or the stream ended. None without a live stream."""
        s = self._stream
        if s is None or not s.writable:
            return None
        s.write_register(empty=True)
        return s.acknowledged()

    def half_close(self) -> None:
        """Half-closes the live stream after what is queued for it."""
        s = self._stream
        if s is not None:
            s.half_close()

    async def wait_ended(self) -> None:
        """Returns once the live stream has ended."""
        s = self._stream
        if s is not None:
            await s.ended.wait()

    def cancel(self) -> asyncio.Task[None] | None:
        """Stops the connection: no new stream, the current one cut. Returns its task, to await (see :func:`settle`)."""
        self.stop_reconnecting()
        if self._task is not None:
            self._task.cancel()
        return self._task

    # The connection's task.

    async def _run(self) -> None:
        lo, hi = self._backoff
        backoff = lo
        failures = 0
        refusal_logged: tuple[str, str] | None = None
        token_logged: str | None = None
        while not self._stopping:
            self._reached_ack = False
            self._last_stream = None
            no_token: str | None = None
            try:
                end = await self._attempt()
            except NoToken as e:
                end, no_token = _End(grpc.StatusCode.UNAUTHENTICATED, str(e)), str(e)
            except Exception as e:  # a bug, or a transport failure outside a status: a failed attempt
                end = _End(grpc.StatusCode.UNKNOWN, repr(e))
                log.debug("Hub stream to %s failed", self.address, exc_info=True)
            acked = self._reached_ack  # set by the attempt's clean-up, whichever way it ended
            stream = self._last_stream
            if self._stopping:
                break
            if acked:  # a stream the hub acknowledged proves the hub back: start again from the floor
                backoff, failures, refusal_logged, token_logged = lo, 0, None, None
            if stream is not None and stream.rolled:
                log.info(
                    "Hub stream to %s rolled over: reconnecting at once with the renewed certificate", self.address
                )
                continue
            failures += 1
            presented = None if stream is None else stream.presented
            if end.code == grpc.StatusCode.UNAUTHENTICATED and presented is not None and self._credentials is not None:
                self._credentials.refused(presented)  # the next stream does not present it again
            if no_token is not None:
                # No stream was opened: no access token could be had. Not the hub's refusal: the ordinary backoff.
                message = (
                    f"no access token for {self._register.service_name} ({self._register.instance_id}): {no_token}"
                )
                self._on_rejection(
                    Rejection(self._last_hub_id or self.address, "UNAUTHENTICATED", message, time.time())
                )
                wait = jitter(backoff, hi, random.random())
                backoff = next_wait(backoff, lo, hi)
                level = logging.DEBUG if message == token_logged else logging.ERROR
                token_logged = message
                log.log(level, "%s; not connecting to %s, retrying in %.2f s", message, self.address, wait)
            elif self._expired_certificate(end, acked):
                # The hub ends a certificate session at the certificate's end: the files are read again at the
                # next connect, so a renewed pair is presented. Any other UNAUTHENTICATED close is a refusal.
                wait = jitter(lo, hi, random.random())
                log.info(
                    "Hub %s ended the session of %s (%s) at its client certificate's end; reconnecting in %.2f s with "
                    "the pair the files hold now",
                    self.address,
                    self._register.service_name,
                    self._register.instance_id,
                    wait,
                )
            elif end.code in REFUSALS:
                hub = self._last_hub_id or self.address
                self._on_rejection(Rejection(hub, end.code.name, end.message, time.time()))
                wait = jitter(hi, hi, random.random())
                # The same refusal repeats at every attempt: one ERROR line, then DEBUG.
                key = (end.code.name, end.message)
                level = logging.DEBUG if key == refusal_logged else logging.ERROR
                refusal_logged = key
                log.log(
                    level,
                    "Hub %s refused the registration of %s (%s): %s %s; retrying in %.2f s",
                    self.address,
                    self._register.service_name,
                    self._register.instance_id,
                    end.code.name,
                    end.message,
                    wait,
                )
            else:
                wait = jitter(backoff, hi, random.random())
                backoff = next_wait(backoff, lo, hi)
                log.log(
                    logging.WARNING if failures == 1 else logging.DEBUG,
                    "Hub stream of %s (%s) to %s closed: %s %s; reconnecting in %.2f s",
                    self._register.service_name,
                    self._register.instance_id,
                    self.address,
                    end.code.name,
                    end.message,
                    wait,
                )
            try:
                async with asyncio.timeout(wait):
                    await self._wake.wait()
            except TimeoutError:
                pass

    def _expired_certificate(self, end: _End, acked: bool) -> bool:
        """The hub ended an acknowledged certificate session at the certificate's end: an ordinary reconnect."""
        return (
            acked
            and self._certificates is not None
            and end.code == grpc.StatusCode.UNAUTHENTICATED
            and CERTIFICATE_EXPIRED in end.message
        )

    async def _attempt(self) -> _End:
        """One stream, from its opening to its end; why it ended. Its clean-up records whether it reached its ack.
        :class:`NoToken` when no stream is opened for want of an access token."""
        credentials = None if self._credentials is None else await self._credentials.for_register()
        certificate = None if self._certificates is None else await self._certificates.at_connect()
        if certificate is not None and certificate[1].not_after <= time.time():
            pair = certificate[1]
            # The hub refuses a certificate that has ended: said here, through the refusal path, without a handshake.
            return _End(
                grpc.StatusCode.UNAUTHENTICATED,
                f"client certificate {pair.fingerprint} of {pair.service} expired at "
                f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(pair.not_after))}: renew the files",
            )
        if self._tls is None:
            channel = grpc.aio.insecure_channel(self.address, options=self._options)
        else:
            channel = await _tls.channel(
                self.address, self._tls, None if certificate is None else certificate[1], self._options
            )
        register = hub_pb2.Register()
        register.CopyFrom(self._register)
        if credentials is not None:
            register.credentials.CopyFrom(credentials)
        stream = _Stream(register, self._timings.heartbeat_before_ack, self._timings.permit_batch)
        stream.presented = credentials
        stream.certificate = certificate
        self._last_stream = stream
        tasks: list[asyncio.Task[Any]] = []
        try:
            call: _Call = hub_pb2_grpc.HubStub(channel).Connect()  # type: ignore[no-untyped-call]
            call.add_done_callback(lambda c: self._keep_status(c, stream))
            self._stream = stream
            stream.write_register(empty=False)  # the Register is the first frame of every stream
            writer = asyncio.create_task(self._write(call, stream), name=f"snodra:writer:{self.address}")
            reader = asyncio.create_task(self._read(call, stream), name=f"snodra:reader:{self.address}")
            beats = asyncio.create_task(self._beat(stream), name=f"snodra:heartbeat:{self.address}")
            tasks = [writer, reader, beats]
            done, _ = await asyncio.wait((reader, beats), return_when=asyncio.FIRST_COMPLETED)
            end = (reader if reader in done else beats).result()
            stream.why = f"hub stream closed: {end.code.name} ({end.message})"
            return end
        finally:
            # What needs no wait first, so that a cancellation cannot skip it; then the tasks and the channel, awaited
            # to their end even when a cancellation lands meanwhile (it is raised after).
            stream.writable = False
            self._reached_ack = stream.ack is not None
            if self._stream is stream:
                self._stream = None
            stream.finish()
            if self._on_lost is not None:
                try:
                    self._on_lost(stream)  # its streaming handlers and subscriptions ended here: the hub forgot them
                except Exception:
                    log.exception("Hub stream to %s: ending what it carried failed", self.address)
            if stream.ack is not None:
                self._on_change()
            for t in tasks:
                t.cancel()  # the reader's cancellation cancels the call when it still runs
            closing = asyncio.create_task(self._clean_up(tasks, channel), name=f"snodra:clean-up:{self.address}")
            await settle([closing])

    @staticmethod
    async def _clean_up(tasks: list[asyncio.Task[Any]], channel: grpc.aio.Channel) -> None:
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await channel.close()

    async def _write(self, call: _Call, stream: _Stream) -> None:
        """The stream's one writer: frames in queue order, then the half-close."""
        try:
            while True:
                m = await stream.out.get()
                if (gate := self._gate) is not None:
                    await gate.wait()
                if isinstance(m, _HalfClose):
                    await call.done_writing()
                    return
                if isinstance(m, OutboundRequest):
                    if not stream.took(m):
                        continue  # its caller gave up while it was buffered
                    m = m.message
                await call.write(m)
        except Exception as e:  # the call ended: the reader reports why
            log.debug("Hub stream to %s: a write failed: %r", self.address, e)

    async def _read(self, call: _Call, stream: _Stream) -> _End:
        loop = asyncio.get_running_loop()
        try:
            while True:
                m = await call.read()
                if m is grpc.aio.EOF:
                    code = await call.code()
                    return stream.status or _End(code, await call.details() or "the hub closed the stream")
                assert isinstance(m, hub_pb2.ServerMessage)
                match m.WhichOneof("message"):
                    case "register_ack":
                        self._last_hub_id = m.register_ack.hub_id or self.address
                        if stream.on_ack(m.register_ack, loop.time()):
                            log.info(
                                "Connected to hub %s at %s as %s (%s): heartbeat every %d ms",
                                m.register_ack.hub_id,
                                self.address,
                                self._register.service_name,
                                self._register.instance_id,
                                round(stream.interval * 1000),
                            )
                            self._on_change()
                            if self._credentials is not None:  # one renewed between the Register and the ack
                                self._follow(stream, self._credentials.latest())
                    case "heartbeat":
                        stream.on_echo(loop.time(), m.heartbeat.sent_at_ms)
                    case "request" if self._on_request is not None:  # also before the ack
                        self._hand_over(self._on_request, stream, m.request)
                    case "cancel" if self._on_cancel is not None:
                        self._hand_over(self._on_cancel, stream, m.cancel)
                    case "flow" if self._on_flow is not None:
                        self._hand_over(self._on_flow, stream, m.flow)
                    case "reply" | "error" | "part" | "end" if self._on_answer is not None:
                        self._hand_over(self._on_answer, stream, m)
        except grpc.aio.AioRpcError as e:
            return stream.status or _End(e.code(), e.details() or "")

    @staticmethod
    def _keep_status(call: grpc.aio.Call, stream: _Stream) -> None:
        """Keeps the status that ended the call as it first landed, which the reader then reports.

        The reader cannot always read the hub's status from the call: when a write meets a stream the hub has just
        closed, grpc.aio's ``call.write`` fails the call with ``INTERNAL`` "Internal error from Core", and that replaces
        the hub's status, a refusal included, before the reader asks. This relies on two behaviours of grpc.aio that its
        documentation does not state: a done callback runs as soon as the call's first final status lands (the hub's,
        before a later write replaces it), and ``code()`` / ``details()`` of a call that is done return without
        suspending. When either does not hold, nothing is kept and the reader reports what the call says, as it would
        without this."""
        if stream.status is not None:
            return
        try:
            code = _completed(call.code())
            details = _completed(call.details())
        except Exception:
            return
        stream.status = _End(code, details or "the hub closed the stream")

    def _hand_over(self, to: Callable[[_Stream, Any], None], stream: _Stream, frame: Any) -> None:
        """A frame for the dispatcher; a failure there is logged, never the end of the read loop."""
        try:
            to(stream, frame)
        except Exception:
            log.exception(
                "Hub stream to %s: a %s frame could not be handled; dropped", self.address, type(frame).__name__
            )

    async def _beat(self, stream: _Stream) -> _End:
        """Heartbeats: every ``heartbeat_before_ack`` until the ack, every ack interval after it. Returns why the
        stream must end: no ack within the handshake timeout, or a silent hub."""
        t = self._timings
        loop = asyncio.get_running_loop()
        opened = loop.time()
        deadline = opened + t.handshake_timeout
        next_beat = opened + t.heartbeat_before_ack
        while not stream.acked.is_set():  # no silence check before the ack: the handshake timeout covers it
            try:
                async with asyncio.timeout_at(min(deadline, next_beat)):
                    await stream.acked.wait()
            except TimeoutError:
                pass
            if stream.acked.is_set():
                break
            now = loop.time()
            if now >= deadline:
                return _End(
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                    f"no RegisterAck within {round(t.handshake_timeout * 1000)} ms",
                )
            if now >= next_beat:
                stream.heartbeat()
                next_beat = now + t.heartbeat_before_ack
        while True:
            interval = stream.interval
            try:
                async with asyncio.timeout(interval):
                    await stream.cut.wait()
            except TimeoutError:
                pass
            else:
                return _End(grpc.StatusCode.CANCELLED, stream.cut_reason)
            # On the tick, before its heartbeat: silent when more than n intervals passed since the last echo (the
            # ack counts as the first) and at least n heartbeats written since it are unanswered.
            since = loop.time() - stream.last_echo
            if stream.unanswered >= t.silent_intervals and since > t.silent_intervals * interval:
                return _End(
                    grpc.StatusCode.UNAVAILABLE,
                    f"no heartbeat echo from the hub for {round(since * 1000)} ms "
                    f"({stream.unanswered} heartbeats unanswered)",
                )
            stream.heartbeat()
            if self._credentials is not None and stream.writable:  # a rotated token file, without a reconnect
                self._follow(stream, await self._credentials.on_tick())


def _completed[T](awaitable: Coroutine[Any, Any, T]) -> T:
    """The value of a coroutine that finishes without suspending; :class:`_Suspended` (the coroutine closed) when it
    would suspend."""
    try:
        awaitable.send(None)
    except StopIteration as done:
        return cast("T", done.value)
    awaitable.close()
    raise _Suspended


class _Suspended(Exception):
    """A coroutine that was to finish at once would suspend."""


async def settle(
    tasks: Collection[asyncio.Future[Any]], bound: float = CLEAN_UP_BOUND, *, log_errors: bool = True
) -> None:
    """Waits until every task is done, without raising their errors (each one is read, and logged at warning level
    unless ``log_errors`` is false: the caller reports them). A cancellation of the caller meanwhile does not stop the
    wait: it goes on for at most ``bound`` seconds more, then the cancellation is raised; a task still running then is
    held until it ends."""
    pending = {t for t in tasks if not t.done()}
    loop = asyncio.get_running_loop()
    cancelled: asyncio.CancelledError | None = None
    deadline: float | None = None
    while pending:
        timeout = None if deadline is None else deadline - loop.time()
        if timeout is not None and timeout <= 0:
            log.warning("%d task(s) still running %.1f s after a cancellation: left behind", len(pending), bound)
            for t in pending:
                _LEFT_BEHIND.add(t)
                t.add_done_callback(_ended_behind if log_errors else _LEFT_BEHIND.discard)
            break
        try:
            _, pending = await asyncio.wait(pending, timeout=timeout)
        except asyncio.CancelledError as e:
            cancelled = cancelled or e
            if deadline is None:
                deadline = loop.time() + bound
    for t in tasks:
        if t.done() and log_errors:
            _read_result(t)
    if cancelled is not None:
        raise cancelled


def _read_result(t: asyncio.Future[Any]) -> None:
    """Reads a done task's outcome, so that its error is logged here rather than reported as never retrieved."""
    if not t.cancelled() and (e := t.exception()) is not None:
        name = t.get_name() if isinstance(t, asyncio.Task) else repr(t)
        log.warning("Task %s failed: %r", name, e)


def _ended_behind(t: asyncio.Future[Any]) -> None:
    _LEFT_BEHIND.discard(t)
    _read_result(t)
