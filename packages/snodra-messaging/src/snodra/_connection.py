"""The connection to one hub: its ``Hub.Connect`` streams, one after another.

A connection runs the state machine of https://snodra.damoreira.com/library-authors/writing-a-library/ ("A
connection to one hub") as one task, ``snodra:connection:<address>``. Each attempt opens a channel and a stream, writes
the ``Register`` and a ``Heartbeat``, and runs three tasks of its own until the stream ends: the writer (the one task
that writes to the stream), the reader, and the heartbeats (which also close a stream that gets no ``RegisterAck``
within the handshake timeout, and a silent hub's). Then it waits its backoff and opens the next stream.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Any, Final

import grpc
import grpc.aio

from snodra._backoff import jitter, next_wait
from snodra._hubpb import hub_pb2, hub_pb2_grpc
from snodra._timings import Timings

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
:class:`Rejection` (https://snodra.damoreira.com/library-authors/protocol/, "Refusals")."""

_TYPE_LISTS: Final = ("command_types", "query_types", "shared_query_types", "subscription_query_types")

CLEAN_UP_BOUND: Final = 2.0
"""After a cancellation, how long a clean-up still waits for the tasks it ends before it gives up on them."""

_LEFT_BEHIND: Final[set[asyncio.Future[Any]]] = set()
"""The tasks a clean-up gave up waiting for, held until they end."""

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


class _Stream:
    """One ``Hub.Connect`` stream: its outbound queue, its handshake and the silent-hub watch."""

    def __init__(self, register: hub_pb2.Register, interval: float) -> None:
        self.register = register
        self.out: asyncio.Queue[hub_pb2.ClientMessage | _HalfClose] = asyncio.Queue()
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

    def put(self, m: hub_pb2.ClientMessage) -> bool:
        """Queues m for the writer; False when the stream takes nothing more."""
        if not self.writable:
            return False
        self.out.put_nowait(m)
        return True

    def write_register(self, *, empty: bool) -> None:
        """The stream's ``Register`` (with no types when empty), then a heartbeat."""
        register = self.register
        if empty:
            register = hub_pb2.Register()
            register.CopyFrom(self.register)
            for name in _TYPE_LISTS:
                register.ClearField(name)
        if self.put(hub_pb2.ClientMessage(register=register)):
            self._registers += 1
        self.heartbeat()

    def heartbeat(self) -> None:
        """A heartbeat; from the ack on, one the stream took counts as unanswered until an echo."""
        beat = hub_pb2.ClientMessage(heartbeat=hub_pb2.Heartbeat(sent_at_ms=time.time_ns() // 1_000_000))
        if self.put(beat) and self.ack is not None:
            self.unanswered += 1

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

    def on_echo(self, now: float) -> None:
        """A heartbeat from the hub proves it alive; before the ack it confirms nothing."""
        if self.ack is not None:
            self.last_echo = now
            self.unanswered = 0

    def finish(self) -> None:
        self.writable = False
        self.ended.set()
        self._release(lambda n: True)

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
    ) -> None:
        self.address = address
        self._register = register
        self._backoff = backoff
        self._timings = timings
        self._on_change = on_change
        self._on_rejection = on_rejection
        self._options = [
            ("grpc.keepalive_time_ms", round(timings.keepalive_time * 1000)),
            ("grpc.keepalive_timeout_ms", round(timings.keepalive_timeout * 1000)),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        self._stream: _Stream | None = None
        self._last_hub_id: str | None = None  # from every RegisterAck, kept after its stream ended
        self._reached_ack = False  # the last attempt's stream got its RegisterAck
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
        while not self._stopping:
            self._reached_ack = False
            try:
                end = await self._attempt()
            except Exception as e:  # a bug, or a transport failure outside a status: a failed attempt
                end = _End(grpc.StatusCode.UNKNOWN, repr(e))
                log.debug("Hub stream to %s failed", self.address, exc_info=True)
            acked = self._reached_ack  # set by the attempt's clean-up, whichever way it ended
            if self._stopping:
                break
            if acked:  # a stream the hub acknowledged proves the hub back: start again from the floor
                backoff, failures, refusal_logged = lo, 0, None
            failures += 1
            if end.code in REFUSALS:
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

    async def _attempt(self) -> _End:
        """One stream, from its opening to its end; why it ended. Its clean-up records whether it reached its ack."""
        stream = _Stream(self._register, self._timings.heartbeat_before_ack)
        channel = grpc.aio.insecure_channel(self.address, options=self._options)
        tasks: list[asyncio.Task[Any]] = []
        try:
            call: _Call = hub_pb2_grpc.HubStub(channel).Connect()  # type: ignore[no-untyped-call]
            self._stream = stream
            stream.write_register(empty=False)  # the Register is the first frame of every stream
            writer = asyncio.create_task(self._write(call, stream), name=f"snodra:writer:{self.address}")
            reader = asyncio.create_task(self._read(call, stream), name=f"snodra:reader:{self.address}")
            beats = asyncio.create_task(self._beat(stream), name=f"snodra:heartbeat:{self.address}")
            tasks = [writer, reader, beats]
            done, _ = await asyncio.wait((reader, beats), return_when=asyncio.FIRST_COMPLETED)
            return (reader if reader in done else beats).result()
        finally:
            # What needs no wait first, so that a cancellation cannot skip it; then the tasks and the channel, awaited
            # to their end even when a cancellation lands meanwhile (it is raised after).
            stream.writable = False
            self._reached_ack = stream.ack is not None
            if self._stream is stream:
                self._stream = None
            stream.finish()
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
                if isinstance(m, _HalfClose):
                    await call.done_writing()
                    return
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
                    return _End(code, await call.details() or "the hub closed the stream")
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
                    case "heartbeat":
                        stream.on_echo(loop.time())
        except grpc.aio.AioRpcError as e:
            return _End(e.code(), e.details() or "")

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
            await asyncio.sleep(interval)
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
