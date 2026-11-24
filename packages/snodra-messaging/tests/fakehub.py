"""A scripted fake hub: an in-process gRPC server speaking ``Hub.Connect``, driven frame by frame by a test.

A test starts a :class:`FakeHub`, lets the library under test connect, takes each stream with
:meth:`FakeHub.next_stream`, reads what the library wrote (:meth:`FakeStream.recv` and the ``recv_<kind>``
helpers) and writes what a hub would (:meth:`FakeStream.send`, :meth:`FakeStream.ack`,
:meth:`FakeStream.close`). The hub times nothing itself: no eviction, no handshake deadline, no request queue. Every
wait here is bounded and the bound only stops a hang; a step that does not happen fails with ``AssertionError``.

Where the library relies on it the hub behaves as snodra-server (see "How the runner's hub differs from
snodra-server" in https://snodra.damoreira.com/library-authors/conformance/), with snodra-server's codes and texts:

- The first frame: one that is not a ``Register``, or a half-close before any frame, closes the stream
  ``FAILED_PRECONDITION``; an empty service name or instance id, an instance id starting with ``snodra-server/``,
  the service name ``snodra-server`` or ``initial_permits`` 0 closes it ``INVALID_ARGUMENT``.
- Every ``Register``, the first and each later one, in this order: a type listed both as a command and as a query,
  a type listed both as a shared query and as a command or exclusive query, and a subscription type that is not one
  of the same ``Register``'s query types close the stream ``INVALID_ARGUMENT``.
- A refused stream is still handed to the script, its offending frame readable; the script learns of the end before
  the client does.
- The ``RegisterAck`` goes out only when the script sends it. A heartbeat is echoed once the stream is acknowledged:
  one that arrives before the ack is held and its echo written right after the ack. An echo the client does not
  take is dropped once 64 frames wait to be written, as snodra-server drops it when its buffer is full.
- A client that half-closes gets the end of the stream with ``OK``. grpc.aio shows a client that cancels its call as
  the end of its frames too, exactly as a half-close, and only cancels the handler a moment later (well under a
  millisecond); nothing else tells them apart. So the hub waits 50 ms after the end of the frames: a cancellation
  within it is a client that left (``CANCELLED``), else it is a half-close and the stream ends ``OK`` (snodra-server
  ends it at once). A library that half-closes and gives up waiting for the end within 50 ms is seen as having left.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from typing import Final

import grpc
from certs import ServerTls
from cryptography import x509
from google.protobuf import text_format
from snodra._hubpb import hub_pb2, hub_pb2_grpc

BOUND: Final = 5.0
"""How long a script step waits, by default, for what it expects."""

KINDS: Final = frozenset(f.name for f in hub_pb2.ClientMessage.DESCRIPTOR.oneofs_by_name["message"].fields)
"""The kinds of frame a library writes: the names of ``ClientMessage``'s oneof."""

_QUIET: Final = frozenset({"heartbeat", "status"})
_EOF_GRACE: Final = 0.05
"""How long after the end of the client's frames its cancellation may still follow (see the module docstring)."""
_ECHO_BACKLOG: Final = 64
"""Echoes are dropped while this many frames wait to be written."""
_HUB_SERVICE: Final = "snodra-server"
_HUB_INSTANCE_PREFIX: Final = "snodra-server/"
_NOT_A_REGISTER: Final = "first message must be Register"

_Refusal = tuple[grpc.StatusCode, str]


class _End:
    """The last item of a stream's inbox: why the stream ended."""

    def __init__(self, reason: str) -> None:
        self.reason = reason


class _Eof:
    """The end of the client's frames, before the hub knows whether it half-closed or left."""


_EOF: Final = _Eof()


def _kind(m: hub_pb2.ClientMessage) -> str:
    return m.WhichOneof("message") or "empty"


def _show(m: hub_pb2.ClientMessage) -> str:
    return text_format.MessageToString(m, as_one_line=True)


def _check_first(m: hub_pb2.ClientMessage) -> _Refusal | None:
    """snodra-server's handshake checks of a stream's first frame (before the type lists)."""
    if m.WhichOneof("message") != "register":
        return grpc.StatusCode.FAILED_PRECONDITION, _NOT_A_REGISTER
    r = m.register
    if not r.instance_id or not r.service_name:
        return grpc.StatusCode.INVALID_ARGUMENT, "Register.instance_id and Register.service_name must not be empty"
    if r.instance_id.startswith(_HUB_INSTANCE_PREFIX):
        return (
            grpc.StatusCode.INVALID_ARGUMENT,
            f"Register.instance_id must not start with {_HUB_INSTANCE_PREFIX}: the hub's own requests use it",
        )
    if r.service_name == _HUB_SERVICE:
        return (
            grpc.StatusCode.INVALID_ARGUMENT,
            f"Register.service_name must not be {_HUB_SERVICE}: the hub's own requests use it",
        )
    if r.initial_permits == 0:
        return grpc.StatusCode.INVALID_ARGUMENT, "Register.initial_permits must be at least 1"
    return None


def _check_types(r: hub_pb2.Register) -> _Refusal | None:
    """snodra-server's checks of a ``Register``'s type lists, run on every ``Register``, in its order."""
    seen: dict[str, tuple[str, bool]] = {}  # type -> (kind, shared)
    listed = itertools.chain(
        ((t, "command", False) for t in r.command_types),
        ((t, "query", False) for t in r.query_types),
        ((t, "query", True) for t in r.shared_query_types),
    )
    for t, kind, shared in listed:
        prev_kind, prev_shared = seen.setdefault(t, (kind, shared))
        if prev_shared != shared:
            return (
                grpc.StatusCode.INVALID_ARGUMENT,
                f"type {t} listed as a shared query type and as a command or exclusive query",
            )
        if prev_kind != kind:
            return grpc.StatusCode.INVALID_ARGUMENT, f"type {t} listed as both command and query"
    for t in sorted(set(r.subscription_query_types)):
        if seen.get(t, ("", False))[0] != "query":
            return (
                grpc.StatusCode.INVALID_ARGUMENT,
                f"type {t} listed as a subscription type but not as a query type",
            )
    return None


def _check(m: hub_pb2.ClientMessage, first: bool) -> _Refusal | None:
    refusal = _check_first(m) if first else None
    if refusal is None and m.WhichOneof("message") == "register":
        refusal = _check_types(m.register)
    return refusal


def _echo(sent_at_ms: int) -> hub_pb2.ServerMessage:
    return hub_pb2.ServerMessage(heartbeat=hub_pb2.Heartbeat(sent_at_ms=sent_at_ms))


_Context = grpc.aio.ServicerContext[hub_pb2.ClientMessage, hub_pb2.ServerMessage]
_Outgoing = tuple[hub_pb2.ServerMessage, "asyncio.Future[None] | None"]


class FakeStream:
    """One ``Connect`` stream. Its methods belong to the script; the hub feeds it from its own tasks."""

    def __init__(self, index: int, context: _Context, peer_certificates: list[x509.Certificate]) -> None:
        self.index = index
        """Counts the streams handed to the script, from 0."""
        self.peer_certificates = peer_certificates
        """The client certificate chain, leaf first, when the hub asks for one; else empty."""
        self.heartbeats: list[float] = []
        """The arrival time (``time.monotonic()``) of every ``Heartbeat``."""
        self.statuses: list[tuple[float, hub_pb2.InstanceStatus]] = []
        """Every ``InstanceStatus`` with its arrival time (``time.monotonic()``)."""

        self._context = context
        self._inbox: asyncio.Queue[hub_pb2.ClientMessage | _Eof | _End] = asyncio.Queue()
        self._kept: deque[hub_pb2.ClientMessage] = deque()  # frames nothing() let pass, for recv
        self._outbox: asyncio.Queue[_Outgoing] = asyncio.Queue()  # one writer, as grpc.aio requires
        self._ended: str | None = None
        self._eof = False  # the client wrote its last frame; whether it half-closed or left is decided after
        self._ended_now = asyncio.Event()
        self._half_closed = asyncio.Event()
        self._closing: asyncio.Future[_Refusal] = asyncio.get_running_loop().create_future()
        self._served = asyncio.Event()  # the RPC handler is done: nothing can be written any more
        self._acked = False
        self._held: list[int] = []  # heartbeats (sent_at_ms) to echo right after the ack
        self._echo: bool | None = None  # None: echo once acknowledged

    # The hub's side.

    async def _serve(self) -> None:
        pump = asyncio.create_task(self._pump(), name=f"fakehub:stream-{self.index}")
        writer = asyncio.create_task(self._writer(), name=f"fakehub:writer-{self.index}")
        try:
            code, message = await self._closing
            self._end(f"{code.name}: {message}")
        except asyncio.CancelledError:
            self._end("CANCELLED: the client left or the hub stopped")
            raise
        finally:
            try:
                pump.cancel()
                writer.cancel()
                await asyncio.gather(pump, writer, return_exceptions=True)
            finally:
                self._served.set()
                self._fail_unwritten()
        if code is not grpc.StatusCode.OK:
            self._context.set_code(code)
            self._context.set_details(message)

    async def _pump(self) -> None:
        first = True
        while True:
            try:
                m = await self._context.read()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # the RPC is over; the handler reports why when it is cancelled
                self._end(f"the read failed: {e!r}")
                return
            if not isinstance(m, hub_pb2.ClientMessage):  # grpc.aio.EOF: a half-close, or a client that left
                self._eof = True
                self._inbox.put_nowait(_EOF)
                await asyncio.sleep(_EOF_GRACE)  # a client that left gets this task cancelled meanwhile
                self._half_closed.set()
                if first:
                    code, message = grpc.StatusCode.FAILED_PRECONDITION, _NOT_A_REGISTER
                    self._end(f"{code.name}: {message} (the client half-closed before any frame)")
                else:
                    code, message = grpc.StatusCode.OK, ""
                    self._end("the client half-closed")
                self._request_close(code, message)
                return
            self._observe(m)
            self._inbox.put_nowait(m)
            refusal = _check(m, first)
            first = False
            if refusal is not None:
                code, message = refusal
                self._end(f"{code.name}: {message}")  # the script learns of it before the client does
                self._request_close(code, message)
                return

    def _observe(self, m: hub_pb2.ClientMessage) -> None:
        """Records heartbeats and statuses and decides a heartbeat's echo before the frame reaches the script: a
        script that has taken a heartbeat knows what became of its echo."""
        match m.WhichOneof("message"):
            case "heartbeat":
                self.heartbeats.append(time.monotonic())
                if self._echo is False:
                    return
                if self._echo or self._acked:
                    self._put_echo(m.heartbeat.sent_at_ms)
                else:
                    self._held.append(m.heartbeat.sent_at_ms)
            case "status":
                self.statuses.append((time.monotonic(), m.status))

    def _put_echo(self, sent_at_ms: int) -> None:
        if not self._served.is_set() and self._outbox.qsize() < _ECHO_BACKLOG:
            self._outbox.put_nowait((_echo(sent_at_ms), None))

    async def _writer(self) -> None:
        while True:
            m, written = await self._outbox.get()
            try:
                await self._context.write(m)
            except Exception as e:
                if written is not None and not written.done():
                    written.set_exception(AssertionError(f"stream ended: the write failed: {e!r}"))
            else:
                if written is not None and not written.done():
                    written.set_result(None)

    def _fail_unwritten(self) -> None:
        while not self._outbox.empty():
            _, written = self._outbox.get_nowait()
            if written is not None and not written.done():
                written.set_exception(AssertionError(f"stream ended before the frame was written: {self._ended}"))

    def _put(self, m: hub_pb2.ServerMessage) -> asyncio.Future[None]:
        if self._served.is_set():
            raise AssertionError(f"stream ended: {self._ended}; nothing can be sent")
        written: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._outbox.put_nowait((m, written))
        return written

    @staticmethod
    async def _written(written: asyncio.Future[None]) -> None:
        try:
            async with asyncio.timeout(BOUND):
                await written
        except TimeoutError:
            raise AssertionError(f"a frame was not written within {BOUND} s (does the client read?)") from None

    def _end(self, reason: str) -> None:
        """Marks the stream ended (the first reason wins); frames already received stay readable."""
        if self._ended is None:
            self._ended = reason
            self._inbox.put_nowait(_End(reason))
            self._ended_now.set()

    def _request_close(self, code: grpc.StatusCode, message: str) -> None:
        if not self._closing.done():
            self._closing.set_result((code, message))

    async def _take(self, deadline: float, timeout_text: str) -> hub_pb2.ClientMessage:
        while True:
            if self._kept:
                return self._kept.popleft()
            if self._ended is not None and self._inbox.empty():
                raise AssertionError(f"stream ended: {self._ended}")
            try:
                async with asyncio.timeout_at(deadline):
                    item = await self._inbox.get()
            except TimeoutError:
                raise AssertionError(timeout_text) from None
            if isinstance(item, _End):
                raise AssertionError(f"stream ended: {item.reason}")
            if isinstance(item, hub_pb2.ClientMessage):
                return item

    # The script's side.

    async def recv(self, within: float = BOUND) -> hub_pb2.ClientMessage:
        """The next frame the library wrote, of any kind, heartbeats and statuses included."""
        return await self._take(asyncio.get_running_loop().time() + within, f"no frame within {within} s")

    async def _recv_kind(self, kind: str, within: float) -> hub_pb2.ClientMessage:
        """The next frame that is not a heartbeat or a status, which must be of kind; the bound covers the whole
        wait, the skipped frames included."""
        deadline = asyncio.get_running_loop().time() + within
        while True:
            m = await self._take(deadline, f"no {kind} frame within {within} s")
            got = _kind(m)
            if got in _QUIET:
                continue
            if got != kind:
                raise AssertionError(f"expected a {kind} frame, got {got}: {_show(m)}")
            return m

    async def recv_register(self, within: float = BOUND) -> hub_pb2.Register:
        """The next ``Register``, skipping heartbeats and statuses, as every ``recv_<kind>``; another kind fails."""
        return (await self._recv_kind("register", within)).register

    async def recv_request(self, within: float = BOUND) -> hub_pb2.Request:
        return (await self._recv_kind("request", within)).request

    async def recv_reply(self, within: float = BOUND) -> hub_pb2.Reply:
        return (await self._recv_kind("reply", within)).reply

    async def recv_permits(self, within: float = BOUND) -> hub_pb2.Permits:
        return (await self._recv_kind("permits", within)).permits

    async def recv_part(self, within: float = BOUND) -> hub_pb2.ReplyPart:
        return (await self._recv_kind("part", within)).part

    async def recv_end(self, within: float = BOUND) -> hub_pb2.ReplyEnd:
        return (await self._recv_kind("end", within)).end

    async def recv_flow(self, within: float = BOUND) -> hub_pb2.Flow:
        return (await self._recv_kind("flow", within)).flow

    async def recv_cancel(self, within: float = BOUND) -> hub_pb2.Cancel:
        return (await self._recv_kind("cancel", within)).cancel

    async def recv_credentials(self, within: float = BOUND) -> hub_pb2.Credentials:
        return (await self._recv_kind("credentials", within)).credentials

    async def nothing(self, seconds: float, *kinds: str) -> None:
        """Fails if a frame of one of kinds arrives within seconds, or if the client's frames ended before or meanwhile
        (an absence on an ended stream proves nothing): at once when the end comes, a half-close or a client that left
        alike. Kinds are the names of ``ClientMessage``'s oneof; with none, every kind but heartbeat and status.
        Frames of other kinds stay available to ``recv``. This is the one step that waits for time to pass, because
        it asserts an absence."""
        unknown = sorted(set(kinds) - KINDS)
        if unknown:
            raise AssertionError(f"unknown frame kind {unknown}; known: {sorted(KINDS)}")
        watched = set(kinds) or KINDS - _QUIET
        self._alive()
        deadline = asyncio.get_running_loop().time() + seconds
        while True:
            try:
                async with asyncio.timeout_at(deadline):
                    item = await self._inbox.get()
            except TimeoutError:
                break
            if isinstance(item, _End):
                raise AssertionError(f"stream ended: {item.reason} (an absence on an ended stream proves nothing)")
            if isinstance(item, _Eof):
                self._alive()
                continue
            if _kind(item) in watched:
                raise AssertionError(f"unexpected {_kind(item)} frame within {seconds} s: {_show(item)}")
            self._kept.append(item)
        self._alive()

    def _alive(self) -> None:
        reason = self._ended or ("the client half-closed or left" if self._eof else None)
        if reason is not None:
            raise AssertionError(f"stream ended: {reason} (an absence on an ended stream proves nothing)")

    async def send(self, message: hub_pb2.ServerMessage) -> None:
        """Writes a frame to the library and waits until it is written."""
        await self._written(self._put(message))

    async def ack(self, ack: hub_pb2.RegisterAck) -> None:
        """Writes a ``RegisterAck``, then the echoes of the heartbeats that arrived before it (unless
        ``echo_heartbeats(False)``), and from then on echoes every heartbeat, as snodra-server."""
        pending = [self._put(hub_pb2.ServerMessage(register_ack=ack))]
        self._acked = True
        held, self._held = self._held, []
        if self._echo is not False:
            pending += [self._put(_echo(ms)) for ms in held]
        for written in pending:
            await self._written(written)

    def echo_heartbeats(self, on: bool) -> None:
        """Overrides the default (echo once acknowledged, as snodra-server): True echoes every heartbeat that arrives
        from now on, at once, even before the ack; False echoes none (a silent hub), held ones included."""
        self._echo = on

    async def close(self, code: grpc.StatusCode, message: str) -> None:
        """Ends the stream with the status (``OK`` ends it cleanly) and waits until the hub has let it go."""
        self._request_close(code, message)
        try:
            async with asyncio.timeout(BOUND):
                await self._served.wait()
        except TimeoutError:
            raise AssertionError(f"the stream did not close within {BOUND} s") from None

    async def half_closed(self, within: float) -> None:
        """Fails unless the library half-closed its side within ``within``; fails at once if the stream ended
        otherwise. The answer comes 50 ms after the half-close (see the module docstring)."""
        try:
            async with asyncio.timeout(within):
                await self._ended_now.wait()
        except TimeoutError:
            raise AssertionError(f"the client did not half-close within {within} s") from None
        if not self._half_closed.is_set():
            raise AssertionError(f"stream ended: {self._ended} (no half-close)")


class _Servicer(hub_pb2_grpc.HubServicer):
    def __init__(self, hub: FakeHub) -> None:
        self._hub = hub

    async def Connect(self, request_iterator: object, context: _Context) -> None:
        await self._hub._connect(context)

    async def Topology(
        self, request: hub_pb2.TopologyRequest, context: grpc.aio.ServicerContext[object, object]
    ) -> hub_pb2.TopologyResponse:
        return hub_pb2.TopologyResponse()


class FakeHub:
    """A scripted hub on 127.0.0.1 (a port the system picks, kept across ``down`` and ``up``)."""

    def __init__(self, tls: ServerTls | None) -> None:
        self._tls = tls
        self._server: grpc.aio.Server | None = None
        self._port = 0
        self._streams: asyncio.Queue[FakeStream] = asyncio.Queue()
        self._count = 0
        self._live: set[FakeStream] = set()  # streams whose RPC handler runs
        self._refusal: tuple[grpc.StatusCode, str] | None = None

    @classmethod
    async def start(cls, *, tls: ServerTls | None = None) -> FakeHub:
        """Listens on 127.0.0.1:0; with tls, serves TLS (mutual TLS when ``tls.client_ca`` is set)."""
        hub = cls(tls)
        await hub._listen()
        return hub

    @property
    def address(self) -> str:
        """``127.0.0.1:<port>``; the same across ``down`` and ``up``."""
        return f"127.0.0.1:{self._port}"

    async def _listen(self) -> None:
        server = grpc.aio.server(options=[("grpc.so_reuseport", 0)])
        hub_pb2_grpc.add_HubServicer_to_server(_Servicer(self), server)  # type: ignore[no-untyped-call]
        address = f"127.0.0.1:{self._port}"
        if self._tls is None:
            port = server.add_insecure_port(address)
        else:
            ca = self._tls.client_ca
            credentials = grpc.ssl_server_credentials(
                [(self._tls.key_pair.key_pem, self._tls.key_pair.cert_pem)],
                root_certificates=None if ca is None else ca.cert_pem,
                require_client_auth=ca is not None,
            )
            port = server.add_secure_port(address, credentials)
        if port == 0:
            raise RuntimeError(f"fakehub: cannot listen on {address}")
        await server.start()
        self._server, self._port = server, port

    async def _connect(self, context: _Context) -> None:
        if self._refusal is not None:
            code, message = self._refusal
            await context.abort(code, message)
        stream = FakeStream(self._count, context, _peer_certificates(context))
        self._count += 1
        self._live.add(stream)
        self._streams.put_nowait(stream)
        try:
            await stream._serve()
        finally:
            self._live.discard(stream)

    async def next_stream(self, within: float = BOUND) -> FakeStream:
        """The next ``Connect`` stream, in arrival order."""
        try:
            async with asyncio.timeout(within):
                return await self._streams.get()
        except TimeoutError:
            raise AssertionError(f"no stream within {within} s") from None

    async def no_stream_within(self, seconds: float) -> None:
        """Fails if a stream arrives within seconds."""
        try:
            async with asyncio.timeout(seconds):
                stream = await self._streams.get()
        except TimeoutError:
            return
        raise AssertionError(f"unexpected stream {stream.index} within {seconds} s")

    def refuse(self, code: grpc.StatusCode, message: str) -> None:
        """Closes every new stream at once with the status, without handing it to the script, until ``accept``."""
        self._refusal = (code, message)

    def accept(self) -> None:
        """Ends ``refuse``."""
        self._refusal = None

    async def down(self) -> None:
        """Stops listening, so connections are refused at the TCP level, and severs the live streams, as a hub that
        went away. Streams already handed out stay readable for what they received. A no-op while down."""
        server, self._server = self._server, None
        if server is None:
            return
        await server.stop(None)
        # The server cancels the handlers without waiting for them: wait, so that no task of the hub outlives it.
        try:
            async with asyncio.timeout(BOUND):
                for stream in list(self._live):
                    await stream._served.wait()
        except TimeoutError:
            raise AssertionError(f"{len(self._live)} stream(s) still open {BOUND} s after the hub went down") from None

    async def up(self) -> None:
        """Listens again on the same port. A no-op while up."""
        if self._server is None:
            await self._listen()

    async def stop(self) -> None:
        """Stops the hub for good: every stream ends. Idempotent."""
        await self.down()


def _peer_certificates(context: _Context) -> list[x509.Certificate]:
    auth = context.auth_context()
    pems = list(auth.get("x509_pem_cert_chain", ())) or list(auth.get("x509_pem_cert", ()))
    return [c for pem in pems for c in x509.load_pem_x509_certificates(pem)]


def ack(
    hub_id: str,
    *,
    interval_ms: int = 3000,
    timeout_ms: int = 10000,
    default_ms: int = 5000,
    max_ms: int = 60000,
    type_timeouts_ms: dict[str, int] | None = None,
) -> hub_pb2.RegisterAck:
    """A ``RegisterAck`` with snodra-server's defaults: heartbeat every 3 s, eviction after 10 s, request timeout
    5 s clamped at 60 s, context ``default``."""
    return hub_pb2.RegisterAck(
        hub_id=hub_id,
        heartbeat_interval_ms=interval_ms,
        heartbeat_timeout_ms=timeout_ms,
        default_timeout_ms=default_ms,
        max_timeout_ms=max_ms,
        context="default",
        type_timeouts_ms=type_timeouts_ms or {},
    )


def forward(r: hub_pb2.Request) -> hub_pb2.ServerMessage:
    """A frame delivering r to the library's handler."""
    return hub_pb2.ServerMessage(request=r)


def flow(id: str, n: int) -> hub_pb2.ServerMessage:
    """Grants n credits on request id."""
    return hub_pb2.ServerMessage(flow=hub_pb2.Flow(request_id=id, credits=n))


def cancel(id: str, reason: str) -> hub_pb2.ServerMessage:
    """Cancels request id."""
    return hub_pb2.ServerMessage(cancel=hub_pb2.Cancel(request_id=id, reason=reason))


def hub_error(id: str, code: int, msg: str) -> hub_pb2.ServerMessage:
    """A failure the hub detected for request id (code: an ``ErrorCode`` value)."""
    return hub_pb2.ServerMessage(error=hub_pb2.Error(request_id=id, code=code, message=msg))  # type: ignore[arg-type]
