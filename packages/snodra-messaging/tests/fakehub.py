"""A scripted fake hub: an in-process gRPC server speaking ``Hub.Connect``, driven frame by frame by a test.

A test starts a :class:`FakeHub`, lets the library under test connect, takes each stream with
:meth:`FakeHub.next_stream`, reads what the library wrote (:meth:`FakeStream.recv` and the ``recv_<kind>``
helpers) and writes what a hub would (:meth:`FakeStream.send`, :meth:`FakeStream.ack`,
:meth:`FakeStream.close`). The hub times nothing itself: no eviction, no handshake deadline, no queue. Every wait
here is bounded and the bound only stops a hang; a step that does not happen fails with ``AssertionError``.

Where the library relies on it the hub behaves as snodra-server (see "How the runner's hub differs from
snodra-server" in https://snodra.damoreira.com/library-authors/conformance/): a first frame that is not a
``Register`` closes the stream ``FAILED_PRECONDITION``; an empty service name or instance id, ``initial_permits`` 0,
the reserved service name or instance prefix, or a subscription type that is not one of the ``Register``'s query
types close it ``INVALID_ARGUMENT``. The refused stream is still handed to the script, its offending frame
readable, and the script learns of the end at once. The ``RegisterAck`` goes out only when the script sends it and
heartbeats are echoed only after it.
"""

from __future__ import annotations

import asyncio
import contextlib
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
"""How long after the end of the client's frames its cancellation may still follow (see ``FakeStream._pump``)."""
_HUB_SERVICE: Final = "snodra-server"
_HUB_INSTANCE_PREFIX: Final = "snodra-server/"


class _End:
    """The last item of a stream's inbox: why the stream ended."""

    def __init__(self, reason: str) -> None:
        self.reason = reason


def _kind(m: hub_pb2.ClientMessage) -> str:
    return m.WhichOneof("message") or "empty"


def _show(m: hub_pb2.ClientMessage) -> str:
    return text_format.MessageToString(m, as_one_line=True)


def _check_first(m: hub_pb2.ClientMessage) -> tuple[grpc.StatusCode, str] | None:
    """The status snodra-server closes a stream with for its first frame, or None when it accepts it."""
    if m.WhichOneof("message") != "register":
        return grpc.StatusCode.FAILED_PRECONDITION, "first message must be Register"
    r = m.register
    if not r.instance_id or not r.service_name:
        return grpc.StatusCode.INVALID_ARGUMENT, "Register.instance_id and Register.service_name must not be empty"
    if r.instance_id.startswith(_HUB_INSTANCE_PREFIX):
        return grpc.StatusCode.INVALID_ARGUMENT, f"Register.instance_id must not start with {_HUB_INSTANCE_PREFIX}"
    if r.service_name == _HUB_SERVICE:
        return grpc.StatusCode.INVALID_ARGUMENT, f"Register.service_name must not be {_HUB_SERVICE}"
    if r.initial_permits == 0:
        return grpc.StatusCode.INVALID_ARGUMENT, "Register.initial_permits must be at least 1"
    queries = set(r.query_types) | set(r.shared_query_types)
    for t in r.subscription_query_types:
        if t not in queries:
            return grpc.StatusCode.INVALID_ARGUMENT, f"subscription type {t} is not one of the query types"
    return None


_Context = grpc.aio.ServicerContext[hub_pb2.ClientMessage, hub_pb2.ServerMessage]


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
        self._inbox: asyncio.Queue[hub_pb2.ClientMessage | _End] = asyncio.Queue()
        self._kept: deque[hub_pb2.ClientMessage] = deque()  # frames nothing() let pass, for recv
        self._ended: str | None = None
        self._eof = False  # the client wrote its last frame; whether it half-closed or left is decided after
        self._ended_now = asyncio.Event()
        self._half_closed = asyncio.Event()
        self._closing: asyncio.Future[tuple[grpc.StatusCode, str]] = asyncio.get_running_loop().create_future()
        self._served = asyncio.Event()  # the RPC handler returned: nothing can be written any more
        self._write_lock = asyncio.Lock()  # one write at a time, as grpc.aio requires
        self._acked = False
        self._echo: bool | None = None  # None: echo after the ack

    # The hub's side.

    async def _serve(self) -> None:
        pump = asyncio.create_task(self._pump(), name=f"fakehub:stream-{self.index}")
        try:
            code, message = await self._closing
            self._end(f"{code.name}: {message}")
        except asyncio.CancelledError:
            self._end("CANCELLED: the client left or the hub stopped")
            raise
        finally:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)
            self._served.set()
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
            if not isinstance(m, hub_pb2.ClientMessage):  # grpc.aio.EOF
                self._eof = True
                # A cancelled client reads as an EOF too, and its handler is cancelled a loop step later, taking
                # this task with it; an EOF that outlives the grace is a half-close.
                await asyncio.sleep(_EOF_GRACE)
                self._half_closed.set()
                self._end("the client half-closed")
                return
            await self._observe(m)
            self._inbox.put_nowait(m)
            if first:
                first = False
                refusal = _check_first(m)
                if refusal is not None:
                    code, message = refusal
                    self._end(f"{code.name}: {message}")  # the script learns of it before the client does
                    self._request_close(code, message)
                    return

    async def _observe(self, m: hub_pb2.ClientMessage) -> None:
        """Records heartbeats and statuses and echoes a heartbeat when the mode says so, before the frame reaches
        the script: a script that has taken a heartbeat knows whether it was echoed."""
        match m.WhichOneof("message"):
            case "heartbeat":
                self.heartbeats.append(time.monotonic())
                async with self._write_lock:
                    if self._echo or (self._echo is None and self._acked):
                        echo = hub_pb2.ServerMessage(heartbeat=hub_pb2.Heartbeat(sent_at_ms=m.heartbeat.sent_at_ms))
                        with contextlib.suppress(Exception):  # the stream is ending; the script learns it from recv
                            await self._context.write(echo)
            case "status":
                self.statuses.append((time.monotonic(), m.status))

    def _end(self, reason: str) -> None:
        """Marks the stream ended (the first reason wins); frames already received stay readable."""
        if self._ended is None:
            self._ended = reason
            self._inbox.put_nowait(_End(reason))
            self._ended_now.set()

    def _request_close(self, code: grpc.StatusCode, message: str) -> None:
        if not self._closing.done():
            self._closing.set_result((code, message))

    async def _write(self, m: hub_pb2.ServerMessage) -> None:
        if self._served.is_set():
            raise AssertionError(f"stream ended: {self._ended}; nothing can be sent")
        try:
            await self._context.write(m)
        except Exception as e:
            raise AssertionError(f"stream ended: the write failed: {e!r}") from e

    async def _take(self, deadline: float, timeout_text: str) -> hub_pb2.ClientMessage:
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
        """Fails if a frame of one of kinds arrives within seconds, or if the stream ended before or meanwhile (an
        absence on an ended stream proves nothing). Kinds are the names of ``ClientMessage``'s oneof; with none,
        every kind but heartbeat and status. Frames of other kinds stay available to ``recv``. This is the one step
        that waits for time to pass, because it asserts an absence."""
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
            if _kind(item) in watched:
                raise AssertionError(f"unexpected {_kind(item)} frame within {seconds} s: {_show(item)}")
            self._kept.append(item)
        self._alive()

    def _alive(self) -> None:
        reason = self._ended or ("the client half-closed or left" if self._eof else None)
        if reason is not None:
            raise AssertionError(f"stream ended: {reason} (an absence on an ended stream proves nothing)")

    async def send(self, message: hub_pb2.ServerMessage) -> None:
        """Writes a frame to the library."""
        async with self._write_lock:
            await self._write(message)

    async def ack(self, ack: hub_pb2.RegisterAck) -> None:
        """Writes a ``RegisterAck``; from then on heartbeats are echoed (unless ``echo_heartbeats`` says otherwise).
        A heartbeat is judged when it arrives, so a script that wants one before the ack consumes it with ``recv``
        first."""
        async with self._write_lock:
            await self._write(hub_pb2.ServerMessage(register_ack=ack))
            self._acked = True

    def echo_heartbeats(self, on: bool) -> None:
        """Overrides the default (echo only after the ack, as snodra-server): True echoes every heartbeat that
        arrives from now on, False none."""
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
        otherwise. grpc.aio shows a client that left as the end of its frames, as a half-close, then cancels the
        call: the end of the frames counts as a half-close once 50 ms pass without that cancellation, so a library
        that half-closes and leaves within 50 ms is seen as having left."""
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
