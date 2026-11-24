"""The scripted fake hub: a raw Connect stream plays the library and checks what the script sees."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol

import grpc
import pytest
from certs import ServerTls, make_ca
from cryptography import x509
from fakehub import FakeHub, FakeStream, ack, cancel, flow, forward, hub_error
from snodra._hubpb import hub_pb2, hub_pb2_grpc

_Frame = hub_pb2.ServerMessage | grpc.aio.AioRpcError | None  # None: the hub ended the stream with OK


class Client:
    """One raw Connect stream; a task reads every frame the hub writes into a queue."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        self._channel = channel
        stub = hub_pb2_grpc.HubStub(channel)  # type: ignore[no-untyped-call]
        self._call: grpc.aio.StreamStreamCall[hub_pb2.ClientMessage, hub_pb2.ServerMessage] = stub.Connect()
        self._frames: asyncio.Queue[_Frame] = asyncio.Queue()
        self._reader = asyncio.create_task(self._read_all(), name="test:client-reader")

    async def _read_all(self) -> None:
        try:
            while True:
                m = await self._call.read()
                if m is grpc.aio.EOF:
                    self._frames.put_nowait(None)
                    return
                assert isinstance(m, hub_pb2.ServerMessage)
                self._frames.put_nowait(m)
        except grpc.aio.AioRpcError as e:
            self._frames.put_nowait(e)

    async def _next(self, within: float) -> _Frame:
        async with asyncio.timeout(within):
            item = await self._frames.get()
        if not isinstance(item, hub_pb2.ServerMessage):
            self._frames.put_nowait(item)  # the end stays the answer of every later read
        return item

    async def write(self, m: hub_pb2.ClientMessage) -> None:
        await self._call.write(m)

    async def done_writing(self) -> None:
        await self._call.done_writing()

    async def read(self, within: float = 5.0) -> hub_pb2.ServerMessage:
        item = await self._next(within)
        if isinstance(item, grpc.aio.AioRpcError):
            raise item
        if item is None:
            raise AssertionError("the hub ended the stream with OK")
        return item

    async def status(self, within: float = 5.0) -> grpc.StatusCode:
        """Skips frames until the stream ends and returns its status code."""
        async with asyncio.timeout(within):
            while True:
                item = await self._next(within)
                if item is None:
                    return grpc.StatusCode.OK
                if isinstance(item, grpc.aio.AioRpcError):
                    return item.code()

    async def nothing(self, seconds: float) -> None:
        try:
            item = await self._next(seconds)
        except TimeoutError:
            return
        raise AssertionError(f"the client got {item!r}")

    def cancel(self) -> None:
        self._call.cancel()

    async def close(self) -> None:
        await self._channel.close()
        self._reader.cancel()
        await asyncio.gather(self._reader, return_exceptions=True)


class Dial(Protocol):
    def __call__(
        self,
        address: str,
        credentials: grpc.ChannelCredentials | None = None,
        options: Sequence[tuple[str, str]] = (),
    ) -> Awaitable[Client]: ...


@pytest.fixture
async def dial() -> AsyncIterator[Dial]:
    clients: list[Client] = []

    async def open_client(
        address: str, credentials: grpc.ChannelCredentials | None = None, options: Sequence[tuple[str, str]] = ()
    ) -> Client:
        if credentials is None:
            channel = grpc.aio.insecure_channel(address, options=options)
        else:
            channel = grpc.aio.secure_channel(address, credentials, options=options)
        c = Client(channel)
        clients.append(c)
        return c

    try:
        yield open_client
    finally:
        for c in clients:
            await c.close()


def register(service: str, instance: str, *, permits: int = 10) -> hub_pb2.ClientMessage:
    return hub_pb2.ClientMessage(
        register=hub_pb2.Register(service_name=service, instance_id=instance, initial_permits=permits)
    )


def heartbeat(ms: int) -> hub_pb2.ClientMessage:
    return hub_pb2.ClientMessage(heartbeat=hub_pb2.Heartbeat(sent_at_ms=ms))


async def raises_at_once(step: Callable[[], Awaitable[object]], within: float = 0.1) -> str:
    """Runs step, which must fail with an AssertionError well before its own bound; returns the text."""
    start = time.monotonic()
    with pytest.raises(AssertionError) as e:
        await step()
    assert time.monotonic() - start < within, f"failed after {time.monotonic() - start:.3f} s"
    return str(e.value)


async def test_a_stream_is_scripted_frame_by_frame(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    await c.write(heartbeat(1))
    s = await hub.next_stream()
    assert s.index == 0
    assert (await s.recv_register()).service_name == "orders-service"
    assert (await s.recv()).WhichOneof("message") == "heartbeat"  # consume it before the ack (see the comment at ack)
    await s.ack(ack("hub-1"))
    assert (await c.read()).register_ack.hub_id == "hub-1"  # the ack first,
    assert (await c.read()).heartbeat.sent_at_ms == 1  # then the echo of the early heartbeat, as snodra-server
    await c.write(heartbeat(7))
    assert (await c.read()).heartbeat.sent_at_ms == 7  # echoed after the ack
    await s.close(grpc.StatusCode.ALREADY_EXISTS, "taken")
    with pytest.raises(grpc.aio.AioRpcError) as e:
        await c.read()
    assert e.value.code() == grpc.StatusCode.ALREADY_EXISTS
    assert e.value.details() == "taken"


async def test_closing_with_ok_ends_the_stream_cleanly(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    await s.close(grpc.StatusCode.OK, "")
    assert await c.status() == grpc.StatusCode.OK
    assert "stream ended: OK" in await raises_at_once(s.recv)


def test_the_ack_has_the_hub_defaults() -> None:
    a = ack("hub-9")
    assert (a.hub_id, a.heartbeat_interval_ms, a.heartbeat_timeout_ms) == ("hub-9", 3000, 10000)
    assert (a.default_timeout_ms, a.max_timeout_ms, a.context) == (5000, 60000, "default")
    assert dict(a.type_timeouts_ms) == {}
    b = ack("hub-2", interval_ms=100, timeout_ms=400, default_ms=200, max_ms=900, type_timeouts_ms={"orders.Slow": 7})
    assert (b.heartbeat_interval_ms, b.heartbeat_timeout_ms, b.default_timeout_ms, b.max_timeout_ms) == (
        100,
        400,
        200,
        900,
    )
    assert dict(b.type_timeouts_ms) == {"orders.Slow": 7}


def test_the_frame_builders() -> None:
    r = hub_pb2.Request(id="r-1", type="orders.PlaceOrder")
    assert forward(r).request == r
    assert (flow("r-1", 5).flow.request_id, flow("r-1", 5).flow.credits) == ("r-1", 5)
    assert (cancel("r-1", "gone").cancel.request_id, cancel("r-1", "gone").cancel.reason) == ("r-1", "gone")
    e = hub_error("r-1", hub_pb2.NO_HANDLER, "nobody").error
    assert (e.request_id, e.code, e.message) == ("r-1", hub_pb2.NO_HANDLER, "nobody")


async def test_hub_frames_reach_the_client_in_order(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))
    await s.send(forward(hub_pb2.Request(id="r-1", type="orders.PlaceOrder")))
    await s.send(flow("r-1", 2))
    await s.send(cancel("r-1", "x"))
    await s.send(hub_error("r-2", hub_pb2.TIMEOUT, "late"))
    assert (await c.read()).register_ack.hub_id == "hub-1"
    assert (await c.read()).request.id == "r-1"
    assert (await c.read()).flow.credits == 2
    assert (await c.read()).cancel.reason == "x"
    assert (await c.read()).error.code == hub_pb2.TIMEOUT


async def test_recv_helpers_skip_heartbeats_and_statuses(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    before = time.monotonic()
    await c.write(register("orders-service", "i-1"))
    await c.write(heartbeat(1))
    await c.write(hub_pb2.ClientMessage(status=hub_pb2.InstanceStatus(handlers_in_flight=2)))
    await c.write(hub_pb2.ClientMessage(permits=hub_pb2.Permits(count=4)))
    await c.write(hub_pb2.ClientMessage(flow=hub_pb2.Flow(request_id="x", credits=1)))
    await c.write(hub_pb2.ClientMessage(credentials=hub_pb2.Credentials(token="t")))
    s = await hub.next_stream()
    await s.recv_register()
    assert (await s.recv_permits()).count == 4
    assert (await s.recv_flow()).credits == 1
    assert (await s.recv_credentials()).token == "t"
    assert len(s.heartbeats) == 1
    assert before <= s.heartbeats[0] <= time.monotonic()
    assert len(s.statuses) == 1
    at, status = s.statuses[0]
    assert before <= at <= time.monotonic()
    assert status.handlers_in_flight == 2


async def test_every_recv_helper_returns_its_kind(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    for m in (
        hub_pb2.ClientMessage(request=hub_pb2.Request(id="a")),
        hub_pb2.ClientMessage(reply=hub_pb2.Reply(request_id="b")),
        hub_pb2.ClientMessage(part=hub_pb2.ReplyPart(request_id="c")),
        hub_pb2.ClientMessage(end=hub_pb2.ReplyEnd(request_id="d")),
        hub_pb2.ClientMessage(cancel=hub_pb2.Cancel(request_id="e")),
    ):
        await c.write(m)
    s = await hub.next_stream()
    await s.recv_register()
    assert (await s.recv_request()).id == "a"
    assert (await s.recv_reply()).request_id == "b"
    assert (await s.recv_part()).request_id == "c"
    assert (await s.recv_end()).request_id == "d"
    assert (await s.recv_cancel()).request_id == "e"


async def test_a_recv_helper_fails_on_another_kind(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    with pytest.raises(AssertionError, match="expected a request frame, got register"):
        await s.recv_request()


async def test_a_recv_helper_gives_up_at_its_bound_despite_heartbeats(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()

    async def beat() -> None:
        while True:
            await c.write(heartbeat(1))
            await asyncio.sleep(0.05)  # the pace of the client under test, not a synchronisation

    beating = asyncio.create_task(beat(), name="test:beat")
    try:
        start = time.monotonic()
        with pytest.raises(AssertionError, match="no reply frame within"):
            await s.recv_reply(within=0.5)
        assert time.monotonic() - start < 1.5
        assert len(s.heartbeats) >= 2
    finally:
        beating.cancel()
        await asyncio.gather(beating, return_exceptions=True)


async def test_recv_fails_at_its_bound(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    with pytest.raises(AssertionError, match=r"no frame within 0\.1 s"):
        await s.recv(within=0.1)


async def test_nothing_fails_on_a_matching_kind_and_keeps_the_rest(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    await c.write(hub_pb2.ClientMessage(permits=hub_pb2.Permits(count=3)))
    await s.nothing(0.1, "request")  # a Permits frame is not a request: kept for later
    assert (await s.recv_permits()).count == 3
    await c.write(heartbeat(1))
    await s.nothing(0.1)  # with no kinds: everything but heartbeats and statuses
    await c.write(hub_pb2.ClientMessage(request=hub_pb2.Request(id="r")))
    start = time.monotonic()
    with pytest.raises(AssertionError, match="unexpected request frame"):
        await s.nothing(5.0, "request")
    assert time.monotonic() - start < 1.0
    with pytest.raises(AssertionError, match="unknown frame kind"):
        await s.nothing(0.1, "requests")


async def test_no_echo_before_the_ack(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    await c.write(heartbeat(1))
    assert (await s.recv()).heartbeat.sent_at_ms == 1
    await c.nothing(0.2)
    await s.ack(ack("hub-1"))
    assert (await c.read()).register_ack.hub_id == "hub-1"  # no echo of heartbeat 1 came first:
    assert (await c.read()).heartbeat.sent_at_ms == 1  # it follows the ack

    c2 = await dial(hub.address)
    await c2.write(register("orders-service", "i-2"))
    s2 = await hub.next_stream()
    await s2.recv_register()
    s2.echo_heartbeats(True)
    await c2.write(heartbeat(2))
    assert (await c2.read()).heartbeat.sent_at_ms == 2  # echoed before any ack


async def test_a_silent_hub_drops_the_held_echoes(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    await c.write(heartbeat(1))
    s = await hub.next_stream()
    await s.recv_register()
    assert (await s.recv()).heartbeat.sent_at_ms == 1
    s.echo_heartbeats(False)
    await s.ack(ack("hub-1"))
    await c.write(heartbeat(2))
    assert (await s.recv()).heartbeat.sent_at_ms == 2
    await s.send(flow("r", 1))
    assert (await c.read()).register_ack.hub_id == "hub-1"
    assert (await c.read()).flow.request_id == "r"  # neither heartbeat was echoed


async def test_every_heartbeat_after_the_register_is_echoed_whatever_the_script_does(hub: FakeHub, dial: Dial) -> None:
    # Whether the pump or the script runs first, the client sees the ack and then every echo, in order.
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    for ms in (1, 2, 3):
        await c.write(heartbeat(ms))
    await s.ack(ack("hub-1"))
    assert (await c.read()).register_ack.hub_id == "hub-1"
    assert [(await c.read()).heartbeat.sent_at_ms for _ in range(3)] == [1, 2, 3]


async def test_the_reader_never_waits_on_a_client_that_does_not_read(hub: FakeHub) -> None:
    # A raw call nobody reads from: the hub's writes back up, its echoes are dropped, and recv keeps working.
    async with grpc.aio.insecure_channel(hub.address) as channel:
        stub = hub_pb2_grpc.HubStub(channel)  # type: ignore[no-untyped-call]
        call: grpc.aio.StreamStreamCall[hub_pb2.ClientMessage, hub_pb2.ServerMessage] = stub.Connect()
        await call.write(register("orders-service", "i-1"))
        s = await hub.next_stream()
        await s.recv_register()
        await s.ack(ack("hub-1"))
        big = forward(hub_pb2.Request(id="r", payload=b"x" * (1 << 20)))
        flooding = asyncio.create_task(_flood(s, big), name="test:flood")
        try:
            for ms in range(1, 201):
                await call.write(heartbeat(ms))
                assert (await s.recv()).heartbeat.sent_at_ms == ms
            assert not flooding.done()  # the writes are stuck: the client reads nothing
        finally:
            flooding.cancel()
            await asyncio.gather(flooding, return_exceptions=True)
            call.cancel()


async def _flood(s: FakeStream, frame: hub_pb2.ServerMessage) -> None:
    while True:
        await s.send(frame)


async def test_echo_can_be_switched_off_after_the_ack(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))
    await c.read()
    s.echo_heartbeats(False)
    await c.write(heartbeat(1))
    assert (await s.recv()).heartbeat.sent_at_ms == 1
    s.echo_heartbeats(True)
    await c.write(heartbeat(2))
    assert (await c.read()).heartbeat.sent_at_ms == 2  # 1 was never echoed


@pytest.mark.parametrize(
    ("first", "code"),
    [
        (heartbeat(1), grpc.StatusCode.FAILED_PRECONDITION),
        (register("orders-service", "i-1", permits=0), grpc.StatusCode.INVALID_ARGUMENT),
        (register("", "i-1"), grpc.StatusCode.INVALID_ARGUMENT),
        (register("orders-service", ""), grpc.StatusCode.INVALID_ARGUMENT),
        (register("snodra-server", "i-1"), grpc.StatusCode.INVALID_ARGUMENT),
        (register("orders-service", "snodra-server/x"), grpc.StatusCode.INVALID_ARGUMENT),
        (
            hub_pb2.ClientMessage(
                register=hub_pb2.Register(
                    service_name="orders-service",
                    instance_id="i-1",
                    initial_permits=1,
                    command_types=["orders.Watch"],
                    subscription_query_types=["orders.Watch"],
                )
            ),
            grpc.StatusCode.INVALID_ARGUMENT,
        ),
    ],
    ids=[
        "heartbeat-first",
        "no-permits",
        "empty-service",
        "empty-instance",
        "reserved-service",
        "reserved-instance",
        "subscription-not-a-query",
    ],
)
async def test_refusals_as_snodra_server(
    hub: FakeHub, dial: Dial, first: hub_pb2.ClientMessage, code: grpc.StatusCode
) -> None:
    c = await dial(hub.address)
    await c.write(first)
    assert await c.status() == code
    s = await hub.next_stream()
    assert await s.recv() == first  # the offending frame stays readable
    want = f"stream ended: {code.name}"
    assert want in await raises_at_once(s.recv)
    assert want in await raises_at_once(lambda: s.recv_register(within=3.0))
    assert want in await raises_at_once(lambda: s.nothing(3.0))
    assert want in await raises_at_once(lambda: s.half_closed(3.0))


async def test_nothing_fails_on_an_ended_stream(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    await c.done_writing()
    await s.half_closed(5.0)
    assert "stream ended: the client half-closed" in await raises_at_once(lambda: s.nothing(3.0))

    # A stream that ends during the wait fails it at once, not at its end.
    c2 = await dial(hub.address)
    await c2.write(register("orders-service", "i-2"))
    s2 = await hub.next_stream()
    await s2.recv_register()
    waiting = asyncio.create_task(s2.nothing(5.0), name="test:nothing")
    try:
        await asyncio.sleep(0)  # let the wait begin; the outcome does not depend on it
        c2.cancel()
        start = time.monotonic()
        # The text shows it failed on the end of the frames, before the hub could tell how the client ended.
        with pytest.raises(AssertionError, match="stream ended: the client half-closed or left"):
            await waiting
        assert time.monotonic() - start < 2.0
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)


async def test_half_closed(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    with pytest.raises(AssertionError, match="did not half-close within"):
        await s.half_closed(0.05)
    await s.ack(ack("hub-1"))
    await c.done_writing()
    await s.half_closed(5.0)
    # As snodra-server, the hub ends a half-closed stream with OK; what was written before reaches the client.
    assert (await c.read()).register_ack.hub_id == "hub-1"
    assert await c.status() == grpc.StatusCode.OK
    assert "stream ended: the client half-closed" in await raises_at_once(s.recv)
    with pytest.raises(AssertionError, match="stream ended"):
        await s.send(flow("r", 1))
    await s.close(grpc.StatusCode.ABORTED, "late")  # too late: a no-op


async def test_a_half_close_before_any_frame_is_refused(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.done_writing()
    s = await hub.next_stream()
    await s.half_closed(5.0)
    assert await c.status() == grpc.StatusCode.FAILED_PRECONDITION
    assert "stream ended: FAILED_PRECONDITION: first message must be Register" in await raises_at_once(s.recv)


def _types(
    commands: Sequence[str] = (),
    queries: Sequence[str] = (),
    shared: Sequence[str] = (),
    subscriptions: Sequence[str] = (),
    *,
    service: str = "orders-service",
    instance: str = "i-1",
    permits: int = 1,
) -> hub_pb2.ClientMessage:
    return hub_pb2.ClientMessage(
        register=hub_pb2.Register(
            service_name=service,
            instance_id=instance,
            initial_permits=permits,
            command_types=commands,
            query_types=queries,
            shared_query_types=shared,
            subscription_query_types=subscriptions,
        )
    )


@pytest.mark.parametrize(
    ("later", "details"),
    [
        (_types(["orders.X"], ["orders.X"]), "type orders.X listed as both command and query"),
        (
            _types([], ["orders.X"], ["orders.X"]),
            "type orders.X listed as a shared query type and as a command or exclusive query",
        ),
        (
            _types(["orders.X"], [], ["orders.X"]),
            "type orders.X listed as a shared query type and as a command or exclusive query",
        ),
        (
            _types(["orders.Watch"], [], [], ["orders.Watch"]),
            "type orders.Watch listed as a subscription type but not as a query type",
        ),
        (  # an empty Register that keeps its subscription types
            _types([], [], [], ["orders.Watch"]),
            "type orders.Watch listed as a subscription type but not as a query type",
        ),
        (  # the hub's order: the kinds first, then the subscriptions, which it reads sorted
            _types(["orders.X"], ["orders.X"], [], ["orders.B", "orders.A"]),
            "type orders.X listed as both command and query",
        ),
        (
            _types([], ["orders.B"], [], ["orders.B", "orders.C", "orders.A"]),
            "type orders.A listed as a subscription type but not as a query type",
        ),
    ],
    ids=[
        "command-and-query",
        "shared-and-exclusive",
        "shared-and-command",
        "subscription-a-command",
        "empty-keeping-subscriptions",
        "kinds-before-subscriptions",
        "subscriptions-sorted",
    ],
)
async def test_every_register_has_its_type_lists_checked(
    hub: FakeHub, dial: Dial, later: hub_pb2.ClientMessage, details: str
) -> None:
    c = await dial(hub.address)
    await c.write(_types([], ["orders.Watch"], [], ["orders.Watch"]))
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))
    await c.write(later)
    assert await s.recv() == later
    assert f"stream ended: INVALID_ARGUMENT: {details}" in await raises_at_once(s.recv)
    assert (await c.read()).register_ack.hub_id == "hub-1"
    with pytest.raises(grpc.aio.AioRpcError) as e:
        await c.read()
    assert (e.value.code(), e.value.details()) == (grpc.StatusCode.INVALID_ARGUMENT, details)


async def test_the_first_register_has_its_type_lists_checked_too(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(_types(["orders.X"], ["orders.X"]))
    assert await c.status() == grpc.StatusCode.INVALID_ARGUMENT
    s = await hub.next_stream()
    await s.recv_register()
    assert "type orders.X listed as both command and query" in await raises_at_once(s.recv)


async def test_a_later_register_is_checked_for_its_types_only(hub: FakeHub, dial: Dial) -> None:
    # As snodra-server: the identity and the permits are read from the first Register only.
    c = await dial(hub.address)
    await c.write(_types([], ["orders.Watch"], [], ["orders.Watch"]))
    s = await hub.next_stream()
    await s.recv_register()
    await c.write(_types(service="", instance="", permits=0))  # an empty Register: a graceful stop
    assert (await s.recv_register()).command_types == []
    await s.nothing(0.1)
    await c.write(_types(["orders.PlaceOrder"], ["orders.Watch"], ["orders.Stock"], ["orders.Watch"]))
    assert list((await s.recv_register()).shared_query_types) == ["orders.Stock"]
    await s.nothing(0.1)


async def test_recv_after_the_client_left_fails(hub: FakeHub, dial: Dial) -> None:
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    s = await hub.next_stream()
    await s.recv_register()
    c.cancel()
    with pytest.raises(AssertionError, match="stream ended: CANCELLED"):
        await s.recv(within=5.0)
    with pytest.raises(AssertionError, match=r"stream ended: CANCELLED.*\(no half-close\)"):
        await s.half_closed(5.0)  # a client that left did not half-close
    with pytest.raises(AssertionError, match="stream ended"):
        await s.send(flow("r", 1))


async def test_streams_are_numbered_and_no_stream_within(hub: FakeHub, dial: Dial) -> None:
    await hub.no_stream_within(0.05)
    c1 = await dial(hub.address)
    await c1.write(register("orders-service", "i-1"))
    c2 = await dial(hub.address)
    await c2.write(register("orders-service", "i-2"))
    a = await hub.next_stream()
    b = await hub.next_stream()
    assert (a.index, b.index) == (0, 1)
    c3 = await dial(hub.address)
    await c3.write(register("orders-service", "i-3"))
    start = time.monotonic()
    with pytest.raises(AssertionError, match="unexpected stream 2"):
        await hub.no_stream_within(5.0)
    assert time.monotonic() - start < 2.0
    with pytest.raises(AssertionError, match=r"no stream within 0\.05 s"):
        await hub.next_stream(within=0.05)


async def test_refuse_down_and_up(hub: FakeHub, dial: Dial) -> None:
    hub.refuse(grpc.StatusCode.UNAVAILABLE, "busy")
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    assert await c.status() == grpc.StatusCode.UNAVAILABLE
    await hub.no_stream_within(0.05)  # a refused stream is never handed to the script

    hub.accept()
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-1"))
    live = await hub.next_stream()
    assert live.index == 0  # refused streams are not counted
    await live.recv_register()

    address = hub.address
    host, port = address.rsplit(":", 1)
    await hub.down()
    assert await c.status() == grpc.StatusCode.UNAVAILABLE  # live streams are severed
    with pytest.raises(AssertionError, match="stream ended: CANCELLED"):
        await live.recv()
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection(host, int(port))
    await hub.down()  # no-op while down

    await hub.up()
    await hub.up()  # no-op while up
    assert hub.address == address
    c = await dial(hub.address)
    await c.write(register("orders-service", "i-2"))
    s = await hub.next_stream()
    assert s.index == 1
    assert (await s.recv_register()).instance_id == "i-2"


async def test_two_hubs_listen_apart(hub: FakeHub, hub2: FakeHub, dial: Dial) -> None:
    assert hub.address != hub2.address
    c = await dial(hub2.address)
    await c.write(register("orders-service", "i-1"))
    assert (await (await hub2.next_stream()).recv_register()).instance_id == "i-1"
    await hub.no_stream_within(0.05)


async def test_topology_is_empty(hub: FakeHub) -> None:
    async with grpc.aio.insecure_channel(hub.address) as channel:
        stub = hub_pb2_grpc.HubStub(channel)  # type: ignore[no-untyped-call]
        r = await stub.Topology(hub_pb2.TopologyRequest(), timeout=5.0)
    assert list(r.services) == []


async def test_stop_ends_every_stream_and_leaves_no_task(dial: Dial) -> None:
    hub = await FakeHub.start()
    try:
        taken = await dial(hub.address)
        await taken.write(register("orders-service", "i-1"))
        s = await hub.next_stream()
        await s.recv_register()
        untaken = await dial(hub.address)
        await untaken.write(register("orders-service", "i-2"))  # never taken by the script
    finally:
        await hub.stop()
    await hub.stop()  # idempotent
    assert await taken.status() == grpc.StatusCode.UNAVAILABLE
    assert await untaken.status() == grpc.StatusCode.UNAVAILABLE
    with pytest.raises(AssertionError, match="stream ended"):
        await s.recv()
    assert [t.get_name() for t in asyncio.all_tasks() if t.get_name().startswith("fakehub:")] == []


async def test_tls_and_the_client_certificate(dial: Dial) -> None:
    ca = make_ca("test-ca")
    client = ca.issue_client("orders-service")
    hub = await FakeHub.start(tls=ServerTls(ca.issue_server("hub.test"), ca))
    try:
        creds = grpc.ssl_channel_credentials(
            root_certificates=ca.cert_pem, private_key=client.key_pem, certificate_chain=client.cert_pem
        )
        c = await dial(hub.address, creds, [("grpc.ssl_target_name_override", "hub.test")])
        await c.write(register("orders-service", "i-1"))
        s = await hub.next_stream()
        await s.recv_register()
        leaf = s.peer_certificates[0]
        assert leaf == client.certificate
        sans = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        assert sans.get_values_for_type(x509.UniformResourceIdentifier) == ["snodra://service/orders-service"]

        # Without a client certificate the hub refuses the handshake.
        bare = await dial(
            hub.address,
            grpc.ssl_channel_credentials(root_certificates=ca.cert_pem),
            [("grpc.ssl_target_name_override", "hub.test")],
        )
        assert await bare.status() == grpc.StatusCode.UNAVAILABLE
    finally:
        await hub.stop()


async def test_tls_without_a_client_certificate(dial: Dial) -> None:
    ca = make_ca("test-ca")
    hub = await FakeHub.start(tls=ServerTls(ca.issue_server("hub.test"), None))
    try:
        creds = grpc.ssl_channel_credentials(root_certificates=ca.cert_pem)
        c = await dial(hub.address, creds, [("grpc.ssl_target_name_override", "hub.test")])
        await c.write(register("orders-service", "i-1"))
        s = await hub.next_stream()
        await s.recv_register()
        assert s.peer_certificates == []
        # The server certificate names the DNS name it was issued for, and nothing else.
        other = await dial(hub.address, creds, [("grpc.ssl_target_name_override", "other.test")])
        assert await other.status() == grpc.StatusCode.UNAVAILABLE
    finally:
        await hub.stop()


def test_issued_certificates() -> None:
    ca = make_ca("test-ca")
    assert ca.certificate.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
    server = ca.issue_server("hub.test").certificate
    assert server.issuer == ca.certificate.subject
    sans = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert sans.get_values_for_type(x509.DNSName) == ["hub.test"]
    now = datetime.now(UTC)
    assert server.not_valid_before_utc <= now <= server.not_valid_after_utc <= now + timedelta(hours=1, minutes=1)
    expired = ca.issue_client("orders-service", not_after=now - timedelta(minutes=5)).certificate
    assert expired.not_valid_after_utc == (now - timedelta(minutes=5)).replace(microsecond=0)
    assert expired.not_valid_before_utc < expired.not_valid_after_utc
    soon = ca.issue_client("orders-service", not_after=now + timedelta(seconds=30)).certificate
    assert soon.not_valid_before_utc <= now
    keys = {k.public_key().public_numbers().n for k in (ca.key, ca.issue_client("a").key, ca.issue_client("b").key)}
    assert len(keys) == 3  # no key is used twice
