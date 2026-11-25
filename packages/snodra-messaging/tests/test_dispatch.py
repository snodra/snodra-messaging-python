"""A forwarded unary request, end to end: lookup, decoding, the handler, the one ``Reply`` and the error mapping.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Handling a request").
"""

import asyncio
import json
import logging
import os
import time
from typing import Any

import grpc
import pytest
from fakehub import FakeHub, ack, cancel, forward
from helpers import GetOrder, Placed, PlaceOrder, acknowledged, eventually, new_app, next_reply, within
from snodra import Codec, HandlerError, JsonCodec, MessageInfo, current_message
from snodra._hubpb import hub_pb2

ORDER = b'{"orderId":"o-1","lines":[]}'


def _request(
    id: str,
    *,
    kind: int = hub_pb2.COMMAND,
    type: str = "orders.PlaceOrder",
    payload: bytes = ORDER,
    headers: dict[str, str] | None = None,
) -> hub_pb2.ServerMessage:
    return forward(hub_pb2.Request(id=id, kind=kind, type=type, payload=payload, headers=headers or {}))  # type: ignore[arg-type]


def _error(r: hub_pb2.Reply) -> tuple[int, str, bytes]:
    assert r.WhichOneof("result") == "error", r
    assert r.error.request_id == r.request_id
    return r.error.code, r.error.message, r.error.details


async def test_a_unary_request_is_answered(hub: FakeHub) -> None:
    app = new_app(hub)
    seen: list[MessageInfo | None] = []

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        seen.append(current_message())
        return Placed(cmd.order_id, 42)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1", headers={"x-tenant": "t-1"}))
    r = await s.recv_reply()
    assert r.request_id == "r-1"
    assert json.loads(r.payload) == {"orderId": "o-1", "total": 42}
    assert seen == [MessageInfo("r-1", "orders.PlaceOrder", "r-1", {"x-tenant": "t-1"})]
    assert current_message() is None  # outside a handler


async def test_a_query_is_answered_by_its_query_handler(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 7)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-2"}'))
    assert json.loads((await s.recv_reply()).payload) == {"orderId": "o-2", "total": 7}


async def test_the_message_info_reaches_the_tasks_the_handler_starts(hub: FakeHub) -> None:
    app = new_app(hub)
    seen: list[MessageInfo | None] = []

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        async def child() -> None:
            seen.append(current_message())

        await asyncio.create_task(child())

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1", headers={"correlationId": "c-9"}))
    await s.recv_reply()
    assert seen == [MessageInfo("r-1", "orders.PlaceOrder", "c-9", {"correlationId": "c-9"})]
    await s.send(_request("r-2", headers={"correlationId": ""}))  # present but empty: kept as it is
    await s.recv_reply()
    assert seen[1] == MessageInfo("r-2", "orders.PlaceOrder", "", {"correlationId": ""})


async def test_a_command_without_result_answers_an_empty_object(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        return None

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1"))
    r = await s.recv_reply()
    assert r.WhichOneof("result") == "payload"
    assert r.payload == b"{}"


async def test_requests_before_the_ack_are_served(hub: FakeHub) -> None:
    app = new_app(hub, max_concurrent=1)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        return Placed(cmd.order_id, 1)

    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    for id in ("r-1", "r-2"):  # the second only once the first one's permit came back
        await s.send(_request(id))
        assert (await s.recv_reply()).request_id == id
        assert (await s.recv_permits()).count == 1
    await s.ack(ack("hub-1"))
    await app.ready(1.0)


async def test_outcomes_are_messages_never_a_dropped_stream(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        if cmd.order_id == "raise":
            raise RuntimeError("out of stock")
        return Placed(cmd.order_id, 1)

    await app.start()
    s = await acknowledged(hub)
    cases: list[tuple[hub_pb2.ServerMessage, int, str]] = [
        (_request("a", type="orders.Unknown"), hub_pb2.NO_HANDLER, "no handler for type orders.Unknown"),
        (_request("b", kind=hub_pb2.QUERY), hub_pb2.NO_HANDLER, "no handler for type orders.PlaceOrder"),
        (
            _request("c", kind=hub_pb2.REQUEST_KIND_UNSPECIFIED),
            hub_pb2.INVALID_REQUEST,
            "unsupported request kind REQUEST_KIND_UNSPECIFIED",
        ),
        (_request("d", kind=7), hub_pb2.INVALID_REQUEST, "unsupported request kind UNRECOGNIZED"),
        (
            _request("e", payload=b'{"orderId":42}'),
            hub_pb2.INVALID_REQUEST,
            "Cannot decode payload as PlaceOrder at orderId: expected a string, found a number",
        ),
        (
            _request("f", payload=b"not json"),
            hub_pb2.INVALID_REQUEST,
            "Cannot decode payload as PlaceOrder: not a JSON document (Expecting value)",
        ),
        (
            _request("g", payload=b'{"orderId":"raise","lines":[]}'),
            hub_pb2.HANDLER_ERROR,
            "RuntimeError: out of stock",
        ),
    ]
    for frame, code, message in cases:
        await s.send(frame)
        r = await next_reply(s)
        assert r.request_id == frame.request.id
        assert _error(r) == (code, message, b"")
        await s.send(_request(f"{frame.request.id}-after"))  # the stream is up and served
        assert json.loads((await next_reply(s)).payload) == {"orderId": "o-1", "total": 1}


async def test_a_handlers_exceptions_become_errors(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        match cmd.order_id:
            case "value":
                raise ValueError("quantity must be positive")
            case "handler":
                raise HandlerError("out of stock", details={"orderId": "o-1", "missing": 2})
            case "lines":
                raise RuntimeError("line one\nline two")
            case "timeout":
                async with asyncio.timeout(0.001):
                    await asyncio.sleep(1)
            case "cancelled":
                # A CancelledError the handler did not get from a cancellation of its own task.
                f: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                f.cancel()
                await f

    await app.start()
    s = await acknowledged(hub)
    expected: dict[str, tuple[int, str, bytes]] = {
        "value": (hub_pb2.HANDLER_ERROR, "quantity must be positive", b"[]"),
        "handler": (hub_pb2.HANDLER_ERROR, "out of stock", b'{"orderId":"o-1","missing":2}'),
        "lines": (hub_pb2.HANDLER_ERROR, "RuntimeError: line one line two", b""),
        "timeout": (hub_pb2.HANDLER_ERROR, "TimeoutError: ", b""),
        "cancelled": (hub_pb2.HANDLER_ERROR, "CancelledError: ", b""),
    }
    for order, outcome in expected.items():
        await s.send(_request(f"r-{order}", payload=b'{"orderId":"%s","lines":[]}' % order.encode()))
        r = await next_reply(s)
        assert r.request_id == f"r-{order}"
        assert _error(r) == outcome


class _FlakyCodec:
    """JSON, except that it fails to read the order "boom" and to write a total of 13."""

    def __init__(self) -> None:
        self._json = JsonCodec()

    def encode(self, value: object) -> bytes:
        if isinstance(value, Placed) and value.total == 13:
            raise RuntimeError("cannot write 13")
        return self._json.encode(value)

    def decode(self, data: bytes, into: type[Any]) -> Any:
        if b"boom" in data:
            raise RuntimeError("codec broke")
        return self._json.decode(data, into)


async def test_a_raising_codec_never_ends_the_read_loop(hub: FakeHub) -> None:
    codec: Codec = _FlakyCodec()
    app = new_app(hub, codec=codec)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        return Placed(cmd.order_id, 13 if cmd.order_id == "thirteen" else 1)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1", payload=b'{"orderId":"boom","lines":[]}'))
    assert _error(await next_reply(s)) == (hub_pb2.HANDLER_ERROR, "RuntimeError: codec broke", b"")
    await s.send(_request("r-2", payload=b'{"orderId":"thirteen","lines":[]}'))
    assert _error(await next_reply(s)) == (hub_pb2.HANDLER_ERROR, "RuntimeError: cannot write 13", b"")
    await s.send(_request("r-3"))
    assert json.loads((await next_reply(s)).payload) == {"orderId": "o-1", "total": 1}


async def test_the_reply_goes_only_to_the_arrival_stream(hub: FakeHub) -> None:
    release = asyncio.Event()
    done = asyncio.Event()
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        await release.wait()
        done.set()
        return Placed(cmd.order_id, 1)

    await app.start()
    s1 = await acknowledged(hub)
    await s1.send(_request("r-1"))
    await s1.close(grpc.StatusCode.UNAVAILABLE, "hub restarting")  # stream 1 lost while the handler runs
    s2 = await acknowledged(hub)
    release.set()
    await asyncio.wait_for(done.wait(), 1.0)
    await s2.nothing(0.4, "reply", "permits")  # neither the reply nor its permit on the new stream
    await s2.send(_request("r-2"))  # the new stream is served
    assert (await next_reply(s2)).request_id == "r-2"


async def test_a_slow_handler_never_delays_heartbeats_or_other_requests(hub: FakeHub) -> None:
    release = asyncio.Event()
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        if cmd.order_id == "slow":
            await release.wait()
        return Placed(cmd.order_id, 1)

    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", interval_ms=50))
    await s.send(_request("slow", payload=b'{"orderId":"slow","lines":[]}'))
    try:
        beats = len(s.heartbeats)
        await eventually(lambda: len(s.heartbeats) >= beats + 4, within=1.0)  # 4 intervals while it runs
        await s.send(_request("fast"))
        assert (await next_reply(s)).request_id == "fast"
    finally:
        release.set()
    assert (await next_reply(s)).request_id == "slow"


async def test_a_cancelled_handler_keeps_its_permit_until_it_ends(hub: FakeHub) -> None:
    release = asyncio.Event()
    caught = asyncio.Event()
    app = new_app(hub, max_concurrent=1)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        if q.order_id == "o-1":
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                caught.set()
                await release.wait()  # goes on after the cancellation, then answers
        return Placed(q.order_id, 1)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("g#1", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-1"}'))
    await eventually(lambda: any(t.get_name().startswith("snodra:handler") for t in asyncio.all_tasks()))
    await s.send(cancel("g#1", "max results"))
    await asyncio.wait_for(caught.wait(), 1.0)
    await s.send(_request("g#2", kind=hub_pb2.QUERY, type="orders.GetOrder", payload=b'{"orderId":"o-2"}'))
    await s.nothing(0.15, "reply", "permits")  # g#1 still runs: no permit back, g#2 waits for the local one
    release.set()
    first = await s.recv()
    while first.WhichOneof("message") in ("heartbeat", "status"):
        first = await s.recv()
    assert first.WhichOneof("message") == "permits", first  # g#1 ended: its permit, never its answer
    assert first.permits.count == 1
    assert (await s.recv_reply()).request_id == "g#2"


async def test_a_request_cancelled_while_waiting_for_a_local_permit_never_runs(hub: FakeHub) -> None:
    release = asyncio.Event()
    ran: list[str] = []
    app = new_app(hub, max_concurrent=1)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        ran.append(q.order_id)
        await release.wait()
        return Placed(q.order_id, 1)

    await app.start()
    s = await acknowledged(hub)
    for id, order in (("g#1", "o-1"), ("g#2", "o-2")):
        payload = b'{"orderId":"%s"}' % order.encode()
        await s.send(_request(id, kind=hub_pb2.QUERY, type="orders.GetOrder", payload=payload))
    await eventually(lambda: ran == ["o-1"])
    await s.send(cancel("g#2", "deadline"))
    assert (await s.recv_permits()).count == 1  # g#2's permit, at once
    release.set()
    assert (await s.recv_reply()).request_id == "g#1"
    assert (await s.recv_permits()).count == 1
    await s.nothing(0.1, "reply")
    assert ran == ["o-1"]


async def test_shutdown_drains_the_handlers_in_flight(hub: FakeHub) -> None:
    release = asyncio.Event()
    started = asyncio.Event()
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        started.set()
        await release.wait()
        return Placed(cmd.order_id, 1)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1"))
    await asyncio.wait_for(started.wait(), 1.0)
    leaving = asyncio.create_task(app.shutdown())
    register = await s.recv_register()
    assert not register.command_types  # the empty Register first
    await s.ack(ack("hub-1"))
    await s.nothing(0.15, "reply")  # the drain waits for the handler; nothing half-closes meanwhile
    release.set()
    assert (await next_reply(s)).request_id == "r-1"  # answered before the half-close
    await s.half_closed(within=1.0)
    await asyncio.wait_for(leaving, 1.0)


async def test_the_drain_cancels_the_handlers_still_running_after_its_bound(
    hub: FakeHub, caplog: pytest.LogCaptureFixture
) -> None:
    cancelled = asyncio.Event()
    started = asyncio.Event()
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1"))
    await asyncio.wait_for(started.wait(), 1.0)
    began = time.monotonic()
    with caplog.at_level(logging.WARNING, logger="snodra"):
        await app.shutdown(timeout=5.0)
    within(time.monotonic() - began, 1.0, 1.0 + 0.1 + 0.2 + 0.3)  # the drain's 1 s (scaled), the ack and close waits
    assert cancelled.is_set()
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len([w for w in warnings if "place_order" in w]) == 1, warnings
    assert not (await s.recv_register()).command_types


async def test_shutdown_returns_by_its_deadline_with_a_handler_ignoring_cancellation(
    hub: FakeHub, caplog: pytest.LogCaptureFixture
) -> None:
    stop = False
    started = asyncio.Event()
    app = new_app(hub)

    @app.command_handler
    async def stubborn(cmd: PlaceOrder) -> None:
        started.set()
        while not stop:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                continue

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1"))
    await asyncio.wait_for(started.wait(), 1.0)
    try:
        began = time.monotonic()
        with caplog.at_level(logging.WARNING, logger="snodra"):
            await app.shutdown(timeout=0.3)
        assert time.monotonic() - began < 0.45
        assert not (await s.recv_register()).command_types  # the hub saw the empty Register first
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len([w for w in warnings if "stubborn" in w]) == 1, warnings
    finally:
        # The handler outlives the shutdown by design; end it here so the test leaves no task behind.
        stop = True
        for t in asyncio.all_tasks():
            if t.get_name().startswith("snodra:handler"):
                t.cancel()
                await asyncio.wait([t], timeout=1.0)


async def test_no_task_is_left_when_the_shutdown_budget_cuts_the_drain(hub: FakeHub) -> None:
    app = new_app(hub)  # 256 handlers at once: a finished request's permit waits for the batch timer
    started = asyncio.Event()

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        started.set()
        await asyncio.Event().wait()  # until cancelled

    await app.start()
    s = await acknowledged(hub)
    await s.send(_request("r-1"))
    await asyncio.wait_for(started.wait(), 1.0)
    await app.shutdown(timeout=0.3)
    current = asyncio.current_task()
    left = sorted(
        t.get_name() for t in asyncio.all_tasks() if t is not current and not t.done() and not _of_the_fake_hub(t)
    )
    assert left == [], f"left after shutdown: {left}"


def _of_the_fake_hub(t: asyncio.Task[Any]) -> bool:
    """A task of the in-process fake hub (its own code or grpc's server), which runs on until the test ends."""
    code = getattr(t.get_coro(), "cr_code", None)
    path = "" if code is None else code.co_filename
    return "fakehub" in path or f"{os.sep}grpc{os.sep}" in path


def test_message_info_is_hashable() -> None:
    a = MessageInfo("r-1", "orders.PlaceOrder", "c-1", {"x-tenant": "t-1"})
    b = MessageInfo("r-1", "orders.PlaceOrder", "c-1", {"x-tenant": "t-1"})
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b, MessageInfo("r-2", "orders.PlaceOrder", "c-1", {})}) == 2
