"""Requests the instance answers itself: a command of an own command type and a point query of an own query type run
in-process, through the same checks as a forwarded request, and never reach a hub (unless always routed through it).

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Requests a service answers itself").
"""

import asyncio
import json
import time

import pytest
from fakehub import FakeHub, forward
from helpers import (
    ISSUER,
    TP,
    CancelOrder,
    GetOrder,
    Jwks,
    Placed,
    PlaceOrder,
    acknowledged,
    new_app,
    next_reply,
)
from snodra import (
    Caller,
    DecodeError,
    ErrorCode,
    HandlerError,
    HubError,
    InvalidRequest,
    MessageInfo,
    NeverSent,
    authenticated,
    current_caller,
    current_message,
)
from snodra._hubpb import hub_pb2


def _handled(
    id: str, type: str, headers: dict[str, str], payload: bytes = b'{"orderId":"o-1"}'
) -> hub_pb2.ServerMessage:
    return forward(hub_pb2.Request(id=id, kind=hub_pb2.COMMAND, type=type, payload=payload, headers=headers))


async def test_own_types_are_answered_in_process(hub: FakeHub) -> None:
    app = new_app(hub)
    seen: list[MessageInfo | None] = []

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        seen.append(current_message())
        if cmd.order_id == "raise":
            raise RuntimeError("out of stock")
        if cmd.order_id == "details":
            raise HandlerError("refused", details={"reason": "stock"})
        if cmd.order_id == "invalid":
            raise InvalidRequest("no lines")
        return Placed(cmd.order_id, len(cmd.lines))

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 7)

    await app.start()
    s = await acknowledged(hub)
    assert await app.send(PlaceOrder("o-1", ["a", "b"]), Placed, routing_key="o-1") == Placed("o-1", 2)
    assert await app.send(PlaceOrder("o-2", [])) is None  # type: ignore[func-returns-value]
    assert await app.query(GetOrder("o-3"), Placed) == Placed("o-3", 7)
    with pytest.raises(HubError) as raised:
        await app.send(PlaceOrder("raise", []), Placed)
    assert (raised.value.code, raised.value.message) == (ErrorCode.HANDLER_ERROR, "RuntimeError: out of stock")
    with pytest.raises(HubError) as raised:
        await app.send(PlaceOrder("details", []), Placed)
    assert (raised.value.code, raised.value.message) == (ErrorCode.HANDLER_ERROR, "refused")
    assert json.loads(raised.value.details) == {"reason": "stock"}
    with pytest.raises(HubError) as raised:
        await app.send(PlaceOrder("invalid", []), Placed)
    assert (raised.value.code, raised.value.message, raised.value.details) == (
        ErrorCode.INVALID_REQUEST,
        "no lines",
        b"",
    )
    with pytest.raises(DecodeError):
        await app.query(GetOrder("o-4"), PlaceOrder)  # the answer does not decode as the result asked for
    await s.nothing(0.2, "request", "permits")  # the hub saw nothing, and no permit came back

    info = seen[0]
    assert info is not None
    assert info.type == "orders.PlaceOrder"
    assert seen[1] is not None
    assert info.id != seen[1].id  # a fresh id per call
    assert info.correlation_id == info.headers["correlationId"]
    assert set(info.headers) == {"correlationId"}  # outside any handling: a fresh correlation id only
    assert current_message() is None  # the caller's context is left as it was


async def test_an_in_process_handler_sees_the_handled_context(hub: FakeHub) -> None:
    app = new_app(hub)
    seen: list[MessageInfo | None] = []

    @app.command_handler
    async def cancel_order(cmd: CancelOrder) -> None:
        await app.send(PlaceOrder(cmd.order_id, []), headers={"x-tenant": "t-1"})
        assert current_message() is not None
        assert current_message().id == "r-1"  # type: ignore[union-attr]

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        seen.append(current_message())

    await app.start()
    s = await acknowledged(hub)
    await s.send(_handled("r-1", "orders.CancelOrder", {"correlationId": "c-1", "traceparent": TP}))
    r = await next_reply(s)
    assert r.WhichOneof("result") == "payload", r
    [info] = seen
    assert info is not None
    assert info.correlation_id == "c-1"
    assert info.id != "r-1"
    assert dict(info.headers) == {"correlationId": "c-1", "traceparent": TP, "x-tenant": "t-1"}


@pytest.mark.parametrize("forward_jwt", [True, False])
async def test_a_guarded_own_handler_sees_the_caller_the_hub_path_would_carry(
    hub: FakeHub, jwks: Jwks, forward_jwt: bool
) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER, forward_jwt=forward_jwt)
    seen: list[Caller | None] = []
    outcomes: list[object] = []

    @app.command_handler
    async def cancel_order(cmd: CancelOrder) -> None:
        try:
            await app.send(PlaceOrder(cmd.order_id, []))
            outcomes.append("ok")
        except HubError as e:
            outcomes.append((e.code, e.message))

    @app.command_handler(guard=authenticated())
    async def place_order(cmd: PlaceOrder) -> None:
        seen.append(current_caller())

    await app.start()
    s = await acknowledged(hub)
    await s.send(_handled("r-1", "orders.CancelOrder", {"authorization": f"Bearer {jwks.mint()}"}))
    assert (await next_reply(s)).WhichOneof("result") == "payload"
    # Outside any handling: an anonymous caller.
    with pytest.raises(HubError) as raised:
        await app.send(PlaceOrder("o-1", []))
    assert raised.value.code == ErrorCode.UNAUTHENTICATED
    assert raised.value.message.endswith("place_order requires authentication")
    if forward_jwt:  # the handled request's caller
        assert outcomes == ["ok"]
        assert [c.subject if c else None for c in seen] == ["u-1"]
    else:  # the hub path would carry no token: anonymous
        assert outcomes == [(ErrorCode.UNAUTHENTICATED, raised.value.message)]
        assert seen == []


async def test_always_route_through_hub_sends_own_types_through_the_hub(hub: FakeHub) -> None:
    app = new_app(hub, always_route_through_hub=True)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        raise AssertionError("the hub routes it")

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        raise AssertionError("the hub routes it")

    await app.start()
    s = await acknowledged(hub)
    call = asyncio.create_task(app.send(PlaceOrder("o-1", []), Placed, routing_key="o-1"))
    r = await s.recv_request()
    assert (r.type, r.routing_key) == ("orders.PlaceOrder", "o-1")
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=r.id, payload=b'{"orderId":"o-1","total":3}')))
    assert await call == Placed("o-1", 3)
    query = asyncio.create_task(app.query(GetOrder("o-1"), Placed))
    r = await s.recv_request()
    assert r.type == "orders.GetOrder"
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=r.id, payload=b'{"orderId":"o-1","total":4}')))
    assert await query == Placed("o-1", 4)


async def test_an_in_process_call_has_no_timeout_or_retry(hub: FakeHub) -> None:
    app = new_app(hub)
    calls: list[str] = []

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        calls.append(cmd.order_id)
        await asyncio.sleep(0.15)
        if cmd.order_id == "fail":
            raise RuntimeError("once")
        return Placed(cmd.order_id, 1)

    await app.start()
    for timeout in (0.01, 0, -1, None):
        assert await app.send(PlaceOrder("o-1", []), Placed, timeout=timeout) == Placed("o-1", 1)
    from snodra import RetryPolicy

    with pytest.raises(HubError, match="RuntimeError: once"):
        await app.send(PlaceOrder("fail", []), Placed, retry=RetryPolicy(max_attempts=3, initial_backoff=0.01))
    assert calls.count("fail") == 1
    with pytest.raises((TypeError, ValueError)):
        await app.send(PlaceOrder("o-1", []), timeout=float("nan"))  # the options are still checked


async def test_a_cancelled_in_process_caller_cancels_its_handler(hub: FakeHub) -> None:
    app = new_app(hub)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    await app.start()
    call = asyncio.create_task(app.send(PlaceOrder("o-1", [])))
    await asyncio.wait_for(started.wait(), 1.0)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert cancelled.is_set()


async def test_a_draining_handler_can_call_a_local_handler(hub: FakeHub, monkeypatch: pytest.MonkeyPatch) -> None:
    app = new_app(hub)
    started = asyncio.Event()
    release = asyncio.Event()

    @app.command_handler
    async def cancel_order(cmd: CancelOrder) -> Placed:
        started.set()
        await release.wait()
        return await app.query(GetOrder(cmd.order_id), Placed)

    @app.query_handler
    async def get_order(q: GetOrder) -> Placed:
        return Placed(q.order_id, 5)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_handled("r-1", "orders.CancelOrder", {}))
    await asyncio.wait_for(started.wait(), 1.0)
    drain = app._dispatcher.drain
    after: list[asyncio.Task[Placed]] = []

    async def drain_then_call(timeout: float) -> None:  # noqa: ASYNC109 (the drain's bound)
        await drain(timeout)
        after.append(asyncio.create_task(app.query(GetOrder("o-2"), Placed)))  # runs once the drain is over

    monkeypatch.setattr(app._dispatcher, "drain", drain_then_call)
    shutdown = asyncio.create_task(app.shutdown())
    assert not (await s.recv_register()).command_types  # the empty Register: the drain is under way
    release.set()
    r = await next_reply(s)
    assert json.loads(r.payload) == {"orderId": "o-1", "total": 5}
    started_at = time.monotonic()
    await shutdown
    with pytest.raises(NeverSent, match=r"^'orders.GetOrder' not sent: the app is shutting down$"):
        await after[0]
    assert time.monotonic() - started_at < 1.0
