"""Flow control: the permits a handled request gives back, when and on which stream.

See https://snodra.damoreira.com/library-authors/protocol/ ("Flow control").
"""

import asyncio
import statistics
import time
from collections.abc import Sequence
from typing import Any

from fakehub import FakeHub, ack, forward
from helpers import PlaceOrder, acknowledged, eventually, new_app, next_reply, within
from snodra import App
from snodra._connection import _Stream
from snodra._hubpb import hub_pb2
from snodra._permits import PermitLedger


def _place(id: str, order_id: str = "o-1", payload: bytes | None = None) -> hub_pb2.ServerMessage:
    body = payload if payload is not None else b'{"orderId":"%s","lines":[]}' % order_id.encode()
    return forward(hub_pb2.Request(id=id, kind=hub_pb2.COMMAND, type="orders.PlaceOrder", payload=body))


async def _serving(hubs: FakeHub | Sequence[FakeHub], gate: asyncio.Event | None = None, **settings: Any) -> App:
    """An app serving orders.PlaceOrder, started: the order "raise" fails; with a gate, the handler waits for it."""
    app = new_app(hubs, **settings)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        if gate is not None:
            await gate.wait()
        if cmd.order_id == "raise":
            raise RuntimeError("out of stock")

    await app.start()
    return app


def test_permit_flush_rule() -> None:
    one = PermitLedger(1)
    one.arrived()
    assert one.finished() == (1, False)  # one permit: every completion goes back at once
    big = PermitLedger(64)
    for _ in range(10):
        big.arrived()
    assert big.finished() == (0, True)  # 54 held > min(16, 32): batched, the 50 ms timer armed
    n = 0
    for _ in range(15):
        n, armed = big.finished()
        assert not armed  # armed once
    assert n == 16  # 16 pending: written
    assert big.flush() == 0  # the timer then finds nothing pending
    small = PermitLedger(8)
    for _ in range(5):
        small.arrived()
    assert small.finished()[0] == 1  # 3 held <= min(16, 4): at once


def test_the_timer_flushes_what_is_pending() -> None:
    ledger = PermitLedger(64)
    for _ in range(3):
        ledger.arrived()
    assert ledger.finished() == (0, True)
    assert ledger.finished() == (0, False)
    assert ledger.flush() == 2
    assert ledger.finished() == (0, True)  # a new batch arms the timer again
    assert ledger.flush() == 1


def test_returned_permits_count_as_held_again() -> None:
    ledger = PermitLedger(4)  # low-water mark min(16, 2) = 2
    for _ in range(3):
        ledger.arrived()  # the hub holds 1
    assert ledger.finished() == (1, False)  # 1 <= 2: at once; the hub holds 2
    assert ledger.finished() == (1, False)  # 2 <= 2: at once; the hub holds 3
    assert ledger.finished() == (0, True)  # 3 > 2: batched


async def test_one_permit_comes_back_at_once(hub: FakeHub) -> None:
    await _serving(hub, max_concurrent=1)
    s = await acknowledged(hub)
    gaps: list[float] = []
    for i in range(10):
        await s.send(_place(f"r-{i}"))
        assert (await s.recv_reply()).request_id == f"r-{i}"
        replied = time.monotonic()
        assert (await s.recv_permits()).count == 1
        gaps.append(time.monotonic() - replied)
    assert statistics.median(gaps) < 0.035, gaps


async def test_permits_are_batched_for_50ms(hub: FakeHub) -> None:
    await _serving(hub, max_concurrent=64)
    s = await acknowledged(hub)
    await s.send(_place("r-1"))
    assert (await s.recv_reply()).request_id == "r-1"
    replied = time.monotonic()
    assert (await s.recv_permits()).count == 1
    within(time.monotonic() - replied, 0.04, 0.15)


async def test_sixteen_pending_grants_go_at_once(hub: FakeHub) -> None:
    gate = asyncio.Event()
    await _serving(hub, gate, max_concurrent=64)
    s = await acknowledged(hub)
    for i in range(16):
        await s.send(_place(f"r-{i}"))
    gate.set()  # the 16 complete together, within one batching window
    replies = 0
    granted: list[int] = []
    while replies < 16 or sum(granted) < 16:
        m = await s.recv()
        match m.WhichOneof("message"):
            case "reply":
                replies += 1
            case "permits":
                granted.append(m.permits.count)
    assert granted == [16]


async def test_permits_go_back_on_the_arrival_stream(hub: FakeHub, hub2: FakeHub) -> None:
    await _serving([hub, hub2], max_concurrent=1)
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    await s2.send(_place("r-1"))
    assert (await s2.recv_reply()).request_id == "r-1"
    assert (await s2.recv_permits()).count == 1
    await s1.nothing(0.2, "reply", "permits")


async def test_handlers_never_exceed_the_capacity(hub: FakeHub) -> None:
    gate = asyncio.Event()
    running = most = 0
    app = new_app(hub, max_concurrent=2)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        nonlocal running, most
        running += 1
        most = max(most, running)
        try:
            await gate.wait()
        finally:
            running -= 1

    await app.start()
    s = await acknowledged(hub)
    for i in range(4):  # a hub ignoring the permits
        await s.send(_place(f"r-{i}"))
    await eventually(lambda: running == 2)
    await s.nothing(0.1, "reply")  # the two others wait for a local permit
    assert most == 2
    gate.set()
    answered = {(await next_reply(s)).request_id for _ in range(4)}
    assert answered == {"r-0", "r-1", "r-2", "r-3"}
    assert most == 2


async def test_a_permit_comes_back_after_every_failure(hub: FakeHub) -> None:
    await _serving(hub, max_concurrent=1)
    s = await acknowledged(hub)
    unknown = hub_pb2.Request(id="r-1", kind=hub_pb2.COMMAND, type="orders.Unknown", payload=b"{}")
    for frame, code in (
        (forward(unknown), hub_pb2.NO_HANDLER),
        (_place("r-2", payload=b"not json"), hub_pb2.INVALID_REQUEST),
        (_place("r-3", "raise"), hub_pb2.HANDLER_ERROR),
    ):
        await s.send(frame)
        assert (await s.recv_reply()).error.code == code
        assert (await s.recv_permits()).count == 1


async def test_early_requests_count_against_the_permits(hub: FakeHub) -> None:
    await _serving(hub, max_concurrent=2)  # low-water mark min(16, 1) = 1
    s = await hub.next_stream()
    await s.recv_register()
    await s.send(_place("r-1"))
    assert (await s.recv_reply()).request_id == "r-1"
    assert (await s.recv_permits()).count == 1  # the hub held 1 <= 1: at once, before any ack
    await s.ack(ack("hub-1"))


async def test_a_request_without_a_local_permit_waits_for_one(hub: FakeHub) -> None:
    gate = asyncio.Event()
    await _serving(hub, gate, max_concurrent=1)
    s = await acknowledged(hub)
    await s.send(_place("r-1"))
    await s.send(_place("r-2"))  # forwarded without a permit
    await s.nothing(0.1, "reply", "permits")
    gate.set()
    for id in ("r-1", "r-2"):
        assert (await s.recv_reply()).request_id == id
        assert (await s.recv_permits()).count == 1


def test_the_stream_ledger_batches_by_the_timings() -> None:
    stream = _Stream(hub_pb2.Register(initial_permits=64), 1.0, permit_batch=4)
    for _ in range(8):
        stream.permits.arrived()
    assert [stream.permits.finished()[0] for _ in range(4)] == [0, 0, 0, 4]
