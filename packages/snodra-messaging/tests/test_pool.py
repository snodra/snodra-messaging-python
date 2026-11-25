"""Several hubs: one stream per hub id, rendezvous and round-robin routing, the permit split and its top-ups, failover
of a request never written, and the ``dns:///`` answer.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Several hubs", "Instance status", "Addresses").
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import grpc
import pytest
from certs import Ca, KeyPair, ServerTls
from fakehub import FakeHub, FakeStream, ack, hub_error
from helpers import (
    SCALED,
    ListOrders,
    Placed,
    PlaceOrder,
    WatchInitial,
    WatchOrders,
    WatchUpdate,
    acknowledged,
    eventually,
    new_app,
    start_app,
    within,
)
from snodra import App, ClientCertificate, ErrorCode, HubError, NeverSent, OutcomeUnknown, Settings, TLSConfig
from snodra._connection import HubConnection
from snodra._hubpb import hub_pb2
from snodra._permits import PermitLedger
from snodra._rendezvous import rank

PLACE = PlaceOrder("o-1", ["x"])


def _reply(request_id: str) -> hub_pb2.ServerMessage:
    reply = hub_pb2.Reply(request_id=request_id, payload=b'{"orderId":"o-1","total":1}')
    return hub_pb2.ServerMessage(reply=reply)


async def _next_request(s: FakeStream, within: float = 5.0) -> hub_pb2.Request:
    """The next ``Request`` on s, skipping the heartbeats, statuses, ``Permits`` and the ``Flow`` a stream's refill
    leaves behind; any other frame fails."""
    deadline = time.monotonic() + within
    while True:
        m = await s.recv(within=max(0.0, deadline - time.monotonic()))
        kind = m.WhichOneof("message")
        if kind == "request":
            return m.request
        if kind not in ("heartbeat", "status", "permits", "flow"):
            raise AssertionError(f"expected a request, got {kind}: {m}")


def _connection(app: App, address: str) -> HubConnection:
    return next(c for c in app._connections() if c.address == address)


def _pending(app: App) -> dict[str, object]:
    return dict(app._sender.pending)


def _buffered(app: App) -> bool:
    """A request waits in the outbound buffer of one of the app's streams, not yet written."""
    return _buffered_on(app) is not None


def _buffered_on(app: App) -> HubConnection | None:
    """The connection whose stream holds a request not yet written."""
    for connection in app._connections():
        stream = connection._stream
        if stream is not None and stream.buffered:
            return connection
    return None


async def _pair(app: App, hub: FakeHub, hub2: FakeHub) -> tuple[FakeStream, FakeStream]:
    """Both hubs' streams, acknowledged as hub-1 and hub-2, and both live."""
    s1, s2 = await acknowledged(hub, "hub-1"), await acknowledged(hub2, "hub-2")
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    return s1, s2


async def _two_hubs(hub: FakeHub, hub2: FakeHub, **settings: object) -> tuple[App, FakeStream, FakeStream]:
    app = await start_app([hub, hub2], **settings)  # type: ignore[arg-type]
    s1, s2 = await _pair(app, hub, hub2)
    return app, s1, s2


async def _kinds_until_end(s: FakeStream, within: float = 3.0) -> list[str]:
    """The kinds of every frame left on s, until the stream ends."""
    kinds: list[str] = []
    deadline = time.monotonic() + within
    while True:
        try:
            m = await s.recv(within=max(0.01, deadline - time.monotonic()))
        except AssertionError as e:
            if "stream ended" not in str(e):
                raise
            return kinds
        kinds.append(m.WhichOneof("message") or "empty")


async def _close_held(s: FakeStream, connection: HubConnection) -> None:
    """Closes s while connection's writer is held, waits until the library saw the end, then lets the writer go."""
    stream = connection._stream
    assert stream is not None
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await eventually(stream.ended.is_set)
    connection._block_writer(False)


# Routing.


async def test_unkeyed_requests_go_round_robin_over_acknowledged_hubs(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    for n, s in enumerate((s1, s2, s1, s2)):
        call = asyncio.create_task(app.send(PlaceOrder(f"o-{n}", ["x"]), Placed))
        r = await s.recv_request()
        await s.send(_reply(r.id))
        assert (await call).order_id == "o-1"
    # One hub reconnecting is skipped: every request goes to the other.
    await s2.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await eventually(lambda: app.hub_ids() == ["hub-1"])
    reconnecting = await hub2.next_stream()  # not acknowledged: the hub is not live
    await reconnecting.recv_register()
    for _ in range(2):
        call = asyncio.create_task(app.send(PLACE, Placed))
        r = await s1.recv_request()
        await s1.send(_reply(r.id))
        await call
    await reconnecting.nothing(0.1, "request")


async def test_a_keyed_request_goes_to_the_rendezvous_winner(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    assert rank(["hub-1", "hub-2"], "order-42")[0] == "hub-2"  # the shared test vectors
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key="order-42"))
    r = await s2.recv_request()
    assert r.routing_key == "order-42"
    await s2.send(_reply(r.id))
    await call
    await s1.nothing(0.1, "request")


async def test_a_keyed_request_stays_on_its_hub_while_the_hubs_are_stable(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    for _ in range(3):
        call = asyncio.create_task(app.send(PLACE, Placed, routing_key="order-42"))
        r = await s2.recv_request()
        await s2.send(_reply(r.id))
        await call
    await s1.nothing(0.1, "request")


# One stream per hub id.


@pytest.fixture
async def hub3() -> AsyncIterator[FakeHub]:
    """A third fake hub, for a target that answers the same hub id as another."""
    h = await FakeHub.start()
    try:
        yield h
    finally:
        await h.stop()


def _without_permits(register: hub_pb2.Register) -> hub_pb2.Register:
    copy = hub_pb2.Register()
    copy.CopyFrom(register)
    copy.ClearField("initial_permits")
    return copy


async def test_one_stream_per_hub_id(hub: FakeHub, hub2: FakeHub, hub3: FakeHub) -> None:
    app = await start_app([hub, hub2, hub3], max_concurrent=5)
    first = await hub.next_stream()
    r1 = await first.recv_register()
    second = await hub2.next_stream()
    r2 = await second.recv_register()
    third = await hub3.next_stream()
    r3 = await third.recv_register()
    # Each stream's share of three hubs' capacity; everything else is identical.
    assert (r1.initial_permits, r2.initial_permits, r3.initial_permits) == (2, 2, 2)
    assert _without_permits(r1) == _without_permits(r2) == _without_permits(r3)
    await first.ack(ack("hub-x"))
    await second.ack(ack("hub-y"))
    await third.ack(ack("hub-x"))  # the same hub as first: the later one is closed and parked
    await third.half_closed(1.0)
    await eventually(lambda: app.hub_ids() == ["hub-x", "hub-y"])
    assert len(app._connections()) == 2  # the parked address is out of the kept connections

    # Its Register replaced the owner's session: the owner reconnects once and stays.
    await first.close(grpc.StatusCode.UNAVAILABLE, "session replaced")
    again = await hub.next_stream()
    await again.recv_register()
    await again.ack(ack("hub-x"))
    await eventually(lambda: app.hub_ids() == ["hub-x", "hub-y"])
    await hub3.no_stream_within(0.2)  # the parked address is not connected again while its id is owned
    await again.nothing(0.2, "register")

    # The owner answers another id, which the other hub owns: the owner is parked itself, and the address parked
    # behind the id it gave up is connected again.
    await again.ack(ack("hub-y"))
    revived = await hub3.next_stream()
    await revived.recv_register()
    await revived.ack(ack("hub-x"))
    await eventually(lambda: app.hub_ids() == ["hub-x", "hub-y"])
    assert hub.address not in {c.address for c in app._connections()}  # the owner is the parked one now
    await hub.no_stream_within(0.2)


# The permit split.


async def test_permits_are_split_and_topped_up(hub: FakeHub, hub2: FakeHub) -> None:
    await start_app([hub, hub2], max_concurrent=5)
    s1 = await hub.next_stream()
    r1 = await s1.recv_register()
    s2 = await hub2.next_stream()
    r2 = await s2.recv_register()
    assert (r1.initial_permits, r2.initial_permits) == (3, 3)  # ceil(5 / 2)
    await s1.ack(ack("hub-1"))
    await s2.ack(ack("hub-2"))

    await s2.close(grpc.StatusCode.UNAVAILABLE, "gone")
    permits = await s1.recv_permits()
    assert (permits.count, permits.grant) == (2, 5)  # the survivor serves at full capacity

    back = await hub2.next_stream()
    assert (await back.recv_register()).initial_permits == 3  # N counts two hubs again
    await back.ack(ack("hub-2"))
    await s1.nothing(0.1, "permits")  # it offered 5 already: nothing is added

    await back.close(grpc.StatusCode.UNAVAILABLE, "gone again")
    await s1.nothing(0.2, "permits")  # repeated flaps never add up


async def test_a_failed_first_attempt_shrinks_n(hub: FakeHub, hub2: FakeHub) -> None:
    await hub2.down()  # the second address refuses TCP at the start
    await start_app([hub, hub2], max_concurrent=5)
    s = await hub.next_stream()
    register = await s.recv_register()
    assert register.initial_permits == 3  # the other address is still on its first attempt
    await s.ack(ack("hub-1"))
    permits = await s.recv_permits()
    assert (permits.count, permits.grant) == (2, 5)  # its first attempt failed: N is 1 now


async def test_a_first_attempt_that_never_acked_shrinks_n(hub: FakeHub, hub2: FakeHub) -> None:
    """The second hub accepts the stream and never acknowledges it: the attempt that ended without the ack counts as
    gone too, and the survivor is raised to the instance's whole capacity."""
    await start_app([hub, hub2], max_concurrent=5)
    s1 = await hub.next_stream()
    assert (await s1.recv_register()).initial_permits == 3
    s2 = await hub2.next_stream()
    assert (await s2.recv_register()).initial_permits == 3
    await s1.ack(ack("hub-1"))  # the other hub's handshake times out, unacknowledged
    permits = await s1.recv_permits()
    assert (permits.count, permits.grant) == (2, 5)  # the attempt ended without an ack: N is 1 now


def test_the_low_water_mark_is_the_streams_register() -> None:
    ledger = PermitLedger(3)  # the stream's Register offered 3
    assert ledger.raise_to(5) == 2  # a top-up to 5: the hub holds five
    assert ledger.raise_to(3) == 0  # never lowered
    for _ in range(3):
        ledger.arrived()
    # 5 - 3 = 2 permits still held: above min(16, 3 // 2) = 1, the mark of the stream's Register, not of 5.
    assert ledger.finished() == (0, True)
    assert ledger.flush() == 1


# A rolled-over hub stays in the split.


@pytest.fixture
def ca() -> Ca:
    from certs import make_ca

    return make_ca("operators")


@pytest.fixture
async def tls_hub(ca: Ca) -> AsyncIterator[FakeHub]:
    h = await FakeHub.start(tls=ServerTls(ca.issue_server("hub.test"), ca))
    try:
        yield h
    finally:
        await h.stop()


@pytest.fixture
async def tls_hub2(ca: Ca) -> AsyncIterator[FakeHub]:
    h = await FakeHub.start(tls=ServerTls(ca.issue_server("hub.test"), ca))
    try:
        yield h
    finally:
        await h.stop()


def _pair_files(tmp_path: Path, ca: Ca) -> tuple[ClientCertificate, Path, Path]:
    pair = ca.issue_client("orders-service")
    cert, key = tmp_path / "client.pem", tmp_path / "client-key.pem"
    key.write_bytes(pair.key_pem)
    cert.write_bytes(pair.cert_pem)
    return ClientCertificate(str(cert), str(key)), cert, key


def _write_pair(cert: Path, key: Path, pair: KeyPair) -> None:
    key.write_bytes(pair.key_pem)
    cert.write_bytes(pair.cert_pem)


async def test_a_rolled_hub_stays_in_the_split(tls_hub: FakeHub, tls_hub2: FakeHub, ca: Ca, tmp_path: Path) -> None:
    files, cert, key = _pair_files(tmp_path, ca)
    ca_file = tmp_path / "ca.pem"
    ca_file.write_bytes(ca.cert_pem)
    app = new_app(
        [tls_hub, tls_hub2],
        tls=TLSConfig(str(ca_file), "hub.test"),
        client_certificate=files,
        max_concurrent=5,
        _timings=replace(SCALED, cert_check_interval=0.05, roll_delay_max=0.0),
    )
    await app.start()
    s1 = await tls_hub.next_stream()
    r1 = await s1.recv_register()
    s2 = await tls_hub2.next_stream()
    r2 = await s2.recv_register()
    assert (r1.initial_permits, r2.initial_permits) == (3, 3)
    await s1.ack(ack("hub-1"))
    await s2.ack(ack("hub-2"))

    _write_pair(cert, key, ca.issue_client("orders-service"))  # a renewed pair: the first hub rolls over
    empty = await s1.recv_register()
    assert not empty.command_types  # the rolled stream announces nothing
    await s1.half_closed(1.0)
    new1 = await tls_hub.next_stream()
    r = await new1.recv_register()
    assert r.initial_permits == 3  # the rolled hub is still counted in N
    await s2.nothing(0.1, "permits")  # no top-up while the old stream ends
    await new1.ack(ack("hub-1"))
    # And none after: the other hub rolls over to the same renewed pair, and its stream saw no Permits meanwhile.
    assert "permits" not in await _kinds_until_end(s2)
    new2 = await tls_hub2.next_stream()
    assert (await new2.recv_register()).initial_permits == 3
    await new2.ack(ack("hub-2"))
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])


# Failover: a request never written moves with its id and its full timeout.


async def _stream_items(app: App) -> list[Placed]:
    items: list[Placed] = []
    async with app.stream(ListOrders("o-1"), Placed, initial_credit=1) as stream:
        async for item in stream:
            items.append(item)
    return items


async def _subscription_updates(app: App) -> list[WatchUpdate]:
    updates: list[WatchUpdate] = []
    async with app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate, initial_credit=1) as sub:
        async for update in sub:
            updates.append(update)
    return updates


async def test_an_unwritten_request_moves_with_its_id_and_full_timeout(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    first = _connection(app, hub.address)
    second = _connection(app, hub2.address)
    key = next(k for k in (f"k-{n}" for n in range(100)) if rank(["hub-1", "hub-2"], k)[0] == "hub-1")

    # A unary request: the first hub's writer is held, its stream breaks before the request was written.
    first._block_writer(True)
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key=key, timeout=5.0))
    await eventually(lambda: _buffered(app))
    (id_,) = _pending(app)
    await _close_held(s1, first)
    r = await s2.recv_request()
    assert (r.id, r.timeout_ms) == (id_, 5000)  # the same id, the caller's timeout whole
    await s2.send(_reply(r.id))
    await call
    assert not _pending(app)
    again = await hub.next_stream()  # the first hub reconnects
    await again.recv_register()
    await again.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])

    # A stream not yet written moves the same way.
    first._block_writer(True)
    stream_call = asyncio.create_task(_stream_items(app))
    await eventually(lambda: _buffered(app))
    (id_,) = _pending(app)
    await _close_held(again, first)
    r = await s2.recv_request()
    assert r.id == id_
    part = hub_pb2.ReplyPart(request_id=r.id, seq=1, payload=b'{"orderId":"o-1","total":1}')
    await s2.send(hub_pb2.ServerMessage(part=part))
    await s2.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=r.id, parts=1)))
    assert await stream_call == [Placed("o-1", 1)]
    assert not _pending(app)

    # A subscription not yet written too. Both writers are held; whichever hub takes it, its stream breaks, and the
    # other (released) hub takes the request with the same id.
    again = await hub.next_stream()
    await again.recv_register()
    await again.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    first._block_writer(True)
    second._block_writer(True)
    subscription_call = asyncio.create_task(_subscription_updates(app))
    await eventually(lambda: _buffered(app))
    (id_,) = _pending(app)
    held = _buffered_on(app)
    assert held is not None
    elsewhere = second if held is first else first
    elsewhere._block_writer(False)
    await _close_held(again if held is first else s2, held)
    landing = s2 if held is first else again
    r = await _next_request(landing)
    assert r.id == id_
    initial = hub_pb2.ReplyPart(request_id=r.id, seq=1, payload=b'{"topic":"t","value":0}')
    update = hub_pb2.ReplyPart(request_id=r.id, seq=2, payload=b'{"topic":"t","value":1}')
    await landing.send(hub_pb2.ServerMessage(part=initial))
    await landing.send(hub_pb2.ServerMessage(part=update))
    await landing.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=r.id, parts=2)))
    assert await subscription_call == [WatchUpdate("t", 1)]
    assert not _pending(app)


async def test_the_ninth_loss_is_never_sent(hub: FakeHub) -> None:
    app = await start_app(hub)
    connection = _connection(app, hub.address)
    s = await hub.next_stream()
    await s.recv_register()
    connection._block_writer(True)
    await s.ack(ack("hub-1"))
    call = asyncio.create_task(app.send(PLACE, timeout=10.0))
    ids: set[str] = set()
    for lost in range(1, 10):
        await eventually(lambda: _buffered(app), within=2.0)
        ids |= set(_pending(app))
        await _close_held(s, connection)
        if lost < 9:
            assert not call.done()
            s = await hub.next_stream()
            await s.recv_register()
            connection._block_writer(True)
            await s.ack(ack("hub-1"))
    with pytest.raises(NeverSent, match=r"^'orders.PlaceOrder' not written: 9 hub streams lost before it was$"):
        await call
    assert len(ids) == 1  # one id, moved eight times
    assert not _pending(app)


# NO_HANDLER: every other live hub once.


async def test_no_handler_is_tried_on_each_other_hub_once(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    # Keyed: the rendezvous order. hub-2 first (the test vectors), then hub-1 with the same id and timeout.
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key="order-42", timeout=5.0))
    r2 = await s2.recv_request()
    await s2.send(hub_error(r2.id, hub_pb2.NO_HANDLER, "no live instance"))
    r1 = await s1.recv_request()
    assert (r1.id, r1.timeout_ms) == (r2.id, 5000)
    await s1.send(_reply(r1.id))
    assert (await call).order_id == "o-1"

    # Both decline: the last NO_HANDLER is the answer, at once after the last answer.
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key="order-42", timeout=5.0))
    r2 = await s2.recv_request()
    await s2.send(hub_error(r2.id, hub_pb2.NO_HANDLER, "no live instance"))
    r1 = await s1.recv_request()
    answered = time.monotonic()
    await s1.send(hub_error(r1.id, hub_pb2.NO_HANDLER, "no live instance"))
    with pytest.raises(HubError) as raised:
        await call
    assert raised.value.code == ErrorCode.NO_HANDLER
    within(time.monotonic() - answered, 0.0, 0.1)
    await s1.nothing(0.1, "request")
    await s2.nothing(0.1, "request")


async def test_no_new_hop_after_the_deadline(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key="order-42", timeout=0.3))
    r2 = await s2.recv_request()
    await asyncio.sleep(0.5)  # the deadline passes while the hub holds the request
    await s2.send(hub_error(r2.id, hub_pb2.NO_HANDLER, "no live instance"))
    with pytest.raises(NeverSent, match=r"^no hub took the request within 300 ms$"):
        await call
    await s1.nothing(0.2, "request")  # hub-1 was untried, and never saw it


async def test_a_hub_that_did_not_take_the_request_is_not_tried_again(
    hub: FakeHub, hub2: FakeHub, hub3: FakeHub
) -> None:
    """The request was lost in the first hub's buffer, which is live again by the time the second hub declines: the
    third takes it, and the first — live, but tried — never sees it."""
    app = await start_app([hub, hub2, hub3])
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    s3 = await acknowledged(hub3, "hub-3")
    order = ["hub-1", "hub-2", "hub-3"]
    key = next(k for k in (f"k-{n}" for n in range(1000)) if rank(order, k) == order)

    first = _connection(app, hub.address)
    first._block_writer(True)  # the first hub's writer is held: the request waits in its buffer
    call = asyncio.create_task(app.send(PLACE, Placed, routing_key=key, timeout=5.0))
    await eventually(lambda: _buffered(app))
    (id_,) = _pending(app)
    await _close_held(s1, first)  # its stream breaks before it was written: the request moves
    r2 = await s2.recv_request()
    assert (r2.id, r2.timeout_ms) == (id_, 5000)

    # The first hub reconnects and is acknowledged before the second declines.
    again = await hub.next_stream()
    await again.recv_register()
    await again.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2", "hub-3"])
    await s2.send(hub_error(r2.id, hub_pb2.NO_HANDLER, "no live instance"))

    r3 = await s3.recv_request()
    assert r3.id == id_  # the third, never the first that did not take it
    await s3.send(_reply(r3.id))
    assert (await call).order_id == "o-1"
    await again.nothing(0.2, "request")  # the live first hub was tried already: it sees no request


async def test_the_unkeyed_next_hub_is_the_reference_order(hub: FakeHub, hub2: FakeHub, hub3: FakeHub) -> None:
    app = await start_app([hub, hub2, hub3])
    s1 = await acknowledged(hub, "hub-1")
    s2 = await acknowledged(hub2, "hub-2")
    s3 = await acknowledged(hub3, "hub-3")
    # The first unkeyed request takes the first turn (hub-1); the next hop is the untried live hubs as a list, at the
    # shared counter: [hub-2, hub-3][1 % 2] = hub-3 (the reference's list[counter % size], not a rotation).
    call = asyncio.create_task(app.send(PLACE, Placed, timeout=2.0))
    r1 = await s1.recv_request()
    await s1.send(hub_error(r1.id, hub_pb2.NO_HANDLER, "no live instance"))
    r3 = await s3.recv_request()
    assert r3.id == r1.id
    await s3.send(_reply(r3.id))
    await call
    await s2.nothing(0.2, "request")


# Written requests stay on their hub.


async def test_streams_stay_on_their_hub_once_written(hub: FakeHub, hub2: FakeHub) -> None:
    app, s1, s2 = await _two_hubs(hub, hub2)
    stream_call = asyncio.create_task(_stream_items(app))
    r = await s1.recv_request()
    part = hub_pb2.ReplyPart(request_id=r.id, seq=1, payload=b'{"orderId":"o-1","total":1}')
    await s1.send(hub_pb2.ServerMessage(part=part))
    await s1.close(grpc.StatusCode.UNAVAILABLE, "hub restart")
    with pytest.raises(OutcomeUnknown):
        await stream_call
    await s2.nothing(0.2, "request")  # never moved: it was written

    s1b = await hub.next_stream()
    await s1b.recv_register()
    await s1b.ack(ack("hub-1"))
    subscription_call = asyncio.create_task(_subscription_updates(app))
    r = await s2.recv_request()  # the second turn of the round-robin
    initial = hub_pb2.ReplyPart(request_id=r.id, seq=1, payload=b'{"topic":"t","value":0}')
    await s2.send(hub_pb2.ServerMessage(part=initial))
    await s2.close(grpc.StatusCode.UNAVAILABLE, "hub restart")
    with pytest.raises(OutcomeUnknown):
        await subscription_call
    await s1b.nothing(0.2, "request")


# The DNS answer.


def _resolver(monkeypatch: pytest.MonkeyPatch, answer: dict[str, list[str]]) -> list[tuple[str, int]]:
    """A stub on the running loop's ``getaddrinfo``: the addresses ``answer`` holds now, on the port asked; returns the
    calls it received."""
    calls: list[tuple[str, int]] = []

    async def resolve(host: str, port: int, **options: object) -> list[tuple[object, ...]]:
        calls.append((host, port))
        return [(2, 1, 6, "", (address, port)) for address in answer["addresses"]]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    return calls


async def test_dns_targets(hub: FakeHub, monkeypatch: pytest.MonkeyPatch) -> None:
    """A resolver stub on ``loop.getaddrinfo``: a new address connects, a vanished one leaves the routing, announces
    the empty ``Register`` and is closed once nothing of the instance is in flight on it."""
    port = int(hub.address.rsplit(":", 1)[1])
    answer = {"addresses": ["127.0.0.1"]}  # one name, one port: the hub's
    calls: list[tuple[str, int]] = []

    async def resolve(host: str, port_: int, **options: object) -> list[tuple[object, ...]]:
        calls.append((host, port_))
        return [(2, 1, 6, "", (address, port_)) for address in answer["addresses"]]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolve)
    app = await start_app(
        [hub],
        hubs=(f"dns:///hubs.test:{port}",),
        _timings=replace(SCALED, dns_refresh=0.05, dns_drain_limit=1.0),
    )
    await eventually(lambda: bool(calls))
    assert calls[0] == ("hubs.test", port)
    first = await hub.next_stream()
    await first.recv_register()
    await first.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1"])

    # A vanished address leaves the routing at once: the empty Register and its Heartbeat, then the stream is closed
    # once nothing of the instance is in flight on it.
    answer["addresses"] = ["127.0.0.9"]  # resolved, and nothing listens there
    empty = await first.recv_register()
    assert not empty.command_types
    assert (await first.recv()).WhichOneof("message") == "heartbeat"  # the Register is followed by its Heartbeat
    await eventually(lambda: app.hub_ids() == [])
    await first.half_closed(2.0)
    await hub.no_stream_within(0.2)

    # A new address in the answer gets a connection.
    answer["addresses"] = ["127.0.0.1"]
    second = await hub.next_stream()
    await second.recv_register()
    await second.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1"])
    await second.nothing(0.2, "register")


async def test_a_drained_stream_withdraws_its_types_before_its_ack(
    hub: FakeHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An address that leaves the DNS answer registers no types on its stream even before the hub acknowledged it: the
    hub must not route new work there while the stream is closed."""
    port = int(hub.address.rsplit(":", 1)[1])
    answer = {"addresses": ["127.0.0.1"]}
    _resolver(monkeypatch, answer)
    app = new_app(
        [hub],
        hubs=(f"dns:///hubs.test:{port}",),
        _timings=replace(SCALED, dns_refresh=0.05, dns_drain_limit=1.0),
    )

    @app.command_handler
    async def place_order(m: PlaceOrder) -> None:
        return None

    await app.start()
    first = await hub.next_stream()
    assert list((await first.recv_register()).command_types) == ["orders.PlaceOrder"]
    answer["addresses"] = ["127.0.0.9"]  # gone, its handshake still unanswered
    empty = await first.recv_register()
    assert not empty.command_types  # the withdrawal does not wait for the ack
    await first.half_closed(2.0)


async def test_a_draining_address_is_connected_again_once_its_connection_stopped(
    hub: FakeHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An address back in the DNS answer after its drain already stopped the connection is a fresh connection, not the
    stopped one revived: that one opens no stream ever again."""
    port = int(hub.address.rsplit(":", 1)[1])
    answer = {"addresses": ["127.0.0.1"]}
    _resolver(monkeypatch, answer)
    # The half-closed stream stays open on the hub longer than the drain needs to stop its connection (the drain then
    # waits for that end): the window in which the address comes back.
    monkeypatch.setattr("fakehub._EOF_GRACE", 0.5)
    app = await start_app(
        [hub],
        hubs=(f"dns:///hubs.test:{port}",),
        _timings=replace(SCALED, dns_refresh=0.05, dns_drain_limit=1.0, close_wait=2.0),
    )
    first = await hub.next_stream()
    await first.recv_register()
    await first.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1"])

    answer["addresses"] = ["127.0.0.9"]  # gone: the empty Register, then the half-close and the stop
    assert not (await first.recv_register()).command_types
    await eventually(lambda: first._eof)  # the drain half-closed: its connection no longer connects
    answer["addresses"] = ["127.0.0.1"]  # back while the old stream has not ended yet

    second = await hub.next_stream(within=2.0)
    await second.recv_register()
    await second.ack(ack("hub-1"))
    await eventually(lambda: app.hub_ids() == ["hub-1"])  # the new connection holds the hub again


def test_a_parked_address_that_leaves_the_dns_answer_is_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    """A parked duplicate whose address the DNS answer no longer names is dropped when the hub it was parked behind
    goes, not connected again; an address the answer still names is."""
    app = App(Settings(service="orders-service", hubs=("dns:///hubs.test:7000",)), _timings=SCALED)
    pool = app._pool
    added: list[list[str]] = []
    monkeypatch.setattr(pool, "_add", lambda addresses: added.append(list(addresses)))
    pool._resolved = ["10.0.0.1:7000"]
    pool._parked["10.0.0.2:7000"] = "hub-x"  # parked behind the target that answered hub-x, now unowned
    pool._revive()
    assert not pool._parked  # dropped
    assert added == []  # and not connected: its address left the answer
    pool._parked["10.0.0.1:7000"] = "hub-x"
    pool._revive()
    assert added == [["10.0.0.1:7000"]]  # an address the answer still names is connected again
