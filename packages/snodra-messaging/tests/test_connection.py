"""One hub stream: the handshake, heartbeats, the silent hub, reconnect, refusals.

Time comes from the fake hub: the heartbeat interval of its ``RegisterAck`` and the scaled timings of
``helpers.SCALED``. Every bound below is the rule's, with room for the connection set-up of a new stream.
"""

import asyncio
import itertools
import logging
import time
from dataclasses import replace

import grpc
import pytest
from fakehub import FakeHub, FakeStream, ack
from helpers import SCALED, PlaceOrder, eventually, start_app, within
from snodra import Rejection, _backoff, _connection
from snodra._connection import _Stream
from snodra._hubpb import hub_pb2
from snodra._meta import client_version

SETUP = 0.05
"""Room for a new stream's connection set-up (a new channel per attempt) on top of a wait."""

QUIET_BEFORE_ACK = replace(SCALED, heartbeat_before_ack=5.0)
"""No heartbeat tick before the ack: every heartbeat after the first one follows the ack."""


def _kind(m: hub_pb2.ClientMessage) -> str:
    return m.WhichOneof("message") or "empty"


async def _opened(hub: FakeHub) -> FakeStream:
    """The next stream, its Register and Heartbeat read."""
    s = await hub.next_stream()
    assert _kind(await s.recv()) == "register"
    assert _kind(await s.recv()) == "heartbeat"
    return s


async def _end(s: FakeStream, within: float = 5.0) -> float:
    """Reads the stream to its end; the time (monotonic) the end came."""
    deadline = time.monotonic() + within
    while True:
        try:
            await s.recv(within=max(0.0, deadline - time.monotonic()))
        except AssertionError as e:
            if "stream ended" not in str(e):
                raise
            return time.monotonic()


def _after(s: FakeStream, t: float) -> int:
    """The heartbeats that arrived after t."""
    return sum(1 for h in s.heartbeats if h > t)


async def test_every_stream_opens_with_register_then_heartbeat(hub: FakeHub) -> None:
    await start_app(hub, max_concurrent=8, tags={"zone": "a"})
    s = await hub.next_stream()
    first = (await s.recv()).register
    assert (first.service_name, first.instance_id, first.initial_permits, first.load_factor) == (
        "orders-service",
        "i-1",
        8,
        100,
    )
    assert first.client_version == client_version()
    assert first.tags["zone"] == "a"
    assert _kind(await s.recv()) == "heartbeat"
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s2 = await hub.next_stream()
    assert (await s2.recv()).register == first  # identical Register
    assert _kind(await s2.recv()) == "heartbeat"


async def test_registered_only_on_the_ack(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await hub.next_stream()
    s.echo_heartbeats(True)  # echoes before the ack: they confirm nothing
    await s.recv_register()
    await s.nothing(0.15)  # the first heartbeat and the one 0.1 s later, both echoed
    assert len(s.heartbeats) >= 2
    assert app.hub_ids() == []
    with pytest.raises(TimeoutError):
        await app.ready(0.01)
    await s.ack(ack("hub-1"))
    await app.ready(1.0)
    assert app.hub_ids() == ["hub-1"]


async def test_an_empty_hub_id_is_the_address(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await _opened(hub)
    await s.ack(ack(""))
    await app.ready(1.0)
    assert app.hub_ids() == [hub.address]


async def test_no_ack_within_the_handshake_timeout_reopens(hub: FakeHub) -> None:
    await start_app(hub)
    s = await hub.next_stream()
    opened = time.monotonic()
    s.echo_heartbeats(True)
    await s.recv_register()
    await hub.next_stream(within=2.0)
    within(time.monotonic() - opened, 0.3, 0.3 + 0.25)


async def test_heartbeats_at_the_ack_interval(hub: FakeHub) -> None:
    await start_app(hub, _timings=QUIET_BEFORE_ACK)
    s = await _opened(hub)
    acked = time.monotonic()
    await s.ack(ack("hub-1", interval_ms=100))
    while _after(s, acked) < 11:  # about 1 s
        await s.recv()
    beats = [h for h in s.heartbeats if h > acked]
    gaps = [b - a for a, b in itertools.pairwise(beats)]
    within(sum(gaps) / len(gaps), 0.08, 0.12)
    assert max(gaps) <= 0.15, gaps
    within(beats[0] - acked, 0.09, 0.15)  # the first tick, one interval after the ack


async def test_a_silent_hub_is_dropped_between_three_and_four_intervals(hub: FakeHub) -> None:
    await start_app(hub, _timings=QUIET_BEFORE_ACK)
    s = await _opened(hub)
    s.echo_heartbeats(False)  # none from now on, the first heartbeat's included: the ack is the last sign of life
    acked = time.monotonic()
    await s.ack(ack("hub-1", interval_ms=100))
    ended = await _end(s)
    within(ended - acked, 0.3, 0.4 + 0.05)
    assert _after(s, acked) == 3  # the dropped stream carried exactly three heartbeats after the ack
    assert (await hub.next_stream()).index == 1  # and a new one comes


async def test_an_echo_restarts_the_silence(hub: FakeHub) -> None:
    await start_app(hub, _timings=QUIET_BEFORE_ACK)
    s = await _opened(hub)
    s.echo_heartbeats(False)  # none from now on, the first heartbeat's included: the ack is the last sign of life
    acked = time.monotonic()
    await s.ack(ack("hub-1", interval_ms=100))
    await eventually(lambda: len(s.heartbeats) >= 3)  # the first heartbeat and two ticks, unanswered
    s.echo_heartbeats(True)
    await eventually(lambda: len(s.heartbeats) >= 4)  # the third tick, echoed: the hub is alive
    s.echo_heartbeats(False)
    last_echo = s.heartbeats[3]
    ended = await _end(s)
    within(ended - last_echo, 0.3, 0.4 + 0.05)
    assert ended - acked > 0.6


async def test_a_heartbeat_follows_every_later_register_and_counts(hub: FakeHub) -> None:
    app = await start_app(hub, _timings=QUIET_BEFORE_ACK)
    s = await _opened(hub)
    s.echo_heartbeats(False)  # none from now on, the first heartbeat's included: the ack is the last sign of life
    acked = time.monotonic()
    await s.ack(ack("hub-1", interval_ms=200))
    await app.ready(1.0)
    app._connections()[0].reregister(empty=False)
    later = await s.recv()
    assert _kind(later) == "register"
    assert later.register.service_name == "orders-service"
    assert _kind(await s.recv()) == "heartbeat"
    ended = await _end(s)
    # the third tick (600 ms) drops the stream, not the fourth (800 ms): the extra heartbeat counted
    within(ended - acked, 0.6, 0.7)
    assert _after(s, acked) == 3


async def test_unanswered_heartbeats_alone_do_not_drop_the_stream(hub: FakeHub) -> None:
    # Two later Registers right after the ack: three heartbeats are unanswered at the second tick (400 ms), but only two
    # intervals have passed; the drop waits for the third tick (600 ms), more than three intervals after the ack.
    app = await start_app(hub, _timings=QUIET_BEFORE_ACK)
    s = await _opened(hub)
    s.echo_heartbeats(False)
    acked = time.monotonic()
    await s.ack(ack("hub-1", interval_ms=200))
    await app.ready(1.0)
    conn = app._connections()[0]
    conn.reregister(empty=False)
    conn.reregister(empty=False)
    ended = await _end(s)
    within(ended - acked, 0.6, 0.7)
    assert _after(s, acked) == 4  # two after the Registers, two ticks


async def test_the_ack_restarts_the_silence_clock(hub: FakeHub) -> None:
    # The ack comes 0.25 s after the stream opened and is the first sign of life: the silence is measured from it,
    # never from the opening. Two later Registers make the count no obstacle, so the time alone decides.
    app = await start_app(hub, _timings=replace(QUIET_BEFORE_ACK, handshake_timeout=2.0))
    s = await _opened(hub)
    s.echo_heartbeats(False)
    await s.nothing(0.25)
    acked = time.monotonic()
    await s.ack(ack("hub-1", interval_ms=100))
    await app.ready(1.0)
    conn = app._connections()[0]
    conn.reregister(empty=False)
    conn.reregister(empty=False)
    ended = await _end(s)
    within(ended - acked, 0.3, 0.4 + 0.05)


async def test_no_silence_check_before_the_ack(hub: FakeHub) -> None:
    # Heartbeats every 50 ms before the ack, none echoed: a silence check would drop the stream at about 0.2 s.
    await start_app(hub, _timings=replace(SCALED, heartbeat_before_ack=0.05))
    s = await hub.next_stream()
    opened = time.monotonic()
    s.echo_heartbeats(False)
    ended = await _end(s)
    within(ended - opened, 0.3 - 0.05, 0.3 + 0.25)  # the handshake timeout ends it
    assert len(s.heartbeats) >= 5


async def test_reconnects_after_the_hub_went_away(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await hub.next_stream()
    first = await s.recv_register()
    await s.ack(ack("hub-1"))
    await app.ready(1.0)
    await hub.down()
    await eventually(lambda: app.hub_ids() == [])
    await hub.up()
    s2 = await hub.next_stream(within=2.0)
    assert await s2.recv_register() == first
    await s2.ack(ack("hub-1"))
    await app.ready(1.0)
    assert app.hub_ids() == ["hub-1"]


def _record_waits(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Records the base of every reconnect wait the connections choose (before the jitter)."""
    bases: list[float] = []
    real = _backoff.jitter

    def recording(base: float, hi: float, unit: float) -> float:
        bases.append(base)
        return real(base, hi, unit)

    monkeypatch.setattr(_connection, "jitter", recording)
    return bases


async def test_the_backoff_resets_after_an_ack(hub: FakeHub, monkeypatch: pytest.MonkeyPatch) -> None:
    bases = _record_waits(monkeypatch)
    app = await start_app(hub)
    for _ in range(2):  # two closures before any ack: the next wait is 0.2 s
        s = await _opened(hub)
        await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s = await _opened(hub)
    await s.ack(ack("hub-1"))
    await app.ready(1.0)
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    closed = time.monotonic()
    await hub.next_stream()
    assert bases == [0.05, 0.1, 0.05]  # without the reset the third would be 0.2
    # Without the reset the wait would be at least 0.16 s; the bound leaves room for the reconnect under load.
    within(time.monotonic() - closed, 0.8 * 0.05, 0.13)


async def test_unavailable_is_an_ordinary_closure(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await _opened(hub)
    for wait in (0.05, 0.1, 0.2):  # doubling before any ack, up to the maximum
        await s.close(grpc.StatusCode.UNAVAILABLE, "jwks not loaded yet")
        closed = time.monotonic()
        s = await _opened(hub)
        within(time.monotonic() - closed, 0.8 * wait, min(1.2 * wait, 0.2) + SETUP)
    assert app.rejection is None


REFUSAL = "type orders.PlaceOrder is owned by billing-service"


async def test_a_refused_register_waits_the_cap_and_is_exposed(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await _opened(hub)
    for code in (
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
    ):
        before = time.time()
        await s.close(code, REFUSAL)
        closed = time.monotonic()
        s = await _opened(hub)
        within(time.monotonic() - closed, 0.8 * 0.2, 0.2 + SETUP)
        rejection = app.rejection
        assert rejection is not None
        assert rejection == Rejection(hub=hub.address, code=code.name, message=REFUSAL, at=rejection.at)
        within(rejection.at, before, time.time())

    # A refusal code on an acknowledged stream is a refusal too.
    await s.ack(ack("hub-1"))
    await app.ready(1.0)
    await s.close(grpc.StatusCode.PERMISSION_DENIED, "access revoked")
    closed = time.monotonic()
    s = await _opened(hub)
    within(time.monotonic() - closed, 0.8 * 0.2, 0.2 + SETUP)
    revoked = app.rejection
    assert revoked is not None
    # Once acknowledged, a refusal names the hub by its id.
    assert (revoked.hub, revoked.code, revoked.message) == ("hub-1", "PERMISSION_DENIED", "access revoked")

    # A later accepted Register leaves the latest refusal as it was.
    await s.ack(ack("hub-1"))
    await app.ready(1.0)
    assert app.rejection == revoked


async def test_a_refusal_names_the_hub_by_its_last_acknowledged_id(hub: FakeHub) -> None:
    # The id from the connection's last RegisterAck, kept after that stream ended; the address before any ack, and
    # when that id is empty.
    app = await start_app(hub)
    s = await _opened(hub)
    await s.close(grpc.StatusCode.ALREADY_EXISTS, REFUSAL)
    await eventually(lambda: app.rejection is not None)
    assert app.rejection is not None
    assert app.rejection.hub == hub.address  # never acknowledged yet: the address dialled
    s = await _opened(hub)
    await s.ack(ack("hub-7"))
    await app.ready(1.0)
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s = await _opened(hub)  # a new stream, refused before its ack
    await s.close(grpc.StatusCode.ALREADY_EXISTS, REFUSAL)
    await eventually(lambda: app.rejection is not None and app.rejection.hub == "hub-7")
    s = await _opened(hub)
    await s.ack(ack(""))  # an empty hub id: the address
    await app.ready(1.0)
    await s.close(grpc.StatusCode.PERMISSION_DENIED, "access revoked")
    await eventually(lambda: app.rejection is not None and app.rejection.message == "access revoked")
    assert app.rejection is not None
    assert app.rejection.hub == hub.address


async def test_a_refusal_is_logged_once_per_distinct_message(hub: FakeHub, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="snodra")
    await start_app(hub)
    s = await _opened(hub)
    for code, message in (
        (grpc.StatusCode.ALREADY_EXISTS, REFUSAL),
        (grpc.StatusCode.ALREADY_EXISTS, REFUSAL),
        (grpc.StatusCode.ALREADY_EXISTS, "type orders.GetOrder is owned by billing-service"),
    ):
        await s.close(code, message)
        s = await _opened(hub)
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR and r.name.startswith("snodra")]
    assert len(errors) == 2, errors
    assert REFUSAL in errors[0]
    assert "orders.GetOrder" in errors[1]
    assert all("ALREADY_EXISTS" in e for e in errors)


async def test_the_ack_interval_drives_the_heartbeats_of_its_stream_only(hub: FakeHub) -> None:
    # A new stream starts again from the heartbeat before the ack, not from the last ack's interval.
    await start_app(hub, _timings=replace(SCALED, heartbeat_before_ack=0.05))
    s = await _opened(hub)
    await s.ack(ack("hub-1", interval_ms=10_000))
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s = await _opened(hub)
    assert _kind(await s.recv(within=0.5)) == "heartbeat"


async def test_an_attempt_that_crashed_after_its_ack_resets_the_backoff(
    hub: FakeHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crash(self: object, now: float) -> None:
        raise RuntimeError("a bug in the read loop")

    monkeypatch.setattr(_Stream, "on_echo", crash)  # the echo that follows the ack ends the attempt with a bug
    bases = _record_waits(monkeypatch)
    app = await start_app(hub)
    for _ in range(2):  # two closures before any ack: the next wait would be 0.2 s
        s = await _opened(hub)
        await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    s = await _opened(hub)
    await s.ack(ack("hub-1"))  # its held echo follows it
    acked = time.monotonic()
    s2 = await hub.next_stream()
    assert s2.index == 3
    assert bases == [0.05, 0.1, 0.05]  # without the reset the third would be 0.2
    within(time.monotonic() - acked, 0.0, 0.13)  # at least 0.16 s without the reset
    assert app.rejection is None


async def test_hub_ids_names_every_acknowledged_hub_sorted(hub: FakeHub, hub2: FakeHub) -> None:
    app = await start_app([hub, hub2])
    s1, s2 = await _opened(hub), await _opened(hub2)
    await s1.ack(ack("hub-b"))
    await s2.ack(ack("hub-a"))
    await eventually(lambda: len(app.hub_ids()) == 2)
    assert app.hub_ids() == ["hub-a", "hub-b"]
    await s1.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await eventually(lambda: app.hub_ids() == ["hub-a"])


async def test_one_connection_per_address(hub: FakeHub) -> None:
    app = await start_app(hub, hubs=(hub.address, f"static://{hub.address}"))
    assert len(app._connections()) == 1
    await _opened(hub)
    await hub.no_stream_within(0.1)


async def test_settle_reads_the_results_it_waited_for(caplog: pytest.LogCaptureFixture) -> None:
    async def failing() -> None:
        raise OSError("channel close failed")

    task = asyncio.create_task(failing(), name="snodra:clean-up:test")
    with caplog.at_level(logging.WARNING, logger="snodra"):
        await _connection.settle([task])
    assert task.done()
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == [
        "Task snodra:clean-up:test failed: OSError('channel close failed')"
    ]


async def test_settle_keeps_what_it_leaves_behind_until_it_ends() -> None:
    release = asyncio.Event()

    task = asyncio.create_task(release.wait(), name="snodra:clean-up:test")
    waiter = asyncio.create_task(_connection.settle([task], bound=0.05))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert task in _connection._LEFT_BEHIND  # held by the library until it ends
    release.set()
    await asyncio.wait_for(task, 1.0)
    await asyncio.sleep(0)  # its done-callback runs
    assert task not in _connection._LEFT_BEHIND


async def test_a_refusal_is_honoured_whatever_write_is_in_flight(hub: FakeHub) -> None:
    # The hub closes the stream while the writer is busy writing requests: the write that meets the closed stream
    # must not turn the hub's status into a transport error (grpc.aio's own write does that).
    app = await start_app(hub, reconnect_backoff=(0.05, 0.4))
    s = await hub.next_stream()
    for refusal in range(1, 6):
        await s.recv_register()
        await s.ack(ack("hub-1"))
        await eventually(lambda: app.hub_ids() == ["hub-1"])
        flood = [asyncio.create_task(app.send(PlaceOrder(f"o-{n}", ["x" * 2000]), timeout=5.0)) for n in range(400)]
        try:
            for _ in range(20):
                await s.recv_request()  # the writer is busy writing the rest
            await s.close(grpc.StatusCode.ALREADY_EXISTS, f"refusal {refusal}")
            closed = time.monotonic()
            await eventually(lambda: app.hub_ids() == [])
            rejection = app.rejection
            assert rejection is not None, f"refusal {refusal}: no rejection recorded"
            assert (rejection.code, rejection.message) == ("ALREADY_EXISTS", f"refusal {refusal}")
            s = await hub.next_stream()
            within(time.monotonic() - closed, 0.8 * 0.4 - SETUP, 0.4 + SETUP)  # the refusal's wait, not the backoff
        finally:
            for t in flood:
                t.cancel()
            await asyncio.gather(*flood, return_exceptions=True)
