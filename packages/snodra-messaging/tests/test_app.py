"""The App's lifecycle: start, ready, run, shutdown."""

import asyncio
import logging
import signal
import time

import grpc
import pytest
from fakehub import FakeHub, FakeStream, ack
from helpers import assert_no_library_tasks, eventually, new_app, start_app, within
from snodra import App, Settings, _connection
from snodra._hubpb import hub_pb2


def _kind(m: hub_pb2.ClientMessage) -> str:
    return m.WhichOneof("message") or "empty"


async def _acknowledged(app: App, hub: FakeHub) -> FakeStream:
    s = await hub.next_stream()
    assert _kind(await s.recv()) == "register"
    assert _kind(await s.recv()) == "heartbeat"
    await s.ack(ack("hub-1"))
    await app.ready(1.0)
    return s


def _empty(r: hub_pb2.Register) -> bool:
    return not (r.command_types or r.query_types or r.shared_query_types or r.subscription_query_types)


async def test_shutdown_leaves_the_hub_in_order(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await _acknowledged(app, hub)
    leaving = asyncio.create_task(app.shutdown())
    first = await s.recv()
    assert _kind(first) == "register"
    assert (first.register.service_name, first.register.instance_id) == ("orders-service", "i-1")
    assert _empty(first.register)
    assert _kind(await s.recv()) == "heartbeat"
    await s.ack(ack("hub-1"))  # the hub acks every accepted Register
    await s.half_closed(within=1.0)
    await asyncio.wait_for(leaving, 1.0)
    assert_no_library_tasks()


async def test_shutdown_does_not_wait_on_the_empty_registers_ack(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await _acknowledged(app, hub)
    started = time.monotonic()
    leaving = asyncio.create_task(app.shutdown())
    assert _empty(await s.recv_register())  # never acknowledged
    assert app.hub_ids() == ["hub-1"]  # still registered meanwhile
    await s.half_closed(within=1.0)
    await asyncio.wait_for(leaving, 1.0)
    within(time.monotonic() - started, 0.0, 0.1 + 0.2 + 0.15)
    again = time.monotonic()
    await app.shutdown()  # a second call returns at once
    assert time.monotonic() - again < 0.05


async def test_shutdown_without_a_hub_returns_at_once(hub: FakeHub) -> None:
    app = await start_app(hub)
    await hub.next_stream()
    await hub.down()
    started = time.monotonic()
    await app.shutdown()
    assert time.monotonic() - started < 0.35
    assert_no_library_tasks()


async def test_shutdown_returns_by_its_timeout(hub: FakeHub) -> None:
    # A hub that neither acks the empty Register nor ends the stream after the half-close.
    app = await start_app(hub)
    s = await _acknowledged(app, hub)
    started = time.monotonic()
    leaving = asyncio.create_task(app.shutdown(timeout=0.05))
    await s.recv_register()
    await asyncio.wait_for(leaving, 1.0)
    within(time.monotonic() - started, 0.0, 0.05 + 0.1)
    assert_no_library_tasks()


async def test_shutdown_before_start(hub: FakeHub) -> None:
    app = new_app(hub)
    await app.shutdown()
    await app.shutdown()
    with pytest.raises(RuntimeError, match="shut down"):
        await app.start()
    await hub.no_stream_within(0.1)


async def test_start_twice_is_refused(hub: FakeHub) -> None:
    app = await start_app(hub)
    with pytest.raises(RuntimeError, match="already started"):
        await app.start()


async def test_shutdown_during_start(hub: FakeHub) -> None:
    app = new_app(hub)
    started, stopped = await asyncio.gather(app.start(), app.shutdown(), return_exceptions=True)
    assert started is None
    assert stopped is None
    assert_no_library_tasks()


async def test_run_returns_on_cancellation(hub: FakeHub) -> None:
    app = new_app(hub)
    running = asyncio.create_task(app.run())
    s = await _acknowledged(app, hub)
    running.cancel()
    assert _empty(await s.recv_register())
    await s.ack(ack("hub-1"))
    await s.half_closed(within=1.0)
    with pytest.raises(asyncio.CancelledError):
        await running
    assert_no_library_tasks()


async def test_run_returns_when_the_app_is_shut_down(hub: FakeHub) -> None:
    app = new_app(hub)
    running = asyncio.create_task(app.run())
    await _acknowledged(app, hub)
    await app.shutdown()
    await asyncio.wait_for(running, 1.0)
    assert_no_library_tasks()


async def test_the_app_is_an_async_context_manager(hub: FakeHub) -> None:
    app = new_app(hub)
    async with app as entered:
        assert entered is app
        s = await _acknowledged(app, hub)
        assert app.hub_ids() == ["hub-1"]
    await s.half_closed(within=1.0)
    assert_no_library_tasks()


async def test_ready_times_out(hub: FakeHub) -> None:
    app = await start_app(hub)
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await app.ready(0.05)
    within(time.monotonic() - started, 0.045, 0.5)


async def test_ready_after_the_ack_returns_at_once(hub: FakeHub) -> None:
    app = await start_app(hub)
    await _acknowledged(app, hub)
    await app.ready(0)


async def test_instance_id_and_no_rejection_at_first(hub: FakeHub) -> None:
    app = new_app(hub, instance="i-42")
    assert app.instance_id == "i-42"
    assert app.rejection is None
    assert app.hub_ids() == []
    assert App(Settings(service="orders-service")).instance_id != App(Settings(service="orders-service")).instance_id


async def test_a_stream_closed_during_shutdown_is_not_reopened(hub: FakeHub) -> None:
    app = await start_app(hub)
    s = await _acknowledged(app, hub)
    leaving = asyncio.create_task(app.shutdown())
    await s.recv_register()
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await asyncio.wait_for(leaving, 1.0)
    await hub.no_stream_within(0.15)


class _SlowChannel:
    """A channel whose close takes 0.5 s (longer than the scaled close wait): a cancellation lands in the middle of a
    stream's clean-up."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        self._channel = channel
        self.closing = asyncio.Event()
        self.closed = False

    def __getattr__(self, name: str) -> object:
        return getattr(self._channel, name)

    async def close(self, grace: float | None = None) -> None:
        self.closing.set()
        await asyncio.sleep(0.5)
        await self._channel.close(grace)
        self.closed = True


@pytest.fixture
def channels(monkeypatch: pytest.MonkeyPatch) -> list[_SlowChannel]:
    made: list[_SlowChannel] = []
    real = grpc.aio.insecure_channel

    def insecure_channel(target: str, options: object = None) -> _SlowChannel:
        c = _SlowChannel(real(target, options=options))  # type: ignore[arg-type]
        made.append(c)
        return c

    monkeypatch.setattr(grpc.aio, "insecure_channel", insecure_channel)
    return made


async def test_shutdown_during_a_streams_clean_up_finishes_it(hub: FakeHub, channels: list[_SlowChannel]) -> None:
    # The stream ends and its clean-up is closing the channel when shutdown cancels the connection.
    app = await start_app(hub)
    s = await _acknowledged(app, hub)
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")
    await asyncio.wait_for(channels[0].closing.wait(), 1.0)
    await asyncio.wait_for(app.shutdown(), 2.0)
    assert all(c.closed for c in channels)
    assert app.hub_ids() == []
    assert_no_library_tasks()


async def test_a_second_cancellation_during_shutdown_leaves_nothing_running(
    hub: FakeHub, channels: list[_SlowChannel]
) -> None:
    app = new_app(hub)
    running = asyncio.create_task(app.run())
    s = await _acknowledged(app, hub)
    running.cancel()  # run() shuts down: the empty Register, then the half-close
    assert _empty(await s.recv_register())
    await s.ack(ack("hub-1"))
    await s.half_closed(within=1.0)
    await asyncio.wait_for(channels[0].closing.wait(), 1.0)
    running.cancel()  # a second cancellation, while the stream's channel is closing
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(running, 2.0)
    assert all(c.closed for c in channels)
    assert app.hub_ids() == []
    assert_no_library_tasks()


async def test_run_returns_when_a_shutdown_elsewhere_is_cancelled(hub: FakeHub, channels: list[_SlowChannel]) -> None:
    app = new_app(hub)
    running = asyncio.create_task(app.run())
    s = await _acknowledged(app, hub)
    closing = asyncio.create_task(app.shutdown())
    await s.recv_register()
    await s.ack(ack("hub-1"))
    await s.half_closed(within=1.0)
    await asyncio.wait_for(channels[0].closing.wait(), 1.0)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await asyncio.wait_for(running, 1.0)  # run() is released all the same
    assert all(c.closed for c in channels)
    assert app.hub_ids() == []
    assert_no_library_tasks()


async def test_a_crashed_connection_is_logged_and_surfaced(
    hub: FakeHub, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def broken(base: float, hi: float, unit: float) -> float:
        raise RuntimeError("broken backoff")

    monkeypatch.setattr(_connection, "jitter", broken)
    app = await start_app(hub)
    s = await hub.next_stream()
    await s.recv_register()
    await s.close(grpc.StatusCode.UNAVAILABLE, "restart")  # the loop crashes computing its wait
    await eventually(
        lambda: any(
            r.levelno == logging.ERROR and "broken backoff" in r.getMessage() + str(r.exc_info) for r in caplog.records
        )
    )
    with pytest.raises(RuntimeError, match="broken backoff"):
        await app.shutdown()
    assert_no_library_tasks()


async def test_run_restores_the_signal_handlers_it_found(hub: FakeHub) -> None:
    def mine(signum: int, frame: object) -> None:
        pass

    before = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        for sig in before:
            signal.signal(sig, mine)
        app = new_app(hub)
        running = asyncio.create_task(app.run())
        await _acknowledged(app, hub)
        assert signal.getsignal(signal.SIGTERM) is not mine  # run() listens meanwhile
        await app.shutdown()
        await asyncio.wait_for(running, 1.0)
        assert signal.getsignal(signal.SIGTERM) is mine
        assert signal.getsignal(signal.SIGINT) is mine
    finally:
        for sig, handler in before.items():
            signal.signal(sig, handler)
