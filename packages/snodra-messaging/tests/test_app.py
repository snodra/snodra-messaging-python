"""The App's lifecycle: start, ready, run, shutdown."""

import asyncio
import time

import grpc
import pytest
from fakehub import FakeHub, FakeStream, ack
from helpers import assert_no_library_tasks, new_app, start_app, within
from snodra import App, Settings
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
