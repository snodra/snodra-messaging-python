"""The instance status every hub stream reports: its period, its content, and the report at once on a change of the
acknowledged hubs.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Instance status").
"""

from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import replace

import grpc
from fakehub import FakeHub, FakeStream, forward
from helpers import SCALED, Noop, acknowledged, eventually, new_app, within
from snodra import App, Settings
from snodra._hubpb import hub_pb2

INTERVAL = 0.2


async def _pair(app: App, hub: FakeHub, hub2: FakeHub) -> tuple[FakeStream, FakeStream]:
    s1, s2 = await acknowledged(hub, "hub-1"), await acknowledged(hub2, "hub-2")
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    return s1, s2


def _reports(s: FakeStream, since: float) -> list[hub_pb2.InstanceStatus]:
    return [status for at, status in s.statuses if at >= since]


async def test_instance_status_on_every_stream(hub: FakeHub, hub2: FakeHub) -> None:
    started = 0
    release = asyncio.Event()
    app = new_app([hub, hub2], _timings=replace(SCALED, status_interval=INTERVAL))

    @app.command_handler
    async def noop(m: Noop) -> None:
        nonlocal started
        started += 1
        await release.wait()
        return None

    await app.start()
    s1, s2 = await _pair(app, hub, hub2)
    # One handler in flight on each hub (a request the hub forwards, not a call this instance answers in-process).
    for n, s in enumerate((s1, s2)):
        await s.send(forward(hub_pb2.Request(id=f"c-{n}", kind=hub_pb2.COMMAND, type="orders.Noop", payload=b"{}")))
    await eventually(lambda: started == 2)

    # No gap above the interval (plus a little): every stream reports at least every status_interval.
    mark = time.monotonic()
    await asyncio.sleep(3 * INTERVAL + 0.1)
    for s in (s1, s2):
        stamps = [at for at, _ in s.statuses if at >= mark]
        assert len(stamps) >= 2
        gaps = [b - a for a, b in itertools.pairwise(stamps)]
        assert not gaps or max(gaps) <= INTERVAL + 0.06, gaps
        for status in _reports(s, mark):
            assert list(status.connected_hub_ids) == ["hub-1", "hub-2"]  # this one included
            assert status.handlers_in_flight == 2  # both handlers run, over both hubs
            assert not status.processors  # this library runs no event processor

    release.set()
    await eventually(
        lambda: all(_reports(s, mark) and _reports(s, mark)[-1].handlers_in_flight == 0 for s in (s1, s2)),
        within=1.0,
    )


async def test_a_change_of_the_acknowledged_hubs_reports_at_once(hub: FakeHub, hub2: FakeHub) -> None:
    """A change reports at once, not on the tick: with a status interval of a second, the surviving stream's new
    report lands within the interval of the periodic one."""
    app = new_app([hub, hub2], _timings=replace(SCALED, status_interval=1.0))
    await app.start()
    s1, s2 = await _pair(app, hub, hub2)
    await eventually(lambda: bool(s1.statuses), within=INTERVAL)
    before = len(s1.statuses)
    last = s1.statuses[-1][0]
    at = time.monotonic()
    await s2.close(grpc.StatusCode.UNAVAILABLE, "restart")  # the hub that is left
    await eventually(lambda: len(s1.statuses) > before, within=INTERVAL)
    assert s1.statuses[-1][0] - last < INTERVAL  # at once, not one status interval later
    within(s1.statuses[-1][0] - at, 0.0, INTERVAL / 4)  # nor later than a quarter interval after the change
    assert len(_reports(s1, at)) == 1  # one report per change: the immediate one, not one more from the wake-up
    assert list(s1.statuses[-1][1].connected_hub_ids) == ["hub-1"]  # the hub that is left names only the other


def test_the_settings_status_interval_reaches_the_pool(hub: FakeHub) -> None:
    """``Settings.status_interval`` is the pool's report period; the ``_timings`` hook still wins when given."""
    app = App(Settings(service="orders-service", hubs=(hub.address,), status_interval=0.75))
    assert app._pool._timings.status_interval == 0.75
    hooked = App(
        Settings(service="orders-service", hubs=(hub.address,), status_interval=0.75),
        _timings=replace(SCALED, status_interval=7.0),
    )
    assert hooked._pool._timings.status_interval == 7.0
