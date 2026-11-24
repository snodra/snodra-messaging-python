"""Helpers of the tests that run an :class:`snodra.App` against fake hubs.

``start_app`` starts an app with the scaled timings below; the ``_stop_apps`` fixture of ``conftest.py`` shuts every
app started in a test down when the test ends, whatever its outcome, and then checks that no task of the library is
left. Waits are bounded, and a bound only stops a hang.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any, Final

from fakehub import FakeHub
from snodra import App, Settings
from snodra._timings import DEFAULT_TIMINGS, Timings

SCALED: Final = replace(
    DEFAULT_TIMINGS,
    heartbeat_before_ack=0.1,
    handshake_timeout=0.3,
    deregister_wait=0.1,
    subscription_end=0.2,
    handler_drain=1.0,
    close_wait=0.2,
)
"""The shipped timings, shortened for the tests."""

_apps: list[App] = []


def new_app(hubs: FakeHub | Sequence[FakeHub], /, *, _timings: Timings | None = None, **settings: Any) -> App:
    """An app on hubs, not started, with the test defaults (``orders-service``, instance ``i-1``, reconnect backoff
    0.05 s to 0.2 s, :data:`SCALED` timings) under ``settings`` (which may name other ``hubs``); shut down when the
    test ends."""
    targets = [hubs] if isinstance(hubs, FakeHub) else list(hubs)
    values: dict[str, Any] = {
        "service": "orders-service",
        "instance": "i-1",
        "reconnect_backoff": (0.05, 0.2),
        "hubs": tuple(h.address for h in targets),
    }
    app = App(Settings(**(values | settings)), _timings=SCALED if _timings is None else _timings)
    _apps.append(app)
    return app


async def start_app(hubs: FakeHub | Sequence[FakeHub], /, *, _timings: Timings | None = None, **settings: Any) -> App:
    """:func:`new_app`, started."""
    app = new_app(hubs, _timings=_timings, **settings)
    await app.start()
    return app


async def stop_apps() -> None:
    """Shuts down every app the test made, then fails if a task of the library is left."""
    try:
        while _apps:
            await _apps.pop().shutdown(timeout=5.0)
    finally:
        _apps.clear()
    assert_no_library_tasks()


def assert_no_library_tasks() -> None:
    """Fails if a task named ``snodra:...`` is still running."""
    left = sorted(t.get_name() for t in asyncio.all_tasks() if t.get_name().startswith("snodra:") and not t.done())
    assert not left, f"library tasks left: {left}"


def within(value: float, lo: float, hi: float) -> None:
    """Fails unless ``lo <= value <= hi``."""
    assert lo <= value <= hi, f"{value:.4f} is not between {lo:.4f} and {hi:.4f}"


async def eventually(predicate: Callable[[], bool], within: float = 1.0) -> None:
    """Waits until predicate holds; fails after ``within`` seconds."""
    deadline = time.monotonic() + within
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"the condition did not hold within {within} s")
        await asyncio.sleep(0.005)
