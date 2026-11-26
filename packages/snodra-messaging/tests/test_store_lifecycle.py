"""The app drives a message store's StoreLifecycle: clock, schema, background work and its shutdown."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest
from eventhelpers import Clock
from fakehub import FakeHub
from helpers import library_tasks, new_app, start_app
from snodra import MemoryStore, SnodraError


class LifecycleStore(MemoryStore):
    """A memory store that implements StoreLifecycle and records what the app calls, in order."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []
        self.bound_clock: Callable[[], int] | None = None
        self.init_error: BaseException | None = None
        self.check_error: BaseException | None = None
        self.background_started = asyncio.Event()
        self.background_stopped = asyncio.Event()

    def use_clock(self, clock: Callable[[], int]) -> None:
        self.calls.append("use_clock")
        self.bound_clock = clock

    async def init_schema(self) -> None:
        self.calls.append("init_schema")
        if self.init_error is not None:
            raise self.init_error

    async def check_schema(self) -> None:
        self.calls.append("check_schema")
        if self.check_error is not None:
            raise self.check_error

    async def background(self) -> None:
        self.calls.append("background")
        self.background_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.background_stopped.set()


async def test_the_app_binds_the_store_lifecycle(hub: FakeHub) -> None:
    store = LifecycleStore()
    clock = Clock()
    app = new_app(hub, store=store, _clock=clock)
    await app.start()
    assert store.calls[:2] == ["use_clock", "init_schema"]
    assert store.bound_clock is clock
    await asyncio.wait_for(store.background_started.wait(), 1.0)
    assert "snodra:store-background" in library_tasks()
    await app.shutdown(timeout=5.0)
    assert store.background_stopped.is_set()
    assert "snodra:store-background" not in library_tasks()


async def test_init_schema_runs_by_default(hub: FakeHub) -> None:
    store = LifecycleStore()
    app = await start_app(hub, store=store)
    assert store.calls.count("init_schema") == 1
    assert "check_schema" not in store.calls
    await app.shutdown(timeout=5.0)


async def test_check_schema_runs_when_initialization_is_off(hub: FakeHub) -> None:
    store = LifecycleStore()
    app = await start_app(hub, store=store, store_init_schema=False)
    assert "init_schema" not in store.calls
    assert store.calls.count("check_schema") == 1
    await app.shutdown(timeout=5.0)


@pytest.mark.parametrize(
    ("prepare", "error_field", "what"),
    [(True, "init_error", "prepare"), (False, "check_error", "check")],
)
async def test_a_schema_failure_fails_start_naming_the_store(
    hub: FakeHub, prepare: bool, error_field: str, what: str
) -> None:
    store = LifecycleStore()
    setattr(store, error_field, RuntimeError("no permission"))
    app = new_app(hub, store=store, store_init_schema=prepare)
    pattern = rf"the message store {store.name} could not {what} its schema: no permission"
    with pytest.raises(SnodraError, match=pattern):
        await app.start()
    assert "background" not in store.calls
    assert "snodra:store-background" not in library_tasks()


async def test_a_store_without_a_lifecycle_is_used_as_it_is(hub: FakeHub) -> None:
    store = MemoryStore()
    app = await start_app(hub, store=store)
    assert "snodra:store-background" not in library_tasks()
    await app.shutdown(timeout=5.0)


async def test_a_command_only_service_without_a_store_starts_no_background(hub: FakeHub) -> None:
    app = await start_app(hub)
    assert "snodra:store-background" not in library_tasks()
    await app.shutdown(timeout=5.0)
