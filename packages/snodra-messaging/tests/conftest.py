"""Shared pytest configuration."""

from collections.abc import AsyncIterator

import pytest
from fakehub import FakeHub
from helpers import jwks as jwks
from helpers import stop_apps
from helpers import token_endpoint as token_endpoint


@pytest.fixture(autouse=True)
async def _stop_apps() -> AsyncIterator[None]:
    """Shuts down every app a test started (``helpers.start_app``), and checks that no library task is left."""
    yield
    await stop_apps()


@pytest.fixture
async def hub() -> AsyncIterator[FakeHub]:
    """A fake hub, stopped when the test ends, whatever its outcome."""
    h = await FakeHub.start()
    try:
        yield h
    finally:
        await h.stop()


@pytest.fixture
async def hub2() -> AsyncIterator[FakeHub]:
    """A second fake hub, for tests with two."""
    h = await FakeHub.start()
    try:
        yield h
    finally:
        await h.stop()
