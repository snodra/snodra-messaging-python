"""Shared pytest configuration."""

from collections.abc import AsyncIterator

import pytest
from fakehub import FakeHub


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
