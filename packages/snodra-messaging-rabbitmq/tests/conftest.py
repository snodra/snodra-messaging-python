"""One RabbitMQ broker for the add-on's tests, and the fixtures that create streams and stores.

The image is pinned by digest and carries the stream and management plugins; the stream port is
chosen before the container starts (the advertised host names it), and every test uses a fresh
super stream and deletes it afterwards. Docker is required; a machine without it skips these
tests.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import suppress
from typing import Any

import pytest
from snodra_rabbitmq import RabbitMQEventStore
from support import (
    Broker,
    create_stream,
    create_super_stream,
    delete_stream,
    delete_super_stream,
    free_port,
    management_ready,
    start_broker,
)

__all__ = ["broker", "create_source", "make_store"]


@pytest.fixture(scope="session")
def broker() -> Iterator[Broker]:
    """The run's RabbitMQ broker; without Docker every add-on test skips."""
    last: Exception | None = None
    for _ in range(3):
        port = free_port()
        container = start_broker(port)
        try:
            container.start()
        except Exception as error:
            last = error
            with suppress(Exception):
                container.stop()
            continue
        management_port = int(container.get_exposed_port(15672))
        found = Broker(host="127.0.0.1", port=port, management=f"http://127.0.0.1:{management_port}")
        try:
            for _ in range(100):
                if management_ready(found):
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("the management API did not answer within 20 s")
            yield found
        finally:
            container.stop()
        return
    pytest.skip(f"the RabbitMQ tests need Docker: {last}")


@pytest.fixture
async def create_source(broker: Broker) -> AsyncIterator[Callable[..., Awaitable[str]]]:
    """Creates one uniquely named super stream (and its dead-letter stream on request), and
    deletes every stream the test created afterwards."""
    created: list[tuple[str, int]] = []

    async def create(
        partitions: int = 6,
        *,
        prefix: str = "snodra.events",
        name: str | None = None,
        dead_letter: bool = False,
    ) -> str:
        source = name if name is not None else f"{prefix}.{uuid.uuid4().hex}"
        if partitions:
            await create_super_stream(broker, source, partitions)
        else:
            await create_stream(broker, source)
        created.append((source, partitions))
        if dead_letter:
            await create_stream(broker, f"{source}.dlt")
        return source

    yield create

    for source, partitions in reversed(created):
        if partitions:
            with suppress(Exception):
                await delete_super_stream(broker, source)
        else:
            with suppress(Exception):
                await delete_stream(broker, source)
        with suppress(Exception):
            await delete_stream(broker, f"{source}.dlt")


@pytest.fixture
async def make_store(broker: Broker) -> AsyncIterator[Callable[..., RabbitMQEventStore]]:
    """A store factory over the run's broker; every store it made is closed afterwards."""
    stores: list[RabbitMQEventStore] = []

    def make(*, management: bool = True, **overrides: Any) -> RabbitMQEventStore:
        values: dict[str, Any] = {"management_url": broker.management if management else None}
        values.update(overrides)
        store = RabbitMQEventStore(broker.config(**values))
        stores.append(store)
        return store

    yield make

    for store in reversed(stores):
        await store.close()
