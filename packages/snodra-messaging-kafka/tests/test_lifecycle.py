"""Subscription lifecycle without a broker: cancellation and a failed consumer start leave nothing."""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest
from aiokafka.errors import KafkaError
from snodra import Failed, SubscriptionSpec
from snodra_kafka import KafkaConfig, KafkaEventStore
from snodra_kafka import kafka as kafka_module

from .conftest import kafka_tasks, wait_for


class SlowConsumer:
    """A consumer whose start never finishes; enough of the client for a cancelled subscribe."""

    instances: ClassVar[list[SlowConsumer]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.stopped = asyncio.Event()
        self.subscribed: tuple[list[str], Any] | None = None
        type(self).instances.append(self)

    async def start(self) -> None:
        await asyncio.Event().wait()

    def subscribe(self, topics: list[str], listener: Any) -> None:
        self.subscribed = (topics, listener)

    async def stop(self) -> None:
        self.stopped.set()


class FailingConsumer(SlowConsumer):
    """A consumer whose start fails before it can subscribe."""

    async def start(self) -> None:
        raise KafkaError("no broker")


def _spec() -> SubscriptionSpec:
    return SubscriptionSpec(name="orders-service.audit", source="snodra.events")


async def test_cancelling_a_subscribe_stops_the_consumer_and_leaves_no_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    SlowConsumer.instances = []
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", SlowConsumer)
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="h:1"))
    task = asyncio.create_task(store.subscribe(_spec()))
    await wait_for("the slow consumer was created", lambda: bool(SlowConsumer.instances))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert SlowConsumer.instances[0].stopped.is_set(), "a cancelled subscribe closes its consumer"
    assert kafka_tasks() == [], "a cancelled subscribe leaves no snodra:kafka: task"
    await store.close()


async def test_a_failed_consumer_start_raises_failed_and_stops_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FailingConsumer.instances = []
    monkeypatch.setattr(kafka_module, "AIOKafkaConsumer", FailingConsumer)
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="h:1"))
    with pytest.raises(Failed):
        await store.subscribe(_spec())
    assert FailingConsumer.instances[0].stopped.is_set(), "a failed start closes its consumer"
    assert kafka_tasks() == [], "a failed start leaves no snodra:kafka: task"
    await store.close()
