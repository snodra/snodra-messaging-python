"""One Apache Kafka broker for the add-on's tests, and the helpers that create topics and read them.

The image is pinned by digest, its host port is chosen before it starts (the advertised listener
names it), and every topic name carries a fresh random suffix and is deleted when its test ends.
Docker is required; a machine without it skips these tests.
"""

from __future__ import annotations

import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import TopicPartition
from snodra_kafka import KafkaConfig, KafkaEventStore
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

KAFKA_IMAGE = "apache/kafka@sha256:76af3d0116abbb33b4cd964af410d4921c33d1f663e7a95d5453a9c35a84869b"
"""Apache Kafka 4.4.0, the version the wire rules were verified against, pinned by digest."""

KAFKA_PORT = 9092


def _free_port() -> int:
    """A port the kernel had free a moment ago."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_broker(bootstrap: str, port: int) -> DockerContainer:
    return (
        DockerContainer(KAFKA_IMAGE)
        .with_exposed_ports(KAFKA_PORT)
        .with_bind_ports(KAFKA_PORT, port)
        .with_envs(
            CLUSTER_ID="5L6g3nShT-eMCtK--X86sw",
            KAFKA_NODE_ID="1",
            KAFKA_PROCESS_ROLES="broker,controller",
            KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
            KAFKA_ADVERTISED_LISTENERS=f"PLAINTEXT://{bootstrap}",
            KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093",
            KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1",
            KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1",
            KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1",
            KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="0",
            KAFKA_AUTO_CREATE_TOPICS_ENABLE="false",
        )
        .waiting_for(LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(120))
    )


@pytest.fixture(scope="session")
def kafka_bootstrap() -> Iterator[str]:
    """The run's broker, started once; without Docker every Kafka test skips."""
    last: Exception | None = None
    for _ in range(3):
        port = _free_port()
        bootstrap = f"127.0.0.1:{port}"
        container = _start_broker(bootstrap, port)
        try:
            container.start()
        except Exception as error:  # the image or Docker itself is missing
            last = error
            with suppress(Exception):
                container.stop()
            continue
        try:
            yield bootstrap
        finally:
            container.stop()
        return
    pytest.skip(f"the Kafka tests need Docker: {last}")


@pytest.fixture
async def admin(kafka_bootstrap: str) -> AsyncIterator[AIOKafkaAdminClient]:
    """An admin client for topics and consumer groups."""
    client = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap, request_timeout_ms=15_000)
    await client.start()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def create_topic(admin: AIOKafkaAdminClient) -> AsyncIterator[Callable[..., Awaitable[str]]]:
    """Creates one uniquely named topic and deletes every topic the test created afterwards."""
    topics: list[str] = []

    async def create(
        partitions: int = 1,
        *,
        prefix: str = "snodra.events",
        name: str | None = None,
        configs: dict[str, str] | None = None,
    ) -> str:
        from uuid import uuid4

        topic = name if name is not None else f"{prefix}.{uuid4().hex}"
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=partitions, replication_factor=1, topic_configs=configs or {})]
        )
        topics.append(topic)
        return topic

    yield create

    for topic in topics:
        with suppress(Exception):
            await admin.delete_topics([topic])


@pytest.fixture
async def make_store(kafka_bootstrap: str) -> AsyncIterator[Callable[..., KafkaEventStore]]:
    """A store factory over the run's broker; every store it made is closed afterwards."""
    stores: list[KafkaEventStore] = []

    def make(**overrides: Any) -> KafkaEventStore:
        config = KafkaConfig(bootstrap_servers=kafka_bootstrap, status_refresh=0.2, admin_timeout=15.0, **overrides)
        store = KafkaEventStore(config)
        stores.append(store)
        return store

    yield make

    for store in reversed(stores):
        await store.close()


async def wait_for(what: str, condition: Callable[[], bool], *, within: float = 15.0) -> None:
    """Waits until ``condition`` holds, failing with ``what`` after ``timeout`` seconds."""
    import asyncio

    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not condition():
        if loop.time() >= deadline:
            raise AssertionError(f"{what} within {within:g} s")
        await asyncio.sleep(0.025)


async def wait_for_async(what: str, condition: Callable[[], Awaitable[bool]], *, within: float = 15.0) -> None:
    """Waits until the awaitable ``condition`` holds, failing with ``what`` after ``timeout``."""
    import asyncio

    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not await condition():
        if loop.time() >= deadline:
            raise AssertionError(f"{what} within {within:g} s")
        await asyncio.sleep(0.05)


async def read_deliveries(
    subscription: Any,
    count: int,
    *,
    ack: Callable[[Any], bool] | None = None,
    within: float = 30.0,
) -> list[Any]:
    """Reads ``count`` deliveries, acknowledging those ``ack`` accepts (all by default)."""
    import asyncio

    from snodra import Delivery

    deliveries: list[Any] = []
    async with asyncio.timeout(within):
        while len(deliveries) < count:
            event = await subscription.next()
            if not isinstance(event, Delivery):
                continue
            if ack is None or ack(event):
                await subscription.ack(event.segment, event.position)
            deliveries.append(event)
    return deliveries


async def read_all(
    subscription: Any,
    *,
    until: Callable[[Any], bool],
    within: float = 30.0,
    ack: bool = True,
) -> list[Any]:
    """Reads deliveries until ``until`` accepts the accumulated list."""
    import asyncio

    from snodra import Delivery

    deliveries: list[Any] = []
    async with asyncio.timeout(within):
        while not until(deliveries):
            event = await subscription.next()
            if isinstance(event, Delivery):
                if ack:
                    await subscription.ack(event.segment, event.position)
                deliveries.append(event)
    return deliveries


async def raw_records(bootstrap: str, topic: str, partitions: int, expected: int) -> list[Any]:
    """Every record of the topic, read by a group-less consumer assigned to every partition."""
    import asyncio

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=15_000,
    )
    await consumer.start()
    try:
        topic_partitions = [TopicPartition(topic, partition) for partition in range(partitions)]
        consumer.assign(topic_partitions)
        await consumer.seek_to_beginning(*topic_partitions)
        records: list[Any] = []
        async with asyncio.timeout(30):
            while len(records) < expected:
                batches = await consumer.getmany(timeout_ms=500, max_records=expected)
                for batch in batches.values():
                    records.extend(batch)
        return records
    finally:
        await consumer.stop()


@dataclass(frozen=True, slots=True)
class GroupMember:
    """One group member as the broker describes it."""

    member_id: str
    client_id: str


@dataclass(frozen=True, slots=True)
class GroupDescription:
    """The broker's description of one consumer group."""

    error_code: int
    state: str
    protocol_type: str
    protocol: str
    members: tuple[GroupMember, ...]


async def describe_group(admin: AIOKafkaAdminClient, group: str) -> GroupDescription | None:
    """The broker's description of one consumer group, or ``None`` when it does not exist."""
    for response in await admin.describe_consumer_groups([group]):
        for described in response.to_object().get("groups", ()):
            if described["group"] == group:
                return GroupDescription(
                    error_code=described["error_code"],
                    state=described["state"],
                    protocol_type=described["protocol_type"],
                    protocol=described["protocol"],
                    members=tuple(
                        GroupMember(member_id=member["member_id"], client_id=member["client_id"])
                        for member in described["members"]
                    ),
                )
    return None


async def group_members(admin: AIOKafkaAdminClient, group: str) -> list[str]:
    """The group's member client ids (empty when it has none)."""
    described = await describe_group(admin, group)
    if described is None or described.error_code:
        return []
    return [member.client_id or member.member_id for member in described.members]


async def committed_offset(admin: AIOKafkaAdminClient, group: str, topic_partition: TopicPartition) -> int | None:
    """The group's committed offset of one partition, or ``None`` when it committed none."""
    listed = await admin.list_consumer_group_offsets(group, partitions=[topic_partition])
    metadata = listed.get(topic_partition)
    if metadata is None or metadata.offset < 0:
        return None
    return int(metadata.offset)


def positions(deliveries: Sequence[Any]) -> list[str]:
    """The positions of ``deliveries``."""
    return [delivery.position for delivery in deliveries]


def ids(deliveries: Sequence[Any]) -> list[str]:
    """The ``id`` headers of ``deliveries``."""
    from snodra import EventHeaders

    return [delivery.headers.get(EventHeaders.ID, "") for delivery in deliveries]
