"""Helpers shared by the add-on's tests: the broker endpoint, stream operations and readers."""

from __future__ import annotations

import asyncio
import base64
import json
import socket
import urllib.request
import uuid
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from rstream import schema
from rstream.client import Client
from rstream.constants import OffsetType
from rstream.exceptions import OffsetNotFound, StreamDoesNotExist
from snodra import Delivery, EventHeaders, OutgoingEvent, SubscriptionEvent
from snodra_rabbitmq import RabbitMQConfig
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

RABBITMQ_IMAGE = "rabbitmq@sha256:753099a19b577c322117f793f864b3129cbba96d40317221a69d73976e3c843c"
"""RabbitMQ 4.4.0 with the management plugin, the image the wire rules were verified against."""

STREAM_PORT = 5552
MANAGEMENT_PORT = 15672
PLUGINS = b"[rabbitmq_management,rabbitmq_stream,rabbitmq_stream_management].\n"


def free_port() -> int:
    """A port the kernel had free a moment ago."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@dataclass(frozen=True, slots=True)
class Broker:
    """The run's broker: the stream endpoint and the management API."""

    host: str
    port: int
    management: str

    def config(self, **overrides: Any) -> RabbitMQConfig:
        values: dict[str, Any] = {
            "hosts": (self.host,),
            "port": self.port,
            "management_url": self.management,
            "status_refresh": 0.5,
            "confirm_timeout": 15.0,
            "request_timeout": 10.0,
            "management_timeout": 5.0,
        }
        values.update(overrides)
        return RabbitMQConfig(**values)


def management_get(broker: Broker, path: str) -> Any:
    request = urllib.request.Request(broker.management + "/api/" + path)
    request.add_header("Authorization", "Basic " + base64.b64encode(b"guest:guest").decode())
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def management_ready(broker: Broker) -> bool:
    try:
        management_get(broker, "overview")
        return True
    except Exception:
        return False


def start_broker(port: int) -> DockerContainer:
    config = f"stream.advertised_host = 127.0.0.1\nstream.advertised_port = {port}\n"
    return (
        DockerContainer(RABBITMQ_IMAGE)
        .with_exposed_ports(STREAM_PORT, MANAGEMENT_PORT)
        .with_bind_ports(STREAM_PORT, port)
        .with_copy_into_container(PLUGINS, "/etc/rabbitmq/enabled_plugins")
        .with_copy_into_container(config.encode(), "/etc/rabbitmq/conf.d/20-stream.conf")
        .waiting_for(LogMessageWaitStrategy("Server startup complete").with_startup_timeout(120))
    )


async def connect(broker: Broker, name: str = "snodra-tests") -> Client:
    """One stream protocol connection to the run's broker."""
    client = Client(
        broker.host,
        broker.port,
        connection_name=name,
        frame_max=1 << 20,
        heartbeat=30,
    )
    try:
        await client.start()
        await client.authenticate("/", "guest", "guest")
    except BaseException:
        with suppress(Exception):
            await client.close()
        raise
    return client


async def create_super_stream(broker: Broker, name: str, partitions: int) -> None:
    client = await connect(broker)
    try:
        await client.create_super_stream(
            name,
            [f"{name}-{index}" for index in range(partitions)],
            [str(index) for index in range(partitions)],
        )
    finally:
        await client.close()


async def create_stream(broker: Broker, name: str) -> None:
    client = await connect(broker)
    try:
        await client.create_stream(name)
    finally:
        await client.close()


async def delete_super_stream(broker: Broker, name: str) -> None:
    client = await connect(broker)
    try:
        with suppress(StreamDoesNotExist):
            await client.delete_super_stream(name)
    finally:
        await client.close()


async def delete_stream(broker: Broker, name: str) -> None:
    client = await connect(broker)
    try:
        with suppress(StreamDoesNotExist):
            await client.delete_stream(name)
    finally:
        await client.close()


async def wait_for(what: str, condition: Callable[[], bool], *, within: float = 30.0) -> None:
    """Waits until ``condition`` holds, failing with ``what`` after ``within`` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not condition():
        if loop.time() >= deadline:
            raise AssertionError(f"{what} within {within:g} s")
        await asyncio.sleep(0.025)


async def wait_for_async(what: str, condition: Callable[[], Awaitable[bool]], *, within: float = 30.0) -> None:
    """Waits until the awaitable ``condition`` holds, failing with ``what`` after ``within``."""
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
    ack: bool = True,
    within: float = 30.0,
) -> list[Delivery]:
    """Reads ``count`` deliveries, acknowledging every one by default."""
    deliveries: list[Delivery] = []
    async with asyncio.timeout(within):
        while len(deliveries) < count:
            event = await subscription.next()
            if not isinstance(event, Delivery):
                continue
            if ack:
                await subscription.ack(event.segment, event.position)
            deliveries.append(event)
    return deliveries


async def read_all(
    subscription: Any,
    *,
    until: Callable[[list[Delivery]], bool],
    ack: bool = True,
    within: float = 30.0,
) -> list[Delivery]:
    """Reads deliveries until ``until`` accepts the accumulated list."""
    deliveries: list[Delivery] = []
    async with asyncio.timeout(within):
        while not until(deliveries):
            event: SubscriptionEvent = await subscription.next()
            if isinstance(event, Delivery):
                if ack:
                    await subscription.ack(event.segment, event.position)
                deliveries.append(event)
    return deliveries


def ids(deliveries: Sequence[Delivery]) -> list[str]:
    return [delivery.headers.get(EventHeaders.ID, "") for delivery in deliveries]


def positions(deliveries: Sequence[Delivery]) -> list[str]:
    return [delivery.position for delivery in deliveries]


def event(
    source: str,
    key: str | None,
    *,
    event_id: str | None = None,
    type_name: str = "orders.Placed",
    extra: dict[str, str] | None = None,
) -> OutgoingEvent:
    """An event of the wire tests: the seven headers, a fresh id and a JSON payload."""
    event_id = event_id or str(uuid.uuid4())
    headers = {
        EventHeaders.TYPE: type_name,
        EventHeaders.ID: event_id,
        EventHeaders.CORRELATION_ID: str(uuid.uuid4()),
        EventHeaders.TIMESTAMP: "2026-11-26T10:00:00.123Z",
        EventHeaders.SOURCE: "orders-service",
        EventHeaders.USER_ID: "user-7",
        EventHeaders.TRACEPARENT: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    }
    if extra:
        headers.update(extra)
    return OutgoingEvent(
        source=source,
        id=event_id,
        type_name=type_name,
        key=key,
        headers=headers,
        payload=f'{{"id":"{event_id}","amount":42}}'.encode(),
    )


async def raw_stream_messages(stream: str, expected: int, *, broker: Broker, within: float = 30.0) -> list[bytes]:
    """Every AMQP message of one plain stream (a partition), read from the first offset."""
    client = await connect(broker, "snodra-tests-reader")
    messages: list[bytes] = []

    async def gate(frame: schema.Deliver) -> None:
        pass

    async def put(frame: schema.Deliver) -> None:
        for data in frame.get_messages():
            messages.append(data)

    try:
        client.add_stream(stream)
        client.add_handler(schema.Deliver, gate, name="1")
        await client.run_queue_listener_task(1, put)
        await client.subscribe(stream, 1, schema.OffsetSpec.from_params(OffsetType.FIRST, None), 10, None)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + within
        while loop.time() < deadline:
            await asyncio.sleep(0.05)
            if messages:
                await client.credit(1, 10)
            if len(messages) >= expected:
                # Take what the same chunk still holds, then let a quiet moment settle the rest.
                await asyncio.sleep(0.2)
                if len(messages) >= expected:
                    break
        if len(messages) < expected:
            raise AssertionError(f"the stream {stream!r} held {len(messages)} messages, wanted {expected}")
        return messages
    finally:
        with suppress(Exception):
            await client.stop_queue_listener_task(1)
        with suppress(Exception):
            await client.close()


async def stream_offset(stream: str, reference: str, *, broker: Broker) -> int | None:
    """The offset stored under ``reference`` on ``stream``, or ``None`` when none is."""
    client = await connect(broker, "snodra-tests-offset")
    try:
        try:
            return await client.query_offset(stream, reference)
        except OffsetNotFound:
            return None
    finally:
        with suppress(Exception):
            await client.close()


def management_consumers(broker: Broker) -> list[dict[str, Any]]:
    """The management API's stream consumer list."""
    body = management_get(broker, "stream/consumers/%2F")
    if isinstance(body, list):
        return body
    return list(body.get("items", []))
