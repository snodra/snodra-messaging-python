"""The RabbitMQ Streams event store: a super stream for the events, one consumer name per processor.

On RabbitMQ Streams a source is a super stream (``snodra.events``, partition streams
``snodra.events-0`` to ``-N`` created by the infrastructure); a source that is a plain stream (the
dead-letter stream ``snodra.events.dlt``) is read and written as one partition. A segment is the
partition stream's suffix after ``<source>-`` (``3``), ``0`` for a plain stream; a position is the
offset in decimal.

The layout on the wire is the one every Snodra library writes
(https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules):

- one AMQP 1.0 message per event, published with protocol version 1, no sub-entry batching and no
  compression: exactly one ``data`` section (the JSON payload), the event id as the string
  ``properties.message-id``, the symbol ``application/json`` as ``properties.content-type``, the
  key as the symbol-keyed message annotation ``x-routing-key`` (absent when the event has none),
  and every header an application property with a string value;
- the partition is the stream clients' hash routing: murmur3 x86_32 of the routing key's UTF-8
  bytes (the event's key, else its id), seed 104729, taken unsigned, modulo the partitions in the
  broker's order;
- a processor's subscription is the consumer name ``<service>.<processor>`` on every partition,
  with ``single-active-consumer=true``, ``name``, ``super-stream=<source>`` (absent on a plain
  stream) and ``snodra-member=<a random id per instance>``;
- the offset stored per partition under the consumer name is the next offset to read (the
  acknowledged offset plus one), stored every 10 000 acknowledgements or 5 s and always on
  deactivation and close; a consumer with none starts at the first offset, and on activation it
  answers the broker's consumer update with the stored offset;
- a dead letter goes to the plain stream ``<source>.dlt``.

The store declares ``lag`` and ``native-dead-letter`` always, and ``replay`` only when the
management API is configured: only that API lists the instances reading a subscription, which a
reset needs. It has no event browser and declares no ``browse``. It never creates or changes
streams: the infrastructure does.

The client: this add-on pins ``rstream`` and uses its low-level :class:`rstream.client.Client` (the
protocol frames, the AMQP 1.0 codec and the connection), not its high-level producer or consumer.
The high-level consumer route hashes UTF-16 rather than the wire rule's UTF-8, its consumer update
listener cannot encode the reply without the internal ``schema.OffsetSpecification`` type, and its
consumer picks a replica rather than the partition's leader. See ``CLIENT-SPIKE.md`` and
``SPEC-GAPS.md``.

Every subscription owns its connections; :meth:`RabbitMQEventStore.close` releases the publisher's
connections and does not close the subscriptions (the caller closes them).

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import cmp_to_key
from typing import Any, Final

# TODO(spec-gap): SG-36
from rstream import AMQPMessage, schema
from rstream._pyamqp.message import Properties  # type: ignore[attr-defined]
from rstream.amqp import amqp_decoder
from rstream.client import Client
from rstream.constants import OffsetType
from rstream.exceptions import (
    AccessRefused,
    FrameTooLarge,
    LeaderNotAvailable,
    OffsetNotFound,
    ServerError,
    StreamDoesNotExist,
)
from snodra import (
    MAX_SEGMENT_BYTES,
    Assigned,
    Capability,
    Delivery,
    Earliest,
    EventStoreAdmin,
    EventStoreSubscription,
    Failed,
    Invalid,
    Latest,
    OutgoingEvent,
    Positions,
    Rejected,
    ReplayRange,
    ResetTarget,
    Revoked,
    SegmentStatus,
    StoreError,
    SubscribersActive,
    SubscriptionEvent,
    SubscriptionSpec,
    Timestamp,
    Unsupported,
    segment_order,
)

from snodra_rabbitmq.routing import (
    SINGLE_SEGMENT,
    compare_positions,
    parse_position,
    partition_for,
    position_of,
    routing_key,
    segment_of,
)

NAME: Final = "rabbitmq-streams"
"""The name this add-on reports as the processors' event store."""

STORE_CONNECTION: Final = "snodra-messaging"
"""The connection name every store connection carries (the wire rules name it)."""

MEMBER_PROPERTY: Final = "snodra-member"
"""The subscribe property that tells the instances of one subscription apart."""

KEY_ANNOTATION: Final = "x-routing-key"
"""The message annotation that carries the event's key (absent for an unkeyed event)."""

CONTENT_TYPE: Final = "application/json"
"""The content type every event message carries."""

DEFAULT_HOST: Final = "rabbitmq"
DEFAULT_PORT: Final = 5552
DEFAULT_TLS_PORT: Final = 5551

HOSTS_VARIABLE: Final = "SNODRA_RABBITMQ_HOSTS"
PORT_VARIABLE: Final = "SNODRA_RABBITMQ_PORT"
VIRTUAL_HOST_VARIABLE: Final = "SNODRA_RABBITMQ_VIRTUAL_HOST"
USERNAME_VARIABLE: Final = "SNODRA_RABBITMQ_USERNAME"
PASSWORD_VARIABLE: Final = "SNODRA_RABBITMQ_PASSWORD"
TLS_VARIABLE: Final = "SNODRA_RABBITMQ_TLS"
MANAGEMENT_URL_VARIABLE: Final = "SNODRA_RABBITMQ_MANAGEMENT_URL"

_ACCESS_REFUSED: Final = 16
_FRAME_TOO_LARGE: Final = 14
"""The publish refusals that no retry can fix."""

READY_LIMIT: Final = 1_000
"""The deliveries a partition buffers before it stops asking for chunks."""

FIRST_SETTLE: Final = 1.0
"""How long a subscription active on no partition waits before its first, empty assignment."""

QUIET: Final = 1.5
"""How long a stream may stay silent before a bounds read is taken as empty."""

STORE_WAIT: Final = 5.0
"""How long a stored offset may take to read back."""

LAYOUT_FOR: Final = 30.0
"""How long a source's layout and a stream's leader are trusted before they are read again."""

FRAME_OVERHEAD: Final = 25
"""The publish frame's own bytes around one message (size, key, version, publisher, count, ids)."""

_MAX_MANAGEMENT_ANSWER: Final = 16 << 20
"""The largest answer read from the management API."""

_CAPABILITIES: Final = frozenset({Capability.LAG, Capability.NATIVE_DEAD_LETTER})
"""What the store offers without the management API; ``replay`` joins it when one is configured."""


@dataclass(slots=True)
class RabbitMQConfig:
    """How the store reaches RabbitMQ, and the add-on's own bounds.

    ``hosts`` are ``host`` or ``host:port`` entries tried in order; a host without a port uses
    :attr:`port`. :meth:`from_env` reads the same ``SNODRA_RABBITMQ_*`` settings the other
    official add-ons read. ``management_url`` is the management API's root without credentials;
    setting it declares ``replay``. The management login defaults to the stream user's.
    """

    hosts: tuple[str, ...] = (DEFAULT_HOST,)
    port: int = 0
    """The port of a host given without one; ``0`` is 5552, or 5551 with TLS."""
    virtual_host: str = "/"
    username: str = "guest"
    password: str = "guest"
    tls: bool = False
    tls_ca_file: str | None = None
    management_url: str | None = None
    management_username: str | None = None
    management_password: str | None = None
    management_timeout: float = 5.0
    confirm_timeout: float = 30.0
    request_timeout: float = 10.0
    offset_store_messages: int = 10_000
    offset_store_interval: float = 5.0
    status_refresh: float = 2.0
    frame_max: int = 1 << 20
    heartbeat: int = 30

    def __post_init__(self) -> None:
        self.hosts = tuple(host.strip() for host in self.hosts if host.strip())
        if not self.hosts:
            raise Invalid("the RabbitMQ hosts are empty")
        if not 0 <= self.port <= 65535:
            raise Invalid(f"the RabbitMQ port must be 0 to 65535, got {self.port}")
        if not self.virtual_host:
            self.virtual_host = "/"
        if self.management_url is not None:
            self.management_url = self.management_url.strip().rstrip("/") or None
            if self.management_url is not None and not self.management_url.startswith(("http://", "https://")):
                raise Invalid(f"the management URL {self.management_url!r} is not http or https")
        if self.management_timeout <= 0:
            raise Invalid(f"the management timeout must be positive, got {self.management_timeout}")
        if self.confirm_timeout <= 0:
            raise Invalid(f"the confirm timeout must be positive, got {self.confirm_timeout}")
        if self.request_timeout <= 0:
            raise Invalid(f"the request timeout must be positive, got {self.request_timeout}")
        if self.offset_store_messages < 1:
            raise Invalid(f"the offset store messages must be at least 1, got {self.offset_store_messages}")
        if self.offset_store_interval <= 0:
            raise Invalid(f"the offset store interval must be positive, got {self.offset_store_interval}")
        if self.status_refresh <= 0:
            raise Invalid(f"the status refresh must be positive, got {self.status_refresh}")
        if self.frame_max < 1:
            raise Invalid(f"the frame max must be at least 1, got {self.frame_max}")
        if self.heartbeat < 0:
            raise Invalid(f"the heartbeat must not be negative, got {self.heartbeat}")

    @property
    def stream_port(self) -> int:
        """The port of a host that names none."""
        if self.port:
            return self.port
        return DEFAULT_TLS_PORT if self.tls else DEFAULT_PORT

    @property
    def management_user(self) -> str:
        """The management API's user (the stream user's when none is configured)."""
        return self.management_username if self.management_username is not None else self.username

    @property
    def management_password_value(self) -> str:
        """The management API's password (the stream password when none is configured)."""
        return self.management_password if self.management_password is not None else self.password

    @classmethod
    def from_env(cls) -> RabbitMQConfig:
        """The configuration from the environment.

        Reads ``SNODRA_RABBITMQ_HOSTS`` (comma-separated, default ``rabbitmq``),
        ``SNODRA_RABBITMQ_PORT`` (default 5552, or 5551 with TLS), ``SNODRA_RABBITMQ_VIRTUAL_HOST``
        (default ``/``), ``SNODRA_RABBITMQ_USERNAME`` and ``SNODRA_RABBITMQ_PASSWORD`` (default
        ``guest``), ``SNODRA_RABBITMQ_TLS`` (default false) and ``SNODRA_RABBITMQ_MANAGEMENT_URL``
        (no default: without it the store declares no ``replay``).
        """
        hosts = tuple(part.strip() for part in os.getenv(HOSTS_VARIABLE, "").split(",") if part.strip())
        port_text = os.getenv(PORT_VARIABLE, "").strip()
        try:
            port = int(port_text) if port_text else 0
        except ValueError as error:
            raise Invalid(f"{PORT_VARIABLE} is not a port: {port_text!r}") from error
        password = os.getenv(PASSWORD_VARIABLE)
        return cls(
            hosts=hosts or (DEFAULT_HOST,),
            port=port,
            virtual_host=os.getenv(VIRTUAL_HOST_VARIABLE, "").strip() or "/",
            username=os.getenv(USERNAME_VARIABLE, "").strip() or "guest",
            password="guest" if password is None else password,
            tls=_env_bool(TLS_VARIABLE),
            management_url=os.getenv(MANAGEMENT_URL_VARIABLE, "").strip() or None,
        )

    def ssl_context(self) -> ssl.SSLContext | None:
        """The TLS configuration, or ``None`` without TLS. The system roots verify a server; a
        ``tls_ca_file`` replaces them."""
        if not self.tls:
            return None
        context = ssl.create_default_context(cafile=self.tls_ca_file)
        return context

    def address_of(self, host: str) -> tuple[str, int]:
        """``host`` as ``(host, port)``, with :attr:`stream_port` when it names none."""
        if host.startswith("["):
            end = host.find("]")
            if end != -1:
                name = host[1:end]
                rest = host[end + 1 :]
                if rest.startswith(":") and rest[1:].isdigit():
                    return name, int(rest[1:])
                return name, self.stream_port
        name, separator, maybe_port = host.rpartition(":")
        if separator and maybe_port.isdigit() and ":" not in name:
            return name, int(maybe_port)
        return host, self.stream_port


def _env_bool(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    if value in {"", "0", "false", "no", "off"}:
        return False
    if value in {"1", "true", "yes", "on"}:
        return True
    raise Invalid(f"{name} is not a boolean: {value!r}")


@dataclass(frozen=True, slots=True)
class _Layout:
    """How a source is partitioned: the streams in the broker's order, and their segments."""

    streams: tuple[str, ...]
    segments: tuple[str, ...]
    super: bool


@dataclass(slots=True)
class _Part:
    """One partition a subscription reads: its connection, its offsets and its buffered events."""

    segment: str
    stream: str
    client: Client
    active: bool = False
    next: int | None = None
    handed: int | None = None
    acked: int | None = None
    stored: int | None = None
    head: int | None = None
    ready: deque[Delivery] = field(default_factory=deque)
    inflight: list[Delivery] = field(default_factory=list)
    owed: bool = False
    unstored: int = 0
    last_store: float = field(default_factory=time.monotonic)
    last_store_offset: int | None = None
    store_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def encode_message(event: OutgoingEvent) -> bytes:
    """The event as the wire rules' AMQP 1.0 message: one data section, the id and content type,
    the key annotation and every header an application property (names sorted, never by place)."""
    names = sorted(event.headers)
    message = AMQPMessage(
        body=event.payload,
        properties=Properties(message_id=event.id, content_type=CONTENT_TYPE),
        message_annotations=None if event.key is None else {KEY_ANNOTATION: event.key},
        application_properties={name: event.headers[name] for name in names},
    )
    return bytes(message)


def decode_message(data: bytes) -> tuple[str | None, dict[str, str], bytes]:
    """The key, headers and payload of one AMQP 1.0 message a subscription delivered."""
    message = amqp_decoder(data)
    annotations = message.message_annotations or {}
    key: str | None = None
    for name, value in annotations.items():
        if _text(name) == KEY_ANNOTATION:
            key = _text(value)
            break
    headers = {_text(name): _text(value) for name, value in (message.application_properties or {}).items()}
    body = message.body
    payload = body if isinstance(body, bytes) else b""
    return key, headers, payload


def _text(value: Any) -> str:
    """An AMQP simple value as text: a string's, symbol's or binary's bytes, other values as their
    text, ``None`` as empty."""
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


class RabbitMQEventStore:
    """The RabbitMQ Streams event store: a publisher connection per leader and a subscription per
    processor instance. The store never creates or changes streams."""

    def __init__(self, config: RabbitMQConfig | None = None) -> None:
        self._config = config if config is not None else RabbitMQConfig.from_env()
        self._ssl = self._config.ssl_context()
        self._closed = False
        self._seed: Client | None = None
        self._seed_lock = asyncio.Lock()
        self._publish_lock = asyncio.Lock()
        self._layouts: dict[str, tuple[float, _Layout]] = {}
        self._leaders: dict[str, tuple[float, tuple[str, int]]] = {}
        self._publishers: dict[tuple[str, int], _PublisherConnection] = {}
        self._next_publishing_id = 1
        self._live: set[tuple[str, str, str]] = set()
        self._management = (
            _Management(
                self._config.management_url,
                self._config.management_user,
                self._config.management_password_value,
                self._config.virtual_host,
                self._config.management_timeout,
            )
            if self._config.management_url is not None
            else None
        )

    @property
    def config(self) -> RabbitMQConfig:
        """The configuration the store was opened with."""
        return self._config

    @property
    def name(self) -> str:
        return NAME

    @property
    def capabilities(self) -> frozenset[Capability]:
        # TODO(spec-gap): SG-39
        if self._management is not None:
            return _CAPABILITIES | {Capability.REPLAY}
        return _CAPABILITIES

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    async def publish(self, event: OutgoingEvent) -> None:
        """Appends ``event`` to its source and returns once the broker confirmed it. A blank
        source, a value that is not UTF-8 text or a frame over the broker's limit is
        :class:`~snodra.Rejected`; a missing stream and any other trouble is
        :class:`~snodra.Failed`."""
        if self._closed:
            raise Failed("rabbitmq: the event store is closed")
        # TODO(spec-gap): SG-18
        if not event.source.strip():
            raise Rejected("an event needs a source")
        try:
            event.id.encode("utf-8")
            if event.key is not None:
                event.key.encode("utf-8")
            for name, value in event.headers.items():
                name.encode("utf-8")
                value.encode("utf-8")
            encoded = encode_message(event)
        except UnicodeEncodeError as error:
            raise Rejected(f"rabbitmq: {error}") from error
        async with self._publish_lock:
            seed = await self._seed_client()
            layout = await self._layout(seed, event.source)
            index = partition_for(routing_key(event.key, event.id), layout.streams)
            stream = layout.streams[index]
            address = await self._leader(seed, stream)
            connection = await self._publisher_connection(address)
            try:
                publisher = await connection.publisher(stream)
            except ServerError as error:
                await self._drop_publisher(address, connection)
                if isinstance(error, AccessRefused | FrameTooLarge):
                    raise Rejected(f"rabbitmq: declare publisher: {error}") from error
                raise Failed(f"rabbitmq: declare publisher on {stream}: {error}") from error
            except Exception as error:
                await self._drop_publisher(address, connection)
                raise Failed(f"rabbitmq: declare publisher on {stream}: {error}") from error
            client = connection.client
            if client is None:
                raise Failed("rabbitmq: the publisher connection is closed")
            if len(encoded) + FRAME_OVERHEAD > client.frame_max:
                raise Rejected(
                    f"rabbitmq: the event {event.id} is {len(encoded) + FRAME_OVERHEAD} bytes on the wire, "
                    f"over the {client.frame_max}-byte frame"
                )
            publishing_id = self._next_publishing_id
            self._next_publishing_id = (self._next_publishing_id + 1) or 1
            future = connection.expect(publishing_id)
            try:
                await connection.send(publisher, publishing_id, encoded)
                await asyncio.wait_for(future, self._config.confirm_timeout)
            except TimeoutError as error:
                connection.forget(publishing_id)
                await self._drop_publisher(address, connection)
                raise Failed(
                    f"rabbitmq: the broker did not confirm an event to {event.source} within "
                    f"{self._config.confirm_timeout:g} s: the connection is replaced"
                ) from error
            except asyncio.CancelledError:
                connection.forget(publishing_id)
                raise
            except StoreError:
                connection.forget(publishing_id)
                await self._drop_publisher(address, connection)
                raise
            except Exception as error:
                connection.forget(publishing_id)
                await self._drop_publisher(address, connection)
                raise Failed(f"rabbitmq: publishing to {stream}: {error}") from error

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        """Joins ``spec`` as one more instance of its subscription. The source must exist: it is
        the infrastructure's, and this add-on never creates streams."""
        # TODO(spec-gap): SG-18
        if not spec.name or not spec.source:
            raise Invalid("a subscription needs a name and a source")
        if self._closed:
            raise Failed("rabbitmq: the event store is closed")
        seed = await self._seed_client()
        layout = await self._layout(seed, spec.source, fresh=True)
        subscription = RabbitMQSubscription(self, spec, layout, self._member_id())
        try:
            await subscription.start()
        except asyncio.CancelledError:
            await subscription._abandon()
            raise
        except StoreError:
            await subscription._abandon()
            raise
        except Exception as error:
            await subscription._abandon()
            raise Failed(f"rabbitmq: the subscription {spec.name!r} could not join: {error}") from error
        self._live.add((spec.source, spec.name, subscription.member))
        return subscription

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        """The reset side of ``spec``; :class:`~snodra.Unsupported` without the management API."""
        # TODO(spec-gap): SG-18
        if not spec.name or not spec.source:
            raise Invalid("a subscription needs a name and a source")
        if self._management is None:
            raise Unsupported(Capability.REPLAY)
        return RabbitMQAdmin(self, spec)

    def compare(self, a: str, b: str) -> int:
        """Offsets by value; text that is not an offset after every offset (by its bytes among
        themselves)."""
        return compare_positions(a, b)

    async def close(self) -> None:
        """Releases the seed and publisher connections. The subscriptions are the caller's to
        close; this call is idempotent."""
        if self._closed:
            return
        self._closed = True
        async with self._seed_lock:
            seed, self._seed = self._seed, None
            if seed is not None:
                with contextlib.suppress(Exception):
                    await seed.close()
        connections = list(self._publishers.values())
        self._publishers.clear()
        for connection in connections:
            await connection.close()
        self._layouts.clear()
        self._leaders.clear()

    def _member_id(self) -> str:
        """A random id per subscription instance, the ``snodra-member`` property."""
        return f"{random.getrandbits(128):032x}"

    def _live_members(self, source: str, name: str) -> list[str]:
        return sorted(
            member for found_source, found_name, member in self._live if found_source == source and found_name == name
        )

    def _forget_live(self, source: str, name: str, member: str) -> None:
        self._live.discard((source, name, member))

    async def _seed_client(self) -> Client:
        if self._closed:
            raise Failed("rabbitmq: the event store is closed")
        async with self._seed_lock:
            if self._seed is not None and self._seed.is_connection_alive():
                return self._seed
            if self._seed is not None:
                with contextlib.suppress(Exception):
                    await self._seed.close()
                self._seed = None
            last: Exception | None = None
            for host in self._config.hosts:
                address = self._config.address_of(host)
                try:
                    self._seed = await self._dial(address)
                    return self._seed
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    last = error
            raise Failed(f"rabbitmq: no host answered: {last}")

    async def _dial(
        self,
        address: tuple[str, int],
        *,
        name: str = STORE_CONNECTION,
        closed: Callable[[Any], Any] | None = None,
    ) -> Client:
        client = Client(
            address[0],
            address[1],
            ssl_context=self._ssl,
            frame_max=self._config.frame_max,
            heartbeat=self._config.heartbeat,
            connection_name=name,
            connection_closed_handler=closed,
        )
        try:
            async with asyncio.timeout(self._config.request_timeout):
                await client.start()
                await client.authenticate(self._config.virtual_host, self._config.username, self._config.password)
        except TimeoutError as error:
            with contextlib.suppress(Exception):
                await client.close()
            raise Failed(
                f"rabbitmq: connecting to {address[0]}:{address[1]} did not finish within "
                f"{self._config.request_timeout:g} s"
            ) from error
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await client.close()
            raise
        except Exception:
            with contextlib.suppress(Exception):
                await client.close()
            raise
        return client

    async def _request[T](self, what: str, call: Awaitable[T]) -> T:
        """One stream request bounded by :attr:`RabbitMQConfig.request_timeout`."""
        try:
            async with asyncio.timeout(self._config.request_timeout):
                return await call
        except TimeoutError as error:
            raise Failed(f"rabbitmq: {what} did not answer within {self._config.request_timeout:g} s") from error

    async def _layout(self, seed: Client, source: str, *, fresh: bool = False) -> _Layout:
        if not fresh:
            cached = self._layouts.get(source)
            if cached is not None and time.monotonic() - cached[0] < LAYOUT_FOR:
                return cached[1]
        try:
            streams = tuple(await seed.partitions(source))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise Failed(f"rabbitmq: the partitions of {source!r}: {error}") from error
        if streams:
            segments = tuple(segment_of(source, stream) for stream in streams)
            for segment in segments:
                if not segment or len(segment.encode()) > MAX_SEGMENT_BYTES:
                    raise Invalid(
                        f"the partition of {source!r} names a segment over {MAX_SEGMENT_BYTES} bytes: {segment!r}"
                    )
            layout = _Layout(streams=streams, segments=segments, super=True)
        else:
            try:
                await seed.query_leader_and_replicas(source)
            except StreamDoesNotExist as error:
                # TODO(spec-gap): SG-42
                raise Failed(
                    f"rabbitmq: neither a super stream nor a stream named {source!r} exists: create it "
                    f"(rabbitmq-streams add_super_stream {source} --partitions 6)"
                ) from error
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise Failed(f"rabbitmq: the metadata of {source!r}: {error}") from error
            layout = _Layout(streams=(source,), segments=(SINGLE_SEGMENT,), super=False)
        self._layouts[source] = (time.monotonic(), layout)
        return layout

    async def _leader(self, seed: Client, stream: str, *, fresh: bool = False) -> tuple[str, int]:
        if not fresh:
            cached = self._leaders.get(stream)
            if cached is not None and time.monotonic() - cached[0] < LAYOUT_FOR:
                return cached[1]
        try:
            broker, _ = await seed.query_leader_and_replicas(stream)
        except LeaderNotAvailable as error:
            raise Failed(f"rabbitmq: the stream {stream!r} has no leader right now") from error
        except StreamDoesNotExist as error:
            raise Failed(f"rabbitmq: the stream {stream!r} does not exist") from error
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise Failed(f"rabbitmq: the leader of {stream!r}: {error}") from error
        address = (broker.host, int(broker.port))
        self._leaders[stream] = (time.monotonic(), address)
        return address

    async def _publisher_connection(self, address: tuple[str, int]) -> _PublisherConnection:
        connection = self._publishers.get(address)
        if connection is not None and connection.alive:
            return connection
        if connection is not None:
            await connection.close()
        connection = _PublisherConnection(self, address)
        try:
            await connection.start()
        except asyncio.CancelledError:
            await connection.close()
            raise
        except Exception as error:
            await connection.close()
            raise Failed(f"rabbitmq: the connection to {address[0]}:{address[1]}: {error}") from error
        self._publishers[address] = connection
        return connection

    async def _drop_publisher(self, address: tuple[str, int], connection: _PublisherConnection) -> None:
        if self._publishers.get(address) is connection:
            del self._publishers[address]
        self._layouts.clear()
        self._leaders.clear()
        await connection.close()

    async def _read_chunk(
        self,
        address: tuple[str, int],
        stream: str,
        offset_type: OffsetType,
        *,
        offset: int | None = None,
        within: float = QUIET,
        credit: int = 1,
    ) -> schema.Deliver | None:
        """One chunk at ``offset_type``, read on a connection of its own; ``None`` when nothing
        comes within ``timeout``."""
        client = await self._dial(address)
        frames: asyncio.Queue[schema.Deliver] = asyncio.Queue()

        async def gate(frame: schema.Deliver) -> None:
            pass

        async def put(frame: schema.Deliver) -> None:
            await frames.put(frame)

        try:
            client.add_stream(stream)
            client.add_handler(schema.Deliver, gate, name="1")
            await client.run_queue_listener_task(1, put)
            spec = schema.OffsetSpec.from_params(offset_type, offset)
            await self._request(f"subscribing {stream!r}", client.subscribe(stream, 1, spec, credit, None))
            try:
                async with asyncio.timeout(within):
                    return await frames.get()
            except TimeoutError:
                return None
        finally:
            with contextlib.suppress(Exception):
                await client.stop_queue_listener_task(1)
            with contextlib.suppress(Exception):
                await client.close()

    async def _bounds(self, address: tuple[str, int], stream: str) -> tuple[int, int, int, bool]:
        """A stream's first offset, its head, its last chunk's time and whether it is empty.
        Both reads silent: empty. One silent beside one that answered: a failure, never a
        shortened stream."""
        # TODO(spec-gap): SG-38
        first_read, last_read = await asyncio.gather(
            self._read_chunk(address, stream, OffsetType.FIRST),
            self._read_chunk(address, stream, OffsetType.LAST),
        )
        if first_read is None and last_read is None:
            return 0, 0, 0, True
        if first_read is None or last_read is None:
            raise Failed(
                f"rabbitmq: the bounds of {stream!r} were not read: one read answered, the other did not "
                f"within {QUIET:g} s"
            )
        first = int(first_read.chunk_first_offset)
        head = int(last_read.chunk_first_offset) + int(last_read.num_records)
        if head < first:
            head = first
        return first, head, int(last_read.timestamp), False

    async def _offset_at(self, address: tuple[str, int], stream: str, timestamp_ms: int, first: int, head: int) -> int:
        """The first offset of a chunk written at or after ``timestamp_ms``, else the head."""
        client = await self._dial(address)
        frames: asyncio.Queue[schema.Deliver] = asyncio.Queue()

        async def gate(frame: schema.Deliver) -> None:
            pass

        async def put(frame: schema.Deliver) -> None:
            await frames.put(frame)

        try:
            client.add_stream(stream)
            client.add_handler(schema.Deliver, gate, name="1")
            await client.run_queue_listener_task(1, put)
            await self._request(
                f"the timestamp read of {stream!r}",
                client.subscribe(
                    stream, 1, schema.OffsetSpec.from_params(OffsetType.TIMESTAMP, timestamp_ms), 16, None
                ),
            )
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._config.request_timeout + QUIET
            while loop.time() < deadline:
                try:
                    async with asyncio.timeout(max(0.001, deadline - loop.time())):
                        frame = await frames.get()
                except TimeoutError:
                    break
                if int(frame.timestamp) >= timestamp_ms:
                    return max(first, int(frame.chunk_first_offset))
                if int(frame.chunk_first_offset) + int(frame.num_records) >= head:
                    return head
                await client.credit(1, 1)
            raise Failed(
                f"rabbitmq: the timestamp read of {stream!r} did not reach {timestamp_ms} within "
                f"{self._config.request_timeout + QUIET:g} s"
            )
        finally:
            with contextlib.suppress(Exception):
                await client.stop_queue_listener_task(1)
            with contextlib.suppress(Exception):
                await client.close()


class _PublisherConnection:
    """One connection to a node with the publishers this store declared on it."""

    def __init__(self, store: RabbitMQEventStore, address: tuple[str, int]) -> None:
        self._store = store
        self._address = address
        self.client: Client | None = None
        self.alive = False
        self._publishers: dict[str, int] = {}
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[None]] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        def closed(info: Any) -> None:
            self._connection_closed(info)

        client = await self._store._dial(self._address, closed=closed)
        client.add_handler(schema.PublishConfirm, self._confirmed, name="publish-confirm")
        client.add_handler(schema.PublishError, self._refused, name="publish-error")
        self.client = client
        self.alive = True

    async def publisher(self, stream: str) -> int:
        if self.client is None or not self.alive:
            raise Failed("rabbitmq: the publisher connection is closed")
        async with self._lock:
            known = self._publishers.get(stream)
            if known is not None:
                return known
            if self._next_id > 255:
                raise Failed(
                    f"rabbitmq: 255 publishers were declared on the connection to "
                    f"{self._address[0]}:{self._address[1]}: it is replaced"
                )
            publisher = self._next_id
            self._next_id += 1
            await self._store._request(
                f"declaring the publisher for {stream!r}",
                self.client.declare_publisher(stream, None, publisher),
            )
            self.client.add_stream(stream)
            self._publishers[stream] = publisher
            return publisher

    def expect(self, publishing_id: int) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._pending[publishing_id] = future
        return future

    def forget(self, publishing_id: int) -> None:
        self._pending.pop(publishing_id, None)

    async def send(self, publisher: int, publishing_id: int, data: bytes) -> None:
        if self.client is None:
            raise Failed("rabbitmq: the publisher connection is closed")
        message = schema.Message(publishing_id=publishing_id, filter_value=None, data=data)
        await self.client.publish([message], publisher)

    def _confirmed(self, frame: schema.PublishConfirm) -> None:
        for publishing_id in frame.publishing_ids:
            future = self._pending.pop(int(publishing_id), None)
            if future is not None and not future.done():
                future.set_result(None)

    def _refused(self, frame: schema.PublishError) -> None:
        for error in frame.errors:
            future = self._pending.pop(int(error.publishing_id), None)
            if future is None or future.done():
                continue
            code = int(error.response_code)
            if code in {_ACCESS_REFUSED, _FRAME_TOO_LARGE}:
                future.set_exception(Rejected(f"rabbitmq: publish: response code 0x{code:02x}"))
            else:
                future.set_exception(Failed(f"rabbitmq: publish: response code 0x{code:02x}"))

    def _connection_closed(self, info: Any) -> None:
        self.alive = False
        for future in self._pending.values():
            if not future.done():
                future.set_exception(Failed(f"rabbitmq: the connection to {self._address[0]} closed: {info.reason}"))
        self._pending.clear()

    async def close(self) -> None:
        self.alive = False
        for future in self._pending.values():
            if not future.done():
                future.set_exception(Failed("rabbitmq: the publisher connection was closed"))
        self._pending.clear()
        client, self.client = self.client, None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.close()


class RabbitMQSubscription:
    """One instance's subscription of a processor: a single active consumer per partition stream,
    its consumer-update replies, stored offsets and buffered deliveries."""

    def __init__(
        self,
        store: RabbitMQEventStore,
        spec: SubscriptionSpec,
        layout: _Layout,
        member: str,
    ) -> None:
        self._store = store
        self._spec = spec
        self._layout = layout
        self._member = member
        self._parts: list[_Part] = []
        self._changes: deque[SubscriptionEvent] = deque()
        self._paused = False
        self._closed = False
        self._abandoned = False
        self._broken: str | None = None
        self._error: str | None = None
        self._announced = False
        self._turn = 0
        self._wake = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._updates: set[asyncio.Task[None]] = set()
        self._close_lock = asyncio.Lock()

    @property
    def member(self) -> str:
        """This subscription instance's ``snodra-member`` id."""
        return self._member

    async def start(self) -> None:
        seed = await self._store._seed_client()
        try:
            for index, stream in enumerate(self._layout.streams):
                address = await self._store._leader(seed, stream, fresh=True)

                def closed(info: Any, stream: str = stream) -> None:
                    self._fail(f"rabbitmq: the connection to {stream!r} closed: {info.reason}")

                client = await self._store._dial(address, closed=closed)
                part = _Part(segment=self._layout.segments[index], stream=stream, client=client)
                self._parts.append(part)
                client.add_stream(stream)
                client.add_handler(schema.Deliver, self._deliver_gate, name="1")
                await client.run_queue_listener_task(1, self._deliver_handler(part))
                client.add_handler(
                    schema.ConsumerUpdateResponse,
                    self._update_handler(part),
                    name="consumer-update",
                )
                client.add_handler(
                    schema.MetadataUpdate,
                    self._metadata_handler(part),
                    name="metadata-update",
                )
                properties = {
                    "single-active-consumer": "true",
                    "name": self._spec.name,
                    MEMBER_PROPERTY: self._member,
                }
                if self._layout.super:
                    properties["super-stream"] = self._spec.source
                try:
                    await self._store._request(
                        f"subscribing {stream!r}",
                        client.subscribe(
                            stream,
                            1,
                            schema.OffsetSpec.from_params(OffsetType.NEXT, None),
                            1,
                            properties,
                        ),
                    )
                except ServerError as error:
                    raise Failed(f"rabbitmq: subscribing {stream!r}: {error}") from error
        except BaseException:
            await self._abandon()
            raise
        self._tasks.append(asyncio.create_task(self._first_settle(), name=f"snodra:rabbitmq:{self._spec.name}"))
        self._tasks.append(asyncio.create_task(self._store_loop(), name=f"snodra:rabbitmq:{self._spec.name}"))
        self._tasks.append(asyncio.create_task(self._head_loop(), name=f"snodra:rabbitmq:{self._spec.name}"))

    async def next(self) -> SubscriptionEvent:
        """The next assignment change or delivery; waits for one."""
        while True:
            if self._closed:
                raise Failed(f"the subscription {self._spec.name!r} is closed")
            if self._changes:
                return self._changes.popleft()
            if not self._paused:
                for offset in range(len(self._parts)):
                    part = self._parts[(self._turn + offset) % len(self._parts)]
                    if not part.active or not part.ready:
                        continue
                    delivery = part.ready.popleft()
                    part.inflight.append(delivery)
                    value = parse_position(delivery.position)
                    if part.handed is None or value + 1 > part.handed:
                        part.handed = value + 1
                    self._turn = (self._turn + offset + 1) % len(self._parts)
                    owed = part.owed and len(part.ready) < READY_LIMIT
                    part.owed = part.owed and not owed
                    if owed:
                        with contextlib.suppress(Exception):
                            await part.client.credit(1, 1)
                    return delivery
            if self._broken is not None:
                raise Failed(self._broken)
            self._wake.clear()
            if self._changes or self._broken is not None or self._closed:
                continue
            await self._wake.wait()

    async def ack(self, segment: str, position: str) -> None:
        """Acknowledges the delivery at ``position`` and every earlier one; the stored offset is
        the next one to read."""
        if self._closed:
            raise Failed(f"the subscription {self._spec.name!r} is closed")
        offset = parse_position(position)
        part = self._part(segment)
        if part is None or not part.active:
            return
        next_offset = offset + 1
        if part.acked is not None and part.acked >= next_offset:
            return
        if part.handed is not None and offset >= part.handed:
            return
        if part.stored is not None and offset < part.stored:
            return
        part.acked = next_offset
        part.inflight = [delivery for delivery in part.inflight if parse_position(delivery.position) > offset]
        part.unstored += 1
        due = (
            part.unstored >= self._store.config.offset_store_messages
            or time.monotonic() - part.last_store >= self._store.config.offset_store_interval
        )
        if due:
            part.unstored = 0
            part.last_store = time.monotonic()
            await self._store_offset(part, next_offset)

    async def redeliver(self, segment: str, position: str) -> None:
        """Hands the delivery at ``position`` and the segment's later ones out again, without
        leaving the subscription."""
        from_offset = parse_position(position)
        part = self._part(segment)
        if part is None or not part.active:
            return
        again = [delivery for delivery in part.inflight if parse_position(delivery.position) >= from_offset]
        part.inflight = [delivery for delivery in part.inflight if parse_position(delivery.position) < from_offset]
        if again:
            part.ready = deque(again + list(part.ready))
        self._signal()

    def pause(self) -> None:
        """Stops delivering without losing the place; assignment changes still come through."""
        self._paused = True

    def resume(self) -> None:
        """Delivers again from where the pause stopped."""
        if not self._paused:
            return
        self._paused = False
        for part in self._parts:
            if part.active and part.owed and len(part.ready) < READY_LIMIT:
                part.owed = False
                task = asyncio.create_task(self._credit(part), name=f"snodra:rabbitmq:{self._spec.name}")
                self._updates.add(task)
                task.add_done_callback(self._updates.discard)
        self._signal()

    def segments(self) -> list[SegmentStatus]:
        """The active segments, in :func:`snodra.segment_order`, with positions, committed
        offsets, heads and lag as last known."""
        statuses: list[SegmentStatus] = []
        for part in self._parts:
            if not part.active:
                continue
            if part.ready:
                position = part.ready[0].position
            elif part.next is not None:
                position = position_of(part.next)
            else:
                position = None
            if part.acked is not None:
                committed = position_of(part.acked)
            elif part.stored is not None:
                committed = position_of(part.stored)
            else:
                committed = None
            base = part.next if part.next is not None else part.stored
            pending = len(part.ready) + len(part.inflight)
            if part.head is not None and base is not None and part.head > base:
                pending += part.head - base
            statuses.append(
                SegmentStatus(
                    segment=part.segment,
                    position=position,
                    committed=committed,
                    head=None if part.head is None else position_of(part.head),
                    lag=pending,
                )
            )
        statuses.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return statuses

    def take_error(self) -> str | None:
        """The latest trouble the subscription recovered from by itself, and clears it."""
        error, self._error = self._error, None
        return error

    async def close(self) -> None:
        """Leaves the subscription after flushing the acknowledgements; idempotent."""
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._signal()
            for task in self._tasks:
                task.cancel()
            for task in self._tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            self._tasks.clear()
            for task in list(self._updates):
                task.cancel()
            for task in list(self._updates):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            self._updates.clear()
            for part in self._parts:
                if part.acked is not None:
                    await self._store_and_verify(part, part.acked)
            for part in self._parts:
                with contextlib.suppress(Exception):
                    if part.client.is_connection_alive():
                        await part.client.unsubscribe(1)
                with contextlib.suppress(Exception):
                    await part.client.stop_queue_listener_task(1)
                with contextlib.suppress(Exception):
                    await part.client.close()
            self._store._forget_live(self._spec.source, self._spec.name, self._member)

    async def _abandon(self) -> None:
        """Closes what a failed join opened, storing nothing."""
        if self._abandoned:
            return
        self._abandoned = True
        self._closed = True
        self._signal()
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        for task in list(self._updates):
            task.cancel()
        self._updates.clear()
        for part in self._parts:
            with contextlib.suppress(Exception):
                await part.client.stop_queue_listener_task(1)
            with contextlib.suppress(Exception):
                await part.client.close()
        self._store._forget_live(self._spec.source, self._spec.name, self._member)

    async def _first_settle(self) -> None:
        await asyncio.sleep(FIRST_SETTLE)
        if self._closed:
            return
        if not self._announced:
            self._announced = True
            self._changes.append(Assigned())
            self._signal()

    async def _store_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self._store.config.offset_store_interval)
            for part in self._parts:
                if not part.active or part.acked is None:
                    continue
                if part.last_store_offset is not None and part.acked <= part.last_store_offset:
                    continue
                await self._store_offset(part, part.acked)
                part.unstored = 0
                part.last_store = time.monotonic()

    async def _head_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self._store.config.status_refresh)
            for part in self._parts:
                if not part.active or self._closed:
                    continue
                try:
                    seed = await self._store._seed_client()
                    address = await self._store._leader(seed, part.stream)
                    last = await self._store._read_chunk(address, part.stream, OffsetType.LAST)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._error = f"rabbitmq: the head of {part.stream!r} was not read: {error}"
                    continue
                if last is not None and part.active:
                    head = int(last.chunk_first_offset) + int(last.num_records)
                    part.head = head if part.head is None else max(part.head, head)

    def _deliver_gate(self, frame: schema.Deliver) -> None:
        """The listener's per-subscription marker; deliveries are queued to the handler below."""

    def _deliver_handler(self, part: _Part) -> Any:
        async def handle(frame: schema.Deliver) -> None:
            if frame.subscription_id != 1:
                return
            await self._delivered(part, frame)

        return handle

    def _update_handler(self, part: _Part) -> Any:
        def handle(frame: schema.ConsumerUpdateResponse) -> None:
            if frame.subscription_id != 1:
                return
            task = asyncio.create_task(
                self._apply_update(part, bool(frame.active), int(frame.correlation_id)),
                name=f"snodra:rabbitmq:{self._spec.name}",
            )
            self._updates.add(task)
            task.add_done_callback(self._updates.discard)

        return handle

    def _metadata_handler(self, part: _Part) -> Any:
        def handle(frame: schema.MetadataUpdate) -> None:
            if frame.metadata_info.stream == part.stream:
                self._store._layouts.clear()
                self._store._leaders.clear()
                self._fail(f"rabbitmq: the stream {part.stream!r} changed (deleted or moved)")

        return handle

    async def _apply_update(self, part: _Part, active: bool, correlation_id: int) -> None:
        try:
            if active:
                await self._activate(part, correlation_id)
            else:
                await self._deactivate(part, correlation_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"rabbitmq: the consumer update of {part.stream!r}: {error}")
            with contextlib.suppress(Exception):
                await self._reply(part, correlation_id, OffsetType.NEXT, 0)

    async def _activate(self, part: _Part, correlation_id: int) -> None:
        stored: int | None
        try:
            stored = await self._store._request(
                f"the stored offset of {part.stream!r}",
                part.client.query_offset(part.stream, self._spec.name),
            )
        except OffsetNotFound:
            stored = None
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(f"rabbitmq: {part.stream}: the stored offset was not read on activation: {error}")
            with contextlib.suppress(Exception):
                await self._reply(part, correlation_id, OffsetType.NEXT, 0)
            return
        async with part.store_lock:
            part.last_store_offset = stored
        if self._closed:
            with contextlib.suppress(Exception):
                await self._reply(part, correlation_id, OffsetType.NEXT, 0)
            return
        part.active = True
        part.next = stored
        part.acked = None
        part.handed = None
        part.stored = stored
        part.ready.clear()
        part.inflight.clear()
        if stored is not None:
            part.head = stored if part.head is None else max(part.head, stored)
        part.owed = self._paused
        part.last_store = time.monotonic()
        part.unstored = 0
        self._announced = True
        self._changes.append(Assigned((part.segment,)))
        self._signal()
        if stored is not None:
            await self._reply(part, correlation_id, OffsetType.OFFSET, stored)
        else:
            await self._reply(part, correlation_id, OffsetType.FIRST, 0)
        if not self._paused:
            with contextlib.suppress(Exception):
                await part.client.credit(1, 1)

    async def _deactivate(self, part: _Part, correlation_id: int) -> None:
        acked: int | None = None
        was_active = part.active
        if was_active:
            acked = part.acked
            part.active = False
            part.handed = None
            part.ready.clear()
            part.inflight.clear()
            self._changes.append(Revoked((part.segment,)))
            self._changes.append(Assigned())
            self._signal()
        if acked is not None:
            await self._store_and_verify(part, acked)
        with contextlib.suppress(Exception):
            await self._reply(part, correlation_id, OffsetType.NEXT, 0)

    async def _reply(self, part: _Part, correlation_id: int, offset_type: OffsetType, offset: int) -> None:
        specification = schema.OffsetSpecification(offset_type=int(offset_type), offset=offset)
        await part.client.consumer_update(correlation_id, specification)

    async def _delivered(self, part: _Part, frame: schema.Deliver) -> None:
        if self._closed:
            return
        if frame.subscription_id != 1:
            return
        try:
            messages = frame.get_messages()
        except Exception as error:
            self._fail(f"rabbitmq: a delivered chunk of {part.stream!r} was not read: {error}")
            return
        first = int(frame.chunk_first_offset)
        head = first + int(frame.num_records)
        active = part.active
        if active:
            for index, data in enumerate(messages):
                offset = first + index
                if part.next is not None and offset < part.next:
                    continue
                try:
                    key, headers, payload = decode_message(data)
                except Exception as error:
                    self._fail(f"rabbitmq: a delivered message of {part.stream!r} was not read: {error}")
                    return
                part.ready.append(
                    Delivery(
                        segment=part.segment,
                        position=position_of(offset),
                        key=key,
                        headers=headers,
                        payload=payload,
                        timestamp_ms=int(frame.timestamp),
                    )
                )
                part.next = offset + 1
                if part.handed is None or part.next > part.handed:
                    part.handed = part.next
            if part.head is None or head > part.head:
                part.head = head
            self._signal()
        credit = not self._paused and len(part.ready) + len(part.inflight) < READY_LIMIT
        if credit:
            with contextlib.suppress(Exception):
                await part.client.credit(1, 1)
        else:
            part.owed = True

    async def _credit(self, part: _Part) -> None:
        with contextlib.suppress(Exception):
            await part.client.credit(1, 1)

    async def _store_offset(self, part: _Part, offset: int) -> None:
        async with part.store_lock:
            if part.last_store_offset is not None and offset <= part.last_store_offset:
                return
            try:
                await part.client.store_offset(part.stream, self._spec.name, offset)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._error = f"rabbitmq: storing the offset of {part.stream!r}: {error}"
                return
            part.last_store_offset = offset
            part.stored = offset

    async def _store_and_verify(self, part: _Part, offset: int) -> None:
        await self._store_offset(part, offset)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + STORE_WAIT
        while part.client.is_connection_alive() and loop.time() < deadline:
            stored: int | None
            try:
                stored = await self._store._request(
                    f"reading back the offset of {part.stream!r}",
                    part.client.query_offset(part.stream, self._spec.name),
                )
            except OffsetNotFound:
                stored = None
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._error = f"rabbitmq: reading back the offset of {part.stream!r}: {error}"
                stored = None
            if stored == offset:
                part.stored = offset
                part.last_store_offset = offset
                return
            await asyncio.sleep(0.05)
        self._error = (
            f"rabbitmq: the offset of {part.stream!r} was stored as {offset} but did not read back within "
            f"{STORE_WAIT:g} s"
        )

    def _part(self, segment: str) -> _Part | None:
        for part in self._parts:
            if part.segment == segment:
                return part
        return None

    def _fail(self, text: str) -> None:
        if not self._closed and self._broken is None:
            self._broken = text
            self._error = text
        self._signal()

    def _signal(self) -> None:
        self._wake.set()


@dataclass(frozen=True, slots=True)
class _Now:
    """One partition's offsets as an admin call read them."""

    segment: str
    stream: str
    stored: int | None
    first: int
    head: int
    last_time: int
    empty: bool
    address: tuple[str, int]


class RabbitMQAdmin:
    """The reset side of one subscription (capability ``replay``): reads the streams on their
    leaders, lists the members through the management API, and stores a reset's offsets."""

    def __init__(self, store: RabbitMQEventStore, spec: SubscriptionSpec) -> None:
        self._store = store
        self._spec = spec

    async def members(self) -> list[str]:
        """The instances reading the subscription now: the management API's consumers with the
        subscription's name on its partitions plus this process's own live subscriptions."""
        try:
            return await self._members()
        except asyncio.CancelledError:
            raise
        except StoreError:
            raise
        except Exception as error:
            raise Failed(f"rabbitmq: the members of {self._spec.name!r}: {error}") from error

    async def _members(self) -> list[str]:
        if self._store._management is None:
            raise Unsupported(Capability.REPLAY)
        layout = await self._layout()
        members = await self._store._management.members(self._spec.name, layout.streams)
        seen = set(members)
        for member in self._store._live_members(self._spec.source, self._spec.name):
            if member not in seen:
                members.append(member)
                seen.add(member)
        return sorted(members)

    async def plan(self, target: ResetTarget) -> list[ReplayRange]:
        """Where ``target`` moves each segment, from the current positions; nothing moves yet."""
        try:
            return await self._plan(target)
        except asyncio.CancelledError:
            raise
        except StoreError:
            raise
        except Exception as error:
            raise Failed(f"rabbitmq: the plan of {self._spec.name!r}: {error}") from error

    async def _plan(self, target: ResetTarget) -> list[ReplayRange]:
        now = await self._now()
        if isinstance(target, Earliest):
            moves = [(item, item.first) for item in now]
        elif isinstance(target, Latest):
            moves = [(item, item.head) for item in now]
        elif isinstance(target, Timestamp):
            moves = []
            for item in now:
                if item.empty or item.last_time < target.timestamp_ms:
                    moves.append((item, item.head))
                else:
                    offset = await self._store._offset_at(
                        item.address, item.stream, target.timestamp_ms, item.first, item.head
                    )
                    moves.append((item, offset))
        elif isinstance(target, Positions):
            if not target.positions:
                raise Invalid("the target lists no segment")
            seen: set[str] = set()
            moves = []
            for segment, position in target.positions:
                if segment in seen:
                    raise Invalid(f"segment {segment} is listed twice")
                seen.add(segment)
                found = next((item for item in now if item.segment == segment), None)
                if found is None:
                    raise Invalid(f"the source {self._spec.source!r} has no segment {segment}")
                offset = parse_position(position)
                if offset > found.head:
                    raise Invalid(f"position {offset} is past the head ({found.head}) of segment {segment}")
                moves.append((found, offset))
        else:
            raise Invalid(f"unknown reset target {target!r}")
        return self._ranges(moves)

    async def apply(self, ranges: Sequence[ReplayRange]) -> None:
        """Moves each range's committed position to its ``start``; refused while a member reads
        the subscription. Validates every range before storing; a storage failure after some
        partitions moved is reported and not rolled back (SG-41)."""
        try:
            await self._apply(ranges)
        except asyncio.CancelledError:
            raise
        except StoreError:
            raise
        except Exception as error:
            raise Failed(f"rabbitmq: the reset of {self._spec.name!r}: {error}") from error

    async def _apply(self, ranges: Sequence[ReplayRange]) -> None:
        # TODO(spec-gap): SG-41
        if not ranges:
            return
        members = await self.members()
        if members:
            raise SubscribersActive(members)
        now = await self._now()
        by_segment = {item.segment: item for item in now}
        moves: list[tuple[_Now, int]] = []
        seen: set[str] = set()
        for replay_range in ranges:
            if replay_range.source != self._spec.source:
                raise Invalid(
                    f"the range's source {replay_range.source!r} is not the subscription's {self._spec.source!r}"
                )
            item = by_segment.get(replay_range.segment)
            if item is None:
                raise Invalid(f"the source {self._spec.source!r} has no segment {replay_range.segment}")
            if replay_range.segment in seen:
                raise Invalid(f"segment {replay_range.segment} is moved twice")
            seen.add(replay_range.segment)
            offset = parse_position(replay_range.start)
            if offset > item.head:
                raise Invalid(f"position {offset} is past the head ({item.head}) of segment {replay_range.segment}")
            moves.append((item, offset))
        connections: dict[str, Client] = {}
        try:
            for item in {item.segment: item for item, _ in moves}.values():
                connections[item.segment] = await self._store._dial(item.address)
            for item, offset in moves:
                await connections[item.segment].store_offset(item.stream, self._spec.name, offset)
            for item, offset in moves:
                await self._wait_stored(connections[item.segment], item.stream, offset, members)
        except ServerError as error:
            raise Failed(f"rabbitmq: the reset of {self._spec.name!r}: {error}") from error
        finally:
            for connection in connections.values():
                with contextlib.suppress(Exception):
                    await connection.close()

    async def _wait_stored(self, client: Client, stream: str, want: int, members: Sequence[str]) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + STORE_WAIT
        while loop.time() < deadline:
            stored: int | None
            try:
                stored = await self._store._request(
                    f"reading back the offset of {stream!r}",
                    client.query_offset(stream, self._spec.name),
                )
            except OffsetNotFound:
                stored = None
            if stored == want:
                return
            if stored is not None:
                raise SubscribersActive(list(members))
            await asyncio.sleep(0.05)
        raise Failed(
            f"rabbitmq: the offset of {stream!r} was stored as {want} but did not read back within {STORE_WAIT:g} s"
        )

    async def _layout(self) -> _Layout:
        seed = await self._store._seed_client()
        return await self._store._layout(seed, self._spec.source, fresh=True)

    async def _now(self) -> list[_Now]:
        seed = await self._store._seed_client()
        layout = await self._store._layout(seed, self._spec.source, fresh=True)
        now: list[_Now] = []
        for stream, segment in zip(layout.streams, layout.segments, strict=True):
            address = await self._store._leader(seed, stream, fresh=True)
            client = await self._store._dial(address)
            try:
                try:
                    stored: int | None = await self._store._request(
                        f"the stored offset of {stream!r}",
                        client.query_offset(stream, self._spec.name),
                    )
                except OffsetNotFound:
                    stored = None
            finally:
                with contextlib.suppress(Exception):
                    await client.close()
            first, head, last_time, empty = await self._store._bounds(address, stream)
            if stored is not None and stored > head and not empty:
                raise Failed(
                    f"rabbitmq: the offset stored for {self._spec.name!r} on {stream!r} ({stored}) is past the "
                    f"head read ({head})"
                )
            now.append(
                _Now(
                    segment=segment,
                    stream=stream,
                    stored=stored,
                    first=first,
                    head=head,
                    last_time=last_time,
                    empty=empty,
                    address=address,
                )
            )
        now.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return now

    def _ranges(self, moves: Sequence[tuple[_Now, int]]) -> list[ReplayRange]:
        ranges = [
            ReplayRange(
                source=self._spec.source,
                segment=item.segment,
                start=position_of(offset),
                until="" if item.stored is None else position_of(item.stored),
            )
            for item, offset in moves
        ]
        ranges.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return ranges


class _Management:
    """The RabbitMQ management API, asked who reads a subscription (the stream protocol cannot
    list consumers)."""

    def __init__(self, url: str, username: str, password: str, vhost: str, timeout: float) -> None:
        self._url = (url or "").rstrip("/")
        self._username = username
        self._password = password
        self._vhost = vhost
        self._timeout = timeout

    async def members(self, name: str, streams: Sequence[str]) -> list[str]:
        # TODO(spec-gap): SG-40
        overview = await self._get("overview")
        if isinstance(overview, Mapping):
            disabled = overview.get("disable_stats")
            if disabled is not None and not isinstance(disabled, bool):
                raise Failed("rabbitmq: the management API's overview: disable_stats is not a boolean")
            if disabled is True:
                raise Failed(
                    "rabbitmq: the management API's metrics collection is off, so it cannot list the subscription's "
                    "members (management.disable_stats / management_agent.disable_metrics_collector)"
                )
        body = await self._get("stream/consumers/" + urllib.parse.quote(self._vhost, safe=""))
        consumers = parse_consumers(body)
        known = set(streams)
        seen: set[str] = set()
        members: list[str] = []
        for consumer in consumers:
            if consumer.name != name or consumer.stream not in known or consumer.member in seen:
                continue
            seen.add(consumer.member)
            members.append(consumer.member)
        return sorted(members)

    async def _get(self, path: str) -> Any:
        try:
            return await asyncio.to_thread(self._get_sync, path)
        except asyncio.CancelledError:
            raise

    def _get_sync(self, path: str) -> Any:
        request = urllib.request.Request(self._url + "/api/" + path)
        credentials = base64.b64encode(f"{self._username}:{self._password}".encode()).decode()
        request.add_header("Authorization", "Basic " + credentials)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                if not 200 <= response.status <= 299:
                    raise Failed(f"rabbitmq: the management API ({self._url}): HTTP {response.status}")
                data = response.read(_MAX_MANAGEMENT_ANSWER + 1)
        except urllib.error.HTTPError as error:
            raise Failed(f"rabbitmq: the management API ({self._url}): HTTP {error.code}") from error
        except urllib.error.URLError as error:
            raise Failed(f"rabbitmq: the management API ({self._url}): {error.reason}") from error
        except OSError as error:
            raise Failed(f"rabbitmq: the management API ({self._url}): {error}") from error
        if len(data) > _MAX_MANAGEMENT_ANSWER:
            raise Failed(f"rabbitmq: the management API ({self._url}): the answer is over the bound")
        try:
            return json.loads(data)
        except ValueError as error:
            raise Failed(f"rabbitmq: the management API ({self._url}): the answer is not JSON") from error


@dataclass(frozen=True, slots=True)
class _Consumer:
    name: str
    stream: str
    member: str


def parse_consumers(body: Any) -> list[_Consumer]:
    """The management API's consumer list: its array (or a paged answer's ``items``), each with
    ``queue.name``, ``properties.name`` and ``properties.snodra-member``. A consumer without the
    member property falls back to ``<connection name>:<subscription id>`` and is a member too
    (SG-37). A consumer without a name is an unnamed reader (a browser's), not a subscription's."""
    # TODO(spec-gap): SG-37
    if isinstance(body, list):
        items = body
    elif isinstance(body, Mapping) and isinstance(body.get("items"), list):
        items = body["items"]
    else:
        raise Failed("rabbitmq: the management API's consumer list has an unexpected shape")
    consumers: list[_Consumer] = []
    for item in items:
        if not isinstance(item, Mapping):
            raise Failed("rabbitmq: the management API's consumer list has a consumer that is not an object")
        queue = item.get("queue")
        if not isinstance(queue, Mapping):
            raise Failed("rabbitmq: the management API's consumer list has a consumer without queue")
        stream = queue.get("name")
        if not isinstance(stream, str):
            raise Failed("rabbitmq: the management API's consumer list has a consumer without queue.name")
        properties = item.get("properties")
        if not isinstance(properties, Mapping):
            properties = {}
        name = _optional_text(properties.get("name"))
        member = _optional_text(properties.get(MEMBER_PROPERTY))
        if not member:
            connection = item.get("connection_details")
            connection_name = _optional_text(connection.get("name")) if isinstance(connection, Mapping) else ""
            member = f"{connection_name}:{item.get('subscription_id')}"
        if not name:
            continue
        consumers.append(_Consumer(name=name, stream=stream, member=member))
    return consumers


def _optional_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise Failed("rabbitmq: the management API's consumer list has a property that is not a string")
    return value


CAPABILITIES: Final = _CAPABILITIES
"""What the store offers without the management API: ``lag`` and ``native-dead-letter``."""

__all__ = [
    "CAPABILITIES",
    "CONTENT_TYPE",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "DEFAULT_TLS_PORT",
    "HOSTS_VARIABLE",
    "KEY_ANNOTATION",
    "MANAGEMENT_URL_VARIABLE",
    "MEMBER_PROPERTY",
    "NAME",
    "PASSWORD_VARIABLE",
    "PORT_VARIABLE",
    "STORE_CONNECTION",
    "TLS_VARIABLE",
    "USERNAME_VARIABLE",
    "VIRTUAL_HOST_VARIABLE",
    "RabbitMQAdmin",
    "RabbitMQConfig",
    "RabbitMQEventStore",
    "RabbitMQSubscription",
    "decode_message",
    "encode_message",
    "parse_consumers",
]
