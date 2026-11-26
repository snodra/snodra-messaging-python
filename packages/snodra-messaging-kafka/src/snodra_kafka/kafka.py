"""The Kafka event store: one record per event on the shared topic, one consumer group per processor.

On Kafka a source is a topic, a segment a partition (its number in decimal) and a position an offset
(in decimal); a subscription's name is its consumer group, ``<service>.<processor>``. The layout on
the wire is the one every Snodra library writes
(https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#kafka-wire-rules):

- one record per event: the value is the JSON payload as given, the key the event key as UTF-8 bytes
  (no record key for an unkeyed event), and every header a record header whose name and value are as
  given. A reader looks a header up by its name, never by its place.
- the producer partitions a keyed record as the Java client's default partitioner does
  (:func:`snodra_kafka.offsets.partition_for`): murmur2 of the key's bytes, masked positive and taken
  modulo the partitions. An empty key is a key, hashed like any other; an unkeyed record has no
  placement rule.
- a processor reads in the consumer group ``<service>.<processor>``, on the classic group protocol
  with the ``range`` assignor, so it shares the group with the other official add-ons.
- the group's committed offset is the next one to read: acknowledging offset ``N`` stores ``N + 1``,
  and a new group starts at the earliest offset.
- a dead letter goes to ``<source>.dlt``.

The store declares ``replay``, ``lag`` and ``native-dead-letter``. This library has no event browser,
so the store declares no ``browse``. It never creates or changes topics: the infrastructure does.

Every subscription owns a Kafka consumer; :meth:`KafkaSubscription.close` flushes what was
acknowledged and leaves the group. :meth:`KafkaEventStore.close` stops the producer and does not
close the subscriptions (the caller closes them).

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
from collections import deque
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import cmp_to_key
from typing import Any, Final

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.errors import (
    ClusterAuthorizationFailedError,
    CommitFailedError,
    ConsumerStoppedError,
    GroupIdNotFound,
    IllegalGenerationError,
    IllegalStateError,
    InvalidRequestError,
    InvalidRequiredAcksError,
    InvalidTimestampError,
    InvalidTopicError,
    KafkaError,
    MessageSizeTooLargeError,
    NonEmptyGroup,
    PolicyViolationError,
    RebalanceInProgressError,
    RecordListTooLargeError,
    RecordTooLargeError,
    TopicAuthorizationFailedError,
    UnknownMemberIdError,
    UnsupportedForMessageFormatError,
)
from aiokafka.structs import ConsumerRecord, TopicPartition
from snodra._eventstore import (
    Assigned,
    Capability,
    Delivery,
    Earliest,
    EventHeaders,
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
    SubscribersActive,
    SubscriptionEvent,
    SubscriptionSpec,
    Timestamp,
    segment_order,
)

from snodra_kafka.offsets import (
    committed_offset,
    compare_positions,
    parse_position,
    parse_segment,
    partition_for,
    position_of,
    segment_names,
    segment_of,
)

log = logging.getLogger(__name__)

NAME: Final = "kafka"
"""The name this add-on reports as the processors' event store."""

DEFAULT_BOOTSTRAP: Final = "kafka:19092"
"""The bootstrap servers when none are configured."""

BOOTSTRAP_VARIABLE: Final = "SNODRA_KAFKA_BOOTSTRAP"
"""The environment variable that carries the bootstrap servers."""

# TODO(spec-gap): SG-27
_CAPABILITIES: Final[frozenset[Capability]] = frozenset(
    {Capability.REPLAY, Capability.LAG, Capability.NATIVE_DEAD_LETTER}
)

POLL_TIMEOUT_MS: Final = 100
"""How long one poll of a subscription waits for records before the subscription looks for an
assignment change again, in milliseconds."""

HEARTBEAT_INTERVAL_MS: Final = 3000
"""The consumer's heartbeat interval, in milliseconds (a third of the default session timeout)."""

# TODO(spec-gap): SG-24
MAX_POLL_INTERVAL_MS: Final = 24 * 60 * 60 * 1000
"""The longest the consumer may go without a fetch, in milliseconds.

The core keeps calling :meth:`KafkaSubscription.next` while a subscription is paused, so a paused
processor stays a group member; the client's own bound is raised so it does not leave the group
either. A dead process is found by the session timeout, which the heartbeat task cannot miss while
it is alive. (SG-24.)
"""

_CLIENT_ID_LIMIT: Final = 255
"""The longest Kafka client id, in bytes."""

_PERMANENT_PUBLISH_ERRORS: Final[tuple[type[BaseException], ...]] = (
    RecordTooLargeError,
    MessageSizeTooLargeError,
    RecordListTooLargeError,
    InvalidTopicError,
    TopicAuthorizationFailedError,
    ClusterAuthorizationFailedError,
    InvalidRequestError,
    InvalidRequiredAcksError,
    UnsupportedForMessageFormatError,
    InvalidTimestampError,
    PolicyViolationError,
)
"""A produce failure no retry can fix: the event is rejected and the outbox relay gives it up."""


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    """How the store reaches Kafka, and the add-on's own bounds.

    ``bootstrap_servers`` is a ``host:port`` list; :meth:`from_env` reads it from
    ``SNODRA_KAFKA_BOOTSTRAP``. ``request_timeout_ms`` bounds one broker request; ``session_timeout_ms``
    bounds how long a crashed instance stays a group member; ``status_refresh`` is how often a
    subscription refreshes the committed offsets and end offsets that :meth:`segments` reports, in
    seconds; ``max_poll_records`` is the most records one poll returns; ``admin_timeout`` bounds one
    reset call, in seconds.
    """

    bootstrap_servers: str = DEFAULT_BOOTSTRAP
    client_id: str = "snodra-messaging"
    request_timeout_ms: int = 30_000
    session_timeout_ms: int = 10_000
    max_poll_records: int = 100
    status_refresh: float = 1.0
    admin_timeout: float = 10.0

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise Invalid("the Kafka bootstrap servers are empty")
        if not self.client_id.strip():
            raise Invalid("the Kafka client id is empty")
        if self.request_timeout_ms < 1:
            raise Invalid(f"the Kafka request timeout must be at least 1 ms, got {self.request_timeout_ms}")
        if self.session_timeout_ms < 1:
            raise Invalid(f"the Kafka session timeout must be at least 1 ms, got {self.session_timeout_ms}")
        if self.max_poll_records < 1:
            raise Invalid(f"the Kafka max poll records must be at least 1, got {self.max_poll_records}")
        if self.status_refresh <= 0:
            raise Invalid(f"the Kafka status refresh must be positive, got {self.status_refresh}")
        if self.admin_timeout <= 0:
            raise Invalid(f"the Kafka admin timeout must be positive, got {self.admin_timeout}")

    @classmethod
    def from_env(cls) -> KafkaConfig:
        """The configuration from the environment: ``SNODRA_KAFKA_BOOTSTRAP`` (default
        ``kafka:19092``); the other fields keep their defaults."""
        bootstrap = os.getenv(BOOTSTRAP_VARIABLE, "").strip()
        return cls(bootstrap_servers=bootstrap or DEFAULT_BOOTSTRAP)

    def producer_settings(self) -> dict[str, Any]:
        """The :class:`aiokafka.AIOKafkaProducer` settings the store fixes."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "request_timeout_ms": self.request_timeout_ms,
            "acks": "all",
            "enable_idempotence": True,
            "partitioner": _partitioner,
        }

    def consumer_settings(self, client_id: str) -> dict[str, Any]:
        """The :class:`aiokafka.AIOKafkaConsumer` settings a subscription fixes."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": client_id,
            "request_timeout_ms": self.request_timeout_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "heartbeat_interval_ms": min(HEARTBEAT_INTERVAL_MS, max(1, self.session_timeout_ms // 3)),
            "max_poll_interval_ms": MAX_POLL_INTERVAL_MS,
            "max_poll_records": self.max_poll_records,
            "consumer_timeout_ms": POLL_TIMEOUT_MS,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            # TODO(spec-gap): SG-28
            # The classic group protocol is aiokafka's only one; the range assignor is the one the
            # other official add-ons share, and aiokafka's default is round-robin.
            "partition_assignment_strategy": (RangePartitionAssignor,),
        }

    def admin_settings(self, client_id: str) -> dict[str, Any]:
        """The :class:`aiokafka.admin.AIOKafkaAdminClient` settings the admin fixes."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": client_id,
            "request_timeout_ms": self.request_timeout_ms,
        }


def _partitioner(key: bytes | None, all_partitions: list[int], available: list[int]) -> int:
    """The producer's partitioner: the Java client's default for a keyed record (murmur2, masked
    positive, modulo the partitions). An unkeyed record goes to an available partition chosen at
    random; the wire rules leave its placement to the store, and it is not a key, so no order is
    owed."""
    if key is None:
        return random.choice(available or all_partitions)
    return partition_for(key, len(all_partitions))


class KafkaEventStore:
    """The Kafka event store: one producer shared by every publish; every subscription and every
    :meth:`admin` call opens its own consumer when it needs one."""

    def __init__(self, config: KafkaConfig | None = None) -> None:
        self._config = config if config is not None else KafkaConfig.from_env()
        self._producer: AIOKafkaProducer | None = None
        self._producer_lock = asyncio.Lock()
        self._closed = False
        self._instances = 0

    @property
    def config(self) -> KafkaConfig:
        """The configuration the store was opened with."""
        return self._config

    @property
    def name(self) -> str:
        return NAME

    @property
    def capabilities(self) -> frozenset[Capability]:
        return _CAPABILITIES

    def supports(self, capability: Capability) -> bool:
        return capability in _CAPABILITIES

    async def publish(self, event: OutgoingEvent) -> None:
        """Appends ``event`` to the topic named by its source and returns once the broker
        acknowledged it. A blank source, an oversized record, an invalid or unknown topic or a
        refused authorization is :class:`~snodra._eventstore.Rejected`; any other failure, an
        unknown topic included (it may appear later), is :class:`~snodra._eventstore.Failed`."""
        # TODO(spec-gap): SG-18
        if not event.source.strip():
            raise Rejected("an event needs a source")
        try:
            headers = record_headers(event.headers)
            key = None if event.key is None else event.key.encode()
        except UnicodeEncodeError as error:  # a header or key that is not UTF-8 text can never be written
            raise Rejected(f"kafka: {error}") from error
        producer = await self._producing()
        try:
            await producer.send_and_wait(event.source, value=event.payload, key=key, headers=headers)
        except asyncio.CancelledError:
            raise
        except _PERMANENT_PUBLISH_ERRORS as error:
            raise Rejected(f"kafka: {error}") from error
        except Exception as error:
            raise Failed(f"kafka: {error}") from error

    async def subscribe(self, spec: SubscriptionSpec) -> EventStoreSubscription:
        """Joins ``spec`` as one more instance: a consumer of the group ``spec.name`` reading the
        topic ``spec.source``. The group joins before the call returns, so the first
        :meth:`~snodra._eventstore.EventStoreSubscription.next` yields the first assignment."""
        # TODO(spec-gap): SG-18
        if not spec.name or not spec.source:
            raise Invalid("a subscription needs a name and a source")
        self._instances += 1
        subscription = KafkaSubscription(self._config, spec, self._client_id(spec, self._instances))
        try:
            await subscription.start()
        except asyncio.CancelledError:
            await subscription._stop_without_commit()
            raise
        except Exception as error:
            await subscription._stop_without_commit()
            raise Failed(f"kafka: the subscription {spec.name!r} could not join: {error}") from error
        return subscription

    def admin(self, spec: SubscriptionSpec) -> EventStoreAdmin:
        """The reset side of ``spec``: the store declares ``replay``, so it is always there."""
        # TODO(spec-gap): SG-18
        if not spec.name or not spec.source:
            raise Invalid("a subscription needs a name and a source")
        return KafkaAdmin(self._config, spec)

    def compare(self, a: str, b: str) -> int:
        """Offsets by value; text that is not an offset after every offset (by its bytes among
        themselves)."""
        return compare_positions(a, b)

    async def close(self) -> None:
        """Stops the producer. The subscriptions are the caller's to close; this call is idempotent."""
        if self._closed:
            return
        self._closed = True
        async with self._producer_lock:
            producer, self._producer = self._producer, None
            if producer is not None:
                with contextlib.suppress(Exception):
                    await producer.stop()

    async def _producing(self) -> AIOKafkaProducer:
        if self._closed:
            raise Failed("kafka: the event store is closed")
        async with self._producer_lock:
            if self._closed:
                raise Failed("kafka: the event store is closed")
            if self._producer is None:
                producer = AIOKafkaProducer(**self._config.producer_settings())
                try:
                    await producer.start()
                except asyncio.CancelledError:
                    with contextlib.suppress(Exception):
                        await producer.stop()
                    raise
                except Exception as error:
                    with contextlib.suppress(Exception):
                        await producer.stop()
                    raise Failed(f"kafka: the producer could not start: {error}") from error
                self._producer = producer
            return self._producer

    def _client_id(self, spec: SubscriptionSpec, instance: int) -> str:
        client_id = f"{self._config.client_id}-{spec.name}-{instance}"
        encoded = client_id.encode()
        if len(encoded) > _CLIENT_ID_LIMIT:
            client_id = encoded[:_CLIENT_ID_LIMIT].decode("utf-8", "ignore")
        return client_id


def record_headers(headers: Mapping[str, str]) -> list[tuple[str, bytes]]:
    """The record's headers: the other headers sorted by name, then ``type``, ``id``, ``timestamp``
    and ``source``.

    The wire rules say the official libraries do not agree on the order and a reader looks a header
    up by its name, never by its place; this add-on writes the reference add-on's deterministic
    order.
    """
    trailing = (EventHeaders.TYPE, EventHeaders.ID, EventHeaders.TIMESTAMP, EventHeaders.SOURCE)
    names = sorted(name for name in headers if name not in trailing)
    names.extend(name for name in trailing if name in headers)
    return [(name, headers[name].encode()) for name in names]


def delivered_headers(record: ConsumerRecord) -> dict[str, str]:
    """The record's headers, by name (a repeated name keeps its last value, as the contract keeps one
    value per name)."""
    headers: dict[str, str] = {}
    for name, value in record.headers or ():
        headers[name] = "" if value is None else value.decode("utf-8", "replace")
    return headers


def _delivery(record: ConsumerRecord) -> Delivery:
    key = None if record.key is None else record.key.decode("utf-8", "replace")
    timestamp = record.timestamp
    return Delivery(
        segment=segment_of(record.partition),
        position=position_of(record.offset),
        key=key,
        headers=delivered_headers(record),
        payload=record.value or b"",
        timestamp_ms=0 if timestamp is None or timestamp < 0 else timestamp,
    )


class KafkaSubscription(ConsumerRebalanceListener):  # type: ignore[misc]  # aiokafka has no type stubs
    """One instance's subscription of a processor: a consumer of the group, its assignment and the
    records it fetched but did not hand out yet."""

    def __init__(self, config: KafkaConfig, spec: SubscriptionSpec, client_id: str) -> None:
        self._config = config
        self._spec = spec
        self._group = spec.name
        self._source = spec.source
        self._consumer: AIOKafkaConsumer | None = None
        self._started = False
        self._closed = False
        self._paused = False
        self._assigned: set[int] = set()
        self._changes: list[SubscriptionEvent] = []
        self._pending: deque[ConsumerRecord] = deque()
        self._positions: dict[int, int] = {}
        self._acked: dict[int, int] = {}
        self._rewind: dict[int, int] = {}
        self._committed: dict[int, int] = {}
        self._ends: dict[int, int] = {}
        self._error: str | None = None
        self._refresh: asyncio.Task[None] | None = None
        self._refresh_wake = asyncio.Event()
        self._client_id = client_id

    async def start(self) -> None:
        """Creates the consumer and joins the group; the first assignment is queued when it comes."""
        consumer = AIOKafkaConsumer(**self._config.consumer_settings(self._client_id), group_id=self._group)
        consumer.subscribe(topics=[self._source], listener=self)
        self._consumer = consumer
        try:
            await consumer.start()
        except BaseException:
            await self._stop_consumer()
            raise
        self._started = True
        if not self._changes:
            self._assignment_changed()
        self._refresh = asyncio.create_task(self._refresh_loop(), name=f"snodra:kafka:{self._spec.name}")

    async def next(self) -> SubscriptionEvent:
        """The next assignment change or delivery; waits for one.

        A paused subscription keeps polling: it never hands a delivery out, but the poll keeps it a
        group member and an assignment change still comes through.
        """
        while True:
            if self._closed:
                raise Failed(f"the subscription {self._spec.name!r} is closed")
            if self._changes:
                return self._changes.pop(0)
            if not self._paused and self._pending:
                record = self._pending.popleft()
                delivery = self._take(record)
                if delivery is not None:
                    return delivery
                continue
            consumer = self._consumer
            if consumer is None:
                raise Failed(f"the subscription {self._spec.name!r} is closed")
            try:
                records = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=self._config.max_poll_records)
            except asyncio.CancelledError:
                raise
            except ConsumerStoppedError as error:
                self._closed = True
                raise Failed(f"kafka: the subscription {self._spec.name!r} was stopped") from error
            except Exception as error:
                self._error = f"kafka: {error}"
                raise Failed(f"kafka: {error}") from error
            self._absorb(records)

    async def ack(self, segment: str, position: str) -> None:
        """Acknowledges the delivery at ``position`` and every earlier one: the group commits the
        offset after it. An acknowledgement of a segment this subscription no longer holds is
        dropped (SG-25)."""
        # TODO(spec-gap): SG-25
        partition = parse_segment(segment)
        if self._closed:
            raise Failed(f"the subscription {self._spec.name!r} is closed")
        if partition not in self._assigned:
            return
        next_offset = committed_offset(position)
        if self._acked.get(partition, 0) >= next_offset:
            return
        consumer = self._consumer
        if consumer is None:
            return
        try:
            await consumer.commit({TopicPartition(self._source, partition): next_offset})
        except IllegalStateError:
            return  # revoked meanwhile: the record comes again to the segment's next holder
        except asyncio.CancelledError:
            raise
        except CommitFailedError as error:
            raise Failed(f"kafka: the group refused the acknowledgement: {error}") from error
        except Exception as error:
            raise Failed(f"kafka: {error}") from error
        self._acked[partition] = next_offset

    async def redeliver(self, segment: str, position: str) -> None:
        """Hands the delivery at ``position`` and the segment's later records out again. A position
        at or after the subscription's next one changes nothing (SG-26)."""
        # TODO(spec-gap): SG-26
        partition = parse_segment(segment)
        offset = parse_position(position)
        if self._closed:
            raise Failed(f"the subscription {self._spec.name!r} is closed")
        if partition not in self._assigned:
            return
        delivered = self._positions.get(partition)
        if delivered is None or offset >= delivered:
            return
        self._rewind[partition] = offset
        self._positions[partition] = offset
        self._seek(partition, offset)

    def pause(self) -> None:
        """Stops delivering without losing the place; an assignment change still comes through."""
        if self._paused:
            return
        self._paused = True
        self._pause_assigned()

    def resume(self) -> None:
        """Delivers again from where the pause stopped."""
        if not self._paused:
            return
        self._paused = False
        consumer = self._consumer
        if consumer is None or not self._assigned:
            return
        with contextlib.suppress(Exception):
            consumer.resume(*_topic_partitions(self._source, sorted(self._assigned)))

    def segments(self) -> list[SegmentStatus]:
        """The assigned segments, in :func:`snodra._eventstore.segment_order`, with their positions,
        committed offsets, heads and lag as last known; never blocking."""
        if self._closed:
            return []
        statuses: list[SegmentStatus] = []
        for partition in sorted(self._assigned):
            committed = self._committed_position(partition)
            position = self._positions.get(partition)
            if position is None:
                position = committed
            head = self._head(partition)
            base = committed if committed is not None else position
            lag = None if head is None or base is None else max(0, head - base)
            statuses.append(
                SegmentStatus(
                    segment=segment_of(partition),
                    position=None if position is None else position_of(position),
                    committed=None if committed is None else position_of(committed),
                    head=None if head is None else position_of(head),
                    lag=lag,
                )
            )
        statuses.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return statuses

    def take_error(self) -> str | None:
        """The latest trouble the subscription recovered from by itself, and clears it."""
        error, self._error = self._error, None
        return error

    async def close(self) -> None:
        """Leaves the group after flushing what was acknowledged. Idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._stop_refresh()
        consumer = self._consumer
        if consumer is None or not self._started:
            return
        self._started = False
        commits = {
            TopicPartition(self._source, partition): offset
            for partition, offset in self._acked.items()
            if partition in self._assigned
        }
        try:
            if commits:
                try:
                    async with asyncio.timeout(self._config.admin_timeout):
                        await consumer.commit(commits)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    log.warning("the acknowledged offsets of %s were not committed: %s", self._spec.name, error)
                    self._error = f"kafka: the acknowledged offsets were not committed: {error}"
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()

    async def _stop_without_commit(self) -> None:
        """Stops a subscription whose start failed: nothing was acknowledged, so nothing is flushed."""
        if self._closed:
            return
        self._closed = True
        await self._stop_refresh()
        await self._stop_consumer()

    async def _crash(self) -> None:
        """Abandons the subscription as a crashed process would: nothing is flushed (the add-on's tests
        use it to prove that unacknowledged records come again)."""
        self._closed = True
        await self._stop_refresh()
        await self._stop_consumer()

    async def _stop_consumer(self) -> None:
        self._started = False
        consumer, self._consumer = self._consumer, None
        if consumer is not None:
            with contextlib.suppress(Exception):
                await consumer.stop()

    async def _stop_refresh(self) -> None:
        refresh, self._refresh = self._refresh, None
        if refresh is None:
            return
        refresh.cancel()
        try:
            await refresh
        except asyncio.CancelledError:
            pass
        except Exception as error:
            log.debug("the status refresher of %s stopped: %s", self._spec.name, error)

    def on_partitions_revoked(self, revoked: Sequence[TopicPartition]) -> None:
        """aiokafka's rebalance callback: the segments are gone, their unacknowledged records with
        them."""
        partitions = sorted(tp.partition for tp in revoked if tp.topic == self._source)
        if not partitions:
            return  # an empty revocation is not an assignment change
        lost = set(partitions)
        for partition in partitions:
            self._assigned.discard(partition)
            self._positions.pop(partition, None)
            self._acked.pop(partition, None)
            self._rewind.pop(partition, None)
            self._committed.pop(partition, None)
            self._ends.pop(partition, None)
        self._pending = deque(record for record in self._pending if record.partition not in lost)
        self._changes.append(Revoked(segment_names(partitions)))

    def on_partitions_assigned(self, assigned: Sequence[TopicPartition]) -> None:
        """aiokafka's rebalance callback: every assignment, an empty one included, closes the
        change; a segment assigned while paused is paused at once."""
        partitions = sorted(tp.partition for tp in assigned if tp.topic == self._source)
        self._assigned.update(partitions)
        if self._paused and partitions:
            with contextlib.suppress(Exception):
                self._consumer.pause(*_topic_partitions(self._source, partitions))  # type: ignore[union-attr]
        self._changes.append(Assigned(segment_names(partitions)))
        self._refresh_wake.set()

    def _assignment_changed(self) -> None:
        """Queues the assignment the consumer reports when the callback did not (defensive)."""
        consumer = self._consumer
        if consumer is None:
            return
        partitions = sorted(tp.partition for tp in consumer.assignment() if tp.topic == self._source)
        self._assigned.update(partitions)
        self._changes.append(Assigned(segment_names(partitions)))
        self._refresh_wake.set()

    def _pause_assigned(self) -> None:
        consumer = self._consumer
        if consumer is None or not self._assigned:
            return
        with contextlib.suppress(Exception):
            consumer.pause(*_topic_partitions(self._source, sorted(self._assigned)))

    def _absorb(self, records: Mapping[TopicPartition, Sequence[ConsumerRecord]]) -> None:
        for topic_partition, batch in records.items():
            if topic_partition.topic != self._source or topic_partition.partition not in self._assigned:
                continue
            self._pending.extend(batch)

    def _take(self, record: ConsumerRecord) -> Delivery | None:
        partition = record.partition
        if partition not in self._assigned:
            return None
        target = self._rewind.get(partition)
        if target is not None:
            if record.offset < target:
                return None  # fetched before the seek took effect
            if record.offset > target:
                self._seek(partition, target)  # an old fetch overtook the sought record
                return None
            del self._rewind[partition]
        if self._paused:
            self._pending.appendleft(record)
            return None
        delivered = self._positions.get(partition)
        if delivered is not None and record.offset < delivered:
            return None  # a duplicate from an old fetch
        self._positions[partition] = record.offset + 1
        return _delivery(record)

    def _seek(self, partition: int, offset: int) -> None:
        self._pending = deque(record for record in self._pending if record.partition != partition)
        consumer = self._consumer
        if consumer is None:
            return
        with contextlib.suppress(IllegalStateError):
            consumer.seek(TopicPartition(self._source, partition), offset)

    def _committed_position(self, partition: int) -> int | None:
        own = self._acked.get(partition)
        broker = self._committed.get(partition)
        if own is None:
            return broker
        if broker is None:
            return own
        return max(own, broker)

    def _head(self, partition: int) -> int | None:
        consumer = self._consumer
        if consumer is not None:
            try:
                highwater = consumer.highwater(TopicPartition(self._source, partition))
            except (AssertionError, IllegalStateError):
                highwater = None
            if highwater is not None:
                return int(highwater)
        return self._ends.get(partition)

    async def _refresh_loop(self) -> None:
        while not self._closed:
            try:
                async with asyncio.timeout(self._config.status_refresh):
                    await self._refresh_wake.wait()
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            self._refresh_wake.clear()
            if self._closed:
                return
            partitions = sorted(self._assigned)
            if not partitions:
                continue
            try:
                await self._refresh_once(partitions)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # a status refresh never ends the subscription
                self._error = f"kafka: the subscription status was not refreshed: {error}"

    async def _refresh_once(self, partitions: Sequence[int]) -> None:
        consumer = self._consumer
        if consumer is None:
            return
        topic_partitions = _topic_partitions(self._source, partitions)
        try:
            ends = await consumer.end_offsets(topic_partitions)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._error = f"kafka: the end offsets were not read: {error}"
            return
        for topic_partition, offset in ends.items():
            self._ends[topic_partition.partition] = offset
        for topic_partition in topic_partitions:
            try:
                committed = await consumer.committed(topic_partition)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._error = f"kafka: the committed offsets were not read: {error}"
                continue
            if committed is not None and topic_partition.partition in self._assigned:
                self._committed[topic_partition.partition] = committed


class KafkaAdmin:
    """The reset side of one subscription's group (capability ``replay``): every call opens the
    clients it needs, does its blocking work off the event loop and closes them again."""

    def __init__(self, config: KafkaConfig, spec: SubscriptionSpec) -> None:
        self._config = config
        self._spec = spec
        self._source = spec.source
        self._group = spec.name
        self._instances = 0

    async def members(self) -> list[str]:
        """The group's instances, by client id; empty when the group does not exist or has none."""
        try:
            async with asyncio.timeout(self._config.admin_timeout):
                async with self._admin() as admin:
                    return await self._members_of(admin)
        except TimeoutError as error:
            raise Failed(f"kafka: the members of {self._group!r} were not read in time") from error

    async def plan(self, target: ResetTarget) -> list[ReplayRange]:
        """Where ``target`` moves each partition, from the topic's offsets and the group's committed
        ones, without moving anything."""
        try:
            async with asyncio.timeout(self._config.admin_timeout):
                async with self._admin() as admin, self._consumer() as consumer:
                    partitions = await self._partitions(admin)
                    topic_partitions = _topic_partitions(self._source, partitions)
                    moves = await self._targets(consumer, partitions, topic_partitions, target)
                    committed = await self._committed_of(admin, topic_partitions)
        except TimeoutError as error:
            raise Failed(f"kafka: the plan of {self._group!r} was not read in time") from error
        ranges = [
            ReplayRange(
                source=self._source,
                segment=segment_of(partition),
                start=position_of(offset),
                until="" if partition not in committed else position_of(committed[partition]),
            )
            for partition, offset in moves.items()
        ]
        ranges.sort(key=cmp_to_key(lambda a, b: segment_order(a.segment, b.segment)))
        return ranges

    async def apply(self, ranges: Sequence[ReplayRange]) -> None:
        """Moves each range's segment's committed position to its ``start``, all or nothing; refused
        with :class:`~snodra._eventstore.SubscribersActive` while an instance reads the group."""
        if not ranges:
            return
        try:
            async with asyncio.timeout(self._config.admin_timeout):
                async with self._admin() as admin, self._consumer() as consumer:
                    partitions = await self._partitions(admin)
                    topic_partitions = _topic_partitions(self._source, partitions)
                    ends = await consumer.end_offsets(topic_partitions)
                    moves = self._moves(ranges, partitions, ends)
                    members = await self._members_of(admin)
                    if members:
                        raise SubscribersActive(members)
                    if not moves:
                        return
                    consumer.assign(list(moves))
                    try:
                        await consumer.commit({partition: offset for partition, offset in moves.items()})
                    except (
                        CommitFailedError,
                        IllegalGenerationError,
                        RebalanceInProgressError,
                        UnknownMemberIdError,
                        NonEmptyGroup,
                    ) as error:
                        raise SubscribersActive(
                            [f"Kafka refused the offsets of group {self._group}: {error}"]
                        ) from error
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        raise Failed(f"kafka: the offsets of {self._group!r} were not committed: {error}") from error
        except TimeoutError as error:
            raise Failed(f"kafka: the reset of {self._group!r} was not applied in time") from error

    @asynccontextmanager
    async def _admin(self) -> AsyncIterator[AIOKafkaAdminClient]:
        self._instances += 1
        client = AIOKafkaAdminClient(**self._config.admin_settings(f"{self._config.client_id}-admin-{self._instances}"))
        try:
            await client.start()
        except BaseException:
            with contextlib.suppress(Exception):
                await client.close()
            raise
        try:
            yield client
        finally:
            with contextlib.suppress(Exception):
                await client.close()

    @asynccontextmanager
    async def _consumer(self) -> AsyncIterator[AIOKafkaConsumer]:
        self._instances += 1
        consumer = AIOKafkaConsumer(
            **self._config.consumer_settings(f"{self._config.client_id}-reset-{self._instances}"),
            group_id=self._group,
        )
        try:
            await consumer.start()
        except BaseException:
            with contextlib.suppress(Exception):
                await consumer.stop()
            raise
        try:
            yield consumer
        finally:
            with contextlib.suppress(Exception):
                await consumer.stop()

    async def _members_of(self, admin: AIOKafkaAdminClient) -> list[str]:
        try:
            responses = await admin.describe_consumer_groups([self._group])
        except GroupIdNotFound:
            return []
        except KafkaError as error:
            raise Failed(f"kafka: the group {self._group!r} was not described: {error}") from error
        members: list[str] = []
        for response in responses:
            for group in response.to_object().get("groups", ()):
                if group["group"] != self._group:
                    continue
                if group["error_code"] == GroupIdNotFound.errno:
                    continue
                if group["error_code"]:
                    raise Failed(f"kafka: the group {self._group!r} was not described: error {group['error_code']}")
                for member in group["members"]:
                    identity = member["client_id"] or member["member_id"]
                    if identity:
                        members.append(identity)
        return members

    async def _partitions(self, admin: AIOKafkaAdminClient) -> list[int]:
        try:
            topics = await admin.describe_topics([self._source])
        except KafkaError as error:
            raise Failed(f"kafka: the topic {self._source!r} was not described: {error}") from error
        for topic in topics:
            if topic.get("topic") != self._source:
                continue
            if topic.get("error_code"):
                raise Invalid(f"the topic {self._source!r} does not exist")
            partitions = sorted(
                partition["partition"] for partition in topic.get("partitions", ()) if not partition.get("error_code")
            )
            if partitions:
                return partitions
            raise Invalid(f"the topic {self._source!r} has no partitions")
        raise Invalid(f"the topic {self._source!r} does not exist")

    async def _committed_of(self, admin: AIOKafkaAdminClient, partitions: Sequence[TopicPartition]) -> dict[int, int]:
        try:
            listed = await admin.list_consumer_group_offsets(self._group, partitions=list(partitions))
        except GroupIdNotFound:
            return {}
        except KafkaError as error:
            raise Failed(f"kafka: the committed offsets of {self._group!r} were not read: {error}") from error
        committed: dict[int, int] = {}
        for topic_partition, metadata in listed.items():
            if metadata.offset >= 0:
                committed[topic_partition.partition] = metadata.offset
        return committed

    async def _targets(
        self,
        consumer: AIOKafkaConsumer,
        partitions: Sequence[int],
        topic_partitions: Sequence[TopicPartition],
        target: ResetTarget,
    ) -> dict[int, int]:
        if isinstance(target, Earliest):
            offsets = await consumer.beginning_offsets(topic_partitions)
            return _checked_offsets(offsets, partitions, self._source, "start")
        if isinstance(target, Latest):
            offsets = await consumer.end_offsets(topic_partitions)
            return _checked_offsets(offsets, partitions, self._source, "end")
        if isinstance(target, Timestamp):
            found = await consumer.offsets_for_times(
                {topic_partition: target.timestamp_ms for topic_partition in topic_partitions}
            )
            ends = await consumer.end_offsets(topic_partitions)
            moves: dict[int, int] = {}
            for topic_partition in topic_partitions:
                found_at = found.get(topic_partition)
                if found_at is not None and found_at.offset >= 0:
                    moves[topic_partition.partition] = found_at.offset
                else:
                    end = ends.get(topic_partition)
                    if end is None:
                        raise Invalid(
                            f"the topic {self._source!r} has no end offset for partition {topic_partition.partition}"
                        )
                    moves[topic_partition.partition] = end
            return moves
        if isinstance(target, Positions):
            if not target.positions:
                raise Invalid("the target lists no segment")
            ends = await consumer.end_offsets(topic_partitions)
            known = set(partitions)
            moves = {}
            for segment, position in target.positions:
                partition = parse_segment(segment)
                if partition not in known:
                    raise Invalid(f"the topic {self._source!r} has no partition {segment}")
                offset = parse_position(position)
                end = ends.get(TopicPartition(self._source, partition))
                if end is not None and offset > end:
                    raise Invalid(f"position {offset} is past the head {end} of segment {partition}")
                if partition in moves:
                    raise Invalid(f"partition {partition} is given twice")
                moves[partition] = offset
            return moves
        raise Invalid(f"unknown reset target {target!r}")

    def _moves(
        self,
        ranges: Sequence[ReplayRange],
        partitions: Sequence[int],
        ends: Mapping[TopicPartition, int],
    ) -> dict[TopicPartition, int]:
        known = set(partitions)
        moves: dict[TopicPartition, int] = {}
        for replay_range in ranges:
            if replay_range.source != self._source:
                raise Invalid(f"a range of {replay_range.source!r} for the subscription of {self._source!r}")
            partition = parse_segment(replay_range.segment)
            if partition not in known:
                raise Invalid(f"the topic {self._source!r} has no partition {replay_range.segment}")
            offset = parse_position(replay_range.start)
            end = ends.get(TopicPartition(self._source, partition))
            if end is not None and offset > end:
                raise Invalid(f"position {offset} is past the head {end} of segment {partition}")
            topic_partition = TopicPartition(self._source, partition)
            if topic_partition in moves:
                raise Invalid(f"segment {partition} is moved twice")
            moves[topic_partition] = offset
        return moves


def _topic_partitions(source: str, partitions: Sequence[int]) -> list[TopicPartition]:
    return [TopicPartition(source, partition) for partition in partitions]


def _checked_offsets(
    offsets: Mapping[TopicPartition, int], partitions: Sequence[int], source: str, what: str
) -> dict[int, int]:
    moves: dict[int, int] = {}
    for partition in partitions:
        offset = offsets.get(TopicPartition(source, partition))
        if offset is None:
            raise Invalid(f"the topic {source!r} has no {what} offset for partition {partition}")
        moves[partition] = offset
    return moves


CAPABILITIES: Final[frozenset[Capability]] = _CAPABILITIES
"""What the store offers: ``replay``, ``lag`` and ``native-dead-letter``."""

__all__ = [
    "BOOTSTRAP_VARIABLE",
    "CAPABILITIES",
    "DEFAULT_BOOTSTRAP",
    "NAME",
    "KafkaAdmin",
    "KafkaConfig",
    "KafkaEventStore",
    "KafkaSubscription",
    "delivered_headers",
    "record_headers",
]
