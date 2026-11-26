"""The Kafka event store on a real broker: the nine conformance checks and the wire rules pinned.

Every test uses topics of its own (a fresh random suffix) and deletes them afterwards; the broker is
one pinned Apache Kafka container for the run (see ``conftest.py``).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition
from snodra import (
    Capability,
    Earliest,
    EventHeaders,
    Failed,
    Invalid,
    Latest,
    MemoryStore,
    OutgoingEvent,
    Positions,
    Rejected,
    SubscribersActive,
    SubscriptionSpec,
    Timestamp,
    dead_letter_source,
)
from snodra.testing import run_event_store_conformance
from snodra_kafka import KafkaEventStore, KafkaSubscription, partition_for

from .conftest import (
    committed_offset,
    describe_group,
    group_members,
    ids,
    kafka_tasks,
    positions,
    raw_records,
    read_all,
    read_deliveries,
    wait_for,
    wait_for_async,
)

pytestmark = pytest.mark.timeout(300)

CreateTopic = Callable[..., Awaitable[str]]
MakeStore = Callable[..., KafkaEventStore]

PINNED = {"order-1": 4, "order-2": 3, "order-42": 0, "customer-7": 1, "": 3}


async def test_the_event_store_passes_the_nine_conformance_checks(
    create_topic: CreateTopic, make_store: MakeStore
) -> None:
    source = await create_topic(4, prefix="snodra.conformance")
    await create_topic(1, name=dead_letter_source(source))
    await run_event_store_conformance(source, lambda: make_store(), MemoryStore())


async def test_the_segment_head_and_lag_advance_while_paused(create_topic: CreateTopic, make_store: MakeStore) -> None:
    topic = await create_topic(1, prefix="paused")
    store = make_store()
    first = event(topic, "paused-0", "order-42")
    second = event(topic, "paused-1", "order-42")
    await store.publish(first)
    spec = SubscriptionSpec(name=unique_group("orders-service", "paused"), source=topic)
    subscription = await store.subscribe(spec)
    try:
        await read_deliveries(subscription, 1)
        subscription.pause()
        await store.publish(second)
        await wait_for(
            "the paused subscription's head and lag move",
            lambda: bool(subscription.segments()) and subscription.segments()[0].head == "2",
        )
        status = subscription.segments()[0]
        assert status.position == "1"
        assert status.committed == "1"
        assert status.lag == 1, "the paused subscription is behind by the record it has not read"
    finally:
        await subscription.close()


async def test_a_key_lands_on_the_java_clients_partition(
    kafka_bootstrap: str, create_topic: CreateTopic, make_store: MakeStore
) -> None:
    topic = await create_topic(6, prefix="partitions")
    store = make_store()
    for index, key in enumerate(PINNED):
        await store.publish(event(topic, f"partition-{index}", key))
    await store.publish(event(topic, "partition-unkeyed", None))

    records = await raw_records(kafka_bootstrap, topic, 6, len(PINNED) + 1)
    partitions = {record.key.decode(): record.partition for record in records if record.key is not None}
    assert partitions == PINNED
    for key, partition in PINNED.items():
        assert partition_for(key.encode(), 6) == partition


async def test_headers_travel_by_name_and_the_key_and_payload_as_published(
    create_topic: CreateTopic, make_store: MakeStore
) -> None:
    topic = await create_topic(1, prefix="headers")
    store = make_store()
    keyed = event(topic, "headers-keyed", "order-42")
    keyed.headers["x-empty"] = ""
    keyed.headers["x-utf8"] = "café"
    keyed.headers["x-order"] = "a reader looks a header up by its name"
    unkeyed = event(topic, "headers-unkeyed", None)
    for outgoing in (keyed, unkeyed):
        await store.publish(outgoing)

    spec = SubscriptionSpec(name=unique_group("orders-service", "headers"), source=topic)
    subscription = await store.subscribe(spec)
    try:
        deliveries = await read_deliveries(subscription, 2)
    finally:
        await subscription.close()
    by_id = {delivery.headers[EventHeaders.ID]: delivery for delivery in deliveries}
    for published in (keyed, unkeyed):
        got = by_id[published.id]
        for name, value in published.headers.items():
            assert got.headers.get(name) == value, name
        assert got.key == published.key
        assert got.payload == published.payload
        assert got.segment == "0"
        assert int(got.position) >= 0
        assert got.timestamp_ms > 0


async def test_the_group_is_named_after_the_processor_on_the_classic_protocol_with_range(
    admin: AIOKafkaAdminClient, create_topic: CreateTopic, make_store: MakeStore
) -> None:
    topic = await create_topic(1, prefix="group")
    store = make_store()
    service = f"orders-service-{uuid.uuid4().hex[:8]}"
    spec = SubscriptionSpec(name=f"{service}.audit", source=topic)
    subscription = await store.subscribe(spec)
    assert kafka_tasks(), "the subscription's refresher runs until it closes"
    try:
        await wait_for_async("the group has a member", lambda: _has_members(admin, spec.name))
        described = await describe_group(admin, spec.name)
        assert described is not None
        assert described.protocol_type == "consumer"
        assert described.protocol == "range", "the assignor the other official add-ons share"
        assert len(described.members) == 1
        members = await store.admin(spec).members()
        assert members == [described.members[0].client_id]
    finally:
        await subscription.close()
    await wait_for_async("the group has no members", lambda: _no_members(admin, spec.name))
    assert kafka_tasks() == [], "close leaves no snodra:kafka: task behind"


async def test_the_group_commits_the_next_offset_and_a_new_subscription_resumes_there(
    admin: AIOKafkaAdminClient, create_topic: CreateTopic, make_store: MakeStore
) -> None:
    topic = await create_topic(1, prefix="offsets")
    store = make_store()
    sent = [event(topic, f"offset-{index}", "order-42") for index in range(3)]
    for outgoing in sent:
        await store.publish(outgoing)
    spec = SubscriptionSpec(name=unique_group("orders-service", "offsets"), source=topic)

    first = await store.subscribe(spec)
    try:
        await read_deliveries(first, 3, ack=lambda delivery: delivery.position == "0")
        await wait_for(
            "the segment status of the first subscription",
            lambda: bool(first.segments()) and first.segments()[0].head == "3",
        )
        status = first.segments()[0]
        assert status.segment == "0"
        assert status.position == "3"
        assert status.committed == "1", "the acknowledged offset plus one"
        assert status.head == "3"
        assert status.lag == 2
    finally:
        await first.close()

    assert await committed_offset(admin, spec.name, TopicPartition(topic, 0)) == 1
    second = await store.subscribe(spec)
    try:
        rest = await read_deliveries(second, 2)
    finally:
        await second.close()
    assert positions(rest) == ["1", "2"]
    assert ids(rest) == [sent[1].id, sent[2].id]


async def test_unacknowledged_records_come_again_after_a_crash(
    admin: AIOKafkaAdminClient, create_topic: CreateTopic, make_store: MakeStore
) -> None:
    topic = await create_topic(1, prefix="crash")
    store = make_store()
    sent = [event(topic, f"crash-{index}", "order-42") for index in range(3)]
    for outgoing in sent:
        await store.publish(outgoing)
    spec = SubscriptionSpec(name=unique_group("orders-service", "crash"), source=topic)

    first = await store.subscribe(spec)
    assert isinstance(first, KafkaSubscription)
    try:
        await read_deliveries(first, 3, ack=lambda delivery: delivery.position == "0")
    finally:
        await first._crash()  # the crash: no close, no flush of the two unacknowledged records
    assert await committed_offset(admin, spec.name, TopicPartition(topic, 0)) == 1
    await wait_for_async("the crashed group has no members", lambda: _no_members(admin, spec.name))

    second = await store.subscribe(spec)
    try:
        again = await read_deliveries(second, 2)
    finally:
        await second.close()
    assert positions(again) == ["1", "2"]
    assert ids(again) == [sent[1].id, sent[2].id]


async def test_a_dead_letter_goes_to_the_dead_letter_topic_when_published_there(
    create_topic: CreateTopic, make_store: MakeStore
) -> None:
    source = await create_topic(1, prefix="snodra.events")
    letter_topic = dead_letter_source(source)
    assert letter_topic == f"{source}.dlt"
    await create_topic(1, name=letter_topic)
    store = make_store()
    assert store.supports(Capability.NATIVE_DEAD_LETTER)
    keyed = event(letter_topic, "dead-letter-1", "order-42")
    keyed.headers.update(
        {
            "snodra-dlt-processor": "default",
            "snodra-dlt-reason": "HANDLER_FAILED",
            "snodra-dlt-error": "the projection refused the order",
            "snodra-dlt-original-topic": source,
            "snodra-dlt-original-partition": "0",
            "snodra-dlt-original-offset": "7",
        }
    )
    await store.publish(keyed)

    spec = SubscriptionSpec(name=unique_group("orders-service", "dead-letters"), source=letter_topic)
    subscription = await store.subscribe(spec)
    try:
        delivered = (await read_deliveries(subscription, 1))[0]
    finally:
        await subscription.close()
    assert delivered.key == "order-42"
    assert delivered.payload == keyed.payload
    for name, value in keyed.headers.items():
        if name.startswith("snodra-dlt-"):
            assert delivered.headers.get(name) == value, name


async def test_a_record_the_broker_refuses_is_rejected(create_topic: CreateTopic, make_store: MakeStore) -> None:
    topic = await create_topic(1, prefix="small-records", configs={"max.message.bytes": "1024"})
    store = make_store()
    outgoing = event(topic, "too-large", "order-42", payload=b'{"padding":"' + b"x" * 4096 + b'"}')
    for _ in range(2):
        with pytest.raises(Rejected):
            await store.publish(outgoing)


async def test_the_reset_side_plans_and_applies_positions_and_refuses_while_read(
    admin: AIOKafkaAdminClient, create_topic: CreateTopic, make_store: MakeStore
) -> None:
    topic = await create_topic(1, prefix="reset")
    store = make_store()
    sent = [event(topic, f"reset-{index}", "order-42") for index in range(2)]
    for outgoing in sent:
        await store.publish(outgoing)
    spec = SubscriptionSpec(name=unique_group("orders-service", "reset"), source=topic)
    admin_side = store.admin(spec)

    subscription = await store.subscribe(spec)
    try:
        await read_all(subscription, until=lambda deliveries: len(deliveries) == 2)
        ranges = await admin_side.plan(Earliest())
        assert [(replay_range.segment, replay_range.start, replay_range.until) for replay_range in ranges] == [
            ("0", "0", "2")
        ]
        assert await admin_side.members()
        with pytest.raises(SubscribersActive):
            await admin_side.apply(ranges)
        assert await committed_offset(admin, spec.name, TopicPartition(topic, 0)) == 2
    finally:
        await subscription.close()

    before = await admin_side.plan(Latest())
    assert [(replay_range.start, replay_range.until) for replay_range in before] == [("2", "2")]
    earliest_in_time = await admin_side.plan(Timestamp(timestamp_ms=0))
    assert [(replay_range.start, replay_range.until) for replay_range in earliest_in_time] == [("0", "2")]
    future = await admin_side.plan(Timestamp(timestamp_ms=time.time_ns() // 1_000_000 + 3_600_000))
    assert [(replay_range.start, replay_range.until) for replay_range in future] == [("2", "2")]
    with pytest.raises(Invalid):
        await admin_side.plan(Positions(positions=(("0", "99"),)))
    with pytest.raises(Invalid):
        await admin_side.plan(Positions(positions=(("9", "0"),)))
    with pytest.raises(Invalid):
        await admin_side.plan(Positions(positions=()))
    fresh = store.admin(SubscriptionSpec(name=unique_group("orders-service", "fresh"), source=topic))
    fresh_ranges = await fresh.plan(Earliest())
    assert [(replay_range.start, replay_range.until) for replay_range in fresh_ranges] == [("0", "")]

    await admin_side.apply(ranges)
    assert await committed_offset(admin, spec.name, TopicPartition(topic, 0)) == 0
    replayed = await store.subscribe(spec)
    try:
        again = await read_deliveries(replayed, 2)
    finally:
        await replayed.close()
    assert ids(again) == [outgoing.id for outgoing in sent]


async def test_a_publish_to_a_missing_topic_fails_and_creates_nothing(
    admin: AIOKafkaAdminClient, make_store: MakeStore
) -> None:
    missing = f"missing-{uuid.uuid4().hex}"
    store = make_store(request_timeout_ms=1_000, session_timeout_ms=100)
    with pytest.raises(Failed):
        await store.publish(event(missing, "missing-topic", "order-42"))
    described = await admin.describe_topics([missing])
    assert described
    assert described[0]["error_code"] != 0, "the broker creates no topic; the store must not either"


async def _no_members(admin: AIOKafkaAdminClient, group: str) -> bool:
    return not await group_members(admin, group)


async def _has_members(admin: AIOKafkaAdminClient, group: str) -> bool:
    return bool(await group_members(admin, group))


def unique_group(service: str, processor: str) -> str:
    return f"{service}-{uuid.uuid4().hex[:8]}.{processor}"


def event(
    source: str,
    event_id: str,
    key: str | None,
    *,
    payload: bytes | None = None,
) -> OutgoingEvent:
    """One outgoing event with the seven headers and a JSON object payload."""
    timestamp = datetime.fromtimestamp(time.time_ns() // 1_000_000_000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return OutgoingEvent(
        source=source,
        id=event_id,
        type_name="orders.Placed",
        key=key,
        headers={
            EventHeaders.TYPE: "orders.Placed",
            EventHeaders.ID: event_id,
            EventHeaders.CORRELATION_ID: f"correlation-{event_id}",
            EventHeaders.TIMESTAMP: timestamp,
            EventHeaders.SOURCE: "orders-service",
            EventHeaders.USER_ID: "user-1",
            EventHeaders.TRACEPARENT: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        },
        payload=b'{"id":"' + event_id.encode() + b'"}' if payload is None else payload,
    )
