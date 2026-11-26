"""The event-store types and the in-memory event store: segment order, positions, routing and resets."""

from __future__ import annotations

import asyncio
from functools import cmp_to_key

import pytest
from snodra._eventstore import (
    MAX_SEGMENT_BYTES,
    Assigned,
    Capability,
    Delivery,
    Earliest,
    EventHeaders,
    EventStoreSubscription,
    Failed,
    Invalid,
    Latest,
    OutgoingEvent,
    Positions,
    ReplayRange,
    Revoked,
    SegmentStatus,
    SubscribersActive,
    SubscriptionSpec,
    Timestamp,
    dead_letter_source,
    segment_order,
)
from snodra._eventstore_memory import DEFAULT_SEGMENTS, MAX_SEGMENTS, MemoryEventStore, segment_of

SPEC = SubscriptionSpec(name="orders.projector", source="orders.events")


def outgoing(
    event_id: str,
    *,
    key: str | None = None,
    source: str = "orders.events",
    payload: bytes = b'{"amount":42}',
) -> OutgoingEvent:
    return OutgoingEvent(
        source=source,
        id=event_id,
        type_name="orders.Placed",
        key=key,
        headers={
            EventHeaders.TYPE: "orders.Placed",
            EventHeaders.ID: event_id,
            EventHeaders.CORRELATION_ID: f"correlation-{event_id}",
            EventHeaders.TIMESTAMP: "2026-11-26T00:00:00.000Z",
            EventHeaders.SOURCE: "orders-service",
            EventHeaders.USER_ID: "user-1",
            EventHeaders.TRACEPARENT: f"00-{'a' * 32}-{'b' * 16}-01",
        },
        payload=payload,
    )


async def next_delivery(subscription: EventStoreSubscription, within: float = 5.0) -> Delivery:
    while True:
        event = await asyncio.wait_for(subscription.next(), within)
        if isinstance(event, Delivery):
            return event


async def drain(
    store: MemoryEventStore,
    spec: SubscriptionSpec,
    count: int,
    *,
    ack: bool = True,
) -> list[Delivery]:
    subscription = await store.subscribe(spec)
    deliveries: list[Delivery] = []
    try:
        while len(deliveries) < count:
            delivery = await next_delivery(subscription)
            deliveries.append(delivery)
            if ack:
                await subscription.ack(delivery.segment, delivery.position)
    finally:
        await subscription.close()
    return deliveries


def test_segments_order_as_the_hub_lists_them() -> None:
    names = [
        "b",
        "+5",
        "7",
        "99999999999999999999999",
        "007",
        "18446744073709551616",
        "10",
        "a",
        "0",
        "",
        "00",
        "5",
    ]
    ordered = sorted(names, key=cmp_to_key(segment_order))
    assert ordered == [
        "0",
        "00",
        "5",
        "007",
        "7",
        "10",
        "18446744073709551616",
        "99999999999999999999999",
        "",
        "+5",
        "a",
        "b",
    ]
    assert segment_order("9", "10") < 0
    assert segment_order("10", "9") > 0
    assert segment_order("007", "7") < 0
    assert segment_order("7", "7") == 0


def test_capabilities_are_the_protocol_names_in_order() -> None:
    assert [capability.value for capability in Capability.all()] == [
        "replay",
        "browse",
        "lag",
        "native-dead-letter",
    ]
    assert str(Capability.REPLAY) == "replay"


def test_the_seven_headers_and_the_dead_letter_source() -> None:
    assert EventHeaders.ALL == ("type", "id", "correlationId", "timestamp", "source", "userId", "traceparent")
    assert dead_letter_source("orders.events") == "orders.events.dlt"
    assert MAX_SEGMENT_BYTES == 128


def test_a_key_goes_to_the_segment_the_reference_gives_it() -> None:
    keys = ["k", "order-1", "order-2", "é", ""]
    assert [segment_of(key, 4) for key in keys] == [2, 1, 0, 1, 1]
    assert [segment_of(key, 6) for key in keys] == [2, 3, 0, 1, 5]


def test_the_memory_store_declares_replay_and_lag_only() -> None:
    store = MemoryEventStore()
    assert store.name == "memory"
    assert store.capabilities == frozenset({Capability.REPLAY, Capability.LAG})
    assert store.supports(Capability.REPLAY)
    assert store.supports(Capability.LAG)
    assert not store.supports(Capability.BROWSE)
    assert not store.supports(Capability.NATIVE_DEAD_LETTER)


def test_the_memory_store_refuses_a_segment_count_out_of_bounds() -> None:
    for segments in (0, -1, MAX_SEGMENTS + 1):
        with pytest.raises(Invalid, match="segments"):
            MemoryEventStore(segments)
    assert DEFAULT_SEGMENTS == 4
    assert MAX_SEGMENTS == 1024


def test_positions_compare_by_value() -> None:
    store = MemoryEventStore()
    assert store.compare("9", "10") < 0
    assert store.compare("10", "9") > 0
    assert store.compare("10", "10") == 0
    for bad in ("", "007", "+1", "-1", "1.0", "x"):
        with pytest.raises(Invalid, match="not a position"):
            store.compare(bad, "1")


async def test_a_keyed_event_goes_to_its_keys_segment_and_an_unkeyed_one_round_the_segments() -> None:
    store = MemoryEventStore(segments=4)
    keyed = [outgoing(f"keyed-{index}", key="order-1") for index in range(5)]
    unkeyed = [outgoing(f"unkeyed-{index}") for index in range(4)]
    for published in [*keyed, *unkeyed]:
        await store.publish(published)

    deliveries = await drain(store, SPEC, 9)
    by_id = {delivery.headers[EventHeaders.ID]: delivery for delivery in deliveries}
    keyed_segment = str(segment_of("order-1", 4))
    assert {by_id[published.id].segment for published in keyed} == {keyed_segment}
    assert [by_id[published.id].position for published in keyed] == ["0", "1", "2", "3", "4"]
    assert {by_id[published.id].segment for published in unkeyed} == {"0", "1", "2", "3"}
    assert [by_id[published.id].position for published in unkeyed] == ["0", "5", "0", "0"]


async def test_a_segments_append_times_never_go_back() -> None:
    ticks = iter([2_000, 1_000])
    store = MemoryEventStore(segments=1, clock=lambda: next(ticks))
    for index in range(2):
        await store.publish(outgoing(f"event-{index}"))
    deliveries = await drain(store, SPEC, 2)
    assert [delivery.timestamp_ms for delivery in deliveries] == [2_000, 2_000]


async def test_a_new_subscription_starts_at_the_earliest_position() -> None:
    store = MemoryEventStore(segments=1)
    for index in range(3):
        await store.publish(outgoing(f"event-{index}"))
    subscription = await store.subscribe(SPEC)
    try:
        first = await subscription.next()
        assert first == Assigned(("0",))
        assert (await next_delivery(subscription)).position == "0"
    finally:
        await subscription.close()


async def test_a_committed_position_survives_a_close_and_an_acknowledgement_commits_earlier_ones() -> None:
    store = MemoryEventStore(segments=1)
    for index in range(3):
        await store.publish(outgoing(f"event-{index}"))

    subscription = await store.subscribe(SPEC)
    try:
        assert (await next_delivery(subscription)).position == "0"
        second = await next_delivery(subscription)
        assert second.position == "1"
        await subscription.ack(second.segment, second.position)
    finally:
        await subscription.close()

    restarted = await drain(store, SPEC, 1)
    assert [(delivery.segment, delivery.position) for delivery in restarted] == [("0", "2")]


async def test_redeliver_hands_out_the_position_and_later_events_again() -> None:
    store = MemoryEventStore(segments=1)
    for index in range(3):
        await store.publish(outgoing(f"event-{index}"))

    subscription = await store.subscribe(SPEC)
    try:
        first = await next_delivery(subscription)
        await subscription.ack(first.segment, first.position)
        second = await next_delivery(subscription)
        await subscription.ack(second.segment, second.position)
        await subscription.redeliver(first.segment, first.position)
        assert [(await next_delivery(subscription)).position for _ in range(3)] == ["0", "1", "2"]
    finally:
        await subscription.close()


async def test_redeliver_does_not_move_a_position_forward() -> None:
    store = MemoryEventStore(segments=1)
    for index in range(3):
        await store.publish(outgoing(f"event-{index}"))

    subscription = await store.subscribe(SPEC)
    try:
        first = await next_delivery(subscription)
        assert first.position == "0"
        await subscription.redeliver(first.segment, "2")
        assert (await next_delivery(subscription)).position == "1"
    finally:
        await subscription.close()


async def test_redeliver_of_a_revoked_segment_is_a_no_op() -> None:
    store = MemoryEventStore(segments=2)
    published = outgoing("event", key="order-1")  # segment 1
    await store.publish(published)
    spec = SubscriptionSpec(name="conformance.revoked", source="orders.events")

    first = await store.subscribe(spec)
    second = None
    try:
        delivery = await next_delivery(first)
        assert delivery.segment == "1"
        second = await store.subscribe(spec)
        while True:
            event = await asyncio.wait_for(first.next(), 5.0)
            if isinstance(event, Revoked) and "1" in event.segments:
                break
        await first.redeliver("1", "0")
        replayed = await next_delivery(second)
        assert (replayed.segment, replayed.position) == ("1", "0")
    finally:
        await first.close()
        if second is not None:
            await second.close()


async def test_segments_report_position_committed_head_and_lag() -> None:
    store = MemoryEventStore(segments=1)
    for index in range(3):
        await store.publish(outgoing(f"event-{index}"))
    subscription = await store.subscribe(SPEC)
    try:
        first = await next_delivery(subscription)
        await subscription.ack(first.segment, first.position)
        assert subscription.segments() == [SegmentStatus("0", "1", "1", "3", 2)]
        assert subscription.take_error() is None
    finally:
        await subscription.close()


async def test_a_closed_subscription_cannot_read() -> None:
    store = MemoryEventStore()
    subscription = await store.subscribe(SPEC)
    await subscription.close()
    await subscription.close()
    with pytest.raises(Failed, match="closed"):
        await subscription.next()


async def test_the_admin_plans_and_applies_a_reset() -> None:
    ticks = iter([1_000, 2_000, 3_000, 4_000])
    store = MemoryEventStore(segments=2, clock=lambda: next(ticks))
    for index in range(4):
        await store.publish(outgoing(f"event-{index}"))
    delivered = await drain(store, SPEC, 4)
    assert [(delivery.segment, delivery.position, delivery.timestamp_ms) for delivery in delivered] == [
        ("0", "0", 1_000),
        ("1", "0", 2_000),
        ("0", "1", 3_000),
        ("1", "1", 4_000),
    ]
    admin = store.admin(SPEC)
    assert await admin.members() == []

    earliest = await admin.plan(Earliest())
    assert [(r.segment, r.start, r.until) for r in earliest] == [("0", "0", "2"), ("1", "0", "2")]
    latest = await admin.plan(Latest())
    assert [(r.segment, r.start, r.until) for r in latest] == [("0", "2", "2"), ("1", "2", "2")]
    at = await admin.plan(Timestamp(2_500))
    assert [(r.segment, r.start, r.until) for r in at] == [("0", "1", "2"), ("1", "1", "2")]
    past = await admin.plan(Timestamp(9_999))
    assert [(r.segment, r.start, r.until) for r in past] == [("0", "2", "2"), ("1", "2", "2")]
    positions = await admin.plan(Positions((("1", "0"), ("0", "2"))))
    assert [(r.segment, r.start, r.until) for r in positions] == [("0", "2", "2"), ("1", "0", "2")]

    for target in (Positions((("9", "0"),)), Positions((("0", "3"),))):
        with pytest.raises(Invalid):
            await admin.plan(target)

    active = await store.subscribe(SPEC)
    try:
        with pytest.raises(SubscribersActive) as caught:
            await admin.apply(earliest)
        assert caught.value.members == ("memory-2",)
    finally:
        await active.close()

    await admin.apply(earliest)
    assert await admin.members() == []
    replayed = await drain(store, SPEC, 4)
    assert [(delivery.segment, delivery.position) for delivery in replayed] == [
        ("0", "0"),
        ("1", "0"),
        ("0", "1"),
        ("1", "1"),
    ]


async def test_the_admin_plans_an_empty_source_and_refuses_a_foreign_range() -> None:
    store = MemoryEventStore(segments=1)
    admin = store.admin(SPEC)
    assert [(r.segment, r.start, r.until) for r in await admin.plan(Earliest())] == [("0", "0", "")]
    with pytest.raises(Invalid, match="source"):
        await admin.apply([ReplayRange(source="other.events", segment="0", start="0", until="")])


async def test_the_admin_moves_only_the_segments_of_its_ranges() -> None:
    store = MemoryEventStore(segments=2)
    for index in range(4):
        await store.publish(outgoing(f"event-{index}"))
    await drain(store, SPEC, 4)
    admin = store.admin(SPEC)
    await admin.apply([ReplayRange(source=SPEC.source, segment="1", start="1", until="2")])
    replayed = await drain(store, SPEC, 1)
    assert [(delivery.segment, delivery.position) for delivery in replayed] == [("1", "1")]
