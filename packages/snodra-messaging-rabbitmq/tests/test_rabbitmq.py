"""The RabbitMQ Streams event store on a real broker: the wire rules pinned and the kit run.

Every test uses a fresh super stream (a random suffix) and deletes it afterwards; the broker is one
pinned RabbitMQ container for the run (see ``conftest.py``).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest
from rstream.exceptions import OffsetNotFound
from snodra import (
    Assigned,
    Capability,
    Delivery,
    Earliest,
    EventHeaders,
    Failed,
    Latest,
    MemoryStore,
    OutgoingEvent,
    Positions,
    Rejected,
    ReplayRange,
    Revoked,
    SubscribersActive,
    SubscriptionSpec,
    Timestamp,
    Unsupported,
)
from snodra.testing import run_event_store_conformance
from snodra_rabbitmq import RabbitMQEventStore, RabbitMQSubscription, decode_message, partition_for
from snodra_rabbitmq.rabbitmq import _Layout, _Part
from support import (
    Broker,
    bind_partition,
    connect,
    create_stream,
    delete_stream,
    delete_super_stream,
    event,
    ids,
    management_consumers,
    partition_streams,
    raw_stream_messages,
    read_all,
    read_deliveries,
    stream_offset,
    wait_for,
    wait_for_async,
)

pytestmark = pytest.mark.timeout(300)

PINNED = {"order-1": 0, "order-2": 2, "customer-123": 5, "order-7": 3, "": 4}
"""The wire rules' pinned partitions for 6 partitions."""

CreateSource = Callable[..., Awaitable[str]]
MakeStore = Callable[..., RabbitMQEventStore]


class Pump:
    """Reads one subscription, acknowledging the deliveries it keeps, like the conformance kit's
    reader."""

    def __init__(self, subscription: object) -> None:
        self.subscription = subscription
        self.seen: list[Delivery] = []
        self.changing = False

    async def pump(self, window: float) -> int:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + window
        taken = 0
        while (left := deadline - loop.time()) > 0:
            try:
                event = await asyncio.wait_for(self.subscription.next(), left)  # type: ignore[attr-defined]
            except TimeoutError:
                return taken
            taken += 1
            if isinstance(event, Assigned):
                self.changing = False
            elif isinstance(event, Revoked):
                self.changing = True
            else:
                await self.subscription.ack(event.segment, event.position)  # type: ignore[attr-defined]
                self.seen.append(event)
        return taken

    async def close(self) -> None:
        await self.subscription.close()  # type: ignore[attr-defined]


async def test_the_event_store_passes_the_nine_conformance_checks(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(4, prefix="snodra.conformance", dead_letter=True)
    await run_event_store_conformance(source, lambda: make_store(), MemoryStore())


async def test_a_key_lands_on_the_stream_clients_partition(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(6, prefix="partitions")
    store = make_store()
    for index, key in enumerate(PINNED):
        await store.publish(event(source, key, event_id=f"partition-{index}"))
    await store.publish(event(source, None, event_id="partition-unkeyed"))

    found: dict[str, int] = {}
    for partition in range(6):
        messages = await raw_stream_messages(f"{source}-{partition}", None, broker=broker, within=10.0)
        for message in messages:
            decoded_key, _, _ = decode_message(message)
            if decoded_key is not None:
                found[decoded_key] = partition
    assert found == PINNED
    for key, partition in PINNED.items():
        assert partition_for(key, [f"{source}-{index}" for index in range(6)]) == partition


async def test_the_headers_key_payload_and_time_round_trip(create_source: CreateSource, make_store: MakeStore) -> None:
    source = await create_source(6, prefix="headers")
    store = make_store()
    keyed = event(source, "order-42", event_id="headers-keyed")
    keyed.headers["x-empty"] = ""
    keyed.headers["x-utf8"] = "café"
    unkeyed = event(source, None, event_id="headers-unkeyed")
    for outgoing in (keyed, unkeyed):
        await store.publish(outgoing)

    spec = SubscriptionSpec(name=f"orders-service.headers-{uuid.uuid4().hex}", source=source)
    subscription = await store.subscribe(spec)
    try:
        deliveries = await read_all(subscription, until=lambda seen: len(seen) == 2)
        by_id = {delivery.headers[EventHeaders.ID]: delivery for delivery in deliveries}
        assert by_id[keyed.id].key == "order-42"
        assert by_id[keyed.id].headers == keyed.headers
        assert by_id[keyed.id].payload == keyed.payload
        assert by_id[keyed.id].timestamp_ms > 0
        assert by_id[unkeyed.id].key is None
        assert by_id[unkeyed.id].headers[EventHeaders.ID] == unkeyed.id
    finally:
        await subscription.close()


async def test_the_stored_offset_is_the_next_one_and_a_new_subscription_resumes(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(1, prefix="offsets")
    store = make_store()
    published = [event(source, "order-1", event_id=f"offset-{index}") for index in range(3)]
    for outgoing in published:
        await store.publish(outgoing)
    name = f"orders-service.offsets-{uuid.uuid4().hex}"
    spec = SubscriptionSpec(name=name, source=source)
    subscription = await store.subscribe(spec)
    try:
        for expected in (published[0], published[1]):
            delivery = (await read_deliveries(subscription, 1))[0]
            assert delivery.headers[EventHeaders.ID] == expected.id
    finally:
        await subscription.close()
    assert await stream_offset(f"{source}-0", name, broker=broker) == 2, "the next offset to read"

    again = await store.subscribe(spec)
    try:
        resumed = await read_deliveries(again, 1)
        assert [delivery.headers[EventHeaders.ID] for delivery in resumed] == [published[2].id]
    finally:
        await again.close()


async def test_a_second_instance_shares_the_processor_and_the_member_is_visible(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(6, prefix="sac")
    store = make_store()
    name = f"orders-service.sac-{uuid.uuid4().hex}"
    spec = SubscriptionSpec(name=name, source=source)
    for index in range(6):
        await store.publish(event(source, f"key-{index}", event_id=f"sac-{index}"))
    first = await store.subscribe(spec)
    second = await store.subscribe(spec)
    assert isinstance(first, RabbitMQSubscription)
    assert isinstance(second, RabbitMQSubscription)
    readers = [Pump(first), Pump(second)]
    try:
        await wait_for("both instances hold a partition", lambda: bool(first.segments()) and bool(second.segments()))

        def managed_members() -> set[str]:
            found: set[str] = set()
            for consumer in management_consumers(broker):
                properties = consumer.get("properties", {})
                if properties.get("name") != name:
                    continue
                member = properties.get("snodra-member")
                if isinstance(member, str):
                    found.add(member)
            return found

        await wait_for(
            "the management API lists both members",
            lambda: {first.member, second.member} <= managed_members(),
        )
        consumers = [
            consumer for consumer in management_consumers(broker) if consumer.get("properties", {}).get("name") == name
        ]
        assert consumers, "the management API lists the subscription"
        assert all(consumer["properties"].get("single-active-consumer") == "true" for consumer in consumers)
        assert all(consumer["properties"].get("super-stream") == source for consumer in consumers)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 60
        while loop.time() < deadline and not (readers[0].seen and readers[1].seen):
            await asyncio.gather(*(reader.pump(0.1) for reader in readers))
        assert readers[0].seen, "the first instance receives some events"
        assert readers[1].seen, "the second instance receives some events"
        assert len(readers[0].seen) + len(readers[1].seen) >= 6
    finally:
        for reader in readers:
            await reader.close()


async def test_replay_is_declared_only_with_the_management_api(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(2, prefix="replay")
    store = make_store(management=False)
    assert store.capabilities == frozenset({Capability.LAG, Capability.NATIVE_DEAD_LETTER})
    with pytest.raises(Unsupported):
        store.admin(SubscriptionSpec(name="orders-service.reset", source=source))

    with_management = make_store()
    assert Capability.REPLAY in with_management.capabilities
    assert Capability.BROWSE not in with_management.capabilities


async def test_a_reset_plans_and_moves_only_with_the_management_api(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(1, prefix="reset")
    store = make_store()
    published = [event(source, "order-1", event_id=f"reset-{index}") for index in range(4)]
    for outgoing in published:
        await store.publish(outgoing)
    name = f"orders-service.reset-{uuid.uuid4().hex}"
    spec = SubscriptionSpec(name=name, source=source)
    subscription = await store.subscribe(spec)
    try:
        deliveries = await read_deliveries(subscription, len(published))
        assert ids(deliveries) == [outgoing.id for outgoing in published]
        admin = store.admin(spec)
        assert await admin.members(), "the live subscription is a member"
        plan = await admin.plan(Earliest())
        assert plan
        with pytest.raises(SubscribersActive):
            await admin.apply(plan)
    finally:
        await subscription.close()

    admin = store.admin(spec)

    async def no_members() -> bool:
        return not await admin.members()

    await wait_for_async("the closed subscription has no members", no_members)
    plan = await admin.plan(Earliest())
    assert plan
    assert all(replay_range.until != "" for replay_range in plan), "the close stored the acknowledged offsets"
    await admin.apply(plan)
    again = await store.subscribe(spec)
    try:
        replayed = await read_all(again, until=lambda seen: len(seen) == len(published))
        assert ids(replayed) == [outgoing.id for outgoing in published]
    finally:
        await again.close()


async def test_compare_orders_offsets_and_rejects_other_text(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(1, prefix="compare")
    store = make_store()
    assert store.compare("2", "10") < 0
    assert store.compare("10", "2") > 0
    assert store.compare("7", "7") == 0
    assert store.compare("", "0") > 0
    assert store.compare("x", "0") > 0
    assert store.compare("x", "y") < 0
    assert source


async def test_a_blank_source_is_rejected_and_a_missing_stream_fails(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    store = make_store()
    with pytest.raises(Rejected):
        await store.publish(event("", "key"))
    with pytest.raises(Failed):
        await store.publish(event(f"missing.{uuid.uuid4().hex}", "key"))
    with pytest.raises(Failed):
        await store.subscribe(SubscriptionSpec(name="orders-service.missing", source=f"missing.{uuid.uuid4().hex}"))
    source = await create_source(1, prefix="rejected")
    oversized = event(source, "key")
    too_big = OutgoingEvent(
        source=oversized.source,
        id=oversized.id,
        type_name=oversized.type_name,
        key=oversized.key,
        headers=oversized.headers,
        payload=b"{" + b"x" * (2 << 20) + b"}",
    )
    with pytest.raises(Rejected):
        await store.publish(too_big)


async def test_the_first_event_is_an_assignment(create_source: CreateSource, make_store: MakeStore) -> None:
    source = await create_source(2, prefix="empty")
    store = make_store()
    name = f"orders-service.empty-{uuid.uuid4().hex}"
    subscription = await store.subscribe(SubscriptionSpec(name=name, source=source))
    try:
        first = await asyncio.wait_for(subscription.next(), 10.0)
        assert isinstance(first, Assigned)
    finally:
        await subscription.close()


async def test_pause_keeps_the_assignment_and_resumes_in_place(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(1, prefix="paused")
    store = make_store()
    interesting = [event(source, "order-1", event_id=f"paused-{index}") for index in range(4)]
    for outgoing in interesting:
        await store.publish(outgoing)
    spec = SubscriptionSpec(name=f"orders-service.paused-{uuid.uuid4().hex}", source=source)
    subscription = await store.subscribe(spec)
    reader = Pump(subscription)
    try:
        await wait_for("the paused subscription is assigned", lambda: any(True for _ in subscription.segments()))
        subscription.pause()
        await reader.pump(1.0)
        assert not reader.seen, "nothing delivered while paused"
        subscription.resume()
        await read_all(subscription, until=lambda seen: len(seen) == len(interesting))
    finally:
        await subscription.close()


async def test_a_dead_letter_source_uses_its_plain_stream(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(2, prefix="dlt", dead_letter=True)
    store = make_store()
    letter = event(f"{source}.dlt", "order-1", event_id="dlt-1")
    letter.headers["snodra-dlt-processor"] = "orders-service.default"
    await store.publish(letter)
    messages = await raw_stream_messages(f"{source}.dlt", 1, broker=broker, within=10.0)
    assert len(messages) == 1
    key, headers, payload = decode_message(messages[0])
    assert key == "order-1"
    assert headers == letter.headers
    assert payload == letter.payload


async def test_the_amqp_layout_on_a_partition(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(1, prefix="amqp")
    store = make_store()
    published = event(source, "order-1", event_id="amqp-1")
    published.headers["x-utf8"] = "café"
    await store.publish(published)
    messages = await raw_stream_messages(f"{source}-0", 1, broker=broker, within=10.0)
    encoded = messages[0]
    assert encoded.count(b"\x00Su") == 1, "exactly one data section"
    assert encoded.count(b"\x00Sr") == 1, "one message-annotations section"
    assert encoded.count(b"\x00Ss") == 1, "one properties section"
    assert encoded.count(b"\x00St") == 1, "one application-properties section"
    assert b"\x00Sw" not in encoded, "no footer"
    assert b"\x00Sq" not in encoded, "no delivery annotations"
    assert b"\xa3\rx-routing-key" in encoded, "the annotation key is a symbol"
    assert b"\xa3\x10application/json" in encoded, "the content type is a symbol"
    key, headers, payload = decode_message(encoded)
    assert key == published.key
    assert headers == published.headers
    assert payload == published.payload


async def test_streams_are_unique_and_cleanup_removes_them(broker: Broker, create_source: CreateSource) -> None:
    first = await create_source(2, prefix="unique")
    second = await create_source(2, prefix="unique")
    assert first != second
    client = await connect(broker)
    try:
        assert await client.partitions(first), "the super stream's partitions"
        assert await client.partitions(second), "the second super stream's partitions"
        await delete_super_stream(broker, first)
        assert await client.partitions(first) == [], "the deleted super stream has no partitions"
    finally:
        await client.close()


async def test_a_plain_stream_is_one_segment_zero(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(0, prefix="plain")
    store = make_store()
    published = event(source, "order-1", event_id="plain-1")
    await store.publish(published)
    spec = SubscriptionSpec(name=f"orders-service.plain-{uuid.uuid4().hex}", source=source)
    subscription = await store.subscribe(spec)
    try:
        deliveries = await read_deliveries(subscription, 1)
        assert deliveries[0].segment == "0"
        assert deliveries[0].headers[EventHeaders.ID] == published.id
    finally:
        await subscription.close()
    assert await stream_offset(source, spec.name, broker=broker) == 1, "the stored offset is the next one"


async def test_the_head_and_lag_advance_while_paused(create_source: CreateSource, make_store: MakeStore) -> None:
    source = await create_source(1, prefix="paused-head")
    store = make_store()
    await store.publish(event(source, "order-1", event_id="paused-head-0"))
    spec = SubscriptionSpec(name=f"orders-service.paused-head-{uuid.uuid4().hex}", source=source)
    subscription = await store.subscribe(spec)
    try:
        deliveries = await read_deliveries(subscription, 1)
        subscription.pause()
        for index in (1, 2):
            await store.publish(event(source, "order-1", event_id=f"paused-head-{index}"))
        await wait_for(
            "the paused subscription's head moves",
            lambda: bool(subscription.segments()) and subscription.segments()[0].head == "3",
        )
        status = subscription.segments()[0]
        assert status.position == "1"
        assert status.committed == "1"
        assert status.lag == 2, "the two events after the acknowledged one are the lag"
        assert deliveries[0].position == "0"
    finally:
        await subscription.close()


async def test_reset_targets_latest_timestamp_positions_and_a_multi_range_apply(
    create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(2, prefix="targets")
    store = make_store()
    published: list[OutgoingEvent] = []
    for index, key in enumerate(("order-1", "order-7")):
        for copy in range(2):
            outgoing = event(source, key, event_id=f"targets-{index}-{copy}")
            published.append(outgoing)
            await store.publish(outgoing)
    name = f"orders-service.targets-{uuid.uuid4().hex}"
    spec = SubscriptionSpec(name=name, source=source)
    subscription = await store.subscribe(spec)
    try:
        deliveries = await read_deliveries(subscription, len(published))
        assert sorted(ids(deliveries)) == sorted(outgoing.id for outgoing in published)
    finally:
        await subscription.close()

    admin = store.admin(spec)
    latest = await admin.plan(Latest())
    assert [(item.segment, item.start, item.until) for item in latest] == [("0", "2", "2"), ("1", "2", "2")]
    early = await admin.plan(Timestamp(0))
    assert [(item.segment, item.start, item.until) for item in early] == [("0", "0", "2"), ("1", "0", "2")]
    future = int(time.time() * 1000) + 60_000
    late = await admin.plan(Timestamp(future))
    assert [(item.segment, item.start) for item in late] == [("0", "2"), ("1", "2")]
    positions = await admin.plan(Positions((("0", "0"), ("1", "0"))))
    assert [(item.segment, item.start, item.until) for item in positions] == [("0", "0", "2"), ("1", "0", "2")]
    await admin.apply(positions)

    again = await store.subscribe(spec)
    try:
        replayed = await read_all(again, until=lambda seen: len(seen) == len(published))
        assert sorted(ids(replayed)) == sorted(outgoing.id for outgoing in published)
    finally:
        await again.close()


async def test_a_partition_added_later_is_seen_by_a_new_subscription_and_the_admin(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(2, prefix="grown")
    store = make_store()
    await store.publish(event(source, "order-1", event_id="grown-0"))
    first = await store.subscribe(
        SubscriptionSpec(name=f"orders-service.grown-first-{uuid.uuid4().hex}", source=source)
    )
    try:
        await wait_for("the first subscription holds two segments", lambda: len(first.segments()) == 2)
        added = f"{source}-2"
        await create_stream(broker, added)
        try:
            bind_partition(broker, source, 2)
            await wait_for_async(
                "the broker lists the added partition",
                lambda: _partition_count_at_least(broker, source, 3),
            )
            second = await store.subscribe(
                SubscriptionSpec(name=f"orders-service.grown-second-{uuid.uuid4().hex}", source=source)
            )
            try:
                await wait_for(
                    "the new subscription holds the added partition",
                    lambda: {status.segment for status in second.segments()} == {"0", "1", "2"},
                )
            finally:
                await second.close()
            admin = store.admin(SubscriptionSpec(name=f"orders-service.grown-admin-{uuid.uuid4().hex}", source=source))
            plan = await admin.plan(Latest())
            assert {item.segment for item in plan} == {"0", "1", "2"}
        finally:
            with contextlib.suppress(Exception):
                await delete_stream(broker, added)
    finally:
        await first.close()


async def _partition_count_at_least(broker: Broker, source: str, count: int) -> bool:
    return len(await partition_streams(broker, source)) >= count


async def test_an_admin_plan_of_a_vanished_stream_is_failed(
    broker: Broker, create_source: CreateSource, make_store: MakeStore
) -> None:
    source = await create_source(1, prefix="vanished")
    store = make_store()
    admin = store.admin(SubscriptionSpec(name=f"orders-service.vanished-{uuid.uuid4().hex}", source=source))
    await delete_super_stream(broker, source)
    with pytest.raises(Failed):
        await admin.plan(Latest())


async def test_an_admin_plan_and_apply_wrap_a_non_store_error_as_failed(
    create_source: CreateSource, make_store: MakeStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = await create_source(1, prefix="admin-timeout")
    store = make_store()
    spec = SubscriptionSpec(name=f"orders-service.admin-timeout-{uuid.uuid4().hex}", source=source)
    admin = store.admin(spec)

    async def slow_bounds(*args: Any, **kwargs: Any) -> tuple[int, int, int, bool]:
        raise TimeoutError("slow")

    monkeypatch.setattr(store, "_bounds", slow_bounds)
    with pytest.raises(Failed, match="plan"):
        await admin.plan(Earliest())
    with pytest.raises(Failed, match="reset"):
        await admin.apply([ReplayRange(source=source, segment="0", start="0", until="")])


class _FakeStreamClient:
    """A stream client for the activation unit test."""

    def __init__(self, stored: int | None) -> None:
        self.stored = stored
        self.updates: list[tuple[int, int, int]] = []
        self.credits = 0

    async def query_offset(self, stream: str, reference: str) -> int:
        if self.stored is None:
            raise OffsetNotFound()
        return self.stored

    async def consumer_update(self, correlation_id: int, specification: Any) -> None:
        self.updates.append((correlation_id, int(specification.offset_type), int(specification.offset)))

    async def credit(self, subscription_id: int, credit: int) -> None:
        self.credits += 1


class _FakeStore:
    async def _request(self, what: str, call: Awaitable[Any]) -> Any:
        return await call


async def test_reactivation_keeps_the_greater_known_head() -> None:
    client = _FakeStreamClient(stored=2)
    store = cast(Any, _FakeStore())
    subscription = RabbitMQSubscription(
        store,
        SubscriptionSpec(name="orders-service.reactivated", source="s"),
        _Layout(streams=("s",), segments=("0",), super=False),
        "member",
    )
    part = _Part(segment="0", stream="s", client=cast(Any, client), head=7, next=5)
    await subscription._activate(part, 1)
    assert part.active
    assert part.next == 2
    assert part.head == 7, "the known head is greater than the stored offset"
    client.stored = None
    part.head = 11
    await subscription._activate(part, 2)
    assert part.next is None
    assert part.head == 11, "an activation without a stored offset leaves the known head"
