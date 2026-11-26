"""Admin and subscription failure mapping, without a broker: fake clients make every branch visible."""

from __future__ import annotations

from collections import deque
from typing import Any

import pytest
from aiokafka.errors import KafkaError
from aiokafka.structs import TopicPartition
from snodra import (
    Earliest,
    Failed,
    Invalid,
    Positions,
    ReplayRange,
    SubscriptionSpec,
    Timestamp,
)
from snodra_kafka import KafkaAdmin, KafkaConfig, KafkaEventStore, KafkaSubscription


def _admin() -> KafkaAdmin:
    return KafkaAdmin(
        KafkaConfig(bootstrap_servers="h:1"),
        SubscriptionSpec(name="orders-service.audit", source="snodra.events"),
    )


class FakeAdmin:
    """Only what :meth:`KafkaAdmin._partitions` reads from an admin client."""

    def __init__(self, topics: list[dict[str, Any]]) -> None:
        self._topics = topics

    async def describe_topics(self, topics: list[str]) -> list[dict[str, Any]]:
        return self._topics


class FakeConsumer:
    """Only the offset reads the targets use; every read can be empty (a client that did not answer)."""

    def __init__(self, ends: dict[TopicPartition, int] | None = None) -> None:
        self._ends = {} if ends is None else ends

    async def beginning_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        return self._ends

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        return self._ends

    async def offsets_for_times(self, timestamps: dict[TopicPartition, int]) -> dict[TopicPartition, Any]:
        return {}


async def test_admin_partitions_include_every_partition_and_fail_closed() -> None:
    described: list[dict[str, Any]] = [
        {
            "topic": "snodra.events",
            "error_code": 0,
            "partitions": [
                {"partition": 2, "error_code": 0},
                {"partition": 0, "error_code": 0},
            ],
        }
    ]
    assert await _admin()._partitions(FakeAdmin(described)) == [0, 2]

    described[0]["partitions"][1]["error_code"] = 6
    with pytest.raises(Failed):
        await _admin()._partitions(FakeAdmin(described))


async def test_admin_partitions_refuse_an_unknown_topic_as_invalid() -> None:
    with pytest.raises(Invalid):
        await _admin()._partitions(FakeAdmin([{"topic": "snodra.events", "error_code": 3, "partitions": []}]))


async def test_every_admin_call_maps_a_broker_failure_to_failed() -> None:
    store = KafkaEventStore(
        KafkaConfig(
            bootstrap_servers="127.0.0.1:1",
            request_timeout_ms=500,
            session_timeout_ms=100,
            admin_timeout=2.0,
        )
    )
    admin = store.admin(SubscriptionSpec(name="orders-service.audit", source="snodra.events"))
    with pytest.raises(Failed):
        await admin.members()
    with pytest.raises(Failed):
        await admin.plan(Earliest())
    with pytest.raises(Failed):
        await admin.apply([ReplayRange(source="snodra.events", segment="0", start="0", until="")])
    await store.close()


def test_the_apply_builder_fails_closed_without_an_end_offset() -> None:
    ranges = [ReplayRange(source="snodra.events", segment="0", start="0", until="")]
    with pytest.raises(Failed):
        _admin()._moves(ranges, [0], {})


async def test_the_position_and_timestamp_builders_fail_closed_without_an_end_offset() -> None:
    topic_partition = TopicPartition("snodra.events", 0)
    admin = _admin()
    with pytest.raises(Failed):
        await admin._targets(FakeConsumer(), [0], [topic_partition], Positions(positions=(("0", "0"),)))
    with pytest.raises(Failed):
        await admin._targets(FakeConsumer(), [0], [topic_partition], Timestamp(timestamp_ms=0))
    assert await admin._targets(FakeConsumer({topic_partition: 7}), [0], [topic_partition], Earliest()) == {0: 7}


class BrokenConsumer:
    """A client that fails the two calls a subscription reports through."""

    def seek(self, topic_partition: TopicPartition, offset: int) -> None:
        raise KafkaError("seek failed")

    def highwater(self, topic_partition: TopicPartition) -> int:
        raise KafkaError("head failed")


async def test_a_client_failure_from_redeliver_or_segments_is_failed() -> None:
    subscription = KafkaSubscription(
        KafkaConfig(bootstrap_servers="h:1"),
        SubscriptionSpec(name="orders-service.audit", source="snodra.events"),
        "client-1",
    )
    subscription._consumer = BrokenConsumer()
    subscription._assigned = {0}
    subscription._positions = {0: 5}
    subscription._pending = deque()
    with pytest.raises(Failed):
        await subscription.redeliver("0", "1")
    with pytest.raises(Failed):
        subscription.segments()
