"""The Kafka wire rules that need no broker: positions, the partitioner and the header mapping."""

from __future__ import annotations

import pytest
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from snodra import (
    Capability,
    EventHeaders,
    Failed,
    Invalid,
    OutgoingEvent,
    Rejected,
)
from snodra_kafka import (
    CAPABILITIES,
    DEFAULT_BOOTSTRAP,
    KafkaConfig,
    KafkaEventStore,
    committed_offset,
    compare_positions,
    murmur2,
    parse_position,
    parse_segment,
    partition_for,
    position_of,
    record_headers,
    segment_of,
)

# The pinned partitions over 6 partitions, computed with the Java client's partitioner
# (murmur2 with seed 0x9747b28c, masked positive, modulo the partitions), as the wire rules list
# them. An empty key is a key, hashed like any other.
PINNED = {"order-1": 4, "order-2": 3, "order-42": 0, "customer-7": 1, "": 3}


@pytest.mark.parametrize(("key", "partition"), PINNED.items())
def test_the_partitioner_gives_a_key_the_partition_the_wire_rules_pin(key: str, partition: int) -> None:
    assert partition_for(key.encode(), 6) == partition


def test_murmur2_matches_the_java_clients_vectors() -> None:
    # The public vectors of Kafka's UtilsTest.testMurmur2.
    assert murmur2(b"21") == -973932308
    assert murmur2(b"foobar") == -790332482
    assert murmur2(b"a-little-bit-long-string") == -985981536
    assert murmur2(b"a-little-bit-longer-string") == -1486304829
    assert murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8") == -58897971


def test_an_empty_key_is_a_key_and_no_partitions_is_refused() -> None:
    assert partition_for(b"", 6) == 3
    assert partition_for(b"", 1) == 0
    with pytest.raises(Invalid):
        partition_for(b"x", 0)


def test_positions_are_decimal_offsets_and_segments_decimal_partitions() -> None:
    assert parse_segment("0") == 0
    assert parse_segment("007") == 7
    assert parse_segment("+7") == 7
    assert parse_position("0") == 0
    assert parse_position("9223372036854775807") == 2**63 - 1
    for refused in ("", "-1", "1.0", " 1", "1 ", "x", "\uff11\uff12"):
        with pytest.raises(Invalid):
            parse_segment(refused)
        with pytest.raises(Invalid):
            parse_position(refused)
    with pytest.raises(Invalid):
        parse_segment(str(2**31))
    with pytest.raises(Invalid):
        parse_position(str(2**63))


def test_the_committed_offset_is_the_next_one() -> None:
    assert committed_offset("0") == 1
    assert committed_offset("41") == 42
    with pytest.raises(Invalid):
        committed_offset(str(2**63 - 1))


def test_segments_and_positions_are_named_in_decimal() -> None:
    assert segment_of(10) == "10"
    assert position_of(42) == "42"


def test_compare_orders_offsets_by_value_and_text_after_them() -> None:
    assert compare_positions("9", "10") < 0
    assert compare_positions("10", "9") > 0
    assert compare_positions("7", "7") == 0
    assert compare_positions("7", "x") < 0
    assert compare_positions("x", "7") > 0
    assert compare_positions("x", "y") < 0
    assert compare_positions("y", "x") > 0


def test_headers_are_written_by_name_with_the_type_id_timestamp_and_source_last() -> None:
    headers = {
        EventHeaders.SOURCE: "orders-service",
        EventHeaders.USER_ID: "user-1",
        EventHeaders.TIMESTAMP: "2026-11-26T10:00:00.000Z",
        EventHeaders.CORRELATION_ID: "correlation-1",
        EventHeaders.ID: "event-1",
        EventHeaders.TYPE: "orders.Placed",
        EventHeaders.TRACEPARENT: "00-trace-span-01",
        "x-empty": "",
    }
    assert record_headers(headers) == [
        ("correlationId", b"correlation-1"),
        ("traceparent", b"00-trace-span-01"),
        ("userId", b"user-1"),
        ("x-empty", b""),
        ("type", b"orders.Placed"),
        ("id", b"event-1"),
        ("timestamp", b"2026-11-26T10:00:00.000Z"),
        ("source", b"orders-service"),
    ]


def test_a_header_that_is_not_present_is_not_written() -> None:
    assert record_headers({EventHeaders.ID: "1"}) == [("id", b"1")]


def test_the_store_declares_replay_lag_and_native_dead_letter_not_browse() -> None:
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="127.0.0.1:1"))
    assert store.name == "kafka"
    assert store.capabilities == frozenset({Capability.REPLAY, Capability.LAG, Capability.NATIVE_DEAD_LETTER})
    assert store.capabilities == CAPABILITIES
    assert not store.supports(Capability.BROWSE)
    assert not hasattr(store, "browser")


def test_the_configuration_defaults_and_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNODRA_KAFKA_BOOTSTRAP", raising=False)
    assert KafkaConfig.from_env().bootstrap_servers == DEFAULT_BOOTSTRAP
    monkeypatch.setenv("SNODRA_KAFKA_BOOTSTRAP", " k1:9092,k2:9092 ")
    assert KafkaConfig.from_env().bootstrap_servers == "k1:9092,k2:9092"
    monkeypatch.setenv("SNODRA_KAFKA_BOOTSTRAP", "  ")
    assert KafkaConfig.from_env().bootstrap_servers == DEFAULT_BOOTSTRAP


@pytest.mark.parametrize(
    "kwargs",
    [
        {"bootstrap_servers": ""},
        {"client_id": " "},
        {"request_timeout_ms": 0},
        {"request_timeout_ms": 10_000},
        {"session_timeout_ms": 0},
        {"session_timeout_ms": 1, "request_timeout_ms": 2},
        {"client_id": "x" * 256},
        {"max_poll_records": 0},
        {"status_refresh": 0},
        {"admin_timeout": 0},
    ],
)
def test_an_invalid_configuration_is_refused(kwargs: dict[str, object]) -> None:
    with pytest.raises(Invalid):
        KafkaConfig(**kwargs)  # type: ignore[arg-type]


def test_the_client_settings_fix_the_group_protocol_the_assignor_and_the_next_offset() -> None:
    config = KafkaConfig(bootstrap_servers="h:1")
    consumer = config.consumer_settings("client-1")
    assert consumer["auto_offset_reset"] == "earliest"
    assert consumer["enable_auto_commit"] is False
    assert consumer["partition_assignment_strategy"] == (RangePartitionAssignor,)
    assert consumer["max_poll_interval_ms"] >= 60 * 60 * 1000
    assert consumer["client_id"] == "client-1"
    producer = config.producer_settings()
    assert producer["acks"] == "all"
    assert producer["enable_idempotence"] is True
    assert producer["bootstrap_servers"] == "h:1"
    assert producer["client_id"] == "snodra-messaging"


async def test_a_publish_after_close_is_failed() -> None:
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="127.0.0.1:1"))
    await store.close()
    with pytest.raises(Failed):
        await store.publish(_event("closed"))
    await store.close()  # idempotent


async def test_a_publish_without_a_source_is_rejected() -> None:
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="127.0.0.1:1"))
    with pytest.raises(Rejected):
        await store.publish(_event("no-source", source=" "))
    await store.close()


async def test_a_header_or_key_that_is_not_utf8_text_is_rejected() -> None:
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="127.0.0.1:1"))
    broken_header = _event("broken-header")
    broken_header.headers["x-broken"] = "\ud800"
    broken_key = _event("broken-key")
    broken_key = OutgoingEvent(
        source=broken_key.source,
        id=broken_key.id,
        type_name=broken_key.type_name,
        key="\ud800",
        headers=broken_key.headers,
        payload=broken_key.payload,
    )
    broken_name = _event("broken-name")
    broken_name.headers["\ud800"] = "x"
    for outgoing in (broken_header, broken_key, broken_name):
        with pytest.raises(Rejected):
            await store.publish(outgoing)
    await store.close()


async def test_a_topic_name_no_kafka_topic_can_have_is_rejected() -> None:
    store = KafkaEventStore(KafkaConfig(bootstrap_servers="127.0.0.1:1"))
    for name in ("invalid topic!", ".", "..", "a" * 250):
        with pytest.raises(Rejected):
            await store.publish(_event("bad-topic", source=name))
    await store.close()


def _event(id_: str, *, source: str = "snodra.events") -> OutgoingEvent:
    return OutgoingEvent(
        source=source,
        id=id_,
        type_name="orders.Placed",
        key="order-1",
        headers={EventHeaders.ID: id_, EventHeaders.TYPE: "orders.Placed"},
        payload=b"{}",
    )
