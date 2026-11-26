"""The routing rules: the stream clients' murmur3, segments, positions and configuration."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from snodra import Invalid, segment_order
from snodra_rabbitmq import (
    HASH_SEED,
    RabbitMQConfig,
    committed_offset,
    compare_positions,
    murmur3,
    parse_position,
    partition_for,
    routing_key,
    segment_of,
)

PINNED = {
    "order-1": (0x3163BA7A, 0),
    "order-2": (0x4AA7BF62, 2),
    "customer-123": (0x6FABE837, 5),
    "order-7": (0x813D1ADB, 3),
    "": (0xC6757D56, 4),
}
"""The wire rules' pinned hashes and partitions for 6 partitions."""

PARTITIONS = [f"snodra.events-{index}" for index in range(6)]


def test_murmur3_matches_the_stream_clients_pinned_values() -> None:
    assert HASH_SEED == 104_729
    for key, (hashed, partition) in PINNED.items():
        assert murmur3(key.encode()) == hashed
        assert partition_for(key, PARTITIONS) == partition


def test_a_signed_modulo_gets_order_seven_wrong() -> None:
    signed = 0x813D1ADB
    if signed >= 2**31:
        signed -= 2**32
    assert signed % 6 != 3, "the pinned value must be one a signed modulo gets wrong"
    assert partition_for("order-7", PARTITIONS) == 3


def test_an_unkeyed_event_routes_by_its_id() -> None:
    assert routing_key("order-42", "e-1") == "order-42"
    assert routing_key(None, "e-1") == "e-1"


def test_segments_are_partition_suffixes_and_a_plain_stream_is_zero() -> None:
    assert segment_of("snodra.events", "snodra.events-0") == "0"
    assert segment_of("snodra.events", "snodra.events-11") == "11"
    assert segment_of("snodra.events", "snodra.events-eu") == "eu"
    assert segment_of("snodra.events", "other-3") == "other-3"
    assert segment_of("snodra.events", "snodra.events-") == "snodra.events-"


def test_positions_are_offsets_and_compare_totally() -> None:
    assert parse_position("0") == 0
    assert parse_position("10") == 10
    for bad in ("", "-1", "1.0", "x", " 1", "\uff11"):
        with pytest.raises(Invalid):
            parse_position(bad)
    assert compare_positions("2", "10") < 0
    assert compare_positions("7", "7") == 0
    assert compare_positions("", "0") > 0
    assert compare_positions("x", "0") > 0
    assert compare_positions("x", "y") < 0
    assert compare_positions("9", "x") < 0


def test_the_committed_offset_is_the_next_one() -> None:
    assert committed_offset("0") == 1
    assert committed_offset("41") == 42
    with pytest.raises(Invalid):
        committed_offset(str(2**64 - 1))


def test_segment_order_is_the_hubs() -> None:
    from functools import cmp_to_key

    names = ["10", "2", "eu", "0", "02"]
    assert sorted(names, key=cmp_to_key(segment_order)) == ["0", "02", "2", "10", "eu"]
    assert segment_order("2", "10") < 0
    assert segment_order("eu", "10") > 0


def test_config_defaults_and_addresses() -> None:
    config = RabbitMQConfig()
    assert config.stream_port == 5552
    assert config.address_of("rabbitmq") == ("rabbitmq", 5552)
    assert config.address_of("rabbitmq:5672") == ("rabbitmq", 5672)
    assert config.address_of("[::1]:5672") == ("::1", 5672)
    assert config.address_of("[::1]") == ("::1", 5552)
    assert RabbitMQConfig(tls=True).stream_port == 5551
    assert RabbitMQConfig(port=5672).address_of("rabbitmq") == ("rabbitmq", 5672)


def test_config_refuses_bad_values() -> None:
    makers: tuple[Callable[[], RabbitMQConfig], ...] = (
        lambda: RabbitMQConfig(hosts=()),
        lambda: RabbitMQConfig(port=70_000),
        lambda: RabbitMQConfig(management_url="ftp://rabbitmq"),
        lambda: RabbitMQConfig(offset_store_messages=0),
        lambda: RabbitMQConfig(confirm_timeout=0),
        lambda: RabbitMQConfig(frame_max=0),
    )
    for made in makers:
        with pytest.raises(Invalid):
            made()


def test_config_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNODRA_RABBITMQ_HOSTS", "one:5552, two ")
    monkeypatch.setenv("SNODRA_RABBITMQ_PORT", "5672")
    monkeypatch.setenv("SNODRA_RABBITMQ_VIRTUAL_HOST", "/events")
    monkeypatch.setenv("SNODRA_RABBITMQ_USERNAME", "orders")
    monkeypatch.setenv("SNODRA_RABBITMQ_PASSWORD", "secret")
    monkeypatch.setenv("SNODRA_RABBITMQ_TLS", "true")
    monkeypatch.setenv("SNODRA_RABBITMQ_MANAGEMENT_URL", "http://one:15672/")
    config = RabbitMQConfig.from_env()
    assert config.hosts == ("one:5552", "two")
    assert config.port == 5672
    assert config.virtual_host == "/events"
    assert config.username == "orders"
    assert config.password == "secret"
    assert config.tls is True
    assert config.management_url == "http://one:15672"
