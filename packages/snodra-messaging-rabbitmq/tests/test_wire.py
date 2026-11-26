"""The AMQP 1.0 layout of an event and the management API's consumer list."""

from __future__ import annotations

from typing import Any

import pytest
from snodra import EventHeaders, Failed, OutgoingEvent
from snodra_rabbitmq import (
    CONTENT_TYPE,
    KEY_ANNOTATION,
    decode_message,
    encode_message,
    parse_consumers,
)

AMQP_ANNOTATIONS = b"\x00Sr"
AMQP_PROPERTIES = b"\x00Ss"
AMQP_APPLICATION_PROPERTIES = b"\x00St"
AMQP_DATA = b"\x00Su"


def event(*, key: str | None = "order-42") -> OutgoingEvent:
    event_id = "0f0e0d0c-0b0a-4908-8706-050403020100"
    headers = {
        EventHeaders.TYPE: "orders.Placed",
        EventHeaders.ID: event_id,
        EventHeaders.CORRELATION_ID: "correlation-1",
        EventHeaders.TIMESTAMP: "2026-11-26T10:00:00.123Z",
        EventHeaders.SOURCE: "orders-service",
        EventHeaders.USER_ID: "user-7",
        EventHeaders.TRACEPARENT: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "x-extra": "café",
    }
    return OutgoingEvent(
        source="snodra.events",
        id=event_id,
        type_name="orders.Placed",
        key=key,
        headers=headers,
        payload=b'{"id":"0f0e0d0c","amount":42}',
    )


def test_the_message_is_one_data_section_with_the_wire_sections() -> None:
    encoded = encode_message(event())
    assert encoded.count(AMQP_DATA) == 1, "exactly one data section"
    assert encoded.count(AMQP_ANNOTATIONS) == 1
    assert encoded.count(AMQP_PROPERTIES) == 1
    assert encoded.count(AMQP_APPLICATION_PROPERTIES) == 1
    assert b"\x00Sw" not in encoded, "no footer"
    assert b"\x00Sq" not in encoded, "no delivery annotations"


def test_the_properties_carry_the_id_as_a_string_and_json_as_a_symbol() -> None:
    encoded = encode_message(event())
    # message-id is an AMQP string, content-type an AMQP symbol.
    assert b"\xa1\x24" + event().id.encode() in encoded
    assert b"\xa3\x10" + CONTENT_TYPE.encode() in encoded


def test_the_annotation_and_application_properties_are_the_events() -> None:
    encoded = encode_message(event())
    assert b"\xa3\r" + KEY_ANNOTATION.encode() in encoded, "the annotation key is a symbol"
    assert b"\xa1\x08order-42" in encoded
    decoded_key, headers, payload = decode_message(encoded)
    assert decoded_key == "order-42"
    assert headers == event().headers
    assert payload == event().payload


def test_an_unkeyed_event_has_no_routing_annotation() -> None:
    encoded = encode_message(event(key=None))
    assert KEY_ANNOTATION.encode() not in encoded
    key, headers, payload = decode_message(encoded)
    assert key is None
    assert headers[EventHeaders.ID] == event().id
    assert payload == event().payload


def test_a_consumer_without_snodra_member_is_still_a_member() -> None:
    body = [
        {
            "queue": {"name": "snodra.events-0", "vhost": "/"},
            "properties": {"name": "orders-service.default", "snodra-member": "member-1"},
            "connection_details": {"name": "snodra-messaging"},
            "subscription_id": 1,
        },
        {
            "queue": {"name": "snodra.events-1", "vhost": "/"},
            "properties": {"name": "orders-service.default"},
            "connection_details": {"name": "other-client"},
            "subscription_id": 4,
        },
        {
            "queue": {"name": "snodra.events-1", "vhost": "/"},
            "properties": {},
            "connection_details": {"name": "snodra-event-browser"},
            "subscription_id": 2,
        },
    ]
    consumers = parse_consumers(body)
    assert [(consumer.name, consumer.stream, consumer.member) for consumer in consumers] == [
        ("orders-service.default", "snodra.events-0", "member-1"),
        ("orders-service.default", "snodra.events-1", "other-client:4"),
    ]


def test_a_paged_consumer_list_is_read() -> None:
    body: Any = {"items": [{"queue": {"name": "s-0"}, "properties": {"name": "p", "snodra-member": "m"}}]}
    assert [(consumer.name, consumer.stream, consumer.member) for consumer in parse_consumers(body)] == [
        ("p", "s-0", "m")
    ]


def test_a_malformed_consumer_list_is_a_failure() -> None:
    bodies: tuple[Any, ...] = ({}, [1], [{"queue": "s-0"}], [{"queue": {"name": 3}}])
    for body in bodies:
        with pytest.raises(Failed):
            parse_consumers(body)
