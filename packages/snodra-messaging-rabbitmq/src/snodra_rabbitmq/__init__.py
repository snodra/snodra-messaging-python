"""The RabbitMQ Streams event store add-on: the core's event-store ports on RabbitMQ 3.13 or later
with the stream plugin.

Hand :class:`RabbitMQEventStore` to :class:`snodra.App` as its ``event_store``; a service that only
sends commands and queries needs none. See :mod:`snodra_rabbitmq.rabbitmq` for the wire layout and
the guarantees, ``CLIENT-SPIKE.md`` for the client decision, and ``SPEC-GAPS.md`` for the choices
the public specification leaves open.
"""

from snodra_rabbitmq.rabbitmq import (
    CAPABILITIES,
    CONTENT_TYPE,
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_TLS_PORT,
    HOSTS_VARIABLE,
    KEY_ANNOTATION,
    MANAGEMENT_URL_VARIABLE,
    MEMBER_PROPERTY,
    NAME,
    PASSWORD_VARIABLE,
    PORT_VARIABLE,
    STORE_CONNECTION,
    TLS_VARIABLE,
    USERNAME_VARIABLE,
    VIRTUAL_HOST_VARIABLE,
    RabbitMQAdmin,
    RabbitMQConfig,
    RabbitMQEventStore,
    RabbitMQSubscription,
    decode_message,
    encode_message,
    parse_consumers,
)
from snodra_rabbitmq.routing import (
    HASH_SEED,
    MAX_OFFSET,
    SINGLE_SEGMENT,
    committed_offset,
    compare_positions,
    murmur3,
    parse_position,
    partition_for,
    position_of,
    routing_key,
    segment_of,
)

__all__ = [
    "CAPABILITIES",
    "CONTENT_TYPE",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "DEFAULT_TLS_PORT",
    "HASH_SEED",
    "HOSTS_VARIABLE",
    "KEY_ANNOTATION",
    "MANAGEMENT_URL_VARIABLE",
    "MAX_OFFSET",
    "MEMBER_PROPERTY",
    "NAME",
    "PASSWORD_VARIABLE",
    "PORT_VARIABLE",
    "SINGLE_SEGMENT",
    "STORE_CONNECTION",
    "TLS_VARIABLE",
    "USERNAME_VARIABLE",
    "VIRTUAL_HOST_VARIABLE",
    "RabbitMQAdmin",
    "RabbitMQConfig",
    "RabbitMQEventStore",
    "RabbitMQSubscription",
    "committed_offset",
    "compare_positions",
    "decode_message",
    "encode_message",
    "murmur3",
    "parse_consumers",
    "parse_position",
    "partition_for",
    "position_of",
    "routing_key",
    "segment_of",
]
