"""The Kafka event store add-on: the core's event-store ports on Apache Kafka.

Hand :class:`KafkaEventStore` to :class:`snodra.App` as its ``event_store``; a service that only
sends commands and queries needs none. See :mod:`snodra_kafka.kafka` for the wire layout and the
guarantees, and ``SPEC-GAPS.md`` for the choices the public specification leaves open.
"""

from snodra_kafka.kafka import (
    BOOTSTRAP_VARIABLE,
    CAPABILITIES,
    DEFAULT_BOOTSTRAP,
    NAME,
    KafkaAdmin,
    KafkaConfig,
    KafkaEventStore,
    KafkaSubscription,
    delivered_headers,
    record_headers,
)
from snodra_kafka.offsets import (
    MAX_OFFSET,
    MAX_PARTITION,
    committed_offset,
    compare_positions,
    murmur2,
    parse_position,
    parse_segment,
    partition_for,
    position_of,
    segment_names,
    segment_of,
)

__all__ = [
    "BOOTSTRAP_VARIABLE",
    "CAPABILITIES",
    "DEFAULT_BOOTSTRAP",
    "MAX_OFFSET",
    "MAX_PARTITION",
    "NAME",
    "KafkaAdmin",
    "KafkaConfig",
    "KafkaEventStore",
    "KafkaSubscription",
    "committed_offset",
    "compare_positions",
    "delivered_headers",
    "murmur2",
    "parse_position",
    "parse_segment",
    "partition_for",
    "position_of",
    "record_headers",
    "segment_names",
    "segment_of",
]
