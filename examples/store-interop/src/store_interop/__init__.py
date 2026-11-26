"""The stores interoperability probe service.

One process runs one instance per store family: kafka and rabbitmq on the in-memory message store,
postgres, mysql and mongo on the matching database add-on. Register the same service name, the
same processor per family and the same subscription in another library, and the two consumers
compete in one Kafka consumer group or under one RabbitMQ Streams single-active-consumer name.

The process serves the commands snodra-conformance's interoperability driver calls
(``orders.interop.Who``, ``Publish``, ``PublishRaw`` and ``State``); the shapes are the neutral
orders-service naming of the other examples.
"""

from __future__ import annotations

from store_interop.settings import (
    EVENT_TYPE,
    FAMILIES,
    FAMILY_ORDER,
    SERVICE,
    Family,
    Settings,
    database_name,
    instance_name,
    marker_id,
    processor_name,
    source_name,
    subscription_name,
)

__all__ = [
    "EVENT_TYPE",
    "FAMILIES",
    "FAMILY_ORDER",
    "SERVICE",
    "Family",
    "Settings",
    "database_name",
    "instance_name",
    "marker_id",
    "processor_name",
    "source_name",
    "subscription_name",
]
