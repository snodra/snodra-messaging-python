"""The probe's wire messages.

They are commands (a command answers, so the driver can address one instance with a routing key);
the shapes are the neutral orders-service naming of the other examples. The codec maps snake_case
field names to camelCase on the wire (``outbox_pending`` is ``outboxPending``).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import snodra


@snodra.message_type("orders.interop.Who")
@dataclass(frozen=True, slots=True)
class Who:
    """Answered with the instance, language and family that serves it."""


@dataclass(frozen=True, slots=True)
class WhoReply:
    instance: str
    language: str
    family: str


@snodra.message_type("orders.interop.Publish")
@dataclass(frozen=True, slots=True)
class Publish:
    """Append ``count`` events to this instance's outbox, cycling through ``keys``."""

    family: str
    publisher: str
    run: str
    count: int
    keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublishReply:
    instance: str
    ids: tuple[str, ...]


@snodra.message_type("orders.interop.PublishRaw")
@dataclass(frozen=True, slots=True)
class PublishRaw:
    """Publish events through the event store directly, in this instance's process."""

    family: str
    publisher: str
    run: str
    ids: tuple[str, ...]
    keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PublishRawReply:
    instance: str
    published: tuple[str, ...]


@snodra.message_type("orders.interop.State")
@dataclass(frozen=True, slots=True)
class State:
    """Ask what this instance delivered, and about the processed markers of ``markers``."""

    markers: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class Delivery:
    """One event delivery this instance's consumer recorded a stage-1 marker for: ``handled`` when
    the handler ran in the same transaction, ``duplicate`` when the marker existed already."""

    id: str
    segment: str
    position: str
    outcome: str
    publisher: str | None = None
    key: str | None = None


@dataclass(frozen=True, slots=True)
class StateReply:
    instance: str
    language: str
    family: str
    run: str
    store: str
    event_store: str
    source: str
    database: str | None
    subscription: str
    outbox_pending: int
    handled: int
    duplicates: int
    deliveries: tuple[Delivery, ...]
    markers: dict[str, bool]


@snodra.message_type("orders.OrderPlaced")
@dataclass(frozen=True, slots=True)
class OrderPlaced:
    """The event the probe publishes. It carries everything a driver assertion reads."""

    id: str
    run: str
    publisher: str
    family: str
    key: str
    seq: int
