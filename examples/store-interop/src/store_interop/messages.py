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


@snodra.message_type("orders.interop.ControlSet")
@dataclass(frozen=True, slots=True)
class ControlSet:
    """Pause or resume this instance's processor by writing the message store's control state."""

    family: str
    run: str
    paused: bool
    by: str


@dataclass(frozen=True, slots=True)
class ControlReply:
    instance: str
    paused: bool
    paused_by: str | None


@snodra.message_type("orders.interop.Poison")
@dataclass(frozen=True, slots=True)
class Poison:
    """Publish one event its handler rejects until the driver allows the event id."""

    family: str
    publisher: str
    run: str
    id: str
    key: str


@dataclass(frozen=True, slots=True)
class PoisonReply:
    instance: str
    published: str


@snodra.message_type("orders.interop.Allow")
@dataclass(frozen=True, slots=True)
class Allow:
    """Let this instance's handlers take the poison events of ``ids``."""

    family: str
    run: str
    ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AllowReply:
    instance: str
    allowed: int


@dataclass(frozen=True, slots=True)
class ReplayRange:
    """One open replay range of the processor's control state."""

    segment: str
    from_: str
    until: str


@dataclass(frozen=True, slots=True)
class LetterBrief:
    """One dead letter as this instance's store lists it."""

    id: str
    reason: str
    event_id: str | None
    attempts: int


@dataclass(frozen=True, slots=True)
class DltEvent:
    """One event the probe's dead-letter subscription received."""

    id: str
    type: str
    key: str | None
    segment: str
    position: str
    headers: dict[str, str]
    payload: object


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
    opted_out: int
    paused: bool
    paused_by: str | None
    operation_id: str | None
    replay: tuple[ReplayRange, ...]
    dead_letters: tuple[LetterBrief, ...]
    dead_letter_total: int
    dlt: tuple[DltEvent, ...]
    dlt_total: int
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
    poison: bool = False
