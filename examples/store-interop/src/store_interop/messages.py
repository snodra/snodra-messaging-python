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


SAGA_TYPE = "interop-saga"
"""The registered saga name both languages share."""
SAGA_ASSOCIATION_KEY = "orderId"
"""The association both languages route the saga events by; its value is the order id."""
SAGA_DEADLINE_NAME = "saga-timeout"
SAGA_STARTED_TYPE = "orders.interop.SagaStarted"
SAGA_ADVANCED_TYPE = "orders.interop.SagaAdvanced"
SAGA_FAIL_TYPE = "orders.interop.SagaFail"
SAGA_SLOW_TYPE = "orders.interop.SagaSlow"
SAGA_SLOW_STARTED_TYPE = "store.runner.SlowStarted"
SAGA_COMMAND_FAILED_TYPE = "snodra.SagaCommandFailed"


@snodra.message_type(SAGA_STARTED_TYPE)
@dataclass(frozen=True, slots=True)
class SagaStarted:
    """Starts a saga; ``mode`` selects what the later advanced event makes it do."""

    order_id: str
    run: str
    family: str
    mode: str


@snodra.message_type(SAGA_ADVANCED_TYPE)
@dataclass(frozen=True, slots=True)
class SagaAdvanced:
    """Continues a live saga; the driver publishes it on the other language's partition."""

    order_id: str
    run: str
    family: str
    mode: str


@dataclass(frozen=True, slots=True)
class SagaDeadlinePayload:
    """What the start handler schedules and the deadline handler receives."""

    order_id: str
    run: str
    family: str


@snodra.message_type(SAGA_FAIL_TYPE)
@dataclass(frozen=True, slots=True)
class SagaFail:
    """The command whose handler returns INVALID, so the saga receives a local notice."""

    saga_id: str
    order_id: str


@dataclass(frozen=True, slots=True)
class SagaFailReply:
    pass


@snodra.message_type(SAGA_SLOW_TYPE)
@dataclass(frozen=True, slots=True)
class SagaSlow:
    """The command whose handler holds the dispatch lease while it works."""

    saga_id: str
    order_id: str


@dataclass(frozen=True, slots=True)
class SagaSlowReply:
    pass


@snodra.message_type(SAGA_SLOW_STARTED_TYPE)
@dataclass(frozen=True, slots=True)
class SagaSlowStarted:
    """What the slow command sends to the driver: which language's instance ran it."""

    saga_id: str
    order_id: str
    language: str


@dataclass
class InteropSagaState:
    """The shared saga state; both languages use the same field names, so either continues the other."""

    order_id: str = ""
    run: str = ""
    family: str = ""
    mode: str = ""
    started_by: str = ""
    advanced_by: str = ""
    advances: int = 0
    deadline_scheduled_by: str = ""
    deadline_fired: bool = False
    deadline_fired_by: str = ""
    slow_recorded_by: str = ""
    notice_by: str = ""
    notice_reason: str = ""
    notice_command_type: str = ""
    notice_key: str = ""
    notice_message: str = ""
    notices: int = 0


@snodra.message_type("orders.interop.SagaPublish")
@dataclass(frozen=True, slots=True)
class SagaPublish:
    """Publishes one saga event through the instance's event store on the driver's key."""

    family: str
    run: str
    id: str
    key: str
    kind: str
    order_id: str
    mode: str


@dataclass(frozen=True, slots=True)
class SagaPublishReply:
    instance: str
    published: str


@snodra.message_type("orders.interop.SagaState")
@dataclass(frozen=True, slots=True)
class SagaStateRequest:
    """Reads a saga by id, or the live instance associated with ``key = value``."""

    family: str
    run: str
    type: str
    key: str = ""
    value: str = ""
    saga_id: str = ""


@dataclass(frozen=True, slots=True)
class SagaAssociationView:
    key: str
    value: str


@dataclass(frozen=True, slots=True)
class SagaStateReply:
    instance: str
    found: bool
    saga_id: str | None = None
    type: str | None = None
    state: dict[str, object] | None = None
    version: int = 0
    ended: bool = False
    associations: tuple[SagaAssociationView, ...] = ()


@snodra.message_type("orders.interop.SagaDispatch")
@dataclass(frozen=True, slots=True)
class SagaDispatchRequest:
    """Reads the first pending dispatch entry and the pending count of a saga."""

    family: str
    run: str
    saga_id: str


@dataclass(frozen=True, slots=True)
class SagaDispatchReply:
    instance: str
    pending: bool
    entry_id: str | None = None
    seq: int = 0
    kind: str | None = None
    type: str | None = None
    attempts: int = 0
    pending_count: int = 0


@snodra.message_type("orders.interop.SagaLease")
@dataclass(frozen=True, slots=True)
class SagaLeaseRequest:
    """Tries to take a saga's dispatch lease as another relay would."""

    family: str
    run: str
    saga_id: str
    owner: str
    until_ms: int


@dataclass(frozen=True, slots=True)
class SagaLeaseReply:
    instance: str
    taken: bool


@snodra.message_type("orders.interop.SagaRelease")
@dataclass(frozen=True, slots=True)
class SagaReleaseRequest:
    """Releases a probe owner's dispatch lease."""

    family: str
    run: str
    saga_id: str
    owner: str


@dataclass(frozen=True, slots=True)
class SagaReleaseReply:
    instance: str
    released: bool


@snodra.message_type("orders.interop.SagaNotice")
@dataclass(frozen=True, slots=True)
class SagaNoticeRequest:
    """Reads a probe instance's local view of one saga's failure notice."""

    family: str
    run: str
    saga_id: str
    notice_id: str


@dataclass(frozen=True, slots=True)
class SagaNoticeReply:
    instance: str
    language: str
    ordinary: int
    published: int
