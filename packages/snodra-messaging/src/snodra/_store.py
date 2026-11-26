"""The message-store ports: everything the library keeps in the service's own database.

One add-on per database implements every store of a :class:`MessageStore`; the interface is frozen at level 2, so a
third-party add-on compiles once and a port not yet exercised by a level reports :class:`Unsupported` until the store
implements it.

- **The transaction is the service's.** The outbox entry, the stage-1 processed marker and the handlers' writes commit
  together. ``async with store.transaction():`` begins one (or joins the one already bound), binds it to the current
  context, commits on a clean exit and rolls back on an exception; a low-level operation takes ``tx=`` explicitly when
  a call needs a transaction of its own. :meth:`MessageStore.plain` hands out :meth:`Tx.none`, what reads and writes
  outside any transaction use.
- **Payloads** cross as JSON bytes and every store refuses the same inputs: see :func:`check_outbox_payload` and
  :data:`MAX_PAYLOAD_DEPTH`.
- **Times** are epoch milliseconds; a time a store stamps itself (an entry's sending, a lease) is the application's
  clock, never the database's.

See https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/.
"""

from __future__ import annotations

import builtins
import json
import math
import time
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Never, Protocol

from snodra._errors import SnodraError
from snodra._eventstore import ProcessedMarker as ProcessedMarker
from snodra._eventstore import ReplayRange as ReplayRange

MAX_PAYLOAD_DEPTH: Final = 100
"""The deepest outbox payload, as MySQL's ``JSON_DEPTH`` counts it: a scalar or an empty object or array is 1, a
non-empty one is 1 + its deepest member (``{}`` 1, ``{"a":1}`` 2, ``{"a":[1]}`` 3)."""

MAX_ERROR_MESSAGE_CHARS: Final = 2000
"""The most characters of a dead letter's ``error.message``."""

RESET_KIND: Final = "reset"
"""The kind of a reset's control operation, the only kind there is."""


def now_ms() -> int:
    """The wall clock in epoch milliseconds."""
    return time.time_ns() // 1_000_000


class StoreError(SnodraError):
    """The base of every failure of a message-store port."""


class Conflict(StoreError):
    """Another writer got there first: a duplicate marker or outbox id, a write conflict, a transient transaction error.
    Retrying the whole attempt resolves it."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"conflict: {why}")


class InvalidMessage(StoreError):
    """A payload no store may hold (see :func:`check_outbox_payload`), or a value outside the stored contract."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"invalid message: {why}")


class Failed(StoreError):
    """The store failed or cannot be reached; trying again may succeed."""

    def __init__(self, why: str) -> None:
        self.why = why
        super().__init__(f"store: {why}")


class Unsupported(StoreError):
    """The store does not implement the port yet (or lacks the feature); the message names it."""

    def __init__(self, feature: str) -> None:
        self.feature = feature
        super().__init__(f"unsupported: {feature}")


@dataclass(frozen=True, slots=True)
class Tx:
    """A transaction of the store, opaque to the caller, or :meth:`none` for none.

    ``inner`` is the add-on's own transaction object; the core only carries it. A transaction that has ended counts as
    none: a write with it commits at once, as the reference store does.
    """

    inner: object | None = None

    @classmethod
    def none(cls) -> Tx:
        """No transaction: what reads and writes outside one use."""
        return cls()

    @property
    def is_none(self) -> bool:
        """Whether it is :meth:`none`."""
        return self.inner is None


class Transactions(Protocol):
    """Starting, committing and abandoning a transaction, and the transaction a context binds."""

    async def begin(self) -> Tx:
        """A transaction, held by the caller until it commits or rolls back."""
        ...

    def plain(self) -> Tx:
        """No transaction: :meth:`Tx.none`."""
        ...

    async def commit(self, tx: Tx) -> None:
        """Commits what ``tx`` wrote, all or nothing; a :class:`Conflict` (and nothing committed) when another write got
        to one of its records first. Ends the transaction either way."""
        ...

    async def rollback(self, tx: Tx) -> None:
        """Abandons ``tx``'s writes; never fails."""
        ...


_CURRENT_TX: ContextVar[Tx | None] = ContextVar("snodra_message_store_tx", default=None)


def current_transaction() -> Tx | None:
    """The transaction bound to the current context by :class:`TransactionScope`, if any."""
    return _CURRENT_TX.get()


class TransactionScope(AbstractAsyncContextManager[Tx]):
    """What :meth:`MessageStore.transaction` returns: begins a transaction and binds it to the current context, or joins
    the one already bound; commits on a clean exit, rolls back on an exception.

    A low-level operation called without ``tx=`` joins the bound transaction; one called with ``tx=`` uses that one.
    """

    def __init__(self, store: Transactions) -> None:
        self._store = store
        self._tx: Tx | None = None
        self._token: Token[Tx | None] | None = None
        self._joined = False

    async def __aenter__(self) -> Tx:
        current = current_transaction()
        if current is not None:
            self._joined = True
            return current
        tx = await self._store.begin()
        self._tx = tx
        self._token = _CURRENT_TX.set(tx)
        return tx

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._joined:
            return
        tx, self._tx = self._tx, None
        if tx is None:
            return
        try:
            if exc_type is None:
                await self._store.commit(tx)
            else:
                await self._store.rollback(tx)
        finally:
            token, self._token = self._token, None
            if token is not None:
                _CURRENT_TX.reset(token)


class AppendListener(AbstractContextManager["AppendListener"]):
    """A wake registered with :meth:`OutboxStore.on_append`; :meth:`close` removes it, and so does leaving its
    ``with``."""

    def __init__(self, remove: Callable[[], None]) -> None:
        self._remove: Callable[[], None] | None = remove

    def close(self) -> None:
        """Removes the listener; a second call does nothing."""
        remove, self._remove = self._remove, None
        if remove is not None:
            remove()

    def __enter__(self) -> AppendListener:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def check_outbox_payload(type_name: str, payload: bytes) -> None:
    """Refuses the payload of an outbox entry no store may hold, with :class:`InvalidMessage` naming the event type.

    The payload must be a JSON object whose integers fit a signed 64-bit integer, at most
    :data:`MAX_PAYLOAD_DEPTH` deep (MySQL's ``JSON_DEPTH``): anything else (not JSON, an array, a bare value, an
    integer above ``i64::MAX`` or below ``i64::MIN``, a number no double holds, a lone surrogate escape, a leading byte
    order mark, bytes that are not UTF-8, one deeper) is refused. The integers are read by the JSON parser itself, so
    one below ``i64::MIN`` is caught even where a parser would read it as a float.
    """
    try:
        text = bytes(payload).decode("utf-8")
    except UnicodeDecodeError as error:
        raise InvalidMessage(
            f"the payload of event {type_name} is not UTF-8, so it cannot be an outbox entry: {error}"
        ) from error
    if text.startswith("\ufeff"):
        raise InvalidMessage(
            f"the payload of event {type_name} starts with a byte order mark, so it cannot be an outbox entry"
        )
    try:
        value: object = json.loads(text, parse_constant=_refuse_constant)
    except (ValueError, RecursionError) as error:
        raise InvalidMessage(
            f"the payload of event {type_name} is not JSON, so it cannot be an outbox entry: {error}"
        ) from error
    if not isinstance(value, dict):
        found = _json_kind(value)
        raise InvalidMessage(
            f"the payload of event {type_name} is {found}, not a JSON object, so it cannot be an outbox entry"
        )
    _check_number(value, type_name)
    _check_text(value, type_name)
    _check_depth(value, type_name)


def _refuse_constant(token: str) -> Never:
    """A JSON constant no JSON text has (``NaN``, ``Infinity``)."""
    raise ValueError(f"{token} is not a JSON number")


def _json_kind(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, str):
        return "a string"
    return "an array"


def _check_number(value: object, type_name: str) -> None:
    """Every float finite, every integer within a signed 64-bit integer (the parsed tree, recursively)."""
    if isinstance(value, bool):
        return
    if isinstance(value, int):
        if not -(2**63) <= value <= 2**63 - 1:
            raise InvalidMessage(
                f"the payload of event {type_name} holds the integer {value}, outside the signed 64-bit range, "
                "so it cannot be an outbox entry"
            )
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidMessage(
                f"the payload of event {type_name} holds a number no double holds ({value!r}), so it cannot be an "
                "outbox entry"
            )
        return
    if isinstance(value, list):
        for item in value:
            _check_number(item, type_name)
    elif isinstance(value, dict):
        for item in value.values():
            _check_number(item, type_name)


def _check_text(value: object, type_name: str) -> None:
    """No lone surrogate in a string or a name: a JSON parser reads an escaped one, a database cannot keep it."""
    if isinstance(value, str):
        if _has_surrogate(value):
            raise InvalidMessage(
                f"the payload of event {type_name} holds a lone surrogate escape, so it cannot be an outbox entry"
            )
    elif isinstance(value, list):
        for item in value:
            _check_text(item, type_name)
    elif isinstance(value, dict):
        for key, item in value.items():
            if _has_surrogate(key):
                raise InvalidMessage(
                    f"the payload of event {type_name} holds a lone surrogate escape, so it cannot be an outbox entry"
                )
            _check_text(item, type_name)


def _has_surrogate(text: str) -> bool:
    return any(0xD800 <= ord(char) <= 0xDFFF for char in text)


def _check_depth(value: object, type_name: str) -> None:
    depth = _json_depth(value)
    if depth > MAX_PAYLOAD_DEPTH:
        raise InvalidMessage(
            f"the payload of event {type_name} is {depth} deep (JSON depth), deeper than the {MAX_PAYLOAD_DEPTH} "
            "every store keeps, so it cannot be an outbox entry"
        )


def _json_depth(value: object) -> int:
    """MySQL's ``JSON_DEPTH``: a scalar or an empty object or array 1, a non-empty one 1 + its deepest member."""
    if isinstance(value, list) and value:
        return 1 + max(_json_depth(item) for item in value)
    if isinstance(value, dict) and value:
        return 1 + max(_json_depth(item) for item in value.values())
    return 1


@dataclass(frozen=True, slots=True)
class OutboxEntry:
    """One event in the outbox.

    ``payload`` is the event as JSON and must pass :func:`check_outbox_payload`. ``seq`` is the appending process's
    counter: with ``created_at_ms`` it orders the outbox, so per-key order holds. ``attempts`` counts failed sends so
    far.
    """

    id: str
    type_name: str
    key: str | None
    headers: dict[str, str]
    payload: bytes
    created_at_ms: int
    seq: int
    attempts: int = 0


class OutboxStore(Protocol):
    """The outbox. An entry is *pending* until it is sent or failed; the relay claims pending entries under a lease, so
    two instances relaying one outbox never both hold an entry, and a crashed one's entries are claimed again once its
    lease has passed."""

    async def append(self, entry: OutboxEntry, *, tx: Tx | None = None) -> None:
        """Appends ``entry`` in ``tx`` (by default the bound transaction): rolled back with it. A payload that is not a
        JSON object is refused with :class:`InvalidMessage`."""
        ...

    async def append_now(self, entry: OutboxEntry) -> None:
        """Appends ``entry`` on its own, outside any transaction, and wakes the relay."""
        ...

    async def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        """At most ``limit`` pending entries, now held by ``owner`` for ``lease`` seconds: those nobody holds, those
        whose holder's lease has passed, and those ``owner`` holds already (its lease renewed); never an entry another
        owner holds. Each is returned as appended, with its attempts so far. ``(created_at_ms, seq)`` is the order."""
        ...

    async def mark_sent(self, ids: Sequence[str]) -> None:
        """The entries were sent: never claimed again, purged after the retention."""
        ...

    async def mark_attempt_failed(self, entry_id: str, error: str) -> None:
        """A send failed and will be retried: ``attempts + 1``; the entry stays pending (and its owner's)."""
        ...

    async def mark_failed(self, entry_id: str, error: str) -> None:
        """The entry is given up: ``attempts + 1``, never claimed again, never purged."""
        ...

    async def purge(self, sent_before_ms: int) -> int:
        """Removes the entries sent before ``sent_before_ms``; how many."""
        ...

    async def pending(self) -> int:
        """How many entries are pending (neither sent nor failed), held or not."""
        ...

    def on_append(self, wake: Callable[[], None]) -> AppendListener:
        """Registers ``wake``, called after each committed append so the relay publishes at once; ``wake`` must return
        at once. The listener is removed when it is closed or dropped."""
        ...

    async def oldest_pending(self) -> int | None:
        """``created_at_ms`` of the oldest pending entry; ``None`` when none is pending."""
        ...


class ProcessedStore(Protocol):
    """The processed events: the ids of the events a handler has handled, recorded in its transaction, so a redelivered
    event is skipped (at-least-once delivery, exactly-once effects)."""

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        """Records ``marker`` in ``tx`` (by default the bound transaction); false when its id is recorded already (a
        duplicate: the caller aborts the transaction)."""
        ...

    async def exists(self, marker_id: str) -> bool:
        """Whether a marker with ``marker_id`` is recorded (committed), read outside any transaction."""
        ...

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        """Deletes the stage-1 markers of ``processor`` in ``ranges`` (the range's source and segment, a position in
        ``[start, until)`` in the event store's order), outside any transaction; how many. Markers without a position
        are never deleted."""
        ...

    async def purge(self, before_ms: int) -> int:
        """Removes the markers handled before ``before_ms``; how many."""
        ...


class DeadLetterReason(StrEnum):
    """Why a record was parked."""

    HANDLER_FAILED = "HANDLER_FAILED"
    """Stage 1 failed on every attempt."""
    SEQUENCED = "SEQUENCED"
    """An earlier letter of the record's key was parked; stage 1 did not run."""
    INVALID = "INVALID"
    """The record cannot be handled (no type header, a value that is not a JSON object); it blocks nothing and can
    only be discarded."""


@dataclass(frozen=True, slots=True)
class DeadLetterError:
    """A letter's ``error``: ``type_name`` is the error variant, ``message`` at most
    :data:`MAX_ERROR_MESSAGE_CHARS` characters."""

    type_name: str
    message: str

    @classmethod
    def new(cls, type_name: str, message: str) -> DeadLetterError:
        """``message`` cut to :data:`MAX_ERROR_MESSAGE_CHARS` characters (never inside one)."""
        return cls(type_name, message[:MAX_ERROR_MESSAGE_CHARS])

    @classmethod
    def sequenced(cls, head_id: str) -> DeadLetterError:
        """A ``SEQUENCED`` letter's error, naming the key's head letter."""
        return cls.new("Sequenced", f"an earlier letter for this key is parked: {head_id}")

    @classmethod
    def invalid(cls, message: str) -> DeadLetterError:
        """An ``INVALID`` letter's error."""
        return cls.new("InvalidMessage", message)


def letter_id(processor: str, source: str, segment: str, position: str) -> str:
    """A letter's id: ``<processor>:<source>:<segment>:<position>`` (on Kafka
    ``<processor>:<topic>:<partition>:<offset>``)."""
    return f"{processor}:{source}:{segment}:{position}"


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """One parked record. ``id`` is :func:`letter_id`, so parking a redelivered record finds the first letter."""

    id: str
    processor: str
    source: str
    segment: str
    position: str
    key: str | None
    event_id: str | None
    event_type: str | None
    headers: dict[str, str]
    payload: bytes
    reason: DeadLetterReason
    error: DeadLetterError
    blocked_by: str | None
    attempts: int
    parked_at_ms: int
    last_attempt_at_ms: int
    expire_at_ms: int
    replay: bool

    def dedupe_id(self) -> str:
        """The id the record's stage 1 is deduplicated by: its ``id`` header, else (none or blank)
        ``<source>-<segment>-<position>``."""
        if self.event_id is not None and self.event_id.strip():
            return self.event_id
        return f"{self.source}-{self.segment}-{self.position}"


@dataclass(frozen=True, slots=True)
class DeadLetterSummary:
    """What :meth:`DeadLetterStore.summaries` reads of a letter."""

    id: str
    key: str | None
    position: str
    reason: DeadLetterReason
    parked_at_ms: int


class Parked(StrEnum):
    """Whether :meth:`DeadLetterStore.park` inserted the letter or found it parked already."""

    NEW = "new"
    ALREADY = "already"


class DeadLetterStore(Protocol):
    """The dead letters (mode ``collection``): records that exhausted their attempts, parked and sequenced per
    (processor, key). Every operation is a write of its own, outside any transaction, but a retry's
    :meth:`delete_in`."""

    async def park(self, letter: DeadLetter) -> Parked:
        """Inserts ``letter``; :attr:`Parked.ALREADY` when a letter of that id exists (a redelivery)."""
        ...

    async def head(self, processor: str, key: str) -> str | None:
        """The id of the head letter of (``processor``, ``key``): the earliest position among its letters that are not
        ``INVALID`` (then the lowest id)."""
        ...

    async def earlier(self, processor: str, key: str, position: str) -> str | None:
        """The id of a letter of (``processor``, ``key``) at a position before ``position``, not ``INVALID`` (the
        earliest): what blocks a retry."""
        ...

    async def parked_keys(self, processor: str, source: str, segments: Sequence[str]) -> set[str]:
        """The keys with a letter (not ``INVALID``) of ``processor`` on the given segments of ``source``."""
        ...

    async def get(self, letter: str) -> DeadLetter | None:
        """The letter, or ``None``."""
        ...

    async def list(self, processor: str | None = None, limit: int | None = None) -> builtins.list[DeadLetter]:
        """Letters by ``parked_at_ms``, then id; every processor's when ``processor`` is ``None``; at most ``limit``."""
        ...

    async def summaries(self, processor: str) -> builtins.list[DeadLetterSummary]:
        """Every letter of ``processor``, without its record, by position, then id."""
        ...

    async def count(self, processor: str | None = None) -> int:
        """How many letters; every processor's when ``processor`` is ``None``."""
        ...

    async def delete(self, letter: str) -> bool:
        """Deletes the letter; false when there was none."""
        ...

    async def delete_in(self, letter: str, *, tx: Tx | None = None) -> bool:
        """Deletes the letter in ``tx``: a retry's, which commits with the record's marker and the handlers' effects or
        not at all. False when there was none."""
        ...

    async def record_failure(self, letter: str, error: DeadLetterError, now_ms: int) -> bool:
        """A retry failed again: ``attempts + 1``, ``error``, ``last_attempt_at_ms = now_ms``, reason
        ``HANDLER_FAILED``, no ``blocked_by`` any more. False when the letter is gone."""
        ...


@dataclass(frozen=True, slots=True)
class ControlOperation:
    """The lock of a reset: held until ``lease_until_ms`` unless renewed."""

    id: str
    kind: str
    lease_until_ms: int
    prepared_at_ms: int
    """The first prepare's time; a renewal keeps it."""

    def holds_at(self, now_ms: int) -> bool:
        """Whether the lease has not passed at ``now_ms``."""
        return self.lease_until_ms > now_ms


@dataclass(frozen=True, slots=True)
class ProcessorControlState:
    """One processor's state; a processor the store knows nothing of is not paused, not locked and not replaying."""

    processor: str
    paused: bool = False
    paused_at_ms: int | None = None
    paused_by: str | None = None
    """The ``control_id`` of the request that paused it, or ``local``."""
    operation: ControlOperation | None = None
    """The reset holding the processor's lock."""
    replay: tuple[ReplayRange, ...] = ()
    """The open replay ranges."""
    updated_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class Locked:
    """What :meth:`ProcessorControlStore.lock` answered: ``taken`` when the caller holds the lock now, else the
    ``holder``."""

    taken: bool
    holder: ControlOperation | None = None

    @classmethod
    def was_taken(cls) -> Locked:
        """The caller holds the lock now."""
        return cls(True)

    @classmethod
    def held(cls, operation: ControlOperation) -> Locked:
        """Another operation holds the lock."""
        return cls(False, operation)


class ProcessorControlStore(Protocol):
    """The processor control state: per processor its pause, its reset lock and its replay ranges, the source of truth
    of all three. Every operation is a write of its own, outside any transaction; times are the caller's clock."""

    async def get(self, processor: str) -> ProcessorControlState:
        """The state of ``processor``; the default (not paused, nothing held) when the store has none."""
        ...

    async def set_paused(self, processor: str, paused: bool, by: str, now_ms: int) -> None:
        """Pauses (``by`` the control id, or ``local``) or resumes ``processor``; idempotent. A resume clears
        ``paused_at_ms`` and ``paused_by``."""
        ...

    async def lock(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> Locked:
        """Takes the lock of ``processor`` for ``operation`` until ``lease_until_ms``, atomically: when nothing holds it
        or the holder's lease has passed (``lease_until_ms <= now_ms``); a renewal (the operation holds it already)
        moves the lease only."""
        ...

    async def extend(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> bool:
        """Moves the lease of ``operation``; false when it does not hold the lock."""
        ...

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        """Clears the lock when ``operation`` holds it (never another operation's); false when it did not."""
        ...

    async def expire(self, processor: str, operation: str, now_ms: int) -> bool:
        """Clears the lock when ``operation`` holds it and its lease has passed at ``now_ms`` (a renewal that came
        meanwhile keeps it)."""
        ...

    async def set_replay(self, processor: str, ranges: Sequence[ReplayRange], now_ms: int) -> None:
        """Replaces the replay ranges of ``processor``."""
        ...

    async def remove_range(self, processor: str, replay_range: ReplayRange, now_ms: int) -> bool:
        """Removes that exact range from the replay ranges of ``processor``; false when it was not there."""
        ...


class ExternalCallStatus(StrEnum):
    """Where an external call stands."""

    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ExternalCall:
    """The record of one idempotent external call. ``reply`` is the JSON text of the handler's reply, ``error`` the JSON
    text of a failed call's error. An ``IN_PROGRESS`` record holds ``owner`` and ``lease_until_ms``; a settled one holds
    neither."""

    key: str
    status: ExternalCallStatus
    reply: str | None = None
    error: str | None = None
    attempts: int = 0
    updated_at_ms: int = 0
    lease_until_ms: int | None = None
    owner: str | None = None


class ExternalCallStore(Protocol):
    """The records of idempotent external calls, keyed by their idempotency key. Every operation is a write of its own,
    outside any transaction (a rolled-back handler never forgets a call that happened)."""

    async def insert(self, call: ExternalCall) -> bool:
        """Inserts ``call``; false when a record of its key exists."""
        ...

    async def find(self, key: str) -> ExternalCall | None:
        """The record of ``key``, or ``None``."""
        ...

    async def take_over(self, seen: ExternalCall, owner: str, lease_until_ms: int, now_ms: int) -> ExternalCall | None:
        """Takes the lapsed call ``seen`` over for ``owner``: ``owner``, ``lease_until_ms``, ``updated_at_ms = now_ms``
        and ``attempts + 1``, when the record is still ``seen``; the record as written."""
        ...

    async def expire(self, seen: ExternalCall, now_ms: int) -> bool:
        """Gives the lapsed call ``seen`` up as ``UNKNOWN`` (no owner, no lease), when the record is still ``seen``."""
        ...

    async def succeed(self, key: str, owner: str, reply: str, now_ms: int) -> bool:
        """``SUCCEEDED`` with ``reply`` (and no error, owner or lease) on the record ``owner`` owns."""
        ...

    async def fail(self, key: str, owner: str, error: str, now_ms: int) -> bool:
        """``FAILED`` with ``error`` (no owner or lease) on the record ``owner`` owns."""
        ...

    async def mark_unknown(self, key: str, owner: str, now_ms: int) -> bool:
        """``UNKNOWN`` (no owner or lease) on the record ``owner`` owns."""
        ...

    async def reset(self, key: str, owner: str) -> bool:
        """Deletes the record ``owner`` owns (a retryable failure: the next call begins afresh)."""
        ...


@dataclass(frozen=True, slots=True)
class Association:
    """An association ``key = value`` of a saga instance: events whose association value is ``value`` for ``key`` are
    routed to it."""

    key: str
    value: str


@dataclass(frozen=True, slots=True)
class SagaDocument:
    """One saga instance. ``state`` is the saga object as JSON text, ``version`` the optimistic version every write
    checks (0 when created, +1 per write). While ``ended`` is false the instance is live: its ``associations`` route
    events to it and its ``start_key`` (``"<type>|<key>=<value>"``, ``None`` for instances started with ``force_new``)
    is unique. Ended instances expire after the store's retention, counted from ``ended_at_ms``."""

    id: str
    type_name: str
    state: str
    associations: tuple[Association, ...]
    start_key: str | None
    ended: bool
    ended_at_ms: int | None
    version: int
    created_at_ms: int
    updated_at_ms: int


class DispatchKind(StrEnum):
    """Whether a saga dispatch entry is a command (through the hub or the service's own handler) or an event
    (through the outbox)."""

    COMMAND = "COMMAND"
    EVENT = "EVENT"


class FailureReason(StrEnum):
    """Why a saga command failed (the retry taxonomy)."""

    NO_HANDLER = "NO_HANDLER"
    DENIED = "DENIED"
    INVALID = "INVALID"
    FAILED = "FAILED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"
    EXHAUSTED = "EXHAUSTED"


@dataclass(frozen=True, slots=True)
class SagaDispatchDocument:
    """One command or event a saga recorded in its transaction, waiting in the saga-side outbox. ``seq`` orders the
    entries of one saga (``version * 1000 + index``); the id ``"<sagaId>:<seq>"`` is the command's idempotency key. A
    command given up on carries its outcome (``failure_reason``, ``failure_message``, written with ``failed_at_ms``),
    and ``notice_pending`` is true until the ``SagaCommandFailed`` was delivered locally."""

    id: str
    saga_id: str
    seq: int
    kind: DispatchKind
    type_name: str
    payload: str
    headers: dict[str, str]
    routing_key: str | None
    created_at_ms: int
    dispatched_at_ms: int | None = None
    attempts: int = 0
    next_attempt_at_ms: int | None = None
    failed_at_ms: int | None = None
    failure_reason: FailureReason | None = None
    failure_message: str | None = None
    notice_pending: bool | None = None
    notice_attempts: int | None = None
    notice_next_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class DispatchFailure:
    """How the relay gives an entry up: ``failed_at_ms``, the reason and the message, with ``attempts`` when given,
    and a pending failure notice due at ``notice_at_ms`` when given (``notice_pending`` true, ``notice_attempts`` 0):
    a command's failure has one, an event's none."""

    attempts: int | None
    reason: FailureReason
    message: str
    failed_at_ms: int
    notice_at_ms: int | None


class DeadlineStatus(StrEnum):
    """The lifecycle of a deadline."""

    SCHEDULED = "SCHEDULED"
    FIRED = "FIRED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class DeadlineDocument:
    """A deadline of a saga instance. ``payload`` is the JSON text of the payload (any JSON value) and ``payload_type``
    its type name, both ``None`` when there is none. A scheduler claims a due deadline with ``claimed_by`` and
    ``claimed_until_ms`` (a lease: an expired one lets it fire again); ``finished_at_ms`` is set when it is ``FIRED`` or
    ``CANCELLED``; ``correlation_id`` is the correlation id of the message that scheduled it."""

    id: str
    saga_type: str
    saga_id: str
    name: str
    payload: str | None
    payload_type: str | None
    due_at_ms: int
    status: DeadlineStatus
    claimed_by: str | None
    claimed_until_ms: int | None
    finished_at_ms: int | None
    correlation_id: str | None


@dataclass(frozen=True, slots=True)
class SagaFailureDocument:
    """A saga event that failed all its attempts, parked in ``saga_failures`` (id ``<sagaType>:<eventId>``): the event
    as received (enough to rebuild its message context), the last error and how many attempts were made."""

    id: str
    saga_type: str
    event_id: str
    event_type: str
    payload: str
    headers: dict[str, str]
    error: str
    attempts: int
    first_failed_at_ms: int
    last_failed_at_ms: int


class SagaRepository(Protocol):
    """The ``sagas`` collection."""

    async def find_by_id(self, saga_id: str, *, tx: Tx | None = None) -> SagaDocument | None:
        """The instance ``saga_id`` as ``tx`` sees it, or ``None``."""
        ...

    async def find_live(self, type_name: str, key: str, value: str, *, tx: Tx | None = None) -> list[SagaDocument]:
        """The live (not ended) instances of ``type_name`` associated with ``key = value``."""
        ...

    async def insert(self, saga: SagaDocument, *, tx: Tx | None = None) -> None:
        """Inserts a new instance; a :class:`Conflict` when its ``start_key`` is held by a live instance."""
        ...

    async def update(self, saga: SagaDocument, expected_version: int, *, tx: Tx | None = None) -> bool:
        """Replaces the stored instance by ``saga`` when the stored ``version`` is ``expected_version``; false when
        it is not."""
        ...

    async def count_live_by_type(self) -> dict[str, int]:
        """How many live (not ended) instances there are per saga type; a type without one is absent."""
        ...


class SagaDispatchStore(Protocol):
    """The saga-side outbox (``saga_dispatch``) and its per-saga dispatch leases. The runtime records what a saga sends
    in the saga's transaction (:meth:`record`); the dispatch relay works through it. Every relay operation is a write of
    its own, outside any transaction, but :meth:`mark_dispatched`, which joins the transaction the relay appends the
    event to the outbox in."""

    async def record(self, entries: Sequence[SagaDispatchDocument], *, tx: Tx | None = None) -> None:
        """Records ``entries`` in ``tx``. An ``EVENT`` entry whose payload the outbox would refuse (see
        :func:`check_outbox_payload`) is refused with :class:`InvalidMessage`: the saga's attempt fails and nothing
        of it commits."""
        ...

    async def due_sagas(self, now_ms: int, except_sagas: Sequence[str], limit: int) -> list[str]:
        """The sagas whose first pending entry (lowest ``seq``) is due (``next_attempt_at_ms`` absent or
        ``<= now_ms``), oldest first pending entry first (its ``created_at_ms``, then the saga id), at most ``limit``,
        leaving out ``except_sagas`` (in flight on this instance)."""
        ...

    async def next_pending(self, saga_id: str) -> SagaDispatchDocument | None:
        """The first pending entry of ``saga_id`` (lowest ``seq``), due or not."""
        ...

    async def mark_dispatched(self, entry_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        """The entry was dispatched at ``now_ms``, in ``tx`` (an event: with its outbox append)."""
        ...

    async def schedule_retry(self, entry_id: str, attempts: int, next_attempt_at_ms: int) -> None:
        """A transient failure: ``attempts`` and the next attempt at ``next_attempt_at_ms``."""
        ...

    async def mark_failed(self, entry_id: str, failure: DispatchFailure) -> None:
        """Gives the entry up: see :class:`DispatchFailure`."""
        ...

    async def claim_due_notice(self, now_ms: int, reserve_until_ms: int) -> SagaDispatchDocument | None:
        """Claims the pending failure notice due first (``notice_next_at_ms`` absent or ``<= now_ms``), moving its
        ``notice_next_at_ms`` to ``reserve_until_ms`` (which reserves it for the caller), atomically; the entry as
        claimed, ``None`` when no notice is due."""
        ...

    async def clear_notice(self, entry_id: str) -> None:
        """The failure notice was delivered (or given up): no notice is pending any more."""
        ...

    async def schedule_notice_retry(self, entry_id: str, notice_attempts: int, next_at_ms: int) -> None:
        """A delivery of the notice failed: ``notice_attempts`` and its next try at ``next_at_ms``."""
        ...

    async def claim_lease(self, saga_id: str, owner: str, now_ms: int, until_ms: int) -> bool:
        """Takes the dispatch lease of ``saga_id`` for ``owner`` until ``until_ms`` when nobody holds it or its lease
        has passed (``< now_ms``), atomically; false when another holder's lease runs."""
        ...

    async def renew_lease(self, saga_id: str, owner: str, until_ms: int) -> bool:
        """Moves the lease ``owner`` holds to ``until_ms``; false when it holds it no longer."""
        ...

    async def release_lease(self, saga_id: str, owner: str) -> None:
        """Lets go of the lease when ``owner`` holds it."""
        ...

    async def pending(self) -> int:
        """How many entries are pending."""
        ...


class DeadlineStore(Protocol):
    """The ``deadlines`` collection, as far as the runtime writes it."""

    async def insert(self, deadline: DeadlineDocument, *, tx: Tx | None = None) -> None:
        """Inserts ``deadline`` in ``tx``."""
        ...

    async def find_scheduled(self, deadline_id: str, *, tx: Tx | None = None) -> DeadlineDocument | None:
        """The deadline ``deadline_id`` when it is still ``SCHEDULED`` (not fired, not cancelled), else ``None``."""
        ...

    async def cancel(self, saga_id: str, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        """Cancels the ``SCHEDULED`` deadline ``deadline_id`` of ``saga_id``; no effect on any other."""
        ...

    async def cancel_by_name(self, saga_id: str, name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        """Cancels the ``SCHEDULED`` deadlines of ``saga_id`` named ``name``."""
        ...

    async def cancel_all(self, saga_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        """Cancels every ``SCHEDULED`` deadline of ``saga_id``."""
        ...

    async def mark_fired(self, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> bool:
        """Sets the ``SCHEDULED`` deadline ``deadline_id`` to ``FIRED``; false when it was not ``SCHEDULED``."""
        ...

    async def claim_due(self, owner: str, now_ms: int, claimed_until_ms: int) -> DeadlineDocument | None:
        """Claims the ``SCHEDULED`` deadline due first (``due_at_ms <= now_ms``) that nobody holds or whose claim has
        passed (``claimed_until_ms < now_ms``), for ``owner`` until ``claimed_until_ms``, atomically; the deadline as
        claimed, ``None`` when nothing is due. Outside any transaction."""
        ...

    async def count_overdue_by_name(self, now_ms: int) -> dict[str, int]:
        """How many ``SCHEDULED`` deadlines are due (``due_at_ms <= now_ms``), per name, claimed or not; a name without
        one is absent."""
        ...


class FailureStore(Protocol):
    """The ``saga_failures`` collection: parked events."""

    async def find(self, failure_id: str, *, tx: Tx | None = None) -> SagaFailureDocument | None:
        """The failure ``failure_id``, or ``None``."""
        ...

    async def save(self, failure: SagaFailureDocument, *, tx: Tx | None = None) -> None:
        """Inserts or replaces the failure with that id."""
        ...

    async def delete(self, failure_id: str, *, tx: Tx | None = None) -> bool:
        """Deletes the failure; false when there was none."""
        ...

    async def failed_again(
        self, failure_id: str, error: str, attempts: int, now_ms: int, *, tx: Tx | None = None
    ) -> bool:
        """Records that a retry of the failure failed again: ``attempts`` grows by ``attempts``, ``error`` and
        ``last_failed_at_ms`` are replaced. Never inserts: false when the failure no longer exists."""
        ...

    async def list(self, saga_type: str | None = None, *, tx: Tx | None = None) -> builtins.list[SagaFailureDocument]:
        """The failures of ``saga_type`` (every type when ``None``), oldest ``last_failed_at_ms`` first."""
        ...


class SagaMarkerStore(Protocol):
    """The exactly-once markers of the saga stage: processed-event ids ``saga:<sagaType>:<eventId>``."""

    async def exists(self, marker_id: str, *, tx: Tx | None = None) -> bool:
        """Whether the marker ``marker_id`` exists, as ``tx`` sees it."""
        ...

    async def insert(self, marker_id: str, type_name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        """Inserts the marker; a :class:`Conflict` when it exists."""
        ...


@dataclass(frozen=True, slots=True)
class SagaStores:
    """The five saga ports together, sharing the store's transactions."""

    sagas: SagaRepository
    dispatch: SagaDispatchStore
    deadlines: DeadlineStore
    failures: FailureStore
    markers: SagaMarkerStore


class MessageStore(Transactions, Protocol):
    """A message-store add-on: every store the library uses, in the service's own database, sharing one transaction."""

    @property
    def name(self) -> str:
        """The add-on's name (``mongo``, ``postgres``, ``mysql``, ``memory``), for logs and errors."""
        ...

    def transaction(self) -> AbstractAsyncContextManager[Tx]:
        """A transaction bound to the current context (joining one already bound), or a new one when none is.

        :class:`TransactionScope` is the core's implementation: an add-on's own context manager must bind the
        transaction the core sees (:func:`current_transaction`), so a low-level operation called without ``tx=`` joins
        it.
        """
        ...

    def use_clock(self, clock: Callable[[], int]) -> None:
        """The application's clock, epoch milliseconds: what the store stamps the times it sets itself with (an outbox
        entry's sending, a lease). Until called, the wall clock."""
        ...

    @property
    def outbox(self) -> OutboxStore: ...

    @property
    def processed(self) -> ProcessedStore: ...

    @property
    def dead_letters(self) -> DeadLetterStore: ...

    @property
    def control(self) -> ProcessorControlStore: ...

    @property
    def external(self) -> ExternalCallStore: ...

    @property
    def sagas(self) -> SagaStores: ...
