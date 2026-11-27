"""The in-memory message store: the level-2 stores of the message-store contract inside one process, for tests, local
runs and single-process demos, with no database.

- **Real transactions.** What a transaction writes is seen by others only at its commit and is gone after a rollback;
  its own later writes see its earlier ones (a repeated marker is a duplicate, a repeated entry id a conflict), while
  the reads that take no transaction (``exists``, ``claim``, ``pending``, ``oldest_pending``) see committed state only.
  A transaction reserves every record it writes: a second live transaction writing one of them is refused at once with
  :class:`~snodra._store.Conflict`, and a commit fails when a record it wrote was changed meanwhile (a write outside any
  transaction). What runs outside a transaction commits at once, and a transaction dropped without a commit releases
  its reservations.
- **The outbox and the processed events** implement their level-2 contract: atomic claims under a lease in
  ``(created_at_ms, seq)`` order, pending counts, purge of what was sent, payload refusal and one marker per id, plus
  the wake-up of the relay after every committed append.
- **The level-3 ports** (dead letters and processor control) are implemented here (mode ``collection``); the level-4
  ports (external calls and sagas) are present and report :class:`~snodra._store.Unsupported`; an official add-on
  implements them by level 4. A retry's letter deletion joins the record's transaction.
- **Times** the store stamps itself (a send, a lease) are the application's clock (the wall clock until
  :meth:`~snodra._store.MessageStore.use_clock` is called), so tests can advance it.
- **Not durable and one process only**: everything is lost when the process ends.

Every method runs on one event loop and does its work without waiting, so each claim and each transaction step is
atomic with respect to the others.
"""

from __future__ import annotations

import builtins
import contextlib
import itertools
import weakref
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import Final

from snodra._eventstore import ProcessedMarker, ReplayRange
from snodra._store import (
    RESET_KIND,
    AppendListener,
    Conflict,
    ControlOperation,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    DeadLetterStore,
    DeadLetterSummary,
    DeadlineDocument,
    DispatchFailure,
    ExternalCall,
    ExternalCallStore,
    Failed,
    Locked,
    OutboxEntry,
    OutboxStore,
    Parked,
    ProcessedStore,
    ProcessorControlState,
    ProcessorControlStore,
    SagaDispatchDocument,
    SagaDocument,
    SagaFailureDocument,
    SagaStores,
    TransactionScope,
    Tx,
    Unsupported,
    bind_transaction,
    check_outbox_payload,
    current_transaction,
    now_ms,
    unbind_transaction,
)

NAME: Final = "memory"
"""The store's name, reported to the hub and in errors."""


@dataclass(slots=True)
class _OutboxRow:
    """One outbox entry and the relay's marks on it."""

    entry: OutboxEntry
    sent_at_ms: int | None = None
    failed_at_ms: int | None = None
    owner: str | None = None
    lease_until_ms: int | None = None

    def pending(self) -> bool:
        """Neither sent nor failed."""
        return self.sent_at_ms is None and self.failed_at_ms is None


@dataclass(slots=True)
class _ProcessedRow:
    """One processed-event marker."""

    marker: ProcessedMarker


@dataclass(slots=True)
class _Table[T]:
    """Committed rows by id, and the store-wide version of each row's last write (a version is kept after a delete, so
    a transaction that read the row sees the change)."""

    rows: dict[str, T] = field(default_factory=dict)
    versions: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class _Write[T]:
    """One transaction's write of a record: the version it saw (``None``: absent) and the value (``None``: deleted)."""

    base: int | None
    value: T | None


@dataclass(slots=True)
class _Overlay[T]:
    """A transaction's writes to one table, by id."""

    writes: dict[str, _Write[T]] = field(default_factory=dict)


@dataclass(slots=True)
class _Data:
    """Everything the store holds, committed."""

    outbox: _Table[_OutboxRow] = field(default_factory=_Table)
    processed: _Table[_ProcessedRow] = field(default_factory=_Table)
    dead: _Table[DeadLetter] = field(default_factory=_Table)
    reserved: dict[tuple[str, str], int] = field(default_factory=dict)
    version: int = 0
    next_tx: int = 1


@dataclass(slots=True)
class _MemoryTx:
    """One transaction's writes, kept apart until its commit."""

    store_id: int
    number: int
    owner: weakref.ReferenceType[MemoryStore] | None = field(default=None, repr=False)
    ended: bool = False
    doomed: bool = False
    reserved: list[tuple[str, str]] = field(default_factory=list)
    outbox: _Overlay[_OutboxRow] = field(default_factory=_Overlay)
    processed: _Overlay[_ProcessedRow] = field(default_factory=_Overlay)
    dead: _Overlay[DeadLetter] = field(default_factory=_Overlay)

    def __del__(self) -> None:
        """A transaction dropped without a commit releases its reservations, as a database's connection does."""
        with contextlib.suppress(Exception):  # the interpreter may be shutting its modules down
            owner = self.owner
            store = None if owner is None else owner()
            if store is not None:
                store.release_abandoned(self)


_STORE_IDS = itertools.count(1)


class MemoryStore:
    """The in-memory message store: see the module docstring.

    ``clock`` is the application's clock in epoch milliseconds (the wall clock by default), for tests that need known
    times; :meth:`~snodra._store.MessageStore.use_clock` replaces it.
    """

    def __init__(self, *, clock: Callable[[], int] | None = None) -> None:
        self._store_id = next(_STORE_IDS)
        self._data = _Data()
        self._clock: Callable[[], int] = now_ms if clock is None else clock
        self._listeners: dict[int, Callable[[], None]] = {}
        self._next_listener = 0
        self.outbox: OutboxStore = _MemoryOutbox(self)
        self.processed: ProcessedStore = _MemoryProcessed(self)
        self.dead_letters: DeadLetterStore = _DeadLetters(self)
        self.control: ProcessorControlStore = _Control()
        self.external: ExternalCallStore = _External()
        self.sagas = SagaStores(
            sagas=_Sagas(),
            dispatch=_Dispatch(),
            deadlines=_Deadlines(),
            failures=_Failures(),
            markers=_Markers(),
        )

    @property
    def name(self) -> str:
        return NAME

    def transaction(self) -> TransactionScope:
        """A transaction bound to the current context, or a new one."""
        return TransactionScope(self)

    def use_clock(self, clock: Callable[[], int]) -> None:
        self._clock = clock

    async def begin(self) -> Tx:
        number = self._data.next_tx
        self._data.next_tx += 1
        return Tx(_MemoryTx(self._store_id, number, weakref.ref(self)))

    def plain(self) -> Tx:
        return Tx.none()

    async def commit(self, tx: Tx) -> None:
        inner = self._open(tx)
        if inner is None:
            return
        inner.ended = True
        committed = False
        try:
            if inner.doomed:
                raise Conflict("write conflict: the transaction met a record another transaction is writing")
            self._check_stale("outbox", self._data.outbox, inner.outbox)
            self._check_stale("processed", self._data.processed, inner.processed)
            self._check_stale("dead", self._data.dead, inner.dead)
            self._apply(inner)
            committed = True
        finally:
            self._release(inner)
        if committed and inner.outbox.writes:
            self._wake()

    async def rollback(self, tx: Tx) -> None:
        try:
            inner = self._open(tx)
        except Failed:
            return
        if inner is None:
            return
        inner.ended = True
        inner.outbox.writes.clear()
        inner.processed.writes.clear()
        inner.dead.writes.clear()
        self._release(inner)

    # ---- what the stores use -------------------------------------------------------------------

    def now(self) -> int:
        """The application's clock in epoch milliseconds."""
        return int(self._clock())

    def resolve(self, tx: Tx | None) -> _MemoryTx | None:
        """The transaction an operation runs in: ``tx`` when given, else the bound one; ``None`` outside one."""
        return self._open(current_transaction() if tx is None else tx)

    def open_outbox(self, entry: OutboxEntry, tx: _MemoryTx | None) -> None:
        """Appends ``entry`` in ``tx`` (or at once); see :meth:`OutboxStore.append`."""
        check_outbox_payload(entry.type_name, entry.payload)
        stored = _copy_entry(entry)
        if tx is None:
            if entry.id in self._data.outbox.rows:
                # TODO(spec-gap): SG-16
                raise Conflict(f"duplicate key: an outbox entry {entry.id} exists")
            self._write("outbox", self._data.outbox, None, None, entry.id, _OutboxRow(stored))
            self._wake()
            return
        if entry.id in tx.outbox.writes or entry.id in self._data.outbox.rows:
            # TODO(spec-gap): SG-16
            raise Conflict(f"duplicate key: an outbox entry {entry.id} exists")
        self._write("outbox", self._data.outbox, tx.outbox, tx, entry.id, _OutboxRow(stored))

    def record_marker(self, marker: ProcessedMarker, tx: _MemoryTx | None) -> bool:
        """Records ``marker`` in ``tx`` (or at once); see :meth:`ProcessedStore.record`."""
        if tx is None:
            if marker.id in self._data.processed.rows:
                return False
            self._write("processed", self._data.processed, None, None, marker.id, _ProcessedRow(marker))
            return True
        if marker.id in tx.processed.writes or marker.id in self._data.processed.rows:
            return False
        self._write("processed", self._data.processed, tx.processed, tx, marker.id, _ProcessedRow(marker))
        return True

    def processed_exists(self, marker_id: str) -> bool:
        """Whether a marker is recorded (committed)."""
        return marker_id in self._data.processed.rows

    def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        """Claims up to ``limit`` pending entries for ``owner``; see :meth:`OutboxStore.claim`."""
        now = self.now()
        until = now + int(lease * 1000)
        rows = [
            row
            for row in self._data.outbox.rows.values()
            if row.pending()
            and (
                row.owner is None
                or row.owner == owner
                or (row.lease_until_ms is not None and row.lease_until_ms <= now)
            )
        ]
        rows.sort(key=lambda row: (row.entry.created_at_ms, row.entry.seq, row.entry.id))
        claimed: list[OutboxEntry] = []
        for row in rows[: max(0, limit)]:
            row.owner = owner
            row.lease_until_ms = until
            self._bump(self._data.outbox, row.entry.id)
            claimed.append(_copy_entry(row.entry))
        return claimed

    def pending(self) -> int:
        """How many outbox entries are pending."""
        return sum(1 for row in self._data.outbox.rows.values() if row.pending())

    def oldest_pending(self) -> int | None:
        """The ``created_at_ms`` of the oldest pending outbox entry."""
        pending = [(row.entry.created_at_ms, row.entry.seq) for row in self._data.outbox.rows.values() if row.pending()]
        return min(pending)[0] if pending else None

    def on_append(self, wake: Callable[[], None]) -> AppendListener:
        """Registers ``wake``, called after every committed append."""
        self._next_listener += 1
        listener = self._next_listener
        self._listeners[listener] = wake

        def remove() -> None:
            self._listeners.pop(listener, None)

        return AppendListener(remove)

    def purge_outbox(self, sent_before_ms: int) -> int:
        """Removes the entries sent before ``sent_before_ms``."""
        gone = [
            entry_id
            for entry_id, row in self._data.outbox.rows.items()
            if row.sent_at_ms is not None and row.sent_at_ms < sent_before_ms
        ]
        for entry_id in gone:
            del self._data.outbox.rows[entry_id]
            self._bump(self._data.outbox, entry_id)
        return len(gone)

    def purge_processed(self, before_ms: int) -> int:
        """Removes the markers handled before ``before_ms``."""
        gone = [
            marker_id for marker_id, row in self._data.processed.rows.items() if row.marker.processed_at_ms < before_ms
        ]
        for marker_id in gone:
            del self._data.processed.rows[marker_id]
            self._bump(self._data.processed, marker_id)
        return len(gone)

    def stamp_sent(self, ids: Sequence[str]) -> None:
        """Marks the entries sent, at the application's clock."""
        now = self.now()
        for entry_id in ids:
            row = self._data.outbox.rows.get(entry_id)
            if row is not None:
                row.sent_at_ms = now
                self._bump(self._data.outbox, entry_id)

    def stamp_attempt_failed(self, entry_id: str) -> None:
        """Counts one failed attempt that will be retried."""
        row = self._data.outbox.rows.get(entry_id)
        if row is not None:
            row.entry = replace(row.entry, attempts=row.entry.attempts + 1)
            self._bump(self._data.outbox, entry_id)

    def stamp_failed(self, entry_id: str) -> None:
        """Counts one failed attempt and gives the entry up."""
        row = self._data.outbox.rows.get(entry_id)
        if row is not None:
            row.entry = replace(row.entry, attempts=row.entry.attempts + 1)
            row.failed_at_ms = self.now()
            self._bump(self._data.outbox, entry_id)

    def release_abandoned(self, tx: _MemoryTx) -> None:
        """Lets go of a dropped transaction's reservations and writes (its finalizer)."""
        tx.ended = True
        tx.outbox.writes.clear()
        tx.processed.writes.clear()
        tx.dead.writes.clear()
        self._release(tx)

    # ---- transactions --------------------------------------------------------------------------

    def _open(self, tx: Tx | None) -> _MemoryTx | None:
        """This store's open transaction in ``tx``; ``None`` for no transaction and for one that ended."""
        if tx is None or tx.inner is None:
            return None
        inner = tx.inner
        if not isinstance(inner, _MemoryTx) or inner.store_id != self._store_id:
            raise Failed("the in-memory store was given another store's transaction")
        return None if inner.ended else inner

    def _write[T](
        self,
        kind: str,
        table: _Table[T],
        overlay: _Overlay[T] | None,
        tx: _MemoryTx | None,
        key: str,
        value: T | None,
    ) -> None:
        """Writes ``value`` as ``key``, committed at once or in ``tx`` (reserving the record)."""
        if tx is None or overlay is None:
            self._data.version += 1
            if value is None:
                table.rows.pop(key, None)
            else:
                table.rows[key] = value
            table.versions[key] = self._data.version
            return
        self._reserve(tx, kind, key)
        write = overlay.writes.get(key)
        if write is None:
            overlay.writes[key] = _Write(table.versions.get(key), value)
        else:
            write.value = value

    def _reserve(self, tx: _MemoryTx, kind: str, key: str) -> None:
        """Reserves one record for ``tx``; :class:`Conflict` when another live transaction holds it."""
        holder = self._data.reserved.get((kind, key))
        if holder is None:
            self._data.reserved[(kind, key)] = tx.number
            tx.reserved.append((kind, key))
        elif holder != tx.number:
            tx.doomed = True
            # TODO(spec-gap): SG-17
            raise Conflict(f"write conflict: {kind} record {key} is written by another transaction")

    def _check_stale[T](self, kind: str, table: _Table[T], overlay: _Overlay[T]) -> None:
        """Raises :class:`Conflict` when a record the transaction wrote changed since it saw it."""
        for key, write in overlay.writes.items():
            if table.versions.get(key) != write.base:
                raise Conflict(f"write conflict: {kind} record {key} changed since the transaction read it")

    def _apply(self, tx: _MemoryTx) -> None:
        """Commits ``tx``'s writes, assigning each a fresh version."""
        self._apply_table(self._data.outbox, tx.outbox)
        self._apply_table(self._data.processed, tx.processed)
        self._apply_table(self._data.dead, tx.dead)

    def _apply_table[T](self, table: _Table[T], overlay: _Overlay[T]) -> None:
        for key, write in overlay.writes.items():
            self._data.version += 1
            if write.value is None:
                table.rows.pop(key, None)
            else:
                table.rows[key] = write.value
            table.versions[key] = self._data.version

    def _release(self, tx: _MemoryTx) -> None:
        for key in tx.reserved:
            if self._data.reserved.get(key) == tx.number:
                del self._data.reserved[key]
        tx.reserved.clear()

    def _bump[T](self, table: _Table[T], key: str) -> None:
        """Counts a committed write of a record outside any transaction."""
        self._data.version += 1
        table.versions[key] = self._data.version

    def _wake(self) -> None:
        # A wake runs with no transaction bound: the relay it starts must not inherit the append's transaction.
        token = bind_transaction(None)
        try:
            for wake in list(self._listeners.values()):
                with contextlib.suppress(Exception):
                    wake()
        finally:
            unbind_transaction(token)


def _copy_entry(entry: OutboxEntry) -> OutboxEntry:
    """A copy the caller cannot change behind the store's back."""
    return replace(entry, headers=dict(entry.headers), payload=bytes(entry.payload))


class _MemoryOutbox:
    """The outbox of :class:`MemoryStore`."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    async def append(self, entry: OutboxEntry, *, tx: Tx | None = None) -> None:
        self._store.open_outbox(entry, self._store.resolve(tx))

    async def append_now(self, entry: OutboxEntry) -> None:
        self._store.open_outbox(entry, None)

    async def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        return self._store.claim(owner, lease, limit)

    async def mark_sent(self, ids: Sequence[str]) -> None:
        self._store.stamp_sent(ids)

    async def mark_attempt_failed(self, entry_id: str, error: str) -> None:
        self._store.stamp_attempt_failed(entry_id)

    async def mark_failed(self, entry_id: str, error: str) -> None:
        self._store.stamp_failed(entry_id)

    async def purge(self, sent_before_ms: int) -> int:
        return self._store.purge_outbox(sent_before_ms)

    async def pending(self) -> int:
        return self._store.pending()

    def on_append(self, wake: Callable[[], None]) -> AppendListener:
        return self._store.on_append(wake)

    async def oldest_pending(self) -> int | None:
        return self._store.oldest_pending()


class _MemoryProcessed:
    """The processed events of :class:`MemoryStore`."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        return self._store.record_marker(marker, self._store.resolve(tx))

    async def exists(self, marker_id: str) -> bool:
        return self._store.processed_exists(marker_id)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        """Deletes the stage-1 markers of ``processor`` in ``ranges`` (positions by value, then text)."""

        def in_range(marker_id: str, marker: _ProcessedRow, replay_range: ReplayRange) -> bool:
            found = marker.marker
            position = found.position
            if position is None or found.processor != processor:
                return False
            if found.source != replay_range.source or found.segment != replay_range.segment:
                return False
            return _position_key(replay_range.start) <= _position_key(position) < _position_key(replay_range.until)

        gone = [
            marker_id
            for marker_id, row in self._store._data.processed.rows.items()
            if any(in_range(marker_id, row, replay_range) for replay_range in ranges)
        ]
        for marker_id in gone:
            del self._store._data.processed.rows[marker_id]
            self._store._bump(self._store._data.processed, marker_id)
        return len(gone)

    async def purge(self, before_ms: int) -> int:
        return self._store.purge_processed(before_ms)


def _refuse(feature: str, level: int) -> Unsupported:
    return Unsupported(f"{feature} in the in-memory message store (level {level})")


def _position_key(position: str) -> tuple[int, int | str]:
    """A position's order in the in-memory store: canonical decimal positions by value, then every other by text (as the
    event stores of the suite name their positions)."""
    if (
        position
        and all("0" <= char <= "9" for char in position)
        and (len(position) == 1 or not position.startswith("0"))
    ):
        return (0, int(position))
    return (1, position)


def _copy_letter(letter: DeadLetter) -> DeadLetter:
    """A letter copy the caller cannot change behind the store's back."""
    return replace(letter, headers=dict(letter.headers), payload=bytes(letter.payload))


class _DeadLetters:
    """The dead letters (mode ``collection``) of :class:`MemoryStore`: every write is its own, except a retry's
    :meth:`delete_in`, which joins the message store's transaction."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    def _rows(self) -> dict[str, DeadLetter]:
        return self._store._data.dead.rows

    def _current(self, letter: str, tx: _MemoryTx | None) -> DeadLetter | None:
        """The letter as ``tx`` sees it (a delete in its overlay counts as gone)."""
        if tx is None:
            return self._rows().get(letter)
        write = tx.dead.writes.get(letter)
        if write is not None:
            return write.value
        return self._rows().get(letter)

    async def park(self, letter: DeadLetter) -> Parked:
        if letter.id in self._rows():
            return Parked.ALREADY
        self._store._write("dead", self._store._data.dead, None, None, letter.id, _copy_letter(letter))
        return Parked.NEW

    async def head(self, processor: str, key: str) -> str | None:
        candidates = [
            row
            for row in self._rows().values()
            if row.processor == processor and row.key == key and row.reason is not DeadLetterReason.INVALID
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda row: (_position_key(row.position), row.id)).id

    async def earlier(self, processor: str, key: str, position: str) -> str | None:
        here = _position_key(position)
        candidates = [
            row
            for row in self._rows().values()
            if row.processor == processor
            and row.key == key
            and row.reason is not DeadLetterReason.INVALID
            and _position_key(row.position) < here
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda row: (_position_key(row.position), row.id)).id

    async def parked_keys(self, processor: str, source: str, segments: Sequence[str]) -> set[str]:
        wanted = set(segments)
        return {
            row.key
            for row in self._rows().values()
            if row.processor == processor
            and row.source == source
            and row.segment in wanted
            and row.key is not None
            and row.reason is not DeadLetterReason.INVALID
        }

    async def get(self, letter: str) -> DeadLetter | None:
        return self._rows().get(letter)

    async def list(self, processor: str | None = None, limit: int | None = None) -> builtins.list[DeadLetter]:
        rows = [row for row in self._rows().values() if processor is None or row.processor == processor]
        rows.sort(key=lambda row: (row.parked_at_ms, row.id))
        if limit is not None:
            rows = rows[: max(0, limit)]
        return [_copy_letter(row) for row in rows]

    async def summaries(self, processor: str) -> builtins.list[DeadLetterSummary]:
        rows = [row for row in self._rows().values() if row.processor == processor]
        rows.sort(key=lambda row: (_position_key(row.position), row.id))
        return [
            DeadLetterSummary(
                id=row.id,
                key=row.key,
                position=row.position,
                reason=row.reason,
                parked_at_ms=row.parked_at_ms,
            )
            for row in rows
        ]

    async def count(self, processor: str | None = None) -> int:
        return sum(1 for row in self._rows().values() if processor is None or row.processor == processor)

    async def delete(self, letter: str) -> bool:
        if letter not in self._rows():
            return False
        self._store._write("dead", self._store._data.dead, None, None, letter, None)
        return True

    async def delete_in(self, letter: str, *, tx: Tx | None = None) -> bool:
        inner = self._store.resolve(tx)
        if self._current(letter, inner) is None:
            return False
        self._store._write("dead", self._store._data.dead, None if inner is None else inner.dead, inner, letter, None)
        return True

    async def record_failure(self, letter: str, error: DeadLetterError, now_ms: int) -> bool:
        row = self._rows().get(letter)
        if row is None:
            return False
        self._store._write(
            "dead",
            self._store._data.dead,
            None,
            None,
            letter,
            replace(
                row,
                attempts=row.attempts + 1,
                error=error,
                last_attempt_at_ms=now_ms,
                reason=DeadLetterReason.HANDLER_FAILED,
                blocked_by=None,
            ),
        )
        return True


class _Control:
    """The processor control state of :class:`MemoryStore`: one document per processor, every write its own."""

    def __init__(self) -> None:
        self._states: dict[str, ProcessorControlState] = {}

    def _get(self, processor: str) -> ProcessorControlState:
        return self._states.get(processor, ProcessorControlState(processor))

    def _put(self, state: ProcessorControlState) -> None:
        self._states[state.processor] = state

    async def get(self, processor: str) -> ProcessorControlState:
        return self._get(processor)

    async def set_paused(self, processor: str, paused: bool, by: str, now_ms: int) -> None:
        state = self._get(processor)
        self._put(
            replace(
                state,
                paused=paused,
                paused_at_ms=now_ms if paused else None,
                paused_by=by if paused else None,
                updated_at_ms=now_ms,
            )
        )

    async def lock(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> Locked:
        state = self._get(processor)
        held = state.operation
        if held is not None and held.id == operation:
            self._put(replace(state, operation=replace(held, lease_until_ms=lease_until_ms), updated_at_ms=now_ms))
            return Locked.was_taken()
        if held is not None and held.holds_at(now_ms):
            return Locked.held(held)
        prepared = now_ms if held is None or held.id != operation else held.prepared_at_ms
        self._put(
            replace(
                state,
                operation=ControlOperation(operation, RESET_KIND, lease_until_ms, prepared),
                updated_at_ms=now_ms,
            )
        )
        return Locked.was_taken()

    async def extend(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> bool:
        state = self._get(processor)
        held = state.operation
        if held is None or held.id != operation:
            return False
        self._put(replace(state, operation=replace(held, lease_until_ms=lease_until_ms), updated_at_ms=now_ms))
        return True

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        state = self._get(processor)
        held = state.operation
        if held is None or held.id != operation:
            return False
        self._put(replace(state, operation=None, updated_at_ms=now_ms))
        return True

    async def expire(self, processor: str, operation: str, now_ms: int) -> bool:
        state = self._get(processor)
        held = state.operation
        if held is None or held.id != operation or held.holds_at(now_ms):
            return False
        self._put(replace(state, operation=None, updated_at_ms=now_ms))
        return True

    async def set_replay(self, processor: str, ranges: Sequence[ReplayRange], now_ms: int) -> None:
        state = self._get(processor)
        self._put(replace(state, replay=tuple(ranges), updated_at_ms=now_ms))

    async def remove_range(self, processor: str, replay_range: ReplayRange, now_ms: int) -> bool:
        state = self._get(processor)
        if replay_range not in state.replay:
            return False
        self._put(
            replace(
                state,
                replay=tuple(item for item in state.replay if item != replay_range),
                updated_at_ms=now_ms,
            )
        )
        return True


class _External:
    """The external-call records, implemented from level 4."""

    async def insert(self, call: ExternalCall) -> bool:
        raise _refuse("external calls", 4)

    async def find(self, key: str) -> ExternalCall | None:
        raise _refuse("external calls", 4)

    async def take_over(self, seen: ExternalCall, owner: str, lease_until_ms: int, now_ms: int) -> ExternalCall | None:
        raise _refuse("external calls", 4)

    async def expire(self, seen: ExternalCall, now_ms: int) -> bool:
        raise _refuse("external calls", 4)

    async def succeed(self, key: str, owner: str, reply: str, now_ms: int) -> bool:
        raise _refuse("external calls", 4)

    async def fail(self, key: str, owner: str, error: str, now_ms: int) -> bool:
        raise _refuse("external calls", 4)

    async def mark_unknown(self, key: str, owner: str, now_ms: int) -> bool:
        raise _refuse("external calls", 4)

    async def reset(self, key: str, owner: str) -> bool:
        raise _refuse("external calls", 4)


class _Sagas:
    """The saga collection, implemented from level 4."""

    async def find_by_id(self, saga_id: str, *, tx: Tx | None = None) -> SagaDocument | None:
        raise _refuse("sagas", 4)

    async def find_live(self, type_name: str, key: str, value: str, *, tx: Tx | None = None) -> list[SagaDocument]:
        raise _refuse("sagas", 4)

    async def insert(self, saga: SagaDocument, *, tx: Tx | None = None) -> None:
        raise _refuse("sagas", 4)

    async def update(self, saga: SagaDocument, expected_version: int, *, tx: Tx | None = None) -> bool:
        raise _refuse("sagas", 4)

    async def count_live_by_type(self) -> dict[str, int]:
        raise _refuse("sagas", 4)


class _Dispatch:
    """The saga dispatch entries and leases, implemented from level 4."""

    async def record(self, entries: Sequence[SagaDispatchDocument], *, tx: Tx | None = None) -> None:
        raise _refuse("saga dispatch", 4)

    async def due_sagas(self, now_ms: int, except_sagas: Sequence[str], limit: int) -> list[str]:
        raise _refuse("saga dispatch", 4)

    async def next_pending(self, saga_id: str) -> SagaDispatchDocument | None:
        raise _refuse("saga dispatch", 4)

    async def mark_dispatched(self, entry_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _refuse("saga dispatch", 4)

    async def schedule_retry(self, entry_id: str, attempts: int, next_attempt_at_ms: int) -> None:
        raise _refuse("saga dispatch", 4)

    async def mark_failed(self, entry_id: str, failure: DispatchFailure) -> None:
        raise _refuse("saga dispatch", 4)

    async def claim_due_notice(self, now_ms: int, reserve_until_ms: int) -> SagaDispatchDocument | None:
        raise _refuse("saga dispatch", 4)

    async def clear_notice(self, entry_id: str) -> None:
        raise _refuse("saga dispatch", 4)

    async def schedule_notice_retry(self, entry_id: str, notice_attempts: int, next_at_ms: int) -> None:
        raise _refuse("saga dispatch", 4)

    async def claim_lease(self, saga_id: str, owner: str, now_ms: int, until_ms: int) -> bool:
        raise _refuse("saga dispatch", 4)

    async def renew_lease(self, saga_id: str, owner: str, until_ms: int) -> bool:
        raise _refuse("saga dispatch", 4)

    async def release_lease(self, saga_id: str, owner: str) -> None:
        raise _refuse("saga dispatch", 4)

    async def pending(self) -> int:
        raise _refuse("saga dispatch", 4)


class _Deadlines:
    """The deadlines, implemented from level 4."""

    async def insert(self, deadline: DeadlineDocument, *, tx: Tx | None = None) -> None:
        raise _refuse("deadlines", 4)

    async def find_scheduled(self, deadline_id: str, *, tx: Tx | None = None) -> DeadlineDocument | None:
        raise _refuse("deadlines", 4)

    async def cancel(self, saga_id: str, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _refuse("deadlines", 4)

    async def cancel_by_name(self, saga_id: str, name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _refuse("deadlines", 4)

    async def cancel_all(self, saga_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _refuse("deadlines", 4)

    async def mark_fired(self, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> bool:
        raise _refuse("deadlines", 4)

    async def claim_due(self, owner: str, now_ms: int, claimed_until_ms: int) -> DeadlineDocument | None:
        raise _refuse("deadlines", 4)

    async def count_overdue_by_name(self, now_ms: int) -> dict[str, int]:
        raise _refuse("deadlines", 4)


class _Failures:
    """The parked saga failures, implemented from level 4."""

    async def find(self, failure_id: str, *, tx: Tx | None = None) -> SagaFailureDocument | None:
        raise _refuse("saga failures", 4)

    async def save(self, failure: SagaFailureDocument, *, tx: Tx | None = None) -> None:
        raise _refuse("saga failures", 4)

    async def delete(self, failure_id: str, *, tx: Tx | None = None) -> bool:
        raise _refuse("saga failures", 4)

    async def failed_again(
        self, failure_id: str, error: str, attempts: int, now_ms: int, *, tx: Tx | None = None
    ) -> bool:
        raise _refuse("saga failures", 4)

    async def list(self, saga_type: str | None = None, *, tx: Tx | None = None) -> builtins.list[SagaFailureDocument]:
        raise _refuse("saga failures", 4)


class _Markers:
    """The saga stage's markers, implemented from level 4."""

    async def exists(self, marker_id: str, *, tx: Tx | None = None) -> bool:
        raise _refuse("saga markers", 4)

    async def insert(self, marker_id: str, type_name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _refuse("saga markers", 4)
