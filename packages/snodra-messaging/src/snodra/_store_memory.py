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
    Association,
    Conflict,
    ControlOperation,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    DeadLetterStore,
    DeadLetterSummary,
    DeadlineDocument,
    DeadlineStatus,
    DispatchFailure,
    DispatchKind,
    ExternalCall,
    ExternalCallStatus,
    ExternalCallStore,
    Failed,
    InvalidMessage,
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
class _Lease:
    """One saga dispatch lease."""

    owner: str
    until_ms: int


@dataclass(slots=True)
class _Data:
    """Everything the store holds, committed."""

    outbox: _Table[_OutboxRow] = field(default_factory=_Table)
    processed: _Table[_ProcessedRow] = field(default_factory=_Table)
    dead: _Table[DeadLetter] = field(default_factory=_Table)
    external: _Table[ExternalCall] = field(default_factory=_Table)
    sagas: _Table[SagaDocument] = field(default_factory=_Table)
    dispatch: _Table[SagaDispatchDocument] = field(default_factory=_Table)
    deadlines: _Table[DeadlineDocument] = field(default_factory=_Table)
    failures: _Table[SagaFailureDocument] = field(default_factory=_Table)
    leases: _Table[_Lease] = field(default_factory=_Table)
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
    sagas: _Overlay[SagaDocument] = field(default_factory=_Overlay)
    dispatch: _Overlay[SagaDispatchDocument] = field(default_factory=_Overlay)
    deadlines: _Overlay[DeadlineDocument] = field(default_factory=_Overlay)
    failures: _Overlay[SagaFailureDocument] = field(default_factory=_Overlay)

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
        self.external: ExternalCallStore = _External(self)
        self.sagas = SagaStores(
            sagas=_Sagas(self),
            dispatch=_Dispatch(self),
            deadlines=_Deadlines(self),
            failures=_Failures(self),
            markers=_Markers(self),
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
            self._check_stale("sagas", self._data.sagas, inner.sagas)
            self._check_stale("dispatch", self._data.dispatch, inner.dispatch)
            self._check_stale("deadlines", self._data.deadlines, inner.deadlines)
            self._check_stale("failures", self._data.failures, inner.failures)
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
        inner.sagas.writes.clear()
        inner.dispatch.writes.clear()
        inner.deadlines.writes.clear()
        inner.failures.writes.clear()
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
        if not owner:
            # TODO(spec-gap): SG-31
            raise InvalidMessage("the in-memory message store needs a non-empty claim owner")
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
        tx.sagas.writes.clear()
        tx.dispatch.writes.clear()
        tx.deadlines.writes.clear()
        tx.failures.writes.clear()
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
        self._apply_table(self._data.sagas, tx.sagas)
        self._apply_table(self._data.dispatch, tx.dispatch)
        self._apply_table(self._data.deadlines, tx.deadlines)
        self._apply_table(self._data.failures, tx.failures)

    def current_row[T](self, table: _Table[T], overlay: _Overlay[T], key: str, tx: Tx | None) -> T | None:
        """The row ``key`` as ``tx`` sees it (a committed row or the transaction's write)."""
        inner = self._open(current_transaction() if tx is None else tx)
        if inner is None:
            return table.rows.get(key)
        write = overlay.writes.get(key)
        if write is not None:
            return write.value
        return table.rows.get(key)

    def current_rows[T](self, table: _Table[T], overlay: _Overlay[T], tx: Tx | None) -> dict[str, T]:
        """Every row as ``tx`` sees them: committed rows with the transaction's writes applied."""
        inner = self._open(current_transaction() if tx is None else tx)
        if inner is None:
            return dict(table.rows)
        rows = dict(table.rows)
        for key, write in overlay.writes.items():
            if write.value is None:
                rows.pop(key, None)
            else:
                rows[key] = write.value
        return rows

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
        row = self._rows().get(letter)
        return None if row is None else _copy_letter(row)

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
    """The external-call records: every write commits on its own, outside any transaction."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    @property
    def _rows(self) -> dict[str, ExternalCall]:
        return self._store._data.external.rows

    async def insert(self, call: ExternalCall) -> bool:
        if call.key in self._rows:
            return False
        self._store._write("external", self._store._data.external, None, None, call.key, call)
        return True

    async def find(self, key: str) -> ExternalCall | None:
        return self._rows.get(key)

    async def take_over(self, seen: ExternalCall, owner: str, lease_until_ms: int, now_ms: int) -> ExternalCall | None:
        if self._rows.get(seen.key) != seen:
            return None
        taken = replace(
            seen, owner=owner, lease_until_ms=lease_until_ms, updated_at_ms=now_ms, attempts=seen.attempts + 1
        )
        self._store._write("external", self._store._data.external, None, None, seen.key, taken)
        return taken

    async def expire(self, seen: ExternalCall, now_ms: int) -> bool:
        if self._rows.get(seen.key) != seen:
            return False
        self._store._write(
            "external",
            self._store._data.external,
            None,
            None,
            seen.key,
            replace(seen, status=ExternalCallStatus.UNKNOWN, owner=None, lease_until_ms=None, updated_at_ms=now_ms),
        )
        return True

    async def succeed(self, key: str, owner: str, reply: str, now_ms: int) -> bool:
        current = self._rows.get(key)
        if current is None or current.owner != owner or current.status is not ExternalCallStatus.IN_PROGRESS:
            return False
        self._store._write(
            "external",
            self._store._data.external,
            None,
            None,
            key,
            replace(
                current,
                status=ExternalCallStatus.SUCCEEDED,
                reply=reply,
                error=None,
                owner=None,
                lease_until_ms=None,
                updated_at_ms=now_ms,
            ),
        )
        return True

    async def fail(self, key: str, owner: str, error: str, now_ms: int) -> bool:
        current = self._rows.get(key)
        if current is None or current.owner != owner or current.status is not ExternalCallStatus.IN_PROGRESS:
            return False
        self._store._write(
            "external",
            self._store._data.external,
            None,
            None,
            key,
            replace(
                current,
                status=ExternalCallStatus.FAILED,
                error=error,
                owner=None,
                lease_until_ms=None,
                updated_at_ms=now_ms,
            ),
        )
        return True

    async def mark_unknown(self, key: str, owner: str, now_ms: int) -> bool:
        current = self._rows.get(key)
        if current is None or current.owner != owner or current.status is not ExternalCallStatus.IN_PROGRESS:
            return False
        self._store._write(
            "external",
            self._store._data.external,
            None,
            None,
            key,
            replace(
                current,
                status=ExternalCallStatus.UNKNOWN,
                owner=None,
                lease_until_ms=None,
                updated_at_ms=now_ms,
            ),
        )
        return True

    async def reset(self, key: str, owner: str) -> bool:
        current = self._rows.get(key)
        if current is None or current.owner != owner or current.status is not ExternalCallStatus.IN_PROGRESS:
            return False
        self._store._write("external", self._store._data.external, None, None, key, None)
        return True


class _Sagas:
    """The saga instances; a write joins the store's transaction."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    def _write(self, tx: Tx | None) -> _MemoryTx | None:
        return self._store.resolve(tx)

    def _rows(self, tx: Tx | None) -> dict[str, SagaDocument]:
        inner = self._write(tx)
        overlay = _EMPTY_SAGAS if inner is None else inner.sagas
        return self._store.current_rows(self._store._data.sagas, overlay, tx)

    async def find_by_id(self, saga_id: str, *, tx: Tx | None = None) -> SagaDocument | None:
        return self._rows(tx).get(saga_id)

    async def find_live(self, type_name: str, key: str, value: str, *, tx: Tx | None = None) -> list[SagaDocument]:
        live = [
            saga
            for saga in self._rows(tx).values()
            if not saga.ended and saga.type_name == type_name and Association(key, value) in saga.associations
        ]
        live.sort(key=lambda saga: (saga.created_at_ms, saga.id))
        return live

    def _start_key_free(self, saga: SagaDocument, tx: Tx | None) -> bool:
        if saga.start_key is None or saga.ended:
            return True
        for other in self._rows(tx).values():
            if other.id != saga.id and not other.ended and other.start_key == saga.start_key:
                return False
        return True

    async def insert(self, saga: SagaDocument, *, tx: Tx | None = None) -> None:
        inner = self._write(tx)
        if saga.id in self._rows(tx):
            raise Conflict(f"duplicate key: a saga {saga.id} exists")
        if not self._start_key_free(saga, tx):
            raise Conflict(f"a live saga holds the start key {saga.start_key}")
        if inner is not None and saga.start_key is not None and not saga.ended:
            self._store._reserve(inner, "saga-start", saga.start_key)
        self._store._write(
            "sagas", self._store._data.sagas, None if inner is None else inner.sagas, inner, saga.id, saga
        )

    async def update(self, saga: SagaDocument, expected_version: int, *, tx: Tx | None = None) -> bool:
        current = self._rows(tx).get(saga.id)
        if current is None or current.version != expected_version:
            return False
        inner = self._write(tx)
        if not self._start_key_free(saga, tx):
            raise Conflict(f"a live saga holds the start key {saga.start_key}")
        if inner is not None and saga.start_key is not None and not saga.ended and current.start_key != saga.start_key:
            self._store._reserve(inner, "saga-start", saga.start_key)
        self._store._write(
            "sagas", self._store._data.sagas, None if inner is None else inner.sagas, inner, saga.id, saga
        )
        return True

    async def count_live_by_type(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for saga in self._store._data.sagas.rows.values():
            if not saga.ended:
                counts[saga.type_name] = counts.get(saga.type_name, 0) + 1
        return counts


class _Dispatch:
    """The saga-side outbox and its leases."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    def _rows(self, tx: Tx | None = None) -> dict[str, SagaDispatchDocument]:
        inner = self._store.resolve(tx)
        overlay = _EMPTY_DISPATCH if inner is None else inner.dispatch
        return self._store.current_rows(self._store._data.dispatch, overlay, tx)

    @staticmethod
    def _pending(entry: SagaDispatchDocument) -> bool:
        return entry.dispatched_at_ms is None and entry.failed_at_ms is None

    async def record(self, entries: Sequence[SagaDispatchDocument], *, tx: Tx | None = None) -> None:
        for entry in entries:
            if entry.kind is DispatchKind.EVENT:
                check_outbox_payload(entry.type_name, entry.payload.encode())
        inner = self._store.resolve(tx)
        current = self._rows(tx)
        for entry in entries:
            if entry.id in current:
                raise Conflict(f"duplicate key: a saga-dispatch entry {entry.id} exists")
        for entry in entries:
            self._store._write(
                "dispatch",
                self._store._data.dispatch,
                None if inner is None else inner.dispatch,
                inner,
                entry.id,
                entry,
            )

    def _committed(self) -> dict[str, SagaDispatchDocument]:
        return self._store._data.dispatch.rows

    async def due_sagas(self, now_ms: int, except_sagas: Sequence[str], limit: int) -> list[str]:
        first: dict[str, SagaDispatchDocument] = {}
        for entry in self._committed().values():
            if not self._pending(entry) or entry.saga_id in except_sagas:
                continue
            found = first.get(entry.saga_id)
            if found is None or entry.seq < found.seq:
                first[entry.saga_id] = entry
        due = [
            entry for entry in first.values() if entry.next_attempt_at_ms is None or entry.next_attempt_at_ms <= now_ms
        ]
        due.sort(key=lambda entry: (entry.created_at_ms, entry.saga_id))
        return [entry.saga_id for entry in due[: max(0, limit)]]

    async def next_pending(self, saga_id: str) -> SagaDispatchDocument | None:
        pending = [entry for entry in self._committed().values() if entry.saga_id == saga_id and self._pending(entry)]
        return min(pending, key=lambda entry: entry.seq) if pending else None

    async def mark_dispatched(self, entry_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        current = self._rows(tx).get(entry_id)
        if current is None:
            return
        inner = self._store.resolve(tx)
        self._store._write(
            "dispatch",
            self._store._data.dispatch,
            None if inner is None else inner.dispatch,
            inner,
            entry_id,
            replace(current, dispatched_at_ms=now_ms),
        )

    async def schedule_retry(self, entry_id: str, attempts: int, next_attempt_at_ms: int) -> None:
        current = self._committed().get(entry_id)
        if current is not None:
            self._store._write(
                "dispatch",
                self._store._data.dispatch,
                None,
                None,
                entry_id,
                replace(current, attempts=attempts, next_attempt_at_ms=next_attempt_at_ms),
            )

    async def mark_failed(self, entry_id: str, failure: DispatchFailure) -> None:
        current = self._committed().get(entry_id)
        if current is None:
            return
        notice = failure.notice_at_ms is not None
        self._store._write(
            "dispatch",
            self._store._data.dispatch,
            None,
            None,
            entry_id,
            replace(
                current,
                attempts=current.attempts if failure.attempts is None else failure.attempts,
                failed_at_ms=failure.failed_at_ms,
                failure_reason=failure.reason,
                failure_message=failure.message,
                notice_pending=True if notice else None,
                notice_attempts=0 if notice else None,
                notice_next_at_ms=failure.notice_at_ms,
            ),
        )

    async def claim_due_notice(self, now_ms: int, reserve_until_ms: int) -> SagaDispatchDocument | None:
        due = [
            entry
            for entry in self._committed().values()
            if entry.notice_pending and (entry.notice_next_at_ms is None or entry.notice_next_at_ms <= now_ms)
        ]
        if not due:
            return None
        due.sort(
            key=lambda entry: (0 if entry.notice_next_at_ms is None else 1, entry.notice_next_at_ms or 0, entry.id)
        )
        claimed = replace(due[0], notice_next_at_ms=reserve_until_ms)
        self._store._write("dispatch", self._store._data.dispatch, None, None, claimed.id, claimed)
        return claimed

    async def clear_notice(self, entry_id: str) -> None:
        current = self._committed().get(entry_id)
        if current is not None:
            self._store._write(
                "dispatch",
                self._store._data.dispatch,
                None,
                None,
                entry_id,
                replace(current, notice_pending=None, notice_attempts=None, notice_next_at_ms=None),
            )

    async def schedule_notice_retry(self, entry_id: str, notice_attempts: int, next_at_ms: int) -> None:
        current = self._committed().get(entry_id)
        if current is not None:
            self._store._write(
                "dispatch",
                self._store._data.dispatch,
                None,
                None,
                entry_id,
                replace(current, notice_attempts=notice_attempts, notice_next_at_ms=next_at_ms),
            )

    async def claim_lease(self, saga_id: str, owner: str, now_ms: int, until_ms: int) -> bool:
        current = self._store._data.leases.rows.get(saga_id)
        if current is not None and current.until_ms >= now_ms:
            return False
        self._store._write("lease", self._store._data.leases, None, None, saga_id, _Lease(owner, until_ms))
        return True

    async def renew_lease(self, saga_id: str, owner: str, until_ms: int) -> bool:
        current = self._store._data.leases.rows.get(saga_id)
        if current is None or current.owner != owner:
            return False
        self._store._write("lease", self._store._data.leases, None, None, saga_id, _Lease(owner, until_ms))
        return True

    async def release_lease(self, saga_id: str, owner: str) -> None:
        current = self._store._data.leases.rows.get(saga_id)
        if current is not None and current.owner == owner:
            self._store._write("lease", self._store._data.leases, None, None, saga_id, None)

    async def pending(self) -> int:
        return sum(1 for entry in self._committed().values() if self._pending(entry))


class _Deadlines:
    """The saga deadlines."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    @staticmethod
    def _clean(deadline: DeadlineDocument) -> DeadlineDocument:
        return replace(deadline, correlation_id=deadline.correlation_id or None)

    def _rows(self, tx: Tx | None = None) -> dict[str, DeadlineDocument]:
        inner = self._store.resolve(tx)
        overlay = _EMPTY_DEADLINES if inner is None else inner.deadlines
        return self._store.current_rows(self._store._data.deadlines, overlay, tx)

    async def insert(self, deadline: DeadlineDocument, *, tx: Tx | None = None) -> None:
        if deadline.id in self._rows(tx):
            raise Conflict(f"duplicate key: a deadline {deadline.id} exists")
        inner = self._store.resolve(tx)
        self._store._write(
            "deadlines",
            self._store._data.deadlines,
            None if inner is None else inner.deadlines,
            inner,
            deadline.id,
            self._clean(deadline),
        )

    async def find_scheduled(self, deadline_id: str, *, tx: Tx | None = None) -> DeadlineDocument | None:
        found = self._rows(tx).get(deadline_id)
        return found if found is not None and found.status is DeadlineStatus.SCHEDULED else None

    async def cancel(self, saga_id: str, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        self._cancel(lambda deadline: deadline.id == deadline_id and deadline.saga_id == saga_id, now_ms, tx)

    async def cancel_by_name(self, saga_id: str, name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        self._cancel(lambda deadline: deadline.saga_id == saga_id and deadline.name == name, now_ms, tx)

    async def cancel_all(self, saga_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        self._cancel(lambda deadline: deadline.saga_id == saga_id, now_ms, tx)

    def _cancel(self, matches: Callable[[DeadlineDocument], bool], now_ms: int, tx: Tx | None) -> None:
        inner = self._store.resolve(tx)
        for deadline in self._rows(tx).values():
            if matches(deadline) and deadline.status is DeadlineStatus.SCHEDULED:
                self._store._write(
                    "deadlines",
                    self._store._data.deadlines,
                    None if inner is None else inner.deadlines,
                    inner,
                    deadline.id,
                    replace(deadline, status=DeadlineStatus.CANCELLED, finished_at_ms=now_ms),
                )

    async def mark_fired(self, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> bool:
        current = self._rows(tx).get(deadline_id)
        if current is None or current.status is not DeadlineStatus.SCHEDULED:
            return False
        inner = self._store.resolve(tx)
        self._store._write(
            "deadlines",
            self._store._data.deadlines,
            None if inner is None else inner.deadlines,
            inner,
            deadline_id,
            replace(current, status=DeadlineStatus.FIRED, finished_at_ms=now_ms),
        )
        return True

    async def claim_due(self, owner: str, now_ms: int, claimed_until_ms: int) -> DeadlineDocument | None:
        due = [
            deadline
            for deadline in self._store._data.deadlines.rows.values()
            if deadline.status is DeadlineStatus.SCHEDULED
            and deadline.due_at_ms <= now_ms
            and (deadline.claimed_until_ms is None or deadline.claimed_until_ms < now_ms)
        ]
        if not due:
            return None
        due.sort(key=lambda deadline: (deadline.due_at_ms, deadline.id))
        claimed = self._clean(replace(due[0], claimed_by=owner, claimed_until_ms=claimed_until_ms))
        self._store._write("deadlines", self._store._data.deadlines, None, None, claimed.id, claimed)
        return claimed

    async def count_overdue_by_name(self, now_ms: int) -> dict[str, int]:
        counts: dict[str, int] = {}
        for deadline in self._store._data.deadlines.rows.values():
            if deadline.status is DeadlineStatus.SCHEDULED and deadline.due_at_ms <= now_ms:
                counts[deadline.name] = counts.get(deadline.name, 0) + 1
        return counts


class _Failures:
    """The parked saga events."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    def _rows(self, tx: Tx | None = None) -> dict[str, SagaFailureDocument]:
        inner = self._store.resolve(tx)
        overlay = _EMPTY_FAILURES if inner is None else inner.failures
        return self._store.current_rows(self._store._data.failures, overlay, tx)

    async def find(self, failure_id: str, *, tx: Tx | None = None) -> SagaFailureDocument | None:
        return self._rows(tx).get(failure_id)

    async def save(self, failure: SagaFailureDocument, *, tx: Tx | None = None) -> None:
        inner = self._store.resolve(tx)
        self._store._write(
            "failures",
            self._store._data.failures,
            None if inner is None else inner.failures,
            inner,
            failure.id,
            failure,
        )

    async def delete(self, failure_id: str, *, tx: Tx | None = None) -> bool:
        if failure_id not in self._rows(tx):
            return False
        inner = self._store.resolve(tx)
        self._store._write(
            "failures", self._store._data.failures, None if inner is None else inner.failures, inner, failure_id, None
        )
        return True

    async def failed_again(
        self, failure_id: str, error: str, attempts: int, now_ms: int, *, tx: Tx | None = None
    ) -> bool:
        current = self._rows(tx).get(failure_id)
        if current is None:
            return False
        inner = self._store.resolve(tx)
        self._store._write(
            "failures",
            self._store._data.failures,
            None if inner is None else inner.failures,
            inner,
            failure_id,
            replace(current, attempts=current.attempts + attempts, error=error, last_failed_at_ms=now_ms),
        )
        return True

    async def list(self, saga_type: str | None = None, *, tx: Tx | None = None) -> builtins.list[SagaFailureDocument]:
        found = [failure for failure in self._rows(tx).values() if saga_type is None or failure.saga_type == saga_type]
        found.sort(key=lambda failure: (failure.last_failed_at_ms, failure.id))
        return found


class _Markers:
    """The saga stage's markers: processed-event ids, in the same table as every other marker."""

    def __init__(self, store: MemoryStore) -> None:
        self._store = store

    def _overlay(self, tx: Tx | None) -> _Overlay[_ProcessedRow]:
        inner = self._store.resolve(tx)
        return _EMPTY_PROCESSED if inner is None else inner.processed

    def _exists(self, marker_id: str, tx: Tx | None) -> bool:
        return marker_id in self._store.current_rows(self._store._data.processed, self._overlay(tx), tx)

    async def exists(self, marker_id: str, *, tx: Tx | None = None) -> bool:
        return self._exists(marker_id, tx)

    async def insert(self, marker_id: str, type_name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        if self._exists(marker_id, tx):
            raise Conflict(f"duplicate key: a marker {marker_id} exists")
        inner = self._store.resolve(tx)
        self._store._write(
            "processed",
            self._store._data.processed,
            None if inner is None else inner.processed,
            inner,
            marker_id,
            _ProcessedRow(ProcessedMarker(id=marker_id, type_name=type_name, processed_at_ms=now_ms)),
        )


_EMPTY_SAGAS: _Overlay[SagaDocument] = _Overlay()
_EMPTY_DISPATCH: _Overlay[SagaDispatchDocument] = _Overlay()
_EMPTY_DEADLINES: _Overlay[DeadlineDocument] = _Overlay()
_EMPTY_FAILURES: _Overlay[SagaFailureDocument] = _Overlay()
_EMPTY_PROCESSED: _Overlay[_ProcessedRow] = _Overlay()
