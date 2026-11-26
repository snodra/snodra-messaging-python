"""The in-memory message store: the level-2 stores of the message-store contract inside one process, for tests, local
runs and single-process demos, with no database.

- **Real transactions.** What a transaction writes is seen by others only at its commit and is gone after a rollback;
  a transaction reads its own writes. A transaction reserves every record it writes: a second live transaction writing
  one of them is refused at once with :class:`~snodra._store.Conflict`, and a commit fails when a record it wrote was
  changed meanwhile (a write outside any transaction). What runs outside a transaction commits at once.
- **The outbox and the processed events** implement their level-2 contract: atomic claims under a lease in
  ``(created_at_ms, seq)`` order, pending counts, purge of what was sent, payload refusal and one marker per id, plus
  the wake-up of the relay after every committed append.
- **The level-3 and level-4 ports** (dead letters, processor control, external calls, sagas) are present and report
  :class:`~snodra._store.Unsupported`; an official add-on implements them by level 4.
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
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from typing import Final

from snodra._eventstore import ProcessedMarker, ReplayRange
from snodra._store import (
    AppendListener,
    Conflict,
    DeadLetter,
    DeadLetterError,
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
    check_outbox_payload,
    current_transaction,
    now_ms,
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
    reserved: dict[tuple[str, str], int] = field(default_factory=dict)
    version: int = 0
    next_tx: int = 1


@dataclass(slots=True)
class _MemoryTx:
    """One transaction's writes, kept apart until its commit."""

    store_id: int
    number: int
    ended: bool = False
    doomed: bool = False
    reserved: list[tuple[str, str]] = field(default_factory=list)
    outbox: _Overlay[_OutboxRow] = field(default_factory=_Overlay)
    processed: _Overlay[_ProcessedRow] = field(default_factory=_Overlay)


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
        self.dead_letters: DeadLetterStore = _DeadLetters()
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
        return Tx(_MemoryTx(self._store_id, number))

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
            self._apply(inner)
            committed = True
        finally:
            self._release(inner)
        if committed and inner.outbox.writes:
            self._wake()

    async def rollback(self, tx: Tx) -> None:
        inner = self._open(tx)
        if inner is None:
            return
        inner.ended = True
        inner.outbox.writes.clear()
        inner.processed.writes.clear()
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
        value: T,
    ) -> None:
        """Writes ``value`` as ``key``, committed at once or in ``tx`` (reserving the record)."""
        if tx is None or overlay is None:
            self._data.version += 1
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
        for wake in list(self._listeners.values()):
            with contextlib.suppress(Exception):
                wake()


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
        raise _refuse("delete_markers", 3)

    async def purge(self, before_ms: int) -> int:
        return self._store.purge_processed(before_ms)


def _refuse(feature: str, level: int) -> Unsupported:
    return Unsupported(f"{feature} in the in-memory message store (level {level})")


class _DeadLetters:
    """The dead letters, implemented from level 3."""

    async def park(self, letter: DeadLetter) -> Parked:
        raise _refuse("dead_letters", 3)

    async def head(self, processor: str, key: str) -> str | None:
        raise _refuse("dead_letters", 3)

    async def earlier(self, processor: str, key: str, position: str) -> str | None:
        raise _refuse("dead_letters", 3)

    async def parked_keys(self, processor: str, source: str, segments: Sequence[str]) -> set[str]:
        raise _refuse("dead_letters", 3)

    async def get(self, letter: str) -> DeadLetter | None:
        raise _refuse("dead_letters", 3)

    async def list(self, processor: str | None = None, limit: int | None = None) -> builtins.list[DeadLetter]:
        raise _refuse("dead_letters", 3)

    async def summaries(self, processor: str) -> builtins.list[DeadLetterSummary]:
        raise _refuse("dead_letters", 3)

    async def count(self, processor: str | None = None) -> int:
        raise _refuse("dead_letters", 3)

    async def delete(self, letter: str) -> bool:
        raise _refuse("dead_letters", 3)

    async def delete_in(self, letter: str, *, tx: Tx | None = None) -> bool:
        raise _refuse("dead_letters", 3)

    async def record_failure(self, letter: str, error: DeadLetterError, now_ms: int) -> bool:
        raise _refuse("dead_letters", 3)


class _Control:
    """The processor control state, implemented from level 3."""

    async def get(self, processor: str) -> ProcessorControlState:
        raise _refuse("processor control", 3)

    async def set_paused(self, processor: str, paused: bool, by: str, now_ms: int) -> None:
        raise _refuse("processor control", 3)

    async def lock(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> Locked:
        raise _refuse("processor control", 3)

    async def extend(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> bool:
        raise _refuse("processor control", 3)

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        raise _refuse("processor control", 3)

    async def expire(self, processor: str, operation: str, now_ms: int) -> bool:
        raise _refuse("processor control", 3)

    async def set_replay(self, processor: str, ranges: Sequence[ReplayRange], now_ms: int) -> None:
        raise _refuse("processor control", 3)

    async def remove_range(self, processor: str, replay_range: ReplayRange, now_ms: int) -> bool:
        raise _refuse("processor control", 3)


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
