"""What the probe's consumer did, in memory.

The stage-1 marker records come from the observed processed port; the handled events come from the
event handler. The real state is the stores: the driver asserts the markers through each instance's
``processed.exists``.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass

from snodra import ProcessedMarker

from store_interop.messages import Delivery, OrderPlaced
from store_interop.settings import processor_name


@dataclass(frozen=True, slots=True)
class _Marker:
    event_id: str
    segment: str
    position: str
    duplicate: bool


class TxRecord:
    """What one transaction writes: the stage-1 markers and the handlers' events.

    The observed store's transaction wrapper binds it while the consumer's transaction runs and
    commits it to the ledger only after the library's transaction committed.
    """

    def __init__(self) -> None:
        self.markers: list[tuple[str, ProcessedMarker, bool]] = []
        self.handled: dict[str, OrderPlaced] = {}


_CURRENT: ContextVar[TxRecord | None] = ContextVar("store_interop_transaction", default=None)


def current_record() -> TxRecord | None:
    """The record of the transaction this task is in, or None."""
    return _CURRENT.get()


def bind_record(record: TxRecord) -> Token[TxRecord | None]:
    """Binds a transaction's record to the current task; the token restores the previous one."""
    return _CURRENT.set(record)


def reset_record(token: Token[TxRecord | None]) -> None:
    """Restores the record a transaction replaced (in a finally)."""
    _CURRENT.reset(token)


class Ledger:
    """The deliveries of one probe instance."""

    def __init__(self) -> None:
        self._markers: list[_Marker] = []
        self._handled: dict[str, OrderPlaced] = {}

    def observe_marker(self, family: str, marker: ProcessedMarker, recorded: bool) -> None:
        """Record one processed port call, when it is this family's stage-1 marker.

        A new marker waits for its transaction to commit; a duplicate is not committed (the consumer rolls back)
        and is recorded at once, as the delivery the other language skipped.
        """
        record = current_record()
        if recorded and record is not None:
            record.markers.append((family, marker, recorded))
            return
        self._append_marker(family, marker, recorded)

    def _append_marker(self, family: str, marker: ProcessedMarker, recorded: bool) -> None:
        prefix = f"processor:{processor_name(family)}:"
        if not marker.id.startswith(prefix):
            return
        self._markers.append(
            _Marker(
                event_id=marker.id.removeprefix(prefix),
                segment=marker.segment or "",
                position=marker.position or "",
                duplicate=not recorded,
            )
        )

    def observe_handled(self, event: OrderPlaced) -> None:
        """Record one event handler run: in the current transaction, or at once outside one."""
        record = current_record()
        if record is not None:
            record.handled[event.id] = event
            return
        self._handled[event.id] = event

    def commit(self, record: TxRecord) -> None:
        """Move a committed transaction's records into the ledger."""
        for family, marker, recorded in record.markers:
            self._append_marker(family, marker, recorded)
        self._handled.update(record.handled)

    def snapshot(self) -> tuple[list[Delivery], int, int]:
        """The deliveries as the driver reads them, and the counts of handled and duplicate ones."""
        deliveries: list[Delivery] = []
        handled = duplicates = 0
        for marker in self._markers:
            if marker.duplicate:
                deliveries.append(
                    Delivery(
                        id=marker.event_id,
                        segment=marker.segment,
                        position=marker.position,
                        outcome="duplicate",
                    )
                )
                duplicates += 1
                continue
            event = self._handled.get(marker.event_id)
            deliveries.append(
                Delivery(
                    id=marker.event_id,
                    segment=marker.segment,
                    position=marker.position,
                    outcome="handled",
                    publisher=None if event is None else event.publisher,
                    key=None if event is None else event.key,
                )
            )
            handled += 1
        return deliveries, handled, duplicates
