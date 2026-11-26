"""The ledger: handled deliveries, duplicate deliveries, and the marker a duplicate found."""

from __future__ import annotations

from snodra import ProcessedMarker
from store_interop.ledger import Ledger
from store_interop.messages import OrderPlaced
from store_interop.settings import POSTGRES, marker_id, processor_name, source_name


def marker(event_id: str) -> ProcessedMarker:
    return ProcessedMarker(
        id=marker_id(POSTGRES, event_id),
        type_name="orders.OrderPlaced",
        processed_at_ms=1,
        processor=processor_name(POSTGRES),
        source=source_name("run42", POSTGRES),
        segment="0",
        position="7",
    )


def event(event_id: str) -> OrderPlaced:
    return OrderPlaced(
        id=event_id,
        run="run42",
        publisher="go",
        family=POSTGRES,
        key="k-1",
        seq=3,
    )


def test_handled_and_duplicate_deliveries() -> None:
    ledger = Ledger()
    ledger.observe_marker(POSTGRES, marker("event-1"), True)
    ledger.observe_handled(event("event-1"))
    ledger.observe_marker(POSTGRES, marker("event-1"), False)
    deliveries, handled, duplicates = ledger.snapshot()
    assert (handled, duplicates) == (1, 1)
    assert [(d.outcome, d.publisher, d.key) for d in deliveries] == [
        ("handled", "go", "k-1"),
        ("duplicate", None, None),
    ]
    assert deliveries[0].segment == "0"
    assert deliveries[1].position == "7"


def test_another_processors_marker_is_not_observed() -> None:
    ledger = Ledger()
    ledger.observe_marker(
        POSTGRES,
        ProcessedMarker(id="processor:other:event-1", type_name="x", processed_at_ms=1),
        True,
    )
    deliveries, handled, duplicates = ledger.snapshot()
    assert (deliveries, handled, duplicates) == ([], 0, 0)
