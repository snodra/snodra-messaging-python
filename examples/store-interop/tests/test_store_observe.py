"""The observed store: the add-on's processed port answers a duplicate, the ledger sees it."""

from __future__ import annotations

import pytest
import snodra
from store_interop.ledger import Ledger
from store_interop.messages import OrderPlaced
from store_interop.observe import ObservedStore
from store_interop.settings import POSTGRES, marker_id, processor_name


async def test_the_observed_processed_port_tells_handled_from_duplicate() -> None:
    inner = snodra.MemoryStore()
    ledger = Ledger()
    store = ObservedStore(inner, POSTGRES, ledger)
    marker = snodra.ProcessedMarker(
        id=marker_id(POSTGRES, "event-1"),
        type_name="orders.OrderPlaced",
        processed_at_ms=1,
        processor=processor_name(POSTGRES),
        source="snodra.interop.run42.postgres",
        segment="0",
        position="7",
    )
    assert await store.processed.record(marker) is True
    assert await store.processed.record(marker) is False
    assert await store.processed.exists(marker.id) is True
    assert store.name == "memory"
    ledger.observe_handled(
        OrderPlaced(id="event-1", run="run42", publisher="python", family=POSTGRES, key="k-1", seq=1)
    )
    deliveries, handled, duplicates = ledger.snapshot()
    assert (handled, duplicates) == (1, 1)
    assert [d.outcome for d in deliveries] == ["handled", "duplicate"]


async def test_the_stores_lifecycle_is_not_the_observers() -> None:
    inner = snodra.MemoryStore()
    store = ObservedStore(inner, POSTGRES, Ledger())
    store.use_clock(lambda: 1)  # the in-memory store takes no clock: nothing to delegate
    await store.init_schema()
    await store.check_schema()
    assert store.outbox is inner.outbox
    assert store.sagas is inner.sagas


async def test_the_ledger_shows_only_committed_transactions() -> None:
    inner = snodra.MemoryStore()
    ledger = Ledger()
    store = ObservedStore(inner, POSTGRES, ledger)
    marker = snodra.ProcessedMarker(
        id=marker_id(POSTGRES, "event-1"),
        type_name="orders.OrderPlaced",
        processed_at_ms=1,
        processor=processor_name(POSTGRES),
        source="snodra.interop.run42.postgres",
        segment="0",
        position="7",
    )
    async with store.transaction():
        assert await store.processed.record(marker) is True
        ledger.observe_handled(
            OrderPlaced(id="event-1", run="run42", publisher="python", family=POSTGRES, key="k-1", seq=1)
        )
        assert ledger.snapshot() == ([], 0, 0)
    deliveries, handled, duplicates = ledger.snapshot()
    assert (handled, duplicates) == (1, 0)
    assert [(d.outcome, d.publisher, d.key) for d in deliveries] == [("handled", "python", "k-1")]

    rolled_back = snodra.ProcessedMarker(
        id=marker_id(POSTGRES, "event-2"),
        type_name="orders.OrderPlaced",
        processed_at_ms=1,
        processor=processor_name(POSTGRES),
        source="snodra.interop.run42.postgres",
        segment="1",
        position="8",
    )

    async def roll_back() -> None:
        async with store.transaction():
            assert await store.processed.record(rolled_back) is True
            ledger.observe_handled(
                OrderPlaced(id="event-2", run="run42", publisher="python", family=POSTGRES, key="k-2", seq=2)
            )
            raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        await roll_back()
    deliveries, handled, duplicates = ledger.snapshot()
    assert (handled, duplicates) == (1, 0)
    assert [(d.id, d.outcome) for d in deliveries] == [("event-1", "handled")]
