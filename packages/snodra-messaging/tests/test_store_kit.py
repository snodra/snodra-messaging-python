"""The message-store conformance kit: the nine level-2 checks on the memory store, its list and its imports."""

from __future__ import annotations

from pathlib import Path

import pytest
from snodra._eventstore_memory import MemoryEventStore
from snodra._store import Tx
from snodra._store_memory import MemoryStore
from snodra.testing import (
    MESSAGE_STORE_CHECKS,
    EventStoreKit,
    StoreCheck,
    redelivered_after_commit_is_skipped,
    run_store_conformance,
)

KIT = Path(__file__).resolve().parents[1] / "src" / "snodra" / "testing"

# The reference kit's ids are authoritative: this literal copies the nine level-2 message-store checks, in order, from
# `src/store/conformance.rs` (the level-3 `delete_markers_removes_only_the_range` is omitted). A missing, renamed or
# reordered check here fails the test.
RUST_STORE_CHECKS = (
    "appended_entry_is_claimed_in_created_seq_order",
    "outbox_rejects_a_payload_that_is_not_a_json_object",
    "pending_counts_entries_neither_sent_nor_failed",
    "rolled_back_transaction_leaves_no_outbox_entry",
    "rolled_back_handler_leaves_no_processed_marker",
    "processed_marker_records_once",
    "concurrent_relays_publish_each_entry_once",
    "expired_lease_is_reclaimed",
    "purge_removes_only_sent_before",
)


@pytest.mark.parametrize(
    "check",
    [pytest.param(check, id=check.__name__) for check in MESSAGE_STORE_CHECKS],
)
async def test_each_check_passes_on_the_memory_store(check: StoreCheck) -> None:
    await check(MemoryStore())


async def test_the_runner_runs_every_check_in_order() -> None:
    await run_store_conformance(MemoryStore)


class BrokenMemoryStore(MemoryStore):
    """A store whose transactions cannot begin, for the runner's failure naming."""

    async def begin(self) -> Tx:
        raise RuntimeError("broken for the test")


async def test_the_runner_names_a_failing_check_and_the_store() -> None:
    with pytest.raises(AssertionError, match=r"appended_entry_is_claimed_in_created_seq_order.*memory"):
        await run_store_conformance(BrokenMemoryStore)


@pytest.mark.repo
def test_the_checked_in_store_check_list_is_the_reference_list() -> None:
    listed = (KIT / "store_checks.txt").read_text(encoding="utf-8").splitlines()
    assert listed == list(RUST_STORE_CHECKS)
    assert [check.__name__ for check in MESSAGE_STORE_CHECKS] == list(RUST_STORE_CHECKS)
    assert len(MESSAGE_STORE_CHECKS) == 9


async def test_the_event_store_kit_records_its_marker_through_the_memory_store() -> None:
    """The message-store hook of the event-store kit: ``store.processed.record(marker)``, outside a transaction."""
    events = MemoryEventStore()
    store = MemoryStore()
    await redelivered_after_commit_is_skipped(EventStoreKit("conformance.events", lambda: events, store))
