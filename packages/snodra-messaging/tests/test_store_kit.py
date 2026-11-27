"""The message-store conformance kit: the nine level-2 and eight level-3 checks on the memory store, its lists and its
imports."""

from __future__ import annotations

from pathlib import Path

import pytest
from snodra._eventstore_memory import MemoryEventStore
from snodra._store import InvalidMessage, OutboxEntry, Tx, Unsupported
from snodra._store_memory import MemoryStore
from snodra.testing import (
    MESSAGE_STORE_CHECKS,
    MESSAGE_STORE_L3_CHECKS,
    EventStoreKit,
    StoreCheck,
    appended_entry_is_claimed_in_created_seq_order,
    expired_lease_is_reclaimed,
    redelivered_after_commit_is_skipped,
    run_store_conformance,
    run_store_l3_conformance,
)
from snodra.testing.store import _checks_for

KIT = Path(__file__).resolve().parents[1] / "src" / "snodra" / "testing"

# The reference kit's ids are authoritative: these literals copy the nine level-2 and eight level-3 message-store
# checks, in order, from `src/store/conformance.rs`. The Rust list places `delete_markers_removes_only_the_range`
# among the level-2 checks; `RUST_STORE_CHECKS` is the order the reference runs them in.
RUST_STORE_L2_CHECKS = (
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

RUST_STORE_L3_CHECKS = (
    "delete_markers_removes_only_the_range",
    "dead_letters_sequence_per_key",
    "dead_letter_parked_inside_rolled_back_transaction_is_kept",
    "dead_letter_deleted_in_a_transaction_goes_with_it",
    "control_lock_is_exclusive_until_lease_expires",
    "pause_state_round_trips",
    "replay_ranges_round_trip",
    "processor_control_write_inside_rolled_back_transaction_is_kept",
)

RUST_STORE_CHECKS = (
    *RUST_STORE_L2_CHECKS[:6],
    RUST_STORE_L3_CHECKS[0],
    *RUST_STORE_L2_CHECKS[6:],
    *RUST_STORE_L3_CHECKS[1:],
)


@pytest.mark.parametrize(
    "check",
    [pytest.param(check, id=check.__name__) for check in (*MESSAGE_STORE_CHECKS, *MESSAGE_STORE_L3_CHECKS)],
)
async def test_each_check_passes_on_the_memory_store(check: StoreCheck) -> None:
    await check(MemoryStore())


async def test_the_runner_runs_every_check_at_level_three() -> None:
    await run_store_conformance(MemoryStore, level=3)
    await run_store_l3_conformance(MemoryStore)


async def test_the_runner_runs_the_level_two_checks_by_default() -> None:
    await run_store_conformance(MemoryStore)


async def test_the_runner_refuses_a_level_it_does_not_know() -> None:
    with pytest.raises(ValueError, match="level 2 or 3"):
        await run_store_conformance(MemoryStore, level=4)


async def test_an_empty_claim_owner_is_refused() -> None:
    """Q19/SG-31: an empty owner cannot be told apart from nobody, so every store refuses it."""
    with pytest.raises(InvalidMessage, match="non-empty claim owner"):
        await MemoryStore().outbox.claim("", 60.0, 1)


class ReversedClaimStore(MemoryStore):
    """A store whose claims are newest-first: the order check must fail on it."""

    def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        return list(reversed(super().claim(owner, lease, limit)))


class LongLeaseStore(MemoryStore):
    """A store that grants every claim a long lease: the expired-lease check must fail on it."""

    def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        return super().claim(owner, max(lease, 3_600.0), limit)


async def test_the_order_check_bites_on_a_reversed_claim_store() -> None:
    with pytest.raises(AssertionError, match="the oldest two"):
        await appended_entry_is_claimed_in_created_seq_order(ReversedClaimStore())


async def test_the_expired_lease_check_bites_on_a_long_lease_store() -> None:
    with pytest.raises(AssertionError, match="reclaimed in order"):
        await expired_lease_is_reclaimed(LongLeaseStore())


class BrokenMemoryStore(MemoryStore):
    """A store whose transactions cannot begin, for the runner's failure naming."""

    async def begin(self) -> Tx:
        raise RuntimeError("broken for the test")


async def test_the_runner_names_a_failing_check_and_the_store() -> None:
    with pytest.raises(AssertionError, match=r"appended_entry_is_claimed_in_created_seq_order.*memory"):
        await run_store_conformance(BrokenMemoryStore, level=3)


class UnsupportedDeadLetters:
    """The dead-letter port of a store that does not implement it; parking is the first call the L3 kit makes."""

    async def park(self, letter: object) -> object:
        raise Unsupported("dead_letters")


class NoDeadLettersStore(MemoryStore):
    """A store without the dead-letter port, for the level-3 runner's failure naming."""

    def __init__(self) -> None:
        super().__init__()
        self.dead_letters = UnsupportedDeadLetters()  # type: ignore[assignment]


async def test_the_runner_names_a_failing_level_three_check() -> None:
    with pytest.raises(AssertionError, match=r"dead_letters_sequence_per_key.*memory"):
        await run_store_l3_conformance(NoDeadLettersStore)


@pytest.mark.repo
def test_the_checked_in_store_check_list_is_the_reference_list() -> None:
    listed = (KIT / "store_checks.txt").read_text(encoding="utf-8").splitlines()
    assert listed == list(RUST_STORE_CHECKS)
    assert [check.__name__ for check in MESSAGE_STORE_CHECKS] == list(RUST_STORE_L2_CHECKS)
    assert [check.__name__ for check in MESSAGE_STORE_L3_CHECKS] == list(RUST_STORE_L3_CHECKS)
    assert [check.__name__ for check in _checks_for(3)] == list(RUST_STORE_CHECKS)
    assert len(MESSAGE_STORE_CHECKS) == 9
    assert len(MESSAGE_STORE_L3_CHECKS) == 8


async def test_the_event_store_kit_records_its_marker_through_the_memory_store() -> None:
    """The message-store hook of the event-store kit: ``store.processed.record(marker)``, outside a transaction."""
    events = MemoryEventStore()
    store = MemoryStore()
    await redelivered_after_commit_is_skipped(EventStoreKit("conformance.events", lambda: events, store))
