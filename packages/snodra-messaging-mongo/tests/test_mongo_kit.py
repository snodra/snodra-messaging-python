"""The nine level-2 and eight level-3 message-store checks on a MongoDB replica set."""

from __future__ import annotations

import pytest
import snodra
from snodra.testing import MESSAGE_STORE_CHECKS, MESSAGE_STORE_L3_CHECKS, run_store_conformance
from snodra_mongo import MongoStore

pytestmark = pytest.mark.timeout(300)


async def test_the_mongo_message_store_passes_the_kit(store: MongoStore) -> None:
    await run_store_conformance(lambda: store, level=3)


async def test_the_store_reports_its_name_and_has_the_six_ports(store: MongoStore) -> None:
    assert store.name == "mongo"
    assert isinstance(store, snodra.StoreLifecycle), "the store implements the public lifecycle"
    assert store.outbox is store.outbox
    assert store.processed is store.processed
    assert store.dead_letters is store.dead_letters
    assert store.control is store.control
    assert store.external is store.external
    assert store.sagas is store.sagas


def test_the_kit_is_the_nine_level_two_checks_and_the_eight_level_three_ones() -> None:
    assert tuple(check.__name__ for check in MESSAGE_STORE_CHECKS) == (
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
    assert tuple(check.__name__ for check in MESSAGE_STORE_L3_CHECKS) == (
        "delete_markers_removes_only_the_range",
        "dead_letters_sequence_per_key",
        "dead_letter_parked_inside_rolled_back_transaction_is_kept",
        "dead_letter_deleted_in_a_transaction_goes_with_it",
        "control_lock_is_exclusive_until_lease_expires",
        "pause_state_round_trips",
        "replay_ranges_round_trip",
        "processor_control_write_inside_rolled_back_transaction_is_kept",
    )
