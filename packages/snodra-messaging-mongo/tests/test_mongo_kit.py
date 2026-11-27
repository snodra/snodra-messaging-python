"""The nine level-2 and eight level-3 message-store checks on a MongoDB replica set."""

from __future__ import annotations

import pytest
import snodra
from snodra.testing import (
    MESSAGE_STORE_CHECKS,
    MESSAGE_STORE_L3_CHECKS,
    MESSAGE_STORE_L4_CHECKS,
    run_store_conformance,
)
from snodra_mongo import MongoStore

pytestmark = pytest.mark.timeout(300)


async def test_the_mongo_message_store_passes_the_kit(store: MongoStore) -> None:
    await run_store_conformance(lambda: store, level=4)


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
    assert tuple(check.__name__ for check in MESSAGE_STORE_L4_CHECKS) == (
        "external_call_idempotency_returns_first_result",
        "saga_ports_round_trip",
        "two_transactions_starting_one_saga_key_leave_one_live_saga",
        "two_transactions_updating_one_saga_version_commit_once",
        "saga_dispatch_store_refuses_an_event_the_outbox_would_refuse",
        "due_deadline_is_claimed_once_until_its_lease_passes",
        "an_empty_deadline_correlation_id_reads_as_absent",
        "two_schedulers_claim_each_due_deadline_once",
        "dispatch_hands_out_each_saga_in_seq_order",
        "two_relays_never_hold_one_saga_lease",
        "concurrent_notice_claims_return_each_notice_once",
    )
