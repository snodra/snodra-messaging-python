"""The nine level-2 message-store checks on PostgreSQL and MySQL."""

from __future__ import annotations

import pytest
from snodra.testing import MESSAGE_STORE_CHECKS, run_store_conformance
from snodra_sql import SqlStore

pytestmark = pytest.mark.timeout(300)


async def test_the_sql_message_store_passes_the_kit(store: SqlStore) -> None:
    await run_store_conformance(lambda: store)


async def test_the_store_reports_its_name_and_has_the_six_ports(store: SqlStore) -> None:
    assert store.name in ("postgres", "mysql")
    assert store.outbox is store.outbox
    assert store.processed is store.processed
    assert store.dead_letters is store.dead_letters
    assert store.control is store.control
    assert store.external is store.external
    assert store.sagas is store.sagas


def test_the_kit_is_the_nine_level_two_checks() -> None:
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
