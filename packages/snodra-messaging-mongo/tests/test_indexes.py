"""The layout's indexes and TTLs: created idempotently, checked, and with whole-second retentions."""

from __future__ import annotations

from typing import Any

import pytest
from snodra import MessageStoreFailed
from snodra_mongo import MongoStore

pytestmark = pytest.mark.timeout(300)

EXPECTED: dict[str, dict[str, dict[str, Any]]] = {
    "outbox": {
        "sentAt_1_createdAt_1_seq_1": {"key": [("sentAt", 1), ("createdAt", 1), ("seq", 1)]},
        "sentAt_ttl": {"key": [("sentAt", 1)], "ttl": 7 * 86_400},
    },
    "processed_events": {
        "processedAt_ttl": {"key": [("processedAt", 1)], "ttl": 30 * 86_400},
    },
    "external_calls": {
        "updatedAt_ttl": {"key": [("updatedAt", 1)], "ttl": 30 * 86_400},
    },
    "dead_letters": {
        "dead_letters_processor_key_offset": {"key": [("processor", 1), ("key", 1), ("offset", 1)]},
        "dead_letters_processor_parked": {"key": [("processor", 1), ("parkedAt", 1)]},
        "dead_letters_expire": {"key": [("expireAt", 1)], "ttl": 0},
    },
    "sagas": {
        "type_associations_live": {
            "key": [("type", 1), ("associations.k", 1), ("associations.v", 1)],
            "partial": {"ended": False},
        },
        "startKey_live_unique": {
            "key": [("startKey", 1)],
            "unique": True,
            "partial": {"ended": False, "startKey": {"$exists": True}},
        },
        "endedAt_ttl": {"key": [("endedAt", 1)], "ttl": 30 * 86_400},
    },
    "saga_dispatch": {
        "sagaId_1_seq_1": {"key": [("sagaId", 1), ("seq", 1)]},
        "dispatchedAt_1_failedAt_1_sagaId_1_seq_1": {
            "key": [("dispatchedAt", 1), ("failedAt", 1), ("sagaId", 1), ("seq", 1)],
        },
        "dispatchedAt_ttl": {"key": [("dispatchedAt", 1)], "ttl": 7 * 86_400},
        "failedAt_ttl": {"key": [("failedAt", 1)], "ttl": 30 * 86_400},
        "noticePending_noticeNextAt": {"key": [("noticeNextAt", 1)], "partial": {"noticePending": True}},
    },
    "saga_dispatch_leases": {
        "until_ttl": {"key": [("until", 1)], "ttl": 0},
    },
    "deadlines": {
        "status_1_dueAt_1": {"key": [("status", 1), ("dueAt", 1)]},
        "sagaId_1_status_1": {"key": [("sagaId", 1), ("status", 1)]},
        "finishedAt_ttl": {"key": [("finishedAt", 1)], "ttl": 7 * 86_400},
    },
    "saga_failures": {
        "sagaType_1_lastFailedAt_1": {"key": [("sagaType", 1), ("lastFailedAt", 1)]},
        "lastFailedAt_ttl": {"key": [("lastFailedAt", 1)], "ttl": 30 * 86_400},
    },
}
"""Every collection and index the layout fixes, with the defaults of a store's config."""


async def test_the_indexes_are_idempotent_and_checkable(store: MongoStore) -> None:
    """init_schema() creates every index of the layout and is idempotent; check_schema() passes afterwards."""
    await store.init_schema()
    await store.check_schema()
    collections = set(await store.database.list_collection_names())
    for collection, indexes in EXPECTED.items():
        assert collection in collections, f"the {collection} collection is not created"
        present = await store.database[collection].index_information()
        for name, wanted in indexes.items():
            assert name in present, f"{collection} has no {name} index"
            actual = present[name]
            assert actual["key"] == wanted["key"], f"{name} keys"
            assert actual.get("expireAfterSeconds") == wanted.get("ttl"), f"{name} TTL"
            assert bool(actual.get("unique", False)) == wanted.get("unique", False), f"{name} unique"
            assert actual.get("partialFilterExpression", {}) == wanted.get("partial", {}), f"{name} partial filter"
            del present[name]
        assert set(present) == {"_id_"}, f"{collection} holds indexes the layout does not name: {set(present)}"


async def test_check_schema_fails_on_an_empty_database(server: Any, make_config: Any) -> None:
    """A database without the layout is refused, naming a collection init_schema() would create."""
    bare = MongoStore(make_config())
    try:
        with pytest.raises(MessageStoreFailed, match="outbox"):
            await bare.check_schema()
    finally:
        await bare.close()


async def test_check_schema_fails_on_another_retention(server: Any, make_config: Any) -> None:
    """Every instance sharing a database needs the same retentions: a TTL that differs is a schema failure."""
    first = MongoStore(make_config())
    second = MongoStore(make_config(outbox_retention=3_600.0))
    try:
        await first.init_schema()
        await first.check_schema()
        with pytest.raises(MessageStoreFailed, match="retentions"):
            await second.check_schema()
    finally:
        await first.close()
        await second.close()


async def test_a_sub_second_retention_is_floored(server: Any, make_config: Any) -> None:
    """A TTL index takes whole seconds: a fraction of one is floored, as the published retentions are stored."""
    store = MongoStore(
        make_config(
            outbox_retention=90.9,
            processed_retention=0.5,
            external_call_retention=0.0,
        )
    )
    try:
        await store.init_schema()
        outbox = await store.database["outbox"].index_information()
        processed = await store.database["processed_events"].index_information()
        external = await store.database["external_calls"].index_information()
        assert outbox["sentAt_ttl"]["expireAfterSeconds"] == 90, "90.9 s floors to 90"
        assert processed["processedAt_ttl"]["expireAfterSeconds"] == 0, "0.5 s floors to 0"
        assert external["updatedAt_ttl"]["expireAfterSeconds"] == 0, "a zero retention is a real zero TTL"
        await store.check_schema()
    finally:
        await store.close()


async def test_a_changed_index_is_a_schema_failure(store: MongoStore) -> None:
    """check_schema() compares the keys: an index dropped and recreated under another key is refused."""
    await store.database["outbox"].drop_index("sentAt_1_createdAt_1_seq_1")
    await store.database["outbox"].create_index(
        [("sentAt", 1), ("createdAt", 1)],
        name="sentAt_1_createdAt_1_seq_1",
    )
    with pytest.raises(MessageStoreFailed, match="keys"):
        await store.check_schema()
