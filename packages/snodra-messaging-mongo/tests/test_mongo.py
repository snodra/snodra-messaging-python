"""The layout's documents, transactions, sessions and clock on a real replica set."""

from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

import pytest
from bson import Int64
from pymongo import AsyncMongoClient
from snodra import (
    Conflict,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    InvalidMessage,
    MessageStoreFailed,
    MessageStoreUnsupported,
    OutboxEntry,
    Parked,
    ProcessedMarker,
    ReplayRange,
    SagaFailureDocument,
    Tx,
    bind_transaction,
    unbind_transaction,
)
from snodra_mongo import MongoConfig, MongoStore

pytestmark = pytest.mark.timeout(300)

OUTBOX = "outbox"
PROCESSED = "processed_events"
DEAD_LETTERS = "dead_letters"
CONTROL = "processor_control"


class _Rollback(Exception):
    """Ends a test transaction without committing it."""


def _entry(type_name: str, created_at_ms: int, seq: int, *, key: str | None = None) -> OutboxEntry:
    key = f"key-{seq}" if key is None else key
    return OutboxEntry(
        id=str(uuid.uuid4()),
        type_name=type_name,
        key=key,
        headers={"correlationId": str(uuid.uuid4()), "userId": ""},
        payload=json.dumps({"seq": seq, "text": "é"}, ensure_ascii=False, separators=(",", ":")).encode(),
        created_at_ms=created_at_ms,
        seq=seq,
    )


def _marker() -> ProcessedMarker:
    return ProcessedMarker(
        id=f"processor:conformance:{uuid.uuid4()}",
        type_name="conformance.Happened",
        processed_at_ms=1_762_250_000_000,
    )


async def _rolled_back(store: MongoStore, entry: OutboxEntry, marker: ProcessedMarker) -> None:
    """Appends and records in one transaction, then abandons it."""
    async with store.transaction():
        await store.outbox.append(entry)
        assert await store.processed.record(marker), "the transaction records the marker"
        raise _Rollback


def _at(milliseconds: int) -> datetime:
    return datetime(1970, 1, 1) + timedelta(milliseconds=milliseconds)


async def _read(store: MongoStore, entry_id: str) -> dict[str, Any]:
    document = await store.database[OUTBOX].find_one({"_id": entry_id})
    assert document is not None
    return dict(document)


async def test_an_entry_without_optional_fields_reads_as_none(store: MongoStore) -> None:
    """A document with key, sentAt and failedAt null (one library's writer), and one that leaves them all out and
    carries a ``_class`` (another's), both read as entries without a key, and only their known fields are read."""
    now = 1_762_250_000_000
    await store.database[OUTBOX].insert_many(
        [
            {
                "_id": "null-fields",
                "type": "conformance.NoKey",
                "key": None,
                "headers": {},
                "payload": {"n": 1},
                "createdAt": _at(now),
                "seq": Int64(1),
                "sentAt": None,
                "attempts": 2,
                "failedAt": None,
            },
            {
                "_id": "absent-fields",
                "_class": "com.example.OutboxDocument",
                "type": "conformance.Absent",
                "headers": {},
                "payload": {"n": Int64(2)},
                "createdAt": _at(now + 1_000),
                "seq": Int64(2),
                "attempts": 0,
            },
        ]
    )
    assert await store.outbox.pending() == 2, "a missing optional field is pending"
    claimed = await store.outbox.claim("relay-a", 60.0, 10)
    assert [entry.id for entry in claimed] == ["null-fields", "absent-fields"], "in (createdAt, seq) order"
    for entry in claimed:
        assert entry.key is None, f"entry {entry.id} has a key"
        assert entry.headers == {}
    assert claimed[0].attempts == 2, "the attempts as stored"
    assert claimed[1].attempts == 0, "a missing attempts reads as none"
    assert json.loads(claimed[0].payload) == {"n": 1}
    assert json.loads(claimed[1].payload) == {"n": 2}


async def test_an_appended_entry_writes_the_layouts_nulls(store: MongoStore) -> None:
    """Append writes key, sentAt and failedAt as null, writes neither owner nor leaseUntil nor ``_class``, and stores
    seq as a long, attempts as an int and createdAt as a date; a claim adds owner and the lease, and a send stamps
    sentAt."""
    entry = _entry("conformance.Layout", 1_762_250_000_000, 7, key="")
    entry = OutboxEntry(
        id=entry.id,
        type_name=entry.type_name,
        key=None,
        headers=entry.headers,
        payload=entry.payload,
        created_at_ms=entry.created_at_ms,
        seq=entry.seq,
    )
    await store.outbox.append(entry)
    document = await _read(store, entry.id)
    for name in ("key", "sentAt", "failedAt"):
        assert name in document, f"{name} is absent"
        assert document[name] is None, f"{name} is not null"
    for name in ("owner", "leaseUntil", "_class"):
        assert name not in document, f"{name} is written before a claim"
    assert isinstance(document["seq"], Int64), "seq is a long"
    assert type(document["attempts"]) is int, "attempts is an int"
    assert isinstance(document["createdAt"], datetime), "createdAt is a date"
    assert document["payload"] == {"seq": 7, "text": "é"}

    claimed = await store.outbox.claim("relay-a", 60.0, 1)
    assert [claimed_entry.id for claimed_entry in claimed] == [entry.id]
    document = await _read(store, entry.id)
    assert document["owner"] == "relay-a"
    assert isinstance(document["leaseUntil"], datetime), "the lease is a date"
    for name in ("key", "sentAt", "failedAt"):
        assert document[name] is None, f"{name} changed by the claim"

    await store.outbox.mark_sent([entry.id])
    document = await _read(store, entry.id)
    assert isinstance(document["sentAt"], datetime), "sentAt is a date"
    assert document["failedAt"] is None, "a sent entry is not failed"
    assert document["owner"] == "relay-a", "owner stays after a send"
    assert "leaseUntil" in document, "the lease stays after a send"


async def test_a_payload_round_trips_with_its_numbers(store: MongoStore) -> None:
    """The payload becomes a BSON document field for field: an integer fits an int32 or a long, a fraction a double,
    and the entry's payload comes back as compact JSON with the same value."""
    payload = {"i": 7, "big": 2**40, "d": 1.5, "nested": {"ok": True}, "none": None, "text": "é"}
    entry = OutboxEntry(
        id=str(uuid.uuid4()),
        type_name="conformance.Payload",
        key="k",
        headers={},
        payload=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(),
        created_at_ms=1_762_250_000_000,
        seq=1,
    )
    await store.outbox.append_now(entry)
    document = await _read(store, entry.id)
    assert document["payload"] == payload
    assert isinstance(document["payload"]["big"], Int64), "an integer over an int32 is a long"
    assert type(document["payload"]["i"]) is int, "an int32-sized integer is an int"

    claimed = await store.outbox.claim("relay-a", 60.0, 1)
    assert len(claimed) == 1
    assert json.loads(claimed[0].payload) == payload
    assert b" " not in claimed[0].payload, "compact JSON"


async def test_a_second_append_of_one_id_conflicts(store: MongoStore) -> None:
    """An outbox entry's id names one event: a second append of it is a conflict and appends nothing."""
    entry = _entry("conformance.Duplicate", 1_762_250_000_000, 1)
    await store.outbox.append_now(entry)
    with pytest.raises(Conflict):
        await store.outbox.append_now(entry)
    assert await store.outbox.pending() == 1, "the first entry stays, no second one is written"


async def test_an_invalid_payload_writes_nothing(store: MongoStore) -> None:
    """A payload the contract refuses is refused before a write, on its own or in a transaction, and nothing is
    appended."""
    entry = _entry("conformance.Invalid", 1_762_250_000_000, 1)
    bad = OutboxEntry(
        id=entry.id,
        type_name=entry.type_name,
        key=entry.key,
        headers=entry.headers,
        payload=b'{"n":1e400}',
        created_at_ms=entry.created_at_ms,
        seq=entry.seq,
    )
    with pytest.raises(InvalidMessage):
        await store.outbox.append_now(bad)
    with pytest.raises(InvalidMessage):
        async with store.transaction():
            await store.outbox.append(bad)
    assert await store.outbox.pending() == 0, "nothing was appended"


async def test_a_rolled_back_transaction_leaves_nothing(store: MongoStore) -> None:
    """An entry appended and a marker recorded in a rolled-back transaction leave nothing; a commit leaves both."""
    rolled_back = _entry("conformance.RolledBack", 1_762_250_000_000, 1)
    marker = _marker()
    with pytest.raises(_Rollback):
        await _rolled_back(store, rolled_back, marker)
    assert await store.outbox.pending() == 0, "the rolled-back entry is gone"
    assert not await store.processed.exists(marker.id), "the rolled-back marker is gone"

    committed = _entry("conformance.Committed", 1_762_250_000_000, 2)
    async with store.transaction():
        await store.outbox.append(committed)
        assert await store.processed.record(marker), "the rolled-back marker records again"
    assert await store.outbox.pending() == 1, "the committed entry stays"
    assert await store.processed.exists(marker.id), "the committed marker stays"


async def test_a_service_session_binds_the_stores_writes(store: MongoStore) -> None:
    """A service's own session transaction commits and rolls back with the library's writes and the service's
    documents, and store.session() returns that session to a handler."""
    await store.database.create_collection("orders")
    session = store.client.start_session()
    try:
        await session.start_transaction()
        async with store.transaction(session=session) as tx:
            assert store.session(tx) is session, "the service's session is returned"
            await store.database["orders"].insert_one({"_id": "order-1"}, session=store.session(tx))
            await store.outbox.append(_entry("conformance.ServiceSession", 1_762_250_000_000, 1))
            assert await store.processed.record(_marker()), "the marker records in the service's transaction"
        await session.abort_transaction()
        assert await store.outbox.pending() == 0, "the abort left an entry"
        assert await store.database["orders"].count_documents({}) == 0, "the abort left the service's document"

        await session.start_transaction()
        marker = _marker()
        async with store.transaction(session=session):
            await store.database["orders"].insert_one({"_id": "order-2"}, session=store.session())
            await store.outbox.append(_entry("conformance.ServiceSession", 1_762_250_000_000, 2))
            assert await store.processed.record(marker), "the marker records again"
        await session.commit_transaction()
        assert await store.outbox.pending() == 1, "the commit left no entry"
        assert await store.processed.exists(marker.id), "the commit left no marker"
        assert await store.database["orders"].count_documents({}) == 1, "the commit left no service document"
    finally:
        await session.end_session()


async def test_a_transaction_that_ended_writes_at_once(store: MongoStore) -> None:
    """A transaction that has ended counts as none: a write with it commits at once, as the core documents."""
    tx = await store.begin()
    await store.rollback(tx)
    entry = _entry("conformance.EndedTx", 1_762_250_000_000, 1)
    await store.outbox.append(entry, tx=tx)
    assert await store.outbox.pending() == 1, "the write went through, outside the ended transaction"


async def test_every_store_gets_a_database_of_its_own(server: Any, make_config: Any) -> None:
    """Every store gets a database of its own: one store's entries are not another's."""
    first = MongoStore(make_config(database=server.database()))
    second = MongoStore(make_config(database=server.database()))
    try:
        assert first.database.name != second.database.name
        await first.outbox.append_now(_entry("conformance.Unique", 1_762_250_000_000, 1))
        assert await first.outbox.pending() == 1
        assert await second.outbox.pending() == 0, "the second store sees the first one's entries"
    finally:
        await first.close()
        await second.close()
        server.drop_database(first.database.name)
        server.drop_database(second.database.name)


async def test_the_application_clock_stamps_the_stores_times(store: MongoStore) -> None:
    """The claim's lease and the entry's sending and failure are stamped by the application's clock, never the
    database's."""
    at = 1_762_250_000_000
    store.use_clock(lambda: at)
    sent = _entry("conformance.Clock", at, 1)
    failed = _entry("conformance.Clock", at, 2)
    await store.outbox.append_now(sent)
    await store.outbox.append_now(failed)
    claimed = await store.outbox.claim("relay-a", 30.0, 10)
    assert len(claimed) == 2
    for entry in (sent, failed):
        document = await _read(store, entry.id)
        assert document["leaseUntil"] == _at(at + 30_000), "the lease is the clock plus the lease"
    await store.outbox.mark_sent([sent.id])
    await store.outbox.mark_failed(failed.id, "conformance: given up")
    assert (await _read(store, sent.id))["sentAt"] == _at(at), "sent at the clock"
    assert (await _read(store, failed.id))["failedAt"] == _at(at), "failed at the clock"
    store.use_clock(None)


async def test_delete_markers_removes_only_the_range(store: MongoStore) -> None:
    """DeleteMarkers removes exactly the stage-1 markers of one processor in a range: numeric positions by value,
    text positions by bytes, never across the two, and never a marker without a position."""
    now = 1_762_250_000_000

    async def record(processor: str, source: str, segment: str, position: str) -> str:
        marker = ProcessedMarker(
            id=f"processor:{processor}:{uuid.uuid4()}",
            type_name="conformance.Happened",
            processed_at_ms=now,
            processor=processor,
            source=source,
            segment=segment,
            position=position,
        )
        assert await store.processed.record(marker)
        return marker.id

    nine = await record("default", "s", "0", "9")
    ten = await record("default", "s", "0", "10")
    text = await record("default", "s", "0", "x7")
    none = await record("default", "s", "0", "")
    other_processor = await record("mail", "s", "0", "10")
    other_source = await record("default", "other", "0", "10")
    other_segment = await record("default", "s", "1", "10")
    old = "processor:default:old-4"
    await store.database[PROCESSED].insert_one(
        {
            "_id": old,
            "type": "conformance.Happened",
            "processedAt": _at(now),
            "processor": "default",
            "topic": "s",
            "partition": 1,
            "offset": Int64(4),
        }
    )

    deleted = await store.processed.delete_markers(
        "default",
        [
            ReplayRange(source="s", segment="0", start="10", until="20"),
            ReplayRange(source="s", segment="0", start="x0", until="y"),
            ReplayRange(source="s", segment="1", start="0", until="10"),
        ],
    )
    assert deleted == 3, "three markers"
    for marker_id, exists in {
        nine: True,
        ten: False,
        text: False,
        none: True,
        old: False,
        other_processor: True,
        other_source: True,
        other_segment: True,
    }.items():
        assert await store.processed.exists(marker_id) == exists, marker_id


async def test_dead_letter_and_control_documents_write_the_layouts_shape(store: MongoStore) -> None:
    """The raw BSON of a letter and a control document follows the published layout: absent optional fields, the
    numeric segment and position, the binary payload, and the control's operation and replay ranges."""
    t0 = 1_762_250_000_000
    letter = DeadLetter(
        id="orders:snodra.events:2:43",
        processor="orders",
        source="snodra.events",
        segment="2",
        position="43",
        key=None,
        event_id="evt-8",
        event_type="orders.OrderShipped",
        headers={"type": "orders.OrderShipped", "id": "evt-8"},
        payload=b"\xff\xfe",
        reason=DeadLetterReason.SEQUENCED,
        error=DeadLetterError.new("Sequenced", "an earlier letter is parked"),
        blocked_by="orders:snodra.events:2:42",
        attempts=0,
        parked_at_ms=t0,
        last_attempt_at_ms=t0,
        expire_at_ms=t0 + 86_400_000,
        replay=True,
    )
    assert await store.dead_letters.park(letter) is Parked.NEW
    document = await store.database[DEAD_LETTERS].find_one({"_id": letter.id})
    assert document is not None
    assert document["processor"] == "orders"
    assert document["topic"] == "snodra.events"
    assert document["partition"] == 2
    assert document["offset"] == Int64(43)
    assert "key" not in document, "an absent key is absent, not null"
    assert "payload" not in document
    assert document["payloadBinary"] == b"\xff\xfe"
    assert document["blockedBy"] == letter.blocked_by
    assert document["replay"] is True
    assert document["parkedAt"] == _at(t0)
    assert document["expireAt"] == _at(t0 + 86_400_000)

    await store.control.set_paused("orders", True, "control-1", t0)
    await store.control.lock("orders", "op-1", t0 + 30_000, t0 + 1_000)
    await store.control.set_replay(
        "orders",
        [ReplayRange(source="snodra.events", segment="0", start="10", until="20")],
        t0 + 2_000,
    )
    control = await store.database[CONTROL].find_one({"_id": "orders"})
    assert control is not None
    assert control["paused"] is True
    assert control["pausedAt"] == _at(t0)
    assert control["pausedBy"] == "control-1"
    assert control["operation"] == {
        "id": "op-1",
        "kind": "reset",
        "leaseUntil": _at(t0 + 30_000),
        "preparedAt": _at(t0 + 1_000),
    }
    (stored_range,) = control["replay"]
    assert stored_range["topic"] == "snodra.events"
    assert stored_range["partition"] == 0
    assert isinstance(stored_range["from"], Int64)
    assert isinstance(stored_range["until"], Int64)
    assert (stored_range["from"], stored_range["until"]) == (10, 20)
    assert control["updatedAt"] == _at(t0 + 2_000)


async def test_the_unsupported_ports_say_so(store: MongoStore) -> None:
    """The ports this level does not implement say so."""
    calls: list[Awaitable[object]] = [
        store.external.reset("key", "owner"),
        store.sagas.sagas.find_by_id("saga"),
        store.sagas.dispatch.clear_notice("id"),
        store.sagas.deadlines.cancel_all("saga", 0),
        store.sagas.failures.save(
            SagaFailureDocument(
                id="id",
                saga_type="type",
                event_id="event",
                event_type="event.Type",
                payload="{}",
                headers={},
                error="why",
                attempts=1,
                first_failed_at_ms=0,
                last_failed_at_ms=0,
            )
        ),
        store.sagas.markers.insert("id", "type", 0),
    ]
    for call in calls:
        with pytest.raises(MessageStoreUnsupported):
            await call


async def test_another_stores_transaction_is_refused(store: MongoStore, make_config: Any) -> None:
    """A transaction of another MongoDB store is refused, never run on this store's client."""
    other = MongoStore(make_config(database=f"{store.database.name}_other"))
    tx = await store.begin()
    try:
        with pytest.raises(MessageStoreFailed, match="another message store"):
            await other.outbox.append(_entry("conformance.Foreign", 1_762_250_000_000, 1), tx=tx)
        with pytest.raises(MessageStoreFailed, match="another message store"):
            await other.processed.record(_marker(), tx=tx)
    finally:
        await store.rollback(tx)
        await other.close()


def test_a_foreign_tx_is_not_bound(store: MongoStore) -> None:
    """A Tx that carries no session counts as none and is refused by session(), never used as a MongoDB one."""
    with pytest.raises(MessageStoreFailed):
        store.session(Tx.none())


async def test_a_foreign_bound_transaction_is_refused_by_the_context(store: MongoStore) -> None:
    """A transaction another store bound is refused when this store's context manager starts, not joined."""
    token = bind_transaction(Tx(object()))
    try:
        with pytest.raises(MessageStoreFailed, match="another message store"):
            async with store.transaction():
                pass
    finally:
        unbind_transaction(token)


async def test_an_empty_claim_owner_is_refused(store: MongoStore) -> None:
    """SG-31: an empty owner matches ``owner: null``, so it cannot be told apart from nobody."""
    with pytest.raises(InvalidMessage, match="non-empty claim owner"):
        await store.outbox.claim("", 60.0, 1)


async def test_attempts_must_fit_an_int32(store: MongoStore) -> None:
    """The layout's attempts is an int; a value outside the signed 32-bit range is refused before any write."""
    for attempts in (2**31, -(2**31) - 1):
        entry = replace(_entry("conformance.Attempts", 1_762_250_000_000, 1), attempts=attempts)
        with pytest.raises(InvalidMessage, match="signed 32-bit"):
            await store.outbox.append_now(entry)
    assert await store.outbox.pending() == 0, "a refused attempts wrote a document"


async def test_on_append_registers_nothing(store: MongoStore) -> None:
    """The MongoDB store has no notification the relay could wait on, so on_append registers nothing."""
    called = False

    def wake() -> None:
        nonlocal called
        called = True

    listener = store.outbox.on_append(wake)
    listener.close()
    listener.close()
    await store.outbox.append_now(_entry("conformance.Wake", 1_762_250_000_000, 1))
    assert not called, "the MongoDB store woke a relay without a notification"


async def test_background_has_nothing_to_run(store: MongoStore) -> None:
    """MongoDB's TTL monitor deletes expired documents on the server, so background() returns at once."""
    await store.background()


async def test_close_is_idempotent_and_leaves_a_service_client_open(store: MongoStore) -> None:
    """A client the service owns is left open by close(); close() twice does nothing the second time."""
    service_client: AsyncMongoClient[Any] = AsyncMongoClient(store.config.url)
    owned = MongoStore(MongoConfig(url="", database=store.database.name), client=service_client)
    try:
        await owned.close()
        await owned.close()
        await service_client.admin.command("ping")
    finally:
        await service_client.close()


async def test_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """The config validates its URL, database and retentions, and from_env reads the two settings."""
    with pytest.raises(ValueError, match="connection string"):
        MongoStore(MongoConfig(url="", database="db"))
    refused = (
        ({"url": "mongodb://127.0.0.1:1/", "database": ""}, "database"),
        ({"url": "mongodb://127.0.0.1:1/", "database": "db", "outbox_retention": -1.0}, "outbox_retention"),
        ({"url": "mongodb://127.0.0.1:1/", "database": "db", "outbox_retention": float("nan")}, "outbox_retention"),
        ({"url": "mongodb://127.0.0.1:1/", "database": "db", "processed_retention": 2**31}, "processed_retention"),
    )
    for fields, reason in refused:
        with pytest.raises(ValueError, match=reason):
            MongoConfig(**fields)
    monkeypatch.setenv("SNODRA_STORE_MONGO_URI", "mongodb://localhost:27017/?directConnection=true")
    monkeypatch.setenv("SNODRA_STORE_MONGO_DATABASE", "orders")
    config = MongoConfig.from_env()
    assert config.url == "mongodb://localhost:27017/?directConnection=true"
    assert config.database == "orders"
    assert config.outbox_retention == 7 * 86_400


def test_the_config_needs_a_database_and_a_store_needs_a_url_without_a_client() -> None:
    """The database is always required; an empty URL is allowed only when the store is given a client."""
    with pytest.raises(ValueError, match="database"):
        MongoConfig(url="mongodb://localhost:27017/", database=" ")
    assert MongoConfig(url=" ", database="orders").url == ""
    with pytest.raises(ValueError, match="connection string"):
        MongoStore(MongoConfig(url=" ", database="orders"))
