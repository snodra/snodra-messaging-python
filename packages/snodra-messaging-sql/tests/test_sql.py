"""The SQL add-on's behaviour on both engines: claims, rollbacks, the service's transaction, isolation, the
application's clock, the PostgreSQL wake-up and the background purge."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest
from snodra import (
    InvalidMessage,
    MessageStoreFailed,
    OutboxEntry,
    ProcessedMarker,
    ReplayRange,
    Tx,
    bind_transaction,
    unbind_transaction,
)
from snodra_sql import SqlConfig, SqlStore
from snodra_sql.sql import _ms_value, _MySQLTarget

pytestmark = pytest.mark.timeout(300)

MakeConfig = Callable[..., SqlConfig]


class _Rollback(Exception):
    """Ends a store transaction without committing it."""


def _entry(*, type_name: str = "conformance.Test", created_at_ms: int | None = None, seq: int = 0) -> OutboxEntry:
    return OutboxEntry(
        id=str(uuid.uuid4()),
        type_name=type_name,
        key=f"key-{seq}",
        headers={"correlationId": str(uuid.uuid4()), "userId": ""},
        payload=b'{"seq":1,"text":"\xc3\xa9"}',
        created_at_ms=created_at_ms if created_at_ms is not None else 1_800_000_000_000,
        seq=seq,
    )


def _marker() -> ProcessedMarker:
    return ProcessedMarker(
        id=f"processor:conformance:{uuid.uuid4()}",
        type_name="conformance.Happened",
        processed_at_ms=1_800_000_000_000,
    )


def _stage_marker(processor: str, source: str, segment: str, position: str) -> ProcessedMarker:
    return ProcessedMarker(
        id=f"processor:{processor}:{uuid.uuid4()}",
        type_name="conformance.Happened",
        processed_at_ms=1_800_000_000_000,
        processor=processor,
        source=source,
        segment=segment,
        position=position,
    )


async def _holds(value: bool) -> bool:
    return value


async def wait_for(what: str, condition: Callable[[], Awaitable[bool]], *, within: float = 10.0) -> None:
    """Waits until the asynchronous ``condition`` holds, failing with ``what`` after ``within`` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not await condition():
        if loop.time() >= deadline:
            raise AssertionError(f"{what} within {within:g} s")
        await asyncio.sleep(0.02)


async def test_a_claim_skips_a_row_another_transaction_holds(store: SqlStore, server: Any, database: str) -> None:
    base = 1_800_000_000_000
    entries = [_entry(created_at_ms=base + offset, seq=offset) for offset in (0, 1000, 2000)]
    for entry in entries:
        await store.outbox.append_now(entry)
    holder = await server.connect(database)
    try:
        if store.name == "postgres":
            held = holder.transaction()
            await held.start()
            await holder.fetchval("SELECT id FROM snodra_outbox WHERE id = $1 FOR UPDATE", entries[1].id)
        else:
            await holder.query("START TRANSACTION")
            async with holder.cursor() as cursor:
                await cursor.execute("SELECT id FROM snodra_outbox WHERE id = %s FOR UPDATE", (entries[1].id,))
        claimed = await store.outbox.claim("relay-a", 60.0, 10)
        assert [entry.id for entry in claimed] == [entries[0].id, entries[2].id], "the held entry is skipped"
        if store.name == "postgres":
            await held.rollback()
        else:
            await holder.rollback()
        claimed = await store.outbox.claim("relay-b", 60.0, 10)
        assert [entry.id for entry in claimed] == [entries[1].id], "the released entry is claimed next"
        await store.outbox.mark_sent([entry.id for entry in entries])
    finally:
        await server.close(holder)


async def test_a_rolled_back_transaction_leaves_nothing(store: SqlStore) -> None:
    entry = _entry()
    marker = _marker()
    try:
        async with store.transaction():
            await store.outbox.append(entry)
            assert await store.processed.record(marker)
            raise _Rollback
    except _Rollback:
        pass
    assert await store.outbox.pending() == 0, "the rolled-back entry is gone"
    assert not await store.processed.exists(marker.id), "the rolled-back marker is gone"

    async with store.transaction():
        await store.outbox.append(entry)
        assert await store.processed.record(marker)
    assert await store.outbox.pending() == 1, "the committed entry is pending"
    assert await store.processed.exists(marker.id), "the committed marker is recorded"


async def test_the_services_connection_binds_the_stores_writes(store: SqlStore, server: Any, database: str) -> None:
    conn = await server.connect(database)
    entry = _entry()
    marker = _marker()
    try:
        if store.name == "postgres":
            service = conn.transaction()
            await service.start()
        else:
            await conn.query("START TRANSACTION")
        async with store.transaction(conn=conn):
            await store.outbox.append(entry)
            assert await store.processed.record(marker)
        assert await store.outbox.pending() == 0, "the store did not commit the service's transaction"
        if store.name == "postgres":
            await service.rollback()
        else:
            await conn.rollback()
        assert await store.outbox.pending() == 0, "the service's rollback removed the entry"
        assert not await store.processed.exists(marker.id), "the service's rollback removed the marker"

        if store.name == "postgres":
            service = conn.transaction()
            await service.start()
        else:
            await conn.query("START TRANSACTION")
        async with store.transaction(conn=conn):
            await store.outbox.append(entry)
            assert await store.processed.record(marker)
        if store.name == "postgres":
            await service.commit()
        else:
            await conn.commit()
        assert await store.outbox.pending() == 1, "the service's commit kept the entry"
        assert await store.processed.exists(marker.id), "the service's commit kept the marker"
        await store.outbox.mark_sent([entry.id])
    finally:
        await server.close(conn)


async def test_the_stores_own_transactions_are_read_committed(store: SqlStore) -> None:
    tx = await store.begin()
    inner = cast(Any, tx.inner)
    conn = inner.conn
    try:
        before = await store._fetchval(conn, "SELECT COUNT(*) AS n FROM snodra_outbox")
        await store.outbox.append_now(_entry())
        after = await store._fetchval(conn, "SELECT COUNT(*) AS n FROM snodra_outbox")
        assert (before, after) == (0, 1), "the own transaction did not see the row committed after it began"
    finally:
        await store.rollback(tx)


async def test_the_application_clock_stamps_the_stores_times(store: SqlStore) -> None:
    at = 1_800_000_000_000
    store.use_clock(lambda: at)
    sent, failed = _entry(seq=0), _entry(seq=1)
    await store.outbox.append_now(sent)
    await store.outbox.append_now(failed)
    claimed = await store.outbox.claim("relay-a", 30.0, 10)
    assert len(claimed) == 2
    async with store._read() as conn:
        row = await store._fetchone(
            conn,
            "SELECT sent_at, failed_at, lease_until FROM snodra_outbox WHERE id = ?",
            (sent.id,),
        )
    assert row is not None
    assert _ms_value(row["lease_until"]) == at + 30_000, "the claim's lease uses the application's clock"
    await store.outbox.mark_sent([sent.id])
    await store.outbox.mark_failed(failed.id, "conformance: given up")
    async with store._read() as conn:
        sent_row = await store._fetchone(conn, "SELECT sent_at FROM snodra_outbox WHERE id = ?", (sent.id,))
        failed_row = await store._fetchone(conn, "SELECT failed_at FROM snodra_outbox WHERE id = ?", (failed.id,))
    assert sent_row is not None
    assert _ms_value(sent_row["sent_at"]) == at
    assert failed_row is not None
    assert _ms_value(failed_row["failed_at"]) == at


async def test_a_commit_wakes_the_relay_and_a_rollback_does_not(store: SqlStore) -> None:
    if store.name != "postgres":
        pytest.skip("MySQL has no append notification: the relay polls")
    wakes: list[None] = []
    listener = store.outbox.on_append(lambda: wakes.append(None))
    try:
        await wait_for("the listener to establish LISTEN", lambda: _holds(store.listens >= 1), within=10)
        await asyncio.sleep(0.3)
        wakes.clear()
        try:
            async with store.transaction():
                await store.outbox.append(_entry())
                raise _Rollback
        except _Rollback:
            pass
        await asyncio.sleep(0.5)
        assert not wakes, "a rollback woke the relay"
        async with store.transaction():
            await store.outbox.append(_entry())
        await wait_for("a commit to wake the relay", lambda: _holds(bool(wakes)), within=5)
        assert len(wakes) == 1, "the commit woke the relay exactly once, not the rollback"
    finally:
        listener.close()


async def test_mysql_has_no_listener_and_appends_poll(store: SqlStore) -> None:
    if store.name != "mysql":
        pytest.skip("PostgreSQL listens; the wake-up is tested there")
    wakes: list[None] = []
    listener = store.outbox.on_append(lambda: wakes.append(None))
    try:
        await store.outbox.append_now(_entry())
        await asyncio.sleep(0.2)
        assert store.listens == 0
        assert not wakes
    finally:
        listener.close()


@pytest.mark.timeout(300)
async def test_the_background_purge_removes_what_the_retention_lets_go(
    server: Any, database: str, make_config: MakeConfig
) -> None:
    store = SqlStore(make_config(purge_interval=0.05, outbox_retention=0.0))
    try:
        await store.open()
        await store.init_schema()
        entry = _entry()
        await store.outbox.append_now(entry)
        assert len(await store.outbox.claim("relay", 60.0, 1)) == 1
        await store.outbox.mark_sent([entry.id])
        task = asyncio.create_task(store.background(), name="snodra:sql:test-background")
        try:

            async def gone() -> bool:
                async with store._read() as conn:
                    value = await store._fetchval(conn, "SELECT COUNT(*) AS n FROM snodra_outbox")
                return int(value or 0) == 0

            await wait_for("the background purge to remove the sent entry", gone, within=10)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await store.close()


async def test_a_foreign_transaction_is_refused(store: SqlStore) -> None:
    with pytest.raises(MessageStoreFailed, match="another message store's transaction"):
        await store.outbox.append(_entry(), tx=Tx(object()))


async def test_a_foreign_bound_transaction_is_refused_by_the_context(store: SqlStore) -> None:
    token = bind_transaction(Tx(object()))
    try:
        with pytest.raises(MessageStoreFailed, match="another message store's transaction"):
            async with store.transaction():
                pass
    finally:
        unbind_transaction(token)


def test_on_append_outside_a_running_loop_is_a_no_op() -> None:
    """A relay may register outside a loop; without one the store has no listener and the relay polls."""
    store = SqlStore(SqlConfig(url="postgres://u@h/db"))
    listener = store.outbox.on_append(lambda: None)
    listener.close()


def test_the_mysql_password_is_not_in_the_array_repr() -> None:
    target = _MySQLTarget.parse("mysql://user:secret@localhost:3306/db")
    assert "secret" not in repr(target)
    assert target.password == "secret"


async def test_an_empty_claim_owner_is_refused(store: SqlStore) -> None:
    """SG-31: an empty owner cannot be told apart from nobody in the owner column."""
    with pytest.raises(InvalidMessage, match="non-empty claim owner"):
        await store.outbox.claim("", 60.0, 1)


async def test_a_lone_surrogate_in_an_entry_is_refused(store: SqlStore) -> None:
    """A Python string can hold a lone surrogate, which is not UTF-8 and no database can keep."""
    entry = _entry()
    entry = OutboxEntry(
        id=entry.id,
        type_name=entry.type_name,
        key=entry.key,
        headers={"x": "\ud800"},
        payload=entry.payload,
        created_at_ms=entry.created_at_ms,
        seq=entry.seq,
    )
    with pytest.raises(InvalidMessage, match="not UTF-8"):
        await store.outbox.append_now(entry)
    assert await store.outbox.pending() == 0


async def test_delete_markers_removes_only_the_range(store: SqlStore) -> None:
    """A reset deletes exactly its processor's stage-1 markers in its ranges: numeric positions by value, text
    positions by bytes, never a marker without a position or another processor/source/segment."""
    nine = _stage_marker("default", "s", "0", "9")
    ten = _stage_marker("default", "s", "0", "10")
    text = _stage_marker("default", "s", "0", "x7")
    none = _stage_marker("default", "s", "0", "")
    other_processor = _stage_marker("mail", "s", "0", "10")
    other_source = _stage_marker("default", "other", "0", "10")
    other_segment = _stage_marker("default", "s", "1", "10")
    for marker in (nine, ten, text, none, other_processor, other_source, other_segment):
        assert await store.processed.record(marker), marker.id
    deleted = await store.processed.delete_markers(
        "default",
        [
            ReplayRange(source="s", segment="0", start="10", until="20"),
            ReplayRange(source="s", segment="0", start="x0", until="y"),
        ],
    )
    assert deleted == 2, "ten and x7"
    for marker, exists in {
        nine: True,
        ten: False,
        text: False,
        none: True,
        other_processor: True,
        other_source: True,
        other_segment: True,
    }.items():
        assert await store.processed.exists(marker.id) == exists, marker.id


async def test_notify_false_on_postgres_registers_no_listener(store: SqlStore) -> None:
    if store.name != "postgres":
        pytest.skip("MySQL has no notification at all")
    from dataclasses import replace

    quiet = SqlStore(replace(store.config, notify=False))
    wakes: list[None] = []
    listener = quiet.outbox.on_append(lambda: wakes.append(None))
    try:
        await quiet.outbox.append_now(_entry())
        await asyncio.sleep(0.2)
        assert quiet.listens == 0
        assert not wakes
    finally:
        listener.close()
        await quiet.close()


async def test_a_negative_purge_interval_waits_for_cancellation(store: SqlStore) -> None:
    assert store.config.purge_interval == -1.0
    task = asyncio.create_task(store.background(), name="snodra:sql:test-negative-background")
    await asyncio.sleep(0.1)
    assert not task.done(), "a negative interval returns instead of waiting"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
