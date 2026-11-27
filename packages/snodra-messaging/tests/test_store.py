"""The in-memory message store: transactions, payload refusal, claims, leases, purge and wake-ups."""

from __future__ import annotations

import asyncio
import gc
import uuid

import pytest
from snodra._eventstore import ProcessedMarker
from snodra._store import (
    Conflict,
    Failed,
    InvalidMessage,
    MessageStore,
    OutboxEntry,
    Tx,
    Unsupported,
    bind_transaction,
    current_transaction,
    now_ms,
    unbind_transaction,
)
from snodra._store_memory import MemoryStore


class _Rollback(Exception):
    """Leaves a transaction without committing it."""


class Clock:
    """An application clock a test advances by hand."""

    def __init__(self, at: int = 1_800_000_000_000) -> None:
        self.at = at

    def __call__(self) -> int:
        return self.at

    def advance(self, milliseconds: int) -> None:
        self.at += milliseconds


def _entry(
    *,
    entry_id: str | None = None,
    type_name: str = "conformance.Test",
    created_at_ms: int | None = None,
    seq: int = 0,
    payload: bytes = b'{"n":1}',
) -> OutboxEntry:
    return OutboxEntry(
        id=entry_id or str(uuid.uuid4()),
        type_name=type_name,
        key="key-1",
        headers={"correlationId": "c-1", "userId": ""},
        payload=payload,
        created_at_ms=now_ms() if created_at_ms is None else created_at_ms,
        seq=seq,
    )


def _marker(marker_id: str | None = None) -> ProcessedMarker:
    return ProcessedMarker(
        id=marker_id or f"processor:conformance:{uuid.uuid4()}",
        type_name="conformance.Test",
        processed_at_ms=now_ms(),
    )


def _nested(depth: int) -> bytes:
    objects = depth - 1
    return ('{"a":' * objects + "1" + "}" * objects).encode()


async def test_the_memory_store_has_the_six_stores() -> None:
    store: MessageStore = MemoryStore()
    assert store.name == "memory"
    assert store.outbox is store.outbox
    assert store.processed is store.processed
    assert store.dead_letters is store.dead_letters
    assert store.control is store.control
    assert store.external is store.external
    assert store.sagas is store.sagas


async def test_a_committed_transaction_keeps_its_writes_and_a_rollback_leaves_nothing() -> None:
    store = MemoryStore()
    rolled_back_entry = _entry()
    rolled_back_marker = _marker()
    try:
        async with store.transaction():
            await store.outbox.append(rolled_back_entry)
            assert await store.processed.record(rolled_back_marker)
            raise _Rollback
    except _Rollback:
        pass
    assert await store.outbox.claim("relay", 60.0, 10) == [], "the rolled-back entry is gone"
    assert await store.outbox.pending() == 0
    assert not await store.processed.exists(rolled_back_marker.id), "the rolled-back marker is gone"

    committed_entry = _entry()
    committed_marker = _marker()
    async with store.transaction():
        await store.outbox.append(committed_entry)
        assert await store.processed.record(committed_marker)
        assert not await store.processed.exists(committed_marker.id), "not committed yet"
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert [entry.id for entry in claimed] == [committed_entry.id], "the committed entry reached the outbox"
    assert await store.processed.exists(committed_marker.id), "the committed marker is recorded"


async def test_a_transaction_joins_the_one_already_bound() -> None:
    store = MemoryStore()
    entry = _entry()
    async with store.transaction() as outer:
        async with store.transaction() as inner:
            assert inner is outer, "a nested transaction joins the bound one"
        await store.outbox.append(entry)
        assert await store.outbox.claim("relay", 60.0, 10) == [], "not committed by the inner scope"
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert [claimed_entry.id for claimed_entry in claimed] == [entry.id]


async def test_a_second_append_of_one_entry_id_conflicts() -> None:
    store = MemoryStore()
    entry = _entry()
    await store.outbox.append_now(entry)
    with pytest.raises(Conflict):
        await store.outbox.append_now(entry)
    with pytest.raises(Conflict):
        async with store.transaction():
            await store.outbox.append(entry)
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert [claimed_entry.id for claimed_entry in claimed] == [entry.id], "the first append stays, one entry"


async def test_two_live_writers_of_one_record_conflict() -> None:
    store = MemoryStore()
    entry = _entry()
    first_wrote = asyncio.Event()
    second_done = asyncio.Event()
    conflicts: list[Conflict] = []

    async def first() -> None:
        async with store.transaction():
            await store.outbox.append(entry)
            first_wrote.set()
            await second_done.wait()

    async def second() -> None:
        await first_wrote.wait()
        try:
            async with store.transaction():
                await store.outbox.append(entry)
        except Conflict as error:
            conflicts.append(error)
        finally:
            second_done.set()

    await asyncio.gather(first(), second())
    assert len(conflicts) == 1, "the second live writer is refused"
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert [claimed_entry.id for claimed_entry in claimed] == [entry.id], "one entry, the first writer's"


async def test_a_committed_append_wakes_the_relay() -> None:
    store = MemoryStore()
    wakes: list[int] = []
    listener = store.outbox.on_append(lambda: wakes.append(1))
    await store.outbox.append_now(_entry())
    assert len(wakes) == 1, "an append on its own wakes the relay"
    async with store.transaction():
        await store.outbox.append(_entry())
        await store.outbox.append(_entry())
    assert len(wakes) == 2, "a committed transaction wakes once, however many entries it appended"
    try:
        async with store.transaction():
            await store.outbox.append(_entry())
            raise _Rollback
    except _Rollback:
        pass
    assert len(wakes) == 2, "a rolled-back transaction wakes nothing"
    listener.close()
    await store.outbox.append_now(_entry())
    assert len(wakes) == 2, "a closed listener is not called"
    listener.close()  # idempotent

    with store.outbox.on_append(lambda: wakes.append(1)):
        await store.outbox.append_now(_entry())
    assert len(wakes) == 3, "a listener in a with block is registered and removed"


async def test_a_dropped_append_listener_unregisters() -> None:
    store = MemoryStore()
    wakes: list[int] = []
    listener = store.outbox.on_append(lambda: wakes.append(1))
    del listener
    gc.collect()
    await store.outbox.append_now(_entry())
    assert wakes == [], "a dropped listener is removed, as on_append promises"


async def test_a_waker_task_does_not_inherit_the_appending_transaction() -> None:
    store = MemoryStore()
    observed: list[Tx | None] = []
    tasks: list[asyncio.Task[None]] = []

    async def waker_task() -> None:
        observed.append(current_transaction())

    def wake() -> None:
        tasks.append(asyncio.get_running_loop().create_task(waker_task()))

    with store.outbox.on_append(wake):
        async with store.transaction():
            assert current_transaction() is not None, "the appending context has its transaction bound"
            await store.outbox.append(_entry())
    await asyncio.gather(*tasks)
    assert observed == [None], "the waker task sees no transaction, and cannot join the appending one"


async def test_bind_and_unbind_a_transaction() -> None:
    tx = Tx.none()
    token = bind_transaction(tx)
    try:
        assert current_transaction() is tx
    finally:
        unbind_transaction(token)
    assert current_transaction() is None


REFUSED_PAYLOADS = [
    pytest.param(b"not json", id="not-json"),
    pytest.param(b'{"n":1e400}', id="above-double"),
    pytest.param(b'{"n":-1e400}', id="below-double"),
    pytest.param(b'{"s":"\\ud800"}', id="lone-high-surrogate"),
    pytest.param(b'{"\\udc00":1}', id="lone-low-surrogate-in-a-name"),
    pytest.param(b"\xef\xbb\xbf{}", id="leading-bom"),
    pytest.param(b'{"s":"\xff"}', id="not-utf8"),
    pytest.param(_nested(101), id="depth-101"),
    pytest.param(_nested(600), id="depth-600"),
    pytest.param(b"[1,2]", id="array"),
    pytest.param(b'"text"', id="string"),
    pytest.param(b"null", id="null"),
    pytest.param(b'{"n":18446744073709551615}', id="above-u64"),
    pytest.param(b'{"n":-9223372036854775809}', id="below-i64"),
    pytest.param(b'{"n":18446744073709551616}', id="above-u64-plus-one"),
    pytest.param(b'{"n":123456789012345678901234567890}', id="a-huge-integer"),
    pytest.param(b'{"a":[1,{"b":[2,-99999999999999999999]}]}', id="a-huge-integer-nested"),
]


@pytest.mark.parametrize("payload", REFUSED_PAYLOADS)
async def test_a_refused_payload_is_never_appended(payload: bytes) -> None:
    store = MemoryStore()
    with pytest.raises(InvalidMessage):
        await store.outbox.append_now(_entry(payload=payload))
    with pytest.raises(InvalidMessage):
        async with store.transaction():
            await store.outbox.append(_entry(payload=payload))
    assert await store.outbox.claim("relay", 60.0, 10) == [], "nothing was appended"


async def test_the_deepest_allowed_payload_and_its_bytes_are_kept() -> None:
    store = MemoryStore()
    deepest = _nested(100)
    await store.outbox.append_now(_entry(payload=deepest))
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert [entry.payload for entry in claimed] == [deepest], "depth 100 is appended and read back byte for byte"
    await store.outbox.mark_sent([entry.id for entry in claimed])
    payload = '{"text":"é","ok":true}'.encode()
    await store.outbox.append_now(_entry(payload=payload))
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert claimed[0].payload == payload, "the payload is kept as appended"


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b'{"s":"\\ud83d\\ude00"}', id="a-surrogate-pair"),
        pytest.param(b'{"n":1e-400}', id="a-double-that-rounds-to-zero"),
    ],
)
async def test_a_payload_within_the_contract_is_appended(payload: bytes) -> None:
    store = MemoryStore()
    await store.outbox.append_now(_entry(payload=payload))
    claimed = await store.outbox.claim("relay", 60.0, 10)
    assert [entry.payload for entry in claimed] == [payload]


async def test_a_lease_expires_on_the_application_clock() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    entry = _entry(created_at_ms=clock())
    await store.outbox.append_now(entry)
    first = await store.outbox.claim("relay-a", 5.0, 10)
    assert [claimed_entry.id for claimed_entry in first] == [entry.id]
    assert await store.outbox.claim("relay-b", 5.0, 10) == [], "held until the lease passes"
    clock.advance(4_999)
    assert await store.outbox.claim("relay-b", 5.0, 10) == [], "the lease has not passed"
    clock.advance(1)
    reclaimed = await store.outbox.claim("relay-b", 5.0, 10)
    assert [claimed_entry.id for claimed_entry in reclaimed] == [entry.id], "the lease has passed exactly"


async def test_purge_uses_the_application_clock_and_never_removes_a_failed_entry() -> None:
    clock = Clock()
    store = MemoryStore(clock=clock)
    sent = _entry(created_at_ms=clock())
    failed = _entry(created_at_ms=clock())
    await store.outbox.append_now(sent)
    await store.outbox.append_now(failed)
    await store.outbox.claim("relay", 60.0, 10)
    clock.advance(100)
    await store.outbox.mark_sent([sent.id])
    await store.outbox.mark_failed(failed.id, "conformance: given up")
    assert await store.outbox.purge(clock()) == 0, "an entry sent at the bound is not before it"
    clock.advance(1)
    assert await store.outbox.purge(clock()) == 1, "the sent entry goes"
    assert await store.outbox.purge(clock()) == 0, "purged once"
    assert await store.outbox.pending() == 0, "the failed entry is not pending"


async def test_a_processed_marker_records_once_and_reads_back_after_commit() -> None:
    store = MemoryStore()
    marker = _marker()
    assert not await store.processed.exists(marker.id)
    async with store.transaction():
        assert await store.processed.record(marker)
        assert await store.processed.record(marker) is False, "a duplicate in the same transaction"
    assert await store.processed.exists(marker.id)
    assert await store.processed.record(marker) is False, "a duplicate after the commit"


async def test_an_abandoned_transaction_releases_its_reservations() -> None:
    store = MemoryStore()
    marker = _marker()
    tx = await store.begin()
    assert await store.processed.record(marker, tx=tx), "the abandoned transaction wrote the marker"
    del tx
    gc.collect()
    async with store.transaction():
        assert await store.processed.record(marker), "a later transaction records the marker it held"
    assert await store.processed.exists(marker.id), "and commits it"


async def test_a_foreign_transaction_is_refused() -> None:
    store = MemoryStore()
    other = MemoryStore()
    tx = await other.begin()
    with pytest.raises(Failed):
        await store.outbox.append(_entry(), tx=tx)


async def test_a_foreign_transaction_rolls_back_without_failing() -> None:
    store = MemoryStore()
    other = MemoryStore()
    tx = await other.begin()
    await store.rollback(tx)
    await other.rollback(tx)


async def test_the_level_4_ports_are_unsupported_with_the_level_3_ones_in_place() -> None:
    store = MemoryStore()
    assert await store.processed.delete_markers("processor", []) == 0
    assert await store.dead_letters.count() == 0
    assert (await store.control.get("processor")).paused is False
    with pytest.raises(Unsupported):
        await store.external.find("key")
    with pytest.raises(Unsupported):
        await store.sagas.sagas.find_by_id("saga")
    with pytest.raises(Unsupported):
        await store.sagas.dispatch.pending()
    with pytest.raises(Unsupported):
        await store.sagas.deadlines.claim_due("scheduler", 0, 1)
    with pytest.raises(Unsupported):
        await store.sagas.failures.find("failure")
    with pytest.raises(Unsupported):
        await store.sagas.markers.exists("saga:conformance:1")
