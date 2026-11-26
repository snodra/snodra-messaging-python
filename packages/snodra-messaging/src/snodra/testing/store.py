"""The message-store conformance kit: the nine level-2 store checks, each named as in the reference kit.

An add-on's own test hands the kit a factory over a store whose schema is ready and which holds nothing::

    from snodra.testing import run_store_conformance

    async def test_my_store_passes_conformance() -> None:
        await run_store_conformance(lambda: MyStore())

- **One store** for the whole run: the checks run one after the other on the store the factory returns, each on ids,
  processors and keys of its own. The outbox checks claim the whole outbox, so each first relays whatever another check
  left pending and leaves nothing pending itself: the store must be the kit's alone while it runs.
- **Every check is public**, so a failing one can be run by itself: it takes the :class:`~snodra._store.MessageStore`.
- **The ids are the reference kit's**, one to one; ``store_checks.txt`` lists them. This module holds the checks, the
  helpers they share and the runner. It uses only the standard library: ``snodra.testing`` imports no test framework and
  no store client.
- The kit waits at most a minute for a lease to pass (its expired-lease check sleeps :data:`LEASE` plus
  :data:`LEASE_MARGIN`); a store operation that never answers hangs the check, and the test harness's timeout fails it.

See https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#the-conformance-kit.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from typing import Final

from snodra._store import (
    MAX_PAYLOAD_DEPTH,
    InvalidMessage,
    MessageStore,
    OutboxEntry,
    OutboxStore,
    ProcessedMarker,
    now_ms,
)

BARRIER_WAIT: Final = 30.0
"""The longest a racing check waits at a barrier for the other task, in seconds."""

LONG_LEASE: Final = 60.0
"""The lease of a claim that must not run out while a check runs, in seconds."""

LEASE: Final = 3.0
"""The lease of the expired-lease check, in seconds."""

LEASE_MARGIN: Final = 1.0
"""How much longer than :data:`LEASE` that check waits, in seconds."""

SENT_GAP: Final = 1.0
"""The gap the purge check leaves between a send before its bound and one after, in seconds."""

TOPIC: Final = "snodra.events"
"""The source the checks' records name."""

ENTRIES: Final = 200
"""How many entries the concurrent-relay check appends."""

BATCH: Final = 7
"""The claim size of each relay in the concurrent-relay check."""

MARKER_TYPE: Final = "conformance.Happened"
"""The type of the checks' events and markers."""

type StoreCheck = Callable[[MessageStore], Awaitable[None]]
"""One conformance check: an async function of the store."""


class _Rollback(Exception):
    """Ends a kit transaction without committing it."""


def _entry(type_name: str, created_at_ms: int, seq: int) -> OutboxEntry:
    """An outbox entry of the kit: a fresh id, a key, the correlation and user headers, and a JSON object payload."""
    return OutboxEntry(
        id=str(uuid.uuid4()),
        type_name=type_name,
        key=f"key-{seq}",
        headers={"correlationId": str(uuid.uuid4()), "userId": ""},
        payload=json.dumps(
            {"seq": seq, "text": "é", "nested": {"ok": True}},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode(),
        created_at_ms=created_at_ms,
        seq=seq,
        attempts=0,
    )


def _ids(entries: Sequence[OutboxEntry]) -> list[str]:
    """The ids of ``entries``."""
    return [entry.id for entry in entries]


def _marker(processed_at_ms: int) -> ProcessedMarker:
    """A marker that carries no position."""
    return ProcessedMarker(
        id=f"processor:conformance:{uuid.uuid4()}",
        type_name=MARKER_TYPE,
        processed_at_ms=processed_at_ms,
    )


def _stage_one(processor: str, topic: str, partition: int, offset: int) -> ProcessedMarker:
    """The stage-1 marker of the record at ``offset`` of ``partition`` of ``topic``, handled by ``processor``."""
    return ProcessedMarker(
        id=f"processor:{processor}:{uuid.uuid4()}",
        type_name=MARKER_TYPE,
        processed_at_ms=now_ms(),
        processor=processor,
        source=topic,
        segment=str(partition),
        position=str(offset),
    )


def _nested(depth: int) -> bytes:
    """A JSON object ``depth`` deep as MySQL's ``JSON_DEPTH`` counts it: ``{"a":{"a":...{"a":1}...}}``, ``{"a":1}``
    being depth 2."""
    objects = depth - 1
    return ('{"a":' * objects + "1" + "}" * objects).encode()


async def _drain(outbox: OutboxStore) -> None:
    """Relays (marks sent) whatever another check left claimable, so the next claim sees only the calling check's
    entries."""
    owner = f"drain-{uuid.uuid4()}"
    while True:
        left = await outbox.claim(owner, LONG_LEASE, 1_000)
        if not left:
            return
        await outbox.mark_sent(_ids(left))


async def appended_entry_is_claimed_in_created_seq_order(store: MessageStore) -> None:
    """Entries are claimed in ``(created_at_ms, seq)`` order however they were appended (in a transaction or not); a
    claim skips what another owner holds, gives an owner back what it holds already, and returns every field as
    appended. A retried entry keeps its place and counts the attempt."""
    outbox = store.outbox
    await _drain(outbox)
    base = now_ms() - 60_000
    e1 = _entry("conformance.Ordered", base, 2)
    e2 = _entry("conformance.Ordered", base, 1)
    e3 = _entry("conformance.Ordered", base - 1_000, 9)
    e4 = _entry("conformance.Ordered", base + 1, 0)
    async with store.transaction():
        await outbox.append(e1)
    await outbox.append_now(e2)
    async with store.transaction():
        await outbox.append(e3)
    await outbox.append_now(e4)

    first = await outbox.claim("relay-a", LONG_LEASE, 2)
    assert _ids(first) == _ids([e3, e2]), f"the oldest two, by (created_at_ms, seq): {_ids(first)}"
    second = await outbox.claim("relay-b", LONG_LEASE, 10)
    assert _ids(second) == _ids([e1, e4]), f"another owner gets what relay-a does not hold: {_ids(second)}"
    found = next(entry for entry in second if entry.id == e1.id)
    assert found.type_name == e1.type_name, "the type as appended"
    assert found.key == e1.key, "the key as appended"
    assert found.headers == e1.headers, "the headers as appended"
    assert json.loads(found.payload) == json.loads(e1.payload), "the payload as appended"
    assert found.created_at_ms == e1.created_at_ms, "created_at_ms as appended"
    assert found.seq == e1.seq, "seq as appended"
    assert found.attempts == 0, "a new entry's attempts"

    await outbox.mark_attempt_failed(e3.id, "conformance: broker unavailable")
    again = await outbox.claim("relay-a", LONG_LEASE, 10)
    assert _ids(again) == _ids([e3, e2]), f"relay-a's own entries, again: {_ids(again)}"
    assert again[0].attempts == 1, "a retried entry counts the failed attempt"

    every = _ids([e1, e2, e3, e4])
    await outbox.mark_sent(every)
    assert not await outbox.claim("relay-c", LONG_LEASE, 10), "a sent entry is never claimed"


async def outbox_rejects_a_payload_that_is_not_a_json_object(store: MessageStore) -> None:
    """An outbox payload is a JSON object whose integers fit in a signed 64-bit integer, at most
    :data:`~snodra._store.MAX_PAYLOAD_DEPTH` deep: anything else (not JSON, an array, a bare value, an integer above
    ``i64::MAX`` or below ``i64::MIN``, a number no double holds, a lone surrogate escape, a leading byte order mark,
    bytes that are not UTF-8, one deeper) is refused with :class:`~snodra._store.InvalidMessage`, appended on its own or
    in a transaction, and nothing is appended. The deepest payload allowed is kept by every store."""
    outbox = store.outbox
    await _drain(outbox)
    too_deep = _nested(MAX_PAYLOAD_DEPTH + 1)
    for payload in (
        b"not json",
        b'{"n":1e400}',
        b'{"n":-1e400}',
        b'{"s":"\\ud800"}',
        b'{"\\udc00":1}',
        b"\xef\xbb\xbf{}",
        b'{"s":"\xff"}',
        too_deep,
        b"[1,2]",
        b'"text"',
        b"null",
        b'{"n":18446744073709551615}',
        b'{"n":-9223372036854775809}',
        b'{"n":18446744073709551616}',
        b'{"n":123456789012345678901234567890}',
        b'{"a":[1,{"b":[2,-99999999999999999999]}]}',
    ):
        shown = payload.decode("utf-8", "replace")
        bad = replace(_entry("conformance.Invalid", now_ms(), 0), payload=payload)
        try:
            await outbox.append_now(bad)
        except InvalidMessage:
            pass
        else:
            raise AssertionError(f"{shown} on its own: appended")
        try:
            async with store.transaction():
                await outbox.append(bad)
        except InvalidMessage:
            pass
        else:
            raise AssertionError(f"{shown} in a transaction: appended")
    claimed = await outbox.claim("relay-a", LONG_LEASE, 100)
    assert not claimed, f"nothing appended: {_ids(claimed)}"

    fine = replace(
        _entry("conformance.Fine", now_ms(), 0),
        payload=(
            b'{"s":"123456789012345678901234567890","d":1.5,"e":1e30,'
            b'"max":9223372036854775807,"min":-9223372036854775808}'
        ),
    )
    await outbox.append_now(fine)
    claimed = await outbox.claim("relay-a", LONG_LEASE, 100)
    assert _ids(claimed) == _ids([fine]), f"a payload within the contract: {_ids(claimed)}"
    await outbox.mark_sent(_ids(claimed))

    deepest = replace(_entry("conformance.Deep", now_ms(), 0), payload=_nested(MAX_PAYLOAD_DEPTH))
    await outbox.append_now(deepest)
    claimed = await outbox.claim("relay-a", LONG_LEASE, 100)
    assert _ids(claimed) == _ids([deepest]), f"the deepest payload allowed: {_ids(claimed)}"
    assert json.loads(claimed[0].payload) == json.loads(deepest.payload), "the deepest payload, kept as appended"
    await outbox.mark_sent(_ids(claimed))


async def pending_counts_entries_neither_sent_nor_failed(store: MessageStore) -> None:
    """``pending`` counts the entries neither sent nor failed, held by a relay or not, and ``oldest_pending`` is the
    ``created_at_ms`` of the oldest of them."""
    outbox = store.outbox
    await _drain(outbox)
    assert await outbox.pending() == 0, "drained"
    assert await outbox.oldest_pending() is None, "none pending"
    now = now_ms()
    e1 = _entry("conformance.Pending", now - 3_000, 1)
    e2 = _entry("conformance.Pending", now - 2_000, 2)
    e3 = _entry("conformance.Pending", now - 1_000, 3)
    for entry in (e3, e1, e2):
        await outbox.append_now(entry)
    assert await outbox.pending() == 3, "every appended entry is pending"
    assert await outbox.oldest_pending() == now - 3_000, "the oldest pending time"
    held = await outbox.claim("relay-a", LONG_LEASE, 3)
    assert len(held) == 3, "all three claimed"
    assert await outbox.pending() == 3, "held is pending"
    assert await outbox.oldest_pending() == now - 3_000, "held is pending"
    await outbox.mark_sent([e1.id])
    assert await outbox.pending() == 2, "sent is not"
    assert await outbox.oldest_pending() == now - 2_000, "sent is not"
    await outbox.mark_failed(e2.id, "conformance: rejected")
    assert await outbox.pending() == 1, "failed is not"
    assert await outbox.oldest_pending() == now - 1_000, "failed is not"
    await outbox.mark_attempt_failed(e3.id, "conformance: retry")
    assert await outbox.pending() == 1, "a retried entry is still pending"
    await outbox.mark_sent([e3.id])
    assert await outbox.pending() == 0, "nothing left pending"
    assert await outbox.oldest_pending() is None


async def rolled_back_transaction_leaves_no_outbox_entry(store: MessageStore) -> None:
    """An entry appended in a transaction that is rolled back never reaches the relay; one appended in a committed
    transaction does."""
    outbox = store.outbox
    await _drain(outbox)
    rolled_back = _entry("conformance.RolledBack", now_ms(), 1)
    committed = _entry("conformance.Committed", now_ms(), 2)
    try:
        async with store.transaction():
            await outbox.append(rolled_back)
            raise _Rollback
    except _Rollback:
        pass
    async with store.transaction():
        await outbox.append(committed)

    claimed = await outbox.claim("relay", LONG_LEASE, 100)
    assert _ids(claimed) == [committed.id], f"only the committed entry: {_ids(claimed)}"
    await outbox.mark_sent([committed.id])


async def rolled_back_handler_leaves_no_processed_marker(store: MessageStore) -> None:
    """A handler's transaction that is rolled back leaves neither its processed-event marker nor the events it
    published: a redelivery handles the event again."""
    outbox = store.outbox
    processed = store.processed
    await _drain(outbox)
    marker = _marker(now_ms())
    published = _entry("conformance.PublishedByHandler", now_ms(), 1)

    try:
        async with store.transaction():
            assert await processed.record(marker), "the first handling records its marker"
            await outbox.append(published)
            raise _Rollback
    except _Rollback:
        pass

    assert not await outbox.claim("relay", LONG_LEASE, 100), "the rolled-back handler's event is gone"
    assert not await processed.exists(marker.id), "the marker rolled back with the handler"
    async with store.transaction():
        assert await processed.record(marker), "the rolled-back handler's marker is gone: the redelivery is recorded"
        assert not await processed.exists(marker.id), "not committed yet"
    assert await processed.exists(marker.id), "the marker committed with the handler"


async def processed_marker_records_once(store: MessageStore) -> None:
    """A marker records once: a second record of its id (in a later transaction, whatever else it carries) is false,
    until a purge past its time removes it. A purge keeps the markers recorded after its bound."""
    processed = store.processed
    now = now_ms()
    first = _stage_one(f"conformance-{uuid.uuid4()}", TOPIC, 3, 42)
    assert not await processed.exists(first.id), "not recorded yet"
    async with store.transaction():
        assert await processed.record(first), "the first record wins"
    assert await processed.exists(first.id), "recorded"

    try:
        async with store.transaction():
            assert not await processed.record(first), "a recorded id is a duplicate"
            raise _Rollback
    except _Rollback:
        pass
    same_id = ProcessedMarker(
        id=first.id,
        type_name="conformance.Other",
        processed_at_ms=now - 1,
    )
    try:
        async with store.transaction():
            assert not await processed.record(same_id), "the id alone decides"
            raise _Rollback
    except _Rollback:
        pass

    other = _marker(now)
    async with store.transaction():
        assert await processed.record(other), "another id records"

    old = _marker(now - 3_600_000)
    async with store.transaction():
        assert await processed.record(old), "an old marker records"
    purged = await processed.purge(now - 1_800_000)
    assert purged >= 1, f"the old marker is purged (purged {purged})"
    assert not await processed.exists(old.id), "the old marker is gone"
    assert await processed.exists(first.id), "the recent marker is kept"

    async with store.transaction():
        assert await processed.record(old), "a purged id records again"
    try:
        async with store.transaction():
            assert not await processed.record(first), "the purge kept the recent marker"
            raise _Rollback
    except _Rollback:
        pass


async def concurrent_relays_publish_each_entry_once(store: MessageStore) -> None:
    """Two relays claiming one outbox at the same time: every entry is claimed by exactly one of them, and each claims
    some while the other holds its first batch (a barrier after both first claims, before any entry is marked sent)."""
    outbox = store.outbox
    await _drain(outbox)
    now = now_ms()
    appended: set[str] = set()
    for seq in range(ENTRIES):
        entry = _entry("conformance.Concurrent", now, seq)
        appended.add(entry.id)
        await outbox.append_now(entry)
    start = asyncio.Barrier(2)
    claimed_barrier = asyncio.Barrier(2)

    async def relay(owner: str) -> list[str]:
        published: list[str] = []
        await asyncio.wait_for(start.wait(), BARRIER_WAIT)
        first = True
        while True:
            batch = await outbox.claim(owner, LONG_LEASE, BATCH)
            if first:
                first = False
                # Both first claims are made before either relay marks anything sent: each claimed while the other
                # held its batch.
                await asyncio.wait_for(claimed_barrier.wait(), BARRIER_WAIT)
            if not batch:
                return published
            ids = _ids(batch)
            await outbox.mark_sent(ids)
            published.extend(ids)
            await asyncio.sleep(0)

    a, b = await asyncio.gather(relay("relay-a"), relay("relay-b"))
    assert len(set(a)) == len(a), "relay-a published an entry twice"
    assert len(set(b)) == len(b), "relay-b published an entry twice"
    assert a, "relay-a claimed entries"
    assert b, "relay-b claimed entries"
    both = set(a) & set(b)
    assert not both, f"published by both relays: {sorted(both)}"
    assert set(a) | set(b) == appended, "every entry is published"


async def expired_lease_is_reclaimed(store: MessageStore) -> None:
    """A claimer that stops (crashes) holds its entries until its lease runs out; then another relay claims them, in
    order, with the attempts counted and without the entry given up on."""
    outbox = store.outbox
    await _drain(outbox)
    now = now_ms()
    entries = [_entry("conformance.Leased", now, seq) for seq in range(3)]
    for entry in entries:
        await outbox.append_now(entry)
    crashed = await outbox.claim("crashed", LEASE, 10)
    assert _ids(crashed) == _ids(entries), f"all three claimed: {_ids(crashed)}"
    await outbox.mark_attempt_failed(entries[0].id, "conformance: broker unavailable")
    await outbox.mark_failed(entries[2].id, "conformance: record too large")
    assert not await outbox.claim("survivor", LONG_LEASE, 10), "held while the lease runs"
    await asyncio.sleep(LEASE + LEASE_MARGIN)
    reclaimed = await outbox.claim("survivor", LONG_LEASE, 10)
    assert _ids(reclaimed) == _ids(entries[:2]), (
        f"reclaimed in order after the lease, without the failed entry: {_ids(reclaimed)}"
    )
    assert reclaimed[0].attempts == 1, "the failed attempt is counted"
    assert reclaimed[1].attempts == 0, "an untouched entry has no failed attempt"
    assert not await outbox.claim("crashed", LONG_LEASE, 10), "the survivor holds them now"
    await outbox.mark_sent(_ids(reclaimed))


async def purge_removes_only_sent_before(store: MessageStore) -> None:
    """A purge removes the entries sent before its bound and nothing else: neither later sent ones, nor pending ones,
    nor failed ones (a failed entry is never claimed again either)."""
    outbox = store.outbox
    await _drain(outbox)
    await outbox.purge(now_ms() + 86_400_000)
    now = now_ms()
    early, late, failed, pending = (_entry("conformance.Purged", now, seq) for seq in range(4))
    for entry in (early, late, failed, pending):
        await outbox.append_now(entry)
    assert len(await outbox.claim("relay", LONG_LEASE, 10)) == 4, "all four claimed"
    await outbox.mark_sent([early.id])
    await asyncio.sleep(SENT_GAP)
    bound = now_ms()
    await asyncio.sleep(SENT_GAP)
    await outbox.mark_sent([late.id])
    await outbox.mark_failed(failed.id, "conformance: given up")

    assert await outbox.purge(bound) == 1, "only the early sent entry"
    assert await outbox.purge(bound) == 0, "purged once"
    left = await outbox.claim("relay", LONG_LEASE, 10)
    assert _ids(left) == [pending.id], f"the pending entry stays, the failed one is not claimed: {_ids(left)}"
    await outbox.mark_sent([pending.id])
    assert await outbox.purge(now_ms() + 60_000) == 2, "the late and the pending entry once sent; never the failed one"


MESSAGE_STORE_CHECKS: tuple[StoreCheck, ...] = (
    appended_entry_is_claimed_in_created_seq_order,
    outbox_rejects_a_payload_that_is_not_a_json_object,
    pending_counts_entries_neither_sent_nor_failed,
    rolled_back_transaction_leaves_no_outbox_entry,
    rolled_back_handler_leaves_no_processed_marker,
    processed_marker_records_once,
    concurrent_relays_publish_each_entry_once,
    expired_lease_is_reclaimed,
    purge_removes_only_sent_before,
)
"""Every level-2 check, in the reference kit's order."""


async def run_store_conformance(open_store: Callable[[], MessageStore]) -> None:
    """Runs every check by name on the store ``open_store`` returns (called once); the first failing check raises
    :class:`AssertionError` naming itself and the store. See the module docstring."""
    store = open_store()
    name = store.name
    for check in MESSAGE_STORE_CHECKS:
        try:
            await check(store)
        except Exception as error:
            raise AssertionError(
                f"store conformance check `{check.__name__}` failed on store `{name}`: {error}"
            ) from error
