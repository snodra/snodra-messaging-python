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
    ControlOperation,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    InvalidMessage,
    Locked,
    MessageStore,
    OutboxEntry,
    OutboxStore,
    Parked,
    ProcessedMarker,
    ProcessorControlState,
    ReplayRange,
    letter_id,
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


def _letter(
    processor: str,
    segment: str,
    position: str,
    key: str | None,
    reason: DeadLetterReason,
    parked_at_ms: int,
) -> DeadLetter:
    """A dead letter of the kit: the fixed event, headers and expiry the reference kit builds."""
    return DeadLetter(
        id=letter_id(processor, TOPIC, segment, position),
        processor=processor,
        source=TOPIC,
        segment=segment,
        position=position,
        key=key,
        event_id=str(uuid.uuid4()),
        event_type=MARKER_TYPE,
        headers={"type": MARKER_TYPE},
        payload=b'{"x":1}',
        reason=reason,
        error=DeadLetterError.new("RemoteHandler", "boom"),
        blocked_by=None,
        attempts=3,
        parked_at_ms=parked_at_ms,
        last_attempt_at_ms=parked_at_ms,
        expire_at_ms=parked_at_ms + 86_400_000,
        replay=False,
    )


def _fresh_processor() -> str:
    """A processor no other check has used."""
    return f"conformance-{uuid.uuid4()}"


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


async def delete_markers_removes_only_the_range(store: MessageStore) -> None:
    """A reset deletes the stage-1 markers of its processor in its ranges and nothing else: no other position,
    segment, source or processor, no marker without a position."""
    processed = store.processed
    processor = _fresh_processor()
    other_processor = _fresh_processor()
    in_range = [_stage_one(processor, TOPIC, 0, offset) for offset in (3, 4, 5)]
    in_range.append(_stage_one(processor, TOPIC, 2, 7))
    kept = [_stage_one(processor, TOPIC, 0, offset) for offset in (0, 1, 2, 6)]
    kept.extend(
        [
            _stage_one(processor, TOPIC, 1, 4),
            _stage_one(processor, "conformance.other", 0, 4),
            _stage_one(other_processor, TOPIC, 0, 4),
            _marker(now_ms()),
            _marker(now_ms()),
        ]
    )
    async with store.transaction():
        for marker in (*in_range, *kept):
            assert await processed.record(marker), f"recorded {marker.id}"

    def range_of(segment: str, start: int, until: int) -> ReplayRange:
        return ReplayRange(source=TOPIC, segment=segment, start=str(start), until=str(until))

    assert await processed.delete_markers(processor, []) == 0, "no ranges"
    assert await processed.delete_markers(processor, [range_of("0", 100, 200)]) == 0, "nothing there"
    assert await processed.delete_markers(processor, [range_of("0", 3, 6), range_of("2", 7, 8)]) == 4
    for marker in in_range:
        assert not await processed.exists(marker.id), f"{marker.id} was not deleted"
    for marker in kept:
        assert await processed.exists(marker.id), f"{marker.id} was deleted"
    assert await processed.delete_markers(processor, [range_of("0", 3, 6)]) == 0, "deleted once"


async def dead_letters_sequence_per_key(store: MessageStore) -> None:
    """Letters are sequenced per (processor, key): the head is the lowest position that is not ``INVALID``, an earlier
    letter blocks a retry, a second park of a record finds the first letter, and a failed retry updates the letter in
    place."""
    letters = store.dead_letters
    processor = _fresh_processor()
    other_processor = _fresh_processor()
    t0 = now_ms()
    head = _letter(processor, "0", "5", "k", DeadLetterReason.HANDLER_FAILED, t0)
    sequenced = _letter(processor, "0", "10", "k", DeadLetterReason.SEQUENCED, t0 + 1)
    sequenced = replace(
        sequenced,
        blocked_by=head.id,
        error=DeadLetterError.sequenced(head.id),
    )
    invalid = replace(
        _letter(processor, "0", "3", "k", DeadLetterReason.INVALID, t0 + 2),
        payload=b"\xff\xfe\x00\x01",
        error=DeadLetterError.invalid("not a JSON object"),
    )
    other_key = _letter(processor, "1", "6", "k2", DeadLetterReason.HANDLER_FAILED, t0 + 3)
    no_key = _letter(processor, "0", "9", None, DeadLetterReason.HANDLER_FAILED, t0 + 4)
    elsewhere = _letter(other_processor, "0", "1", "k", DeadLetterReason.HANDLER_FAILED, t0)
    for letter in (head, sequenced, invalid, other_key, no_key, elsewhere):
        assert await letters.park(letter) is Parked.NEW, letter.id
    assert await letters.park(head) is Parked.ALREADY, "a redelivery"

    assert await letters.head(processor, "k") == head.id
    assert await letters.earlier(processor, "k", "7") == head.id
    assert await letters.earlier(processor, "k", "5") is None, "an INVALID letter blocks nothing"
    assert await letters.parked_keys(processor, TOPIC, ("0", "1")) == {"k", "k2"}
    assert await letters.parked_keys(processor, TOPIC, ("1",)) == {"k2"}
    assert await letters.parked_keys(processor, TOPIC, ()) == set()

    assert await letters.get(head.id) == head
    found = await letters.get(invalid.id)
    assert found is not None
    assert found.payload == invalid.payload, "a payload that is not UTF-8 reads back byte for byte"

    listed = await letters.list(processor)
    assert [row.id for row in listed] == [head.id, sequenced.id, invalid.id, other_key.id, no_key.id], "by parked_at"
    assert len(await letters.list(processor, 2)) == 2
    assert await letters.count(processor) == 5
    summaries = await letters.summaries(processor)
    assert [(row.id, row.key, row.position, row.reason, row.parked_at_ms) for row in summaries] == [
        (invalid.id, "k", "3", DeadLetterReason.INVALID, t0 + 2),
        (head.id, "k", "5", DeadLetterReason.HANDLER_FAILED, t0),
        (other_key.id, "k2", "6", DeadLetterReason.HANDLER_FAILED, t0 + 3),
        (no_key.id, None, "9", DeadLetterReason.HANDLER_FAILED, t0 + 4),
        (sequenced.id, "k", "10", DeadLetterReason.SEQUENCED, t0 + 1),
    ], "by position (decimal ones by value), then id"

    assert await letters.delete(head.id)
    assert not await letters.delete(head.id), "gone already"
    assert await letters.head(processor, "k") == sequenced.id, "the next letter of the key is the head"
    error = DeadLetterError.new("RemoteHandler", "failed again")
    assert await letters.record_failure(sequenced.id, error, t0 + 60_000)
    failed = await letters.get(sequenced.id)
    assert failed is not None
    assert failed.attempts == sequenced.attempts + 1
    assert failed.reason is DeadLetterReason.HANDLER_FAILED
    assert failed.error == error
    assert failed.last_attempt_at_ms == t0 + 60_000
    assert failed.blocked_by is None
    assert not await letters.record_failure(head.id, error, t0), "no letter"
    for letter in (sequenced, invalid, other_key, no_key, elsewhere):
        assert await letters.delete(letter.id), letter.id
    assert await letters.count(processor) == 0


async def dead_letter_parked_inside_rolled_back_transaction_is_kept(store: MessageStore) -> None:
    """A dead letter commits on its own: parked, or its failure recorded, while a transaction that is then rolled
    back is open, it stays."""
    letters = store.dead_letters
    processor = _fresh_processor()
    t0 = now_ms()
    parked = _letter(processor, "0", "1", "k1", DeadLetterReason.HANDLER_FAILED, t0)
    try:
        async with store.transaction():
            assert await letters.park(parked) is Parked.NEW
            raise _Rollback
    except _Rollback:
        pass
    assert await letters.get(parked.id) == parked, "a dead letter commits on its own"
    assert await letters.head(processor, "k1") == parked.id

    error = DeadLetterError.new("Boom", "failed again")
    try:
        async with store.transaction():
            assert await letters.record_failure(parked.id, error, t0 + 1_000)
            raise _Rollback
    except _Rollback:
        pass
    found = await letters.get(parked.id)
    assert found is not None
    assert found.error == error, "a retry's failure is recorded on its own too"
    assert await letters.delete(parked.id)


async def dead_letter_deleted_in_a_transaction_goes_with_it(store: MessageStore) -> None:
    """A retry deletes its letter in the record's transaction: a rollback keeps the letter, a commit removes it."""
    letters = store.dead_letters
    processor = _fresh_processor()
    parked = _letter(processor, "0", "7", "k1", DeadLetterReason.HANDLER_FAILED, now_ms())
    assert await letters.park(parked) is Parked.NEW
    try:
        async with store.transaction():
            assert await letters.delete_in(parked.id)
            raise _Rollback
    except _Rollback:
        pass
    assert await letters.get(parked.id) == parked, "rolled back: the letter stays"
    async with store.transaction():
        assert await letters.delete_in(parked.id)
    assert await letters.get(parked.id) is None, "committed: gone"
    try:
        async with store.transaction():
            assert not await letters.delete_in(parked.id), "no letter to delete"
            raise _Rollback
    except _Rollback:
        pass


async def control_lock_is_exclusive_until_lease_expires(store: MessageStore) -> None:
    """A reset lock is held by one operation until its lease passes: another operation is refused with the holder,
    the holder renews, and once the lease has passed another takes it. Only the holder releases it; an expiry clears
    it only past the lease."""
    control = store.control
    processor = _fresh_processor()
    t0 = 1_800_000_000_000
    state = await control.get(processor)
    assert state.processor == processor
    assert not state.paused
    assert state.operation is None
    assert not state.replay, "absent: nothing held"

    assert await control.lock(processor, "op-1", t0 + 5_000, t0) == Locked.was_taken()
    held = ControlOperation(id="op-1", kind="reset", lease_until_ms=t0 + 5_000, prepared_at_ms=t0)
    assert await control.lock(processor, "op-2", t0 + 6_000, t0 + 1_000) == Locked.held(held), (
        "another operation is refused while the lease runs"
    )
    assert await control.lock(processor, "op-1", t0 + 8_000, t0 + 2_000) == Locked.was_taken(), "the holder renews"
    renewed = (await control.get(processor)).operation
    assert renewed is not None
    assert renewed.lease_until_ms == t0 + 8_000
    assert renewed.prepared_at_ms == t0, "a renewal keeps the first prepare's time"
    assert await control.extend(processor, "op-1", t0 + 9_000, t0 + 3_000)
    assert not await control.extend(processor, "op-2", t0 + 9_000, t0 + 3_000)
    refused = await control.lock(processor, "op-2", t0 + 10_000, t0 + 8_999)
    assert not refused.taken, "held until the lease passes"
    assert not await control.expire(processor, "op-1", t0 + 8_999), "not past the lease"
    assert await control.lock(processor, "op-2", t0 + 15_000, t0 + 9_000) == Locked.was_taken(), "the lease has passed"
    assert not await control.release(processor, "op-1", t0 + 9_500), "not the holder"
    assert await control.release(processor, "op-2", t0 + 9_500)
    assert (await control.get(processor)).operation is None
    assert await control.lock(processor, "op-3", t0 + 20_000, t0 + 10_000) == Locked.was_taken()
    assert await control.expire(processor, "op-3", t0 + 20_000), "past the lease"
    assert (await control.get(processor)).operation is None


async def pause_state_round_trips(store: MessageStore) -> None:
    """A pause round-trips with who paused and when, and is idempotent; a resume clears both. A pause leaves the reset
    lock alone and the lock leaves the pause alone."""
    control = store.control
    processor = _fresh_processor()
    t0 = 1_800_000_000_000
    await control.set_paused(processor, True, "control-1", t0)
    await control.set_paused(processor, True, "control-1", t0)
    paused = await control.get(processor)
    assert paused == ProcessorControlState(
        processor,
        paused=True,
        paused_at_ms=t0,
        paused_by="control-1",
        updated_at_ms=t0,
    ), "the paused state"
    assert await control.lock(processor, "op-1", t0 + 30_000, t0 + 1_000) == Locked.was_taken()
    assert (await control.get(processor)).paused, "a lock leaves the pause alone"
    await control.set_paused(processor, False, "control-2", t0 + 2_000)
    resumed = await control.get(processor)
    assert not resumed.paused
    assert (resumed.paused_at_ms, resumed.paused_by) == (None, None)
    assert resumed.updated_at_ms == t0 + 2_000
    assert resumed.operation is not None
    assert resumed.operation.id == "op-1", "a resume leaves the lock alone"

    fresh = _fresh_processor()
    await control.set_paused(fresh, False, "local", t0)
    state = await control.get(fresh)
    assert state == ProcessorControlState(fresh, updated_at_ms=t0), (
        "a resume of a processor the store did not know: not paused"
    )


async def replay_ranges_round_trip(store: MessageStore) -> None:
    """The replay ranges round-trip in order; a removal takes exactly that range of that processor, once; a
    replacement replaces them all. The ranges leave the lock alone."""
    control = store.control
    processor = _fresh_processor()
    other_processor = _fresh_processor()
    t0 = 1_800_000_000_000
    r1 = ReplayRange(source=TOPIC, segment="0", start="10", until="20")
    r2 = ReplayRange(source=TOPIC, segment="1", start="0", until="5")
    r3 = ReplayRange(source="conformance.other", segment="0", start="10", until="20")
    assert await control.lock(processor, "op-1", t0 + 30_000, t0) == Locked.was_taken()
    await control.set_replay(processor, (r1, r2, r3), t0 + 1_000)
    state = await control.get(processor)
    assert state.replay == (r1, r2, r3), "the ranges, in order"
    assert state.updated_at_ms == t0 + 1_000
    assert state.operation is not None
    assert state.operation.id == "op-1", "the lock is untouched"

    assert await control.remove_range(processor, r2, t0 + 2_000)
    assert not await control.remove_range(processor, r2, t0 + 2_000), "removed once"
    assert not await control.remove_range(
        processor, ReplayRange(source=r1.source, segment=r1.segment, start=r1.start, until="21"), t0 + 2_000
    ), "exactly that range"
    assert not await control.remove_range(other_processor, r1, t0 + 2_000), "another processor's"
    assert (await control.get(processor)).replay == (r1, r3)

    await control.set_replay(processor, (r2,), t0 + 3_000)
    assert (await control.get(processor)).replay == (r2,), "set_replay replaces"
    await control.set_replay(processor, (), t0 + 4_000)
    assert (await control.get(processor)).replay == ()


async def processor_control_write_inside_rolled_back_transaction_is_kept(store: MessageStore) -> None:
    """A processor-control write commits on its own: made while a transaction is open that is then rolled back, it
    stays."""
    control = store.control
    paused = _fresh_processor()
    locked = _fresh_processor()
    t0 = 1_800_000_000_000
    replay = ReplayRange(source=TOPIC, segment="0", start="1", until="2")
    try:
        async with store.transaction():
            await control.set_paused(paused, True, "control-1", t0)
            assert await control.lock(locked, "op-1", t0 + 30_000, t0) == Locked.was_taken()
            await control.set_replay(locked, (replay,), t0)
            raise _Rollback
    except _Rollback:
        pass

    assert (await control.get(paused)).paused, "a pause commits on its own"
    state = await control.get(locked)
    assert state.operation is not None
    assert state.operation.id == "op-1", "a lock commits on its own"
    assert state.replay == (replay,), "a replay range commits on its own"


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

MESSAGE_STORE_L3_CHECKS: tuple[StoreCheck, ...] = (
    delete_markers_removes_only_the_range,
    dead_letters_sequence_per_key,
    dead_letter_parked_inside_rolled_back_transaction_is_kept,
    dead_letter_deleted_in_a_transaction_goes_with_it,
    control_lock_is_exclusive_until_lease_expires,
    pause_state_round_trips,
    replay_ranges_round_trip,
    processor_control_write_inside_rolled_back_transaction_is_kept,
)
"""Every level-3 check, in the reference kit's order (``delete_markers`` is first: the Rust list places it among the
level-2 checks)."""


def _checks_for(level: int) -> tuple[StoreCheck, ...]:
    """The checks a run of ``level`` uses, in the Rust kit's order."""
    if level == 2:
        return MESSAGE_STORE_CHECKS
    # The Rust list places delete_markers among the level-2 checks, and the other seven level-3 checks at the end.
    return (
        *MESSAGE_STORE_CHECKS[:6],
        MESSAGE_STORE_L3_CHECKS[0],
        *MESSAGE_STORE_CHECKS[6:],
        *MESSAGE_STORE_L3_CHECKS[1:],
    )


async def run_store_conformance(
    open_store: Callable[[], MessageStore],
    *,
    level: int = 2,
) -> None:
    """Runs the checks of ``level`` by name on the store ``open_store`` returns (called once); the first failing check
    raises :class:`AssertionError` naming itself and the store. ``level=2`` runs the nine level-2 checks; ``level=3``
    runs them and the eight level-3 control and dead-letter checks. See the module docstring."""
    if level not in (2, 3):
        raise ValueError(f"the store kit runs level 2 or 3, not {level}")
    store = open_store()
    name = store.name
    for check in _checks_for(level):
        try:
            await check(store)
        except Exception as error:
            raise AssertionError(
                f"store conformance check `{check.__name__}` failed on store `{name}`: {error}"
            ) from error


async def run_store_l3_conformance(open_store: Callable[[], MessageStore]) -> None:
    """The level-3 form of :func:`run_store_conformance` (the nine level-2 checks and the eight level-3 checks). Kept
    as the named entry point; ``level=3`` is the same run."""
    await run_store_conformance(open_store, level=3)
