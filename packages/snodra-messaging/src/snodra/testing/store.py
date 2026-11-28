"""The message-store conformance kit: the nine level-2, eight level-3 and eleven level-4 store checks, each named as
in the reference kit.

An add-on's own test hands the kit a factory over a store whose schema is ready and which holds nothing::

    from snodra.testing import run_store_conformance

    async def test_my_store_passes_conformance() -> None:
        await run_store_conformance(lambda: MyStore(), level=4)

- **One store** for the whole run: the checks run one after the other on the store the factory returns, each on ids,
  processors and keys of its own. The outbox checks claim the whole outbox, so each first relays whatever another check
  left pending and leaves nothing pending itself: the store must be the kit's alone while it runs.
- **Every check is public**, so a failing one can be run by itself: it takes the :class:`~snodra._store.MessageStore`.
  :data:`MESSAGE_STORE_CHECKS` holds the nine level-2 checks, :data:`MESSAGE_STORE_L3_CHECKS` the eight level-3 ones
  and :data:`MESSAGE_STORE_L4_CHECKS` the eleven level-4 ones.
- **The runner's level** is ``level=2`` by default, running only the level-2 checks, so a level-2 third-party add-on
  keeps the published entry point. ``level=3`` runs the level-2 and level-3 sets and ``level=4`` all three, in the
  reference kit's order; :func:`run_store_l3_conformance` and :func:`run_store_l4_conformance` are the named aliases.
- **The ids are the reference kit's**, one to one; ``store_checks.txt`` lists all 28 in its order. This module holds the
  checks, the helpers they share and the runner. It uses only the standard library: ``snodra.testing`` imports no test
  framework and no store client.
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
    Association,
    Conflict,
    ControlOperation,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    DeadlineDocument,
    DeadlineStatus,
    DeadlineStore,
    DispatchFailure,
    DispatchKind,
    ExternalCall,
    ExternalCallStatus,
    FailureReason,
    InvalidMessage,
    Locked,
    MessageStore,
    OutboxEntry,
    OutboxStore,
    Parked,
    ProcessedMarker,
    ProcessorControlState,
    ReplayRange,
    SagaDispatchDocument,
    SagaDispatchStore,
    SagaDocument,
    SagaFailureDocument,
    letter_id,
    now_ms,
)

BARRIER_WAIT: Final = 30.0
"""The longest a racing check waits at a barrier for the other task, in seconds."""

START_WAIT: Final = 5.0
"""The longest a saga start may block on another transaction before the kit calls the store broken, in seconds."""

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


def _in_progress(key: str, owner: str, now: int, lease_until_ms: int) -> ExternalCall:
    """An in-progress external call of the kit."""
    return ExternalCall(
        key=key,
        status=ExternalCallStatus.IN_PROGRESS,
        attempts=1,
        updated_at_ms=now,
        lease_until_ms=lease_until_ms,
        owner=owner,
    )


def _saga(
    type_name: str,
    saga_id: str,
    *,
    start_key: str | None,
    now: int,
    state: str = '{"step":0}',
    associations: tuple[Association, ...] = (),
) -> SagaDocument:
    """A saga document of the kit."""
    return SagaDocument(
        id=saga_id,
        type_name=type_name,
        state=state,
        associations=associations,
        start_key=start_key,
        ended=False,
        ended_at_ms=None,
        version=0,
        created_at_ms=now,
        updated_at_ms=now,
    )


def _dispatch_entry(
    saga_id: str,
    seq: int,
    kind: DispatchKind,
    created_at_ms: int,
) -> SagaDispatchDocument:
    """A pending dispatch entry of the kit."""
    return SagaDispatchDocument(
        id=f"{saga_id}:{seq}",
        saga_id=saga_id,
        seq=seq,
        kind=kind,
        type_name="conformance.Dispatched",
        payload='{"amount":42}',
        headers={"correlation-id": "c-1"},
        routing_key=None,
        created_at_ms=created_at_ms,
    )


def _deadline_document(
    *,
    deadline_id: str,
    saga_type: str,
    saga_id: str,
    name: str,
    due_at_ms: int,
    correlation_id: str | None = None,
    payload: str | None = None,
    payload_type: str | None = None,
) -> DeadlineDocument:
    """A scheduled deadline of the kit."""
    return DeadlineDocument(
        id=deadline_id,
        saga_type=saga_type,
        saga_id=saga_id,
        name=name,
        payload=payload,
        payload_type=payload_type,
        due_at_ms=due_at_ms,
        status=DeadlineStatus.SCHEDULED,
        claimed_by=None,
        claimed_until_ms=None,
        finished_at_ms=None,
        correlation_id=correlation_id,
    )


async def _claim_mine(
    deadlines: DeadlineStore,
    owner: str,
    now: int,
    until: int,
    saga_id: str,
) -> DeadlineDocument | None:
    """Claims due deadlines until one of ``saga_id`` comes out (another check's are left claimed)."""
    while True:
        found = await deadlines.claim_due(owner, now, until)
        if found is None:
            return None
        if found.saga_id == saga_id:
            return found


async def _claim_notice(
    queue: SagaDispatchStore,
    now: int,
    reserve_until: int,
    entry_id: str,
) -> SagaDispatchDocument | None:
    """Claims due notices until the one of ``entry_id`` comes out (another check's stay reserved)."""
    while True:
        found = await queue.claim_due_notice(now, reserve_until)
        if found is None:
            return None
        if found.id == entry_id:
            return found


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


async def external_call_idempotency_returns_first_result(store: MessageStore) -> None:
    """The first call of a key wins: a repeat finds its record and, once it succeeded, the first reply; a lapsed call
    is taken over or given up only when the record is still the one read."""
    calls = store.external
    t0 = now_ms()
    key = f"conformance:{uuid.uuid4()}"
    first = _in_progress(key, "owner-1", t0, t0 + 60_000)
    assert await calls.insert(first)
    assert not await calls.insert(_in_progress(key, "owner-2", t0, t0 + 60_000)), "the key is taken"
    assert await calls.find(key) == first
    assert not await calls.succeed(key, "owner-2", '{"n":2}', t0 + 1), "not the owner"
    assert await calls.succeed(key, "owner-1", '{"n":1}', t0 + 2)
    assert not await calls.succeed(key, "owner-1", '{"n":3}', t0 + 3), "settled"
    assert not await calls.insert(_in_progress(key, "owner-3", t0, t0 + 60_000))
    settled = await calls.find(key)
    assert settled is not None
    assert settled.status is ExternalCallStatus.SUCCEEDED
    assert settled.reply == '{"n":1}', "the first result"
    assert (settled.owner, settled.lease_until_ms) == (None, None), "a settled record has no owner"
    assert settled.updated_at_ms == t0 + 2

    lapsed_key = f"conformance:{uuid.uuid4()}"
    lapsed = _in_progress(lapsed_key, "owner-1", t0, t0 + 10)
    assert await calls.insert(lapsed)
    taken = await calls.take_over(lapsed, "owner-2", t0 + 60_000, t0 + 20)
    assert taken is not None
    assert (taken.attempts, taken.owner, taken.lease_until_ms) == (2, "owner-2", t0 + 60_000)
    assert await calls.take_over(lapsed, "owner-3", t0 + 60_000, t0 + 21) is None, (
        "the record changed since it was read"
    )
    assert not await calls.fail(lapsed_key, "owner-1", '{"code":"X"}', t0 + 22), "not the owner any more"
    assert await calls.fail(lapsed_key, "owner-2", '{"code":"HANDLER_ERROR","message":"m"}', t0 + 23)
    failed = await calls.find(lapsed_key)
    assert failed is not None
    assert failed.status is ExternalCallStatus.FAILED
    assert failed.error == '{"code":"HANDLER_ERROR","message":"m"}'
    assert (failed.owner, failed.lease_until_ms) == (None, None)

    unknown_key = f"conformance:{uuid.uuid4()}"
    stale = _in_progress(unknown_key, "owner-1", t0, t0 + 10)
    assert await calls.insert(stale)
    changed = replace(stale, owner="someone-else")
    assert not await calls.expire(changed, t0 + 20), "not the record read"
    assert await calls.expire(stale, t0 + 20)
    unknown = await calls.find(unknown_key)
    assert unknown is not None
    assert unknown.status is ExternalCallStatus.UNKNOWN
    assert (unknown.owner, unknown.lease_until_ms) == (None, None)

    owned_key = f"conformance:{uuid.uuid4()}"
    assert await calls.insert(_in_progress(owned_key, "owner-1", t0, t0 + 60_000))
    assert not await calls.mark_unknown(owned_key, "owner-2", t0 + 1)
    assert await calls.mark_unknown(owned_key, "owner-1", t0 + 1)
    marked = await calls.find(owned_key)
    assert marked is not None
    assert marked.status is ExternalCallStatus.UNKNOWN
    reset_key = f"conformance:{uuid.uuid4()}"
    assert await calls.insert(_in_progress(reset_key, "owner-1", t0, t0 + 60_000))
    assert not await calls.reset(reset_key, "owner-2"), "not the owner"
    assert await calls.reset(reset_key, "owner-1")
    assert await calls.find(reset_key) is None
    assert await calls.insert(_in_progress(reset_key, "owner-2", t0, t0 + 60_000)), "begun afresh"


async def saga_ports_round_trip(store: MessageStore) -> None:
    """Each saga port writes what it reads back, in a committed transaction; an aborted one leaves nothing."""
    sagas = store.sagas
    txs = store
    now = now_ms()
    type_name = f"conformance.Saga{uuid.uuid4().hex}"
    saga = _saga(
        type_name,
        str(uuid.uuid4()),
        start_key=f"{type_name}|orderId=o-1",
        now=now,
        state='{"step":1}',
        associations=(Association("orderId", "o-1"),),
    )
    async with txs.transaction():
        await sagas.sagas.insert(saga)
    async with txs.transaction():
        assert await sagas.sagas.find_by_id(saga.id) == saga
        assert await sagas.sagas.find_live(type_name, "orderId", "o-1") == [saga]
        assert await sagas.sagas.find_live(type_name, "orderId", "o-2") == []
        updated = replace(saga, state='{"step":2}', version=1)
        assert await sagas.sagas.update(updated, 0)
    assert (await sagas.sagas.count_live_by_type()).get(type_name) == 1, "one live instance of the type"
    async with txs.transaction():
        assert not await sagas.sagas.update(updated, 0), "a stale version"
    try:
        async with txs.transaction():
            twin = replace(saga, id=str(uuid.uuid4()))
            try:
                await sagas.sagas.insert(twin)
            except Conflict:
                raise _Rollback from None
            raise AssertionError("a live start key is not a conflict")
    except _Rollback:
        pass
    rolled_back = replace(saga, id=str(uuid.uuid4()), start_key=None)
    try:
        async with txs.transaction():
            await sagas.sagas.insert(rolled_back)
            raise _Rollback
    except _Rollback:
        pass
    assert await sagas.sagas.find_by_id(rolled_back.id, tx=txs.plain()) is None

    dispatch = replace(
        _dispatch_entry(saga.id, 1000, DispatchKind.COMMAND, now),
        headers={
            "correlation-id": "c-1",
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        },
        routing_key="o-1",
    )

    def deadline(name: str) -> DeadlineDocument:
        return _deadline_document(
            deadline_id=str(uuid.uuid4()),
            saga_type=type_name,
            saga_id=saga.id,
            name=name,
            due_at_ms=now + 60_000,
            correlation_id="c-1",
            payload='{"why":"timeout"}',
            payload_type="conformance.Timeout",
        )

    fired = deadline("fired")
    by_id = deadline("by-id")
    by_name = deadline("by-name")
    rest = deadline("rest")
    async with txs.transaction():
        await sagas.dispatch.record([dispatch])
        await sagas.dispatch.record([])
        for item in (fired, by_id, by_name, rest):
            await sagas.deadlines.insert(item)
    assert await sagas.dispatch.next_pending(saga.id) == dispatch, "the recorded entry reads back"

    later = now + 1_000
    async with txs.transaction():
        assert await sagas.deadlines.find_scheduled(fired.id) == fired
        assert await sagas.deadlines.mark_fired(fired.id, later)
        assert not await sagas.deadlines.mark_fired(fired.id, later), "fired once"
        await sagas.deadlines.cancel(saga.id, by_id.id, later)
        await sagas.deadlines.cancel_by_name(saga.id, "by-name", later)
        assert await sagas.deadlines.find_scheduled(rest.id) == rest, "untouched"
        await sagas.deadlines.cancel_all(saga.id, later)
    for item in (fired, by_id, by_name, rest):
        assert await sagas.deadlines.find_scheduled(item.id, tx=txs.plain()) is None

    failure = SagaFailureDocument(
        id=f"{type_name}:{uuid.uuid4()}",
        saga_type=type_name,
        event_id=str(uuid.uuid4()),
        event_type="conformance.Happened",
        payload='{"x":1}',
        headers={"correlationId": "c-1"},
        error="boom",
        attempts=3,
        first_failed_at_ms=now,
        last_failed_at_ms=now,
    )
    async with txs.transaction():
        await sagas.failures.save(failure)
    assert await sagas.failures.find(failure.id, tx=txs.plain()) == failure
    assert await sagas.failures.failed_again(failure.id, "boom again", 2, later, tx=txs.plain())
    again = await sagas.failures.find(failure.id, tx=txs.plain())
    assert again is not None
    assert (again.attempts, again.error, again.last_failed_at_ms) == (5, "boom again", later)
    assert await sagas.failures.list(type_name, tx=txs.plain()) == [again]
    assert await sagas.failures.delete(failure.id, tx=txs.plain())
    assert not await sagas.failures.delete(failure.id, tx=txs.plain())
    assert not await sagas.failures.failed_again(failure.id, "x", 1, later, tx=txs.plain()), "never inserts"
    assert await sagas.failures.list(type_name, tx=txs.plain()) == []

    marker_id = f"saga:{type_name}:{uuid.uuid4()}"
    async with txs.transaction():
        assert not await sagas.markers.exists(marker_id)
        await sagas.markers.insert(marker_id, "conformance.Happened", now)
    try:
        async with txs.transaction():
            assert await sagas.markers.exists(marker_id)
            try:
                await sagas.markers.insert(marker_id, "conformance.Happened", now)
            except Conflict:
                raise _Rollback from None
            raise AssertionError("a marker inserts once")
    except _Rollback:
        pass
    ended = replace(updated, ended=True, ended_at_ms=later, version=2, updated_at_ms=later)
    async with txs.transaction():
        assert await sagas.sagas.update(ended, 1)
    assert await sagas.sagas.find_by_id(saga.id, tx=txs.plain()) == ended
    assert (await sagas.sagas.count_live_by_type()).get(type_name) is None, "no live instance of the type"
    async with txs.transaction():
        await sagas.dispatch.mark_dispatched(dispatch.id, later)


async def two_transactions_starting_one_saga_key_leave_one_live_saga(store: MessageStore) -> None:
    """Two live transactions each start a saga holding one start key: exactly one commits, the other conflicts, and
    one live saga holds the key."""
    sagas = store.sagas.sagas
    type_name = f"conformance.Race{uuid.uuid4().hex}"
    start_key = f"{type_name}|orderId=o-1"
    ids = [f"race-{uuid.uuid4()}", f"race-{uuid.uuid4()}"]
    now = now_ms()
    documents = [
        _saga(
            type_name,
            saga_id,
            start_key=start_key,
            now=now,
            state=f'{{"by":"{saga_id}"}}',
            associations=(Association("orderId", "o-1"),),
        )
        for saga_id in ids
    ]
    first = await store.begin()
    second = await store.begin()
    pairs = ((first, documents[0]), (second, documents[1]))
    outcomes: list[Exception | None] = [None, None]
    inserted = [False, False]
    for index, (tx, document) in enumerate(pairs):
        try:
            await asyncio.wait_for(sagas.insert(document, tx=tx), START_WAIT)
        except TimeoutError:
            await store.rollback(tx)
            raise AssertionError(f"the start blocked for more than {START_WAIT:g} s") from None
        except Exception as error:
            outcomes[index] = error
            await store.rollback(tx)
        else:
            inserted[index] = True
    for index, (tx, _document) in enumerate(pairs):
        if not inserted[index]:
            continue
        try:
            await asyncio.wait_for(store.commit(tx), START_WAIT)
        except TimeoutError:
            raise AssertionError(f"the commit blocked for more than {START_WAIT:g} s") from None
        except Exception as error:
            outcomes[index] = error
    wins = [index for index, outcome in enumerate(outcomes) if outcome is None]
    assert len(wins) == 1, f"exactly one commits: {outcomes}"
    lost = outcomes[1 - wins[0]]
    assert lost is not None, "the other conflicts"
    live = await sagas.find_live(type_name, "orderId", "o-1", tx=store.plain())
    assert [document.id for document in live] == [ids[wins[0]]], "one live saga holds the key, the winner's"


async def two_transactions_updating_one_saga_version_commit_once(store: MessageStore) -> None:
    """Two live transactions update one saga from one version: exactly one commits, the other conflicts, and the
    stored saga is the winner's."""
    sagas = store.sagas.sagas
    type_name = f"conformance.Race{uuid.uuid4().hex}"
    saga_id = f"race-{uuid.uuid4()}"
    now = now_ms()
    saga = _saga(type_name, saga_id, start_key=None, now=now, associations=(Association("orderId", "o-1"),))
    async with store.transaction():
        await sagas.insert(saga)
    a = replace(saga, state='{"step":"a"}', version=1)
    b = replace(saga, state='{"step":"b"}', version=1)
    first = await store.begin()
    second = await store.begin()
    pairs = ((first, a), (second, b))
    outcomes: list[Exception | None] = [None, None]
    performed = [False, False]
    for index, (tx, document) in enumerate(pairs):
        try:
            changed = await sagas.update(document, 0, tx=tx)
        except Exception as error:
            outcomes[index] = error
            await store.rollback(tx)
        else:
            assert changed, "an update at the version both read is refused by the check"
            performed[index] = True
    for index, (tx, _document) in enumerate(pairs):
        if not performed[index]:
            continue
        try:
            await store.commit(tx)
        except Exception as error:
            outcomes[index] = error
    wins = [index for index, outcome in enumerate(outcomes) if outcome is None]
    assert len(wins) == 1, f"exactly one commits: {outcomes}"
    assert outcomes[1 - wins[0]] is not None, "the other conflicts"
    stored = await sagas.find_by_id(saga_id, tx=store.plain())
    assert stored == [a, b][wins[0]], "the winner's"


async def saga_dispatch_store_refuses_an_event_the_outbox_would_refuse(store: MessageStore) -> None:
    """A saga event entry whose payload the outbox would refuse is refused inside the saga's transaction, and nothing
    of the attempt commits."""
    sagas = store.sagas
    now = now_ms()
    too_deep = _nested(MAX_PAYLOAD_DEPTH + 1).decode()
    for payload in (
        "[1]",
        "null",
        '{"n":18446744073709551615}',
        '{"n":-9223372036854775809}',
        '{"n":123456789012345678901234567890}',
        '{"n":1e400}',
        '{"s":"\\ud800"}',
        too_deep,
    ):
        saga_id = str(uuid.uuid4())
        fine = _dispatch_entry(saga_id, 1000, DispatchKind.EVENT, now)
        refused = replace(_dispatch_entry(saga_id, 1001, DispatchKind.EVENT, now), payload=payload)
        try:
            async with store.transaction():
                await sagas.dispatch.record([fine, refused])
                raise AssertionError(f"{payload}: recorded")
        except InvalidMessage:
            pass
        assert await sagas.dispatch.next_pending(saga_id) is None, f"{payload}: nothing recorded"
    saga_id = str(uuid.uuid4())
    command = replace(_dispatch_entry(saga_id, 1000, DispatchKind.COMMAND, now), payload="null")
    async with store.transaction():
        await sagas.dispatch.record([command])
    async with store.transaction():
        await sagas.dispatch.mark_dispatched(command.id, now)


async def due_deadline_is_claimed_once_until_its_lease_passes(store: MessageStore) -> None:
    """A due deadline is claimed by one scheduler at a time, earliest first, until its claim passes; one not due,
    fired or cancelled never is. The overdue count counts the due SCHEDULED deadlines per name, claimed or not."""
    sagas = store.sagas
    now = now_ms()
    saga_id = str(uuid.uuid4())
    first_name = f"first-{uuid.uuid4().hex}"
    second_name = f"second-{uuid.uuid4().hex}"
    first = _deadline_document(
        deadline_id=str(uuid.uuid4()),
        saga_type="conformance.Deadlines",
        saga_id=saga_id,
        name=first_name,
        due_at_ms=now - 2_000,
    )
    second = _deadline_document(
        deadline_id=str(uuid.uuid4()),
        saga_type="conformance.Deadlines",
        saga_id=saga_id,
        name=second_name,
        due_at_ms=now - 1_000,
    )
    later = _deadline_document(
        deadline_id=str(uuid.uuid4()),
        saga_type="conformance.Deadlines",
        saga_id=saga_id,
        name=first_name,
        due_at_ms=now + 60_000,
    )
    async with store.transaction():
        for item in (second, later, first):
            await sagas.deadlines.insert(item)
    overdue = await sagas.deadlines.count_overdue_by_name(now)
    assert (overdue.get(first_name), overdue.get(second_name)) == (1, 1)
    until = now + 30_000
    a = await _claim_mine(sagas.deadlines, "scheduler-a", now, until, saga_id)
    assert a == replace(first, claimed_by="scheduler-a", claimed_until_ms=until), "the earliest due deadline"
    b = await _claim_mine(sagas.deadlines, "scheduler-b", now, until, saga_id)
    assert b is not None
    assert b.id == second.id, "the next one"
    assert await _claim_mine(sagas.deadlines, "scheduler-a", now, until, saga_id) is None
    overdue = await sagas.deadlines.count_overdue_by_name(now)
    assert (overdue.get(first_name), overdue.get(second_name)) == (1, 1), "claimed deadlines are still overdue"
    past = until + 1
    again = await _claim_mine(sagas.deadlines, "scheduler-b", past, past + 30_000, saga_id)
    assert again is not None
    assert again.id == first.id, "a passed claim is claimed again"
    async with store.transaction():
        assert await sagas.deadlines.mark_fired(first.id, past)
        await sagas.deadlines.cancel(saga_id, second.id, past)
    far = now + 3_600_000
    later_claim = await _claim_mine(sagas.deadlines, "scheduler-a", far, far + 30_000, saga_id)
    assert later_claim is not None
    assert later_claim.id == later.id
    overdue = await sagas.deadlines.count_overdue_by_name(far)
    assert (overdue.get(first_name), overdue.get(second_name)) == (1, None)
    async with store.transaction():
        await sagas.deadlines.cancel_all(saga_id, far)


async def an_empty_deadline_correlation_id_reads_as_absent(store: MessageStore) -> None:
    """A deadline written with an empty correlation id reads back without one, from find_scheduled and from
    claim_due."""
    sagas = store.sagas
    now = now_ms()
    saga_id = str(uuid.uuid4())
    deadline = _deadline_document(
        deadline_id=str(uuid.uuid4()),
        saga_type="conformance.Deadlines",
        saga_id=saga_id,
        name=f"empty-{uuid.uuid4().hex}",
        due_at_ms=now - 1_000,
        correlation_id="",
    )
    async with store.transaction():
        await sagas.deadlines.insert(deadline)
    async with store.transaction():
        found = await sagas.deadlines.find_scheduled(deadline.id)
    assert found is not None
    assert found.correlation_id is None, f"find_scheduled: {found}"
    claimed = await _claim_mine(sagas.deadlines, "scheduler-empty", now, now + 30_000, saga_id)
    assert claimed is not None
    assert claimed.correlation_id is None, f"claim_due: {claimed}"
    async with store.transaction():
        await sagas.deadlines.cancel_all(saga_id, now)


async def two_schedulers_claim_each_due_deadline_once(store: MessageStore) -> None:
    """Two schedulers claiming at once: every due deadline goes to exactly one of them."""
    sagas = store.sagas
    now = now_ms()
    saga_id = str(uuid.uuid4())
    mine: set[str] = set()
    async with store.transaction():
        for number in range(40):
            deadline = _deadline_document(
                deadline_id=str(uuid.uuid4()),
                saga_type="conformance.Raced",
                saga_id=saga_id,
                name="raced",
                due_at_ms=now - 10_000 + number,
            )
            mine.add(deadline.id)
            await sagas.deadlines.insert(deadline)
    start = asyncio.Barrier(2)
    first_claims = asyncio.Barrier(2)

    async def scheduler(owner: str) -> list[str]:
        got: list[str] = []
        await asyncio.wait_for(start.wait(), BARRIER_WAIT)
        first = True
        while True:
            found = await sagas.deadlines.claim_due(owner, now, now + 60_000)
            if first:
                first = False
                await asyncio.wait_for(first_claims.wait(), BARRIER_WAIT)
            if found is None:
                return got
            if found.saga_id == saga_id:
                got.append(found.id)
            await asyncio.sleep(0)

    a, b = await asyncio.gather(scheduler("scheduler-a"), scheduler("scheduler-b"))
    assert len(set(a)) == len(a), "scheduler-a claimed a deadline twice"
    assert len(set(b)) == len(b), "scheduler-b claimed a deadline twice"
    assert a, f"scheduler-a claimed deadlines: {len(a)}"
    assert b, f"scheduler-b claimed deadlines: {len(b)}"
    assert not set(a) & set(b), "claimed by both"
    assert set(a) | set(b) == mine, "every due deadline is claimed"
    async with store.transaction():
        await sagas.deadlines.cancel_all(saga_id, now)


async def dispatch_hands_out_each_saga_in_seq_order(store: MessageStore) -> None:
    """A saga's pending dispatch entries come in seq order, the sagas whose first entry is due oldest first; a saga's
    lease is held by one relay; an entry dispatched in a rolled-back transaction stays pending; a failed one carries
    its outcome and notice."""
    queue = store.sagas.dispatch
    now = now_ms()
    pending_before = await queue.pending()
    older = str(uuid.uuid4())
    newer = str(uuid.uuid4())
    command = replace(
        _dispatch_entry(newer, 1000, DispatchKind.COMMAND, now - 1_000),
        routing_key="o-1",
    )
    event = _dispatch_entry(newer, 1001, DispatchKind.EVENT, now - 1_000)
    old = _dispatch_entry(older, 0, DispatchKind.EVENT, now - 5_000)
    async with store.transaction():
        await queue.record([event, command])
        await queue.record([old])
    assert await queue.pending() == pending_before + 3
    due = await queue.due_sagas(now, [], 10_000)
    at = {saga_id: index for index, saga_id in enumerate(due)}
    assert older in at, f"the older saga is due: {due}"
    assert newer in at, f"the newer saga is due: {due}"
    assert at[older] < at[newer], f"the older work first: {due}"
    assert older not in await queue.due_sagas(now, [older], 10_000), "a saga in flight is left out"
    assert await queue.next_pending(newer) == command, "the lowest seq first, as recorded"

    lease = now + 60_000
    assert await queue.claim_lease(newer, "relay-a", now, lease)
    assert not await queue.claim_lease(newer, "relay-b", now, lease), "held by relay-a"
    assert not await queue.renew_lease(newer, "relay-b", lease), "only the holder renews"
    assert await queue.renew_lease(newer, "relay-a", now + 120_000)
    assert not await queue.claim_lease(newer, "relay-b", now + 61_000, now + 121_000), "the renewed lease runs"
    await queue.release_lease(newer, "relay-b")
    assert not await queue.claim_lease(newer, "relay-b", now, lease), "only the holder releases"
    await queue.release_lease(newer, "relay-a")
    assert await queue.claim_lease(newer, "relay-b", now, lease), "a released lease is free"
    await queue.release_lease(newer, "relay-b")
    assert await queue.claim_lease(older, "relay-a", now, now + 1_000)
    assert await queue.claim_lease(older, "relay-b", now + 2_000, now + 5_000), "a passed lease is free"
    await queue.release_lease(older, "relay-b")

    try:
        async with store.transaction():
            await queue.mark_dispatched(command.id, now)
            raise _Rollback
    except _Rollback:
        pass
    assert await queue.next_pending(newer) == command, "rolled back: still pending"
    async with store.transaction():
        await queue.mark_dispatched(command.id, now)
    assert await queue.next_pending(newer) == event, "the next entry of the saga"

    await queue.schedule_retry(event.id, 1, now + 60_000)
    assert await queue.next_pending(newer) == replace(event, attempts=1, next_attempt_at_ms=now + 60_000)
    assert newer not in await queue.due_sagas(now, [], 10_000), "not due before its next attempt"
    assert newer in await queue.due_sagas(now + 60_000, [], 10_000), "due at its next attempt"

    failure = DispatchFailure(
        attempts=2,
        reason=FailureReason.EXHAUSTED,
        message="gave up",
        failed_at_ms=now,
        notice_at_ms=now + 30_000,
    )
    await queue.mark_failed(event.id, failure)
    assert await queue.next_pending(newer) is None
    assert await _claim_notice(queue, now, now + 90_000, event.id) is None, "the notice is not due yet"
    failed = replace(
        event,
        attempts=2,
        next_attempt_at_ms=now + 60_000,
        failed_at_ms=now,
        failure_reason=FailureReason.EXHAUSTED,
        failure_message="gave up",
        notice_pending=True,
        notice_attempts=0,
        notice_next_at_ms=now + 90_000,
    )
    assert await _claim_notice(queue, now + 30_000, now + 90_000, event.id) == failed, "the due notice, reserved"
    assert await _claim_notice(queue, now + 30_000, now + 90_000, event.id) is None, "a reserved notice once"
    await queue.schedule_notice_retry(event.id, 1, now + 100_000)
    assert await _claim_notice(queue, now + 100_000, now + 160_000, event.id) == replace(
        failed, notice_attempts=1, notice_next_at_ms=now + 160_000
    ), "a failed delivery is tried again"
    await queue.clear_notice(event.id)
    assert await _claim_notice(queue, now + 3_600_000, now + 3_660_000, event.id) is None

    await queue.mark_failed(
        old.id,
        DispatchFailure(
            attempts=None,
            reason=FailureReason.INVALID,
            message="not a JSON object",
            failed_at_ms=now,
            notice_at_ms=None,
        ),
    )
    assert await queue.next_pending(older) is None
    assert await _claim_notice(queue, now + 3_600_000, now + 3_660_000, old.id) is None
    assert await queue.pending() == pending_before, "neither dispatched nor failed: none left"


async def two_relays_never_hold_one_saga_lease(store: MessageStore) -> None:
    """Two relays claiming the dispatch leases of the same sagas at once: each saga's lease goes to exactly one."""
    queue = store.sagas.dispatch
    now = now_ms()
    ids = [str(uuid.uuid4()) for _ in range(40)]
    start = asyncio.Barrier(2)

    async def relay(owner: str) -> set[str]:
        held: set[str] = set()
        await asyncio.wait_for(start.wait(), BARRIER_WAIT)
        for saga_id in ids:
            if await queue.claim_lease(saga_id, owner, now, now + 60_000):
                held.add(saga_id)
            await asyncio.sleep(0)
        return held

    a, b = await asyncio.gather(relay("relay-a"), relay("relay-b"))
    assert not a & b, "held by both relays"
    assert a | b == set(ids), "every lease has a holder"
    for saga_id in ids:
        await queue.release_lease(saga_id, "relay-a" if saga_id in a else "relay-b")


async def concurrent_notice_claims_return_each_notice_once(store: MessageStore) -> None:
    """Two relays claiming due failure notices at once: every notice goes to exactly one of them, once."""
    queue = store.sagas.dispatch
    now = now_ms()
    entries = [_dispatch_entry(str(uuid.uuid4()), 1000, DispatchKind.COMMAND, now - 1_000) for _ in range(40)]
    async with store.transaction():
        await queue.record(entries)
    for entry in entries:
        await queue.mark_failed(
            entry.id,
            DispatchFailure(
                attempts=1,
                reason=FailureReason.NO_HANDLER,
                message="nobody",
                failed_at_ms=now,
                notice_at_ms=now,
            ),
        )
    mine = {entry.id for entry in entries}
    start = asyncio.Barrier(2)

    async def relay() -> list[str]:
        got: list[str] = []
        await asyncio.wait_for(start.wait(), BARRIER_WAIT)
        while True:
            found = await queue.claim_due_notice(now, now + 60_000)
            if found is None:
                return got
            if found.id in mine:
                got.append(found.id)
            await asyncio.sleep(0)

    a, b = await asyncio.gather(relay(), relay())
    claimed = set(a) | set(b)
    assert claimed == mine, "every due notice was claimed"
    assert len(a) + len(b) == len(entries), f"a notice was claimed twice: {a} and {b}"
    for entry_id in mine:
        await queue.clear_notice(entry_id)


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


MESSAGE_STORE_L4_CHECKS: tuple[StoreCheck, ...] = (
    external_call_idempotency_returns_first_result,
    saga_ports_round_trip,
    two_transactions_starting_one_saga_key_leave_one_live_saga,
    two_transactions_updating_one_saga_version_commit_once,
    saga_dispatch_store_refuses_an_event_the_outbox_would_refuse,
    due_deadline_is_claimed_once_until_its_lease_passes,
    an_empty_deadline_correlation_id_reads_as_absent,
    two_schedulers_claim_each_due_deadline_once,
    dispatch_hands_out_each_saga_in_seq_order,
    two_relays_never_hold_one_saga_lease,
    concurrent_notice_claims_return_each_notice_once,
)
"""Every level-4 check, in the reference kit's order."""


def _checks_for(level: int) -> tuple[StoreCheck, ...]:
    """The checks a run of ``level`` uses, in the Rust kit's order."""
    level3 = (
        *MESSAGE_STORE_CHECKS[:6],
        MESSAGE_STORE_L3_CHECKS[0],
        *MESSAGE_STORE_CHECKS[6:],
        *MESSAGE_STORE_L3_CHECKS[1:],
    )
    if level == 2:
        return MESSAGE_STORE_CHECKS
    if level == 3:
        return level3
    return (*level3, *MESSAGE_STORE_L4_CHECKS)


async def run_store_conformance(
    open_store: Callable[[], MessageStore],
    *,
    level: int = 2,
) -> None:
    """Runs the checks of ``level`` by name on the store ``open_store`` returns (called once); the first failing check
    raises :class:`AssertionError` naming itself and the store. ``level=2`` runs the nine level-2 checks; ``level=3``
    runs them and the eight level-3 control and dead-letter checks; ``level=4`` runs both and the eleven level-4 saga
    and external-call checks. See the module docstring."""
    if level not in (2, 3, 4):
        raise ValueError(f"the store kit runs level 2, 3 or 4, not {level}")
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


async def run_store_l4_conformance(open_store: Callable[[], MessageStore]) -> None:
    """The level-4 form of :func:`run_store_conformance` (level 2 and 3 and the eleven level-4 checks). Kept as the
    named entry point; ``level=4`` is the same run."""
    await run_store_conformance(open_store, level=4)
