"""The in-memory dead letters and processor control: the level-3 ports as their contracts read.

See https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/#dead-letters and
https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control").
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from snodra._eventstore import ProcessedMarker, ReplayRange
from snodra._store import (
    ControlOperation,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
    Locked,
    MessageStore,
    Parked,
    ProcessorControlState,
)
from snodra._store_memory import MemoryStore

PROCESSOR = "mail"
SOURCE = "snodra.events"


class _Rollback(Exception):
    pass


def _letter(
    position: str,
    *,
    processor: str = PROCESSOR,
    segment: str = "0",
    key: str | None = "k1",
    reason: DeadLetterReason = DeadLetterReason.HANDLER_FAILED,
    parked_at_ms: int = 1_000,
) -> DeadLetter:
    return DeadLetter(
        id=f"{processor}:{SOURCE}:{segment}:{position}",
        processor=processor,
        source=SOURCE,
        segment=segment,
        position=position,
        key=key,
        event_id=f"e-{position}",
        event_type="orders.PlaceOrder",
        headers={"type": "orders.PlaceOrder", "id": f"e-{position}"},
        payload=b'{"orderId":"o-1"}',
        reason=reason,
        error=DeadLetterError.new("RuntimeError", "boom"),
        blocked_by=None,
        attempts=1,
        parked_at_ms=parked_at_ms,
        last_attempt_at_ms=parked_at_ms,
        expire_at_ms=parked_at_ms + 1_000,
        replay=False,
    )


async def test_park_is_idempotent_and_reads_keep_every_field() -> None:
    store: MessageStore = MemoryStore()
    letter = _letter("3")
    assert await store.dead_letters.park(letter) is Parked.NEW
    assert await store.dead_letters.park(letter) is Parked.ALREADY
    found = await store.dead_letters.get(letter.id)
    assert found == letter
    assert await store.dead_letters.count() == 1
    assert await store.dead_letters.count(PROCESSOR) == 1
    assert await store.dead_letters.count("default") == 0


async def test_head_and_earlier_ignore_invalid_letters_and_order_positions() -> None:
    store = MemoryStore()
    head = _letter("9", parked_at_ms=3_000)
    earlier = _letter("4", parked_at_ms=2_000)
    invalid_small = _letter("1", reason=DeadLetterReason.INVALID, parked_at_ms=1_000)
    other_key = _letter("2", key="k2", parked_at_ms=500)
    for letter in (head, earlier, invalid_small, other_key):
        await store.dead_letters.park(letter)
    assert await store.dead_letters.head(PROCESSOR, "k1") == earlier.id
    assert await store.dead_letters.earlier(PROCESSOR, "k1", "9") == earlier.id
    assert await store.dead_letters.earlier(PROCESSOR, "k1", "4") is None
    assert await store.dead_letters.head(PROCESSOR, "k2") == other_key.id
    assert await store.dead_letters.head(PROCESSOR, "none") is None


async def test_parked_keys_names_the_keys_of_the_given_segments_outside_invalid() -> None:
    store = MemoryStore()
    for letter in (
        _letter("1", segment="0", key="a"),
        _letter("2", segment="1", key="b"),
        _letter("3", segment="0", key="c", reason=DeadLetterReason.INVALID),
        _letter("4", segment="2", key="d"),
        _letter("5", segment="0", key=None),
    ):
        await store.dead_letters.park(letter)
    assert await store.dead_letters.parked_keys(PROCESSOR, SOURCE, ("0", "1")) == {"a", "b"}
    assert await store.dead_letters.parked_keys(PROCESSOR, "other", ("0", "1", "2")) == set()


async def test_list_summaries_and_delete() -> None:
    store = MemoryStore()
    first = _letter("2", parked_at_ms=1_000)
    second = _letter("1", parked_at_ms=2_000)
    await store.dead_letters.park(first)
    await store.dead_letters.park(second)
    assert [row.id for row in await store.dead_letters.list()] == [first.id, second.id]
    assert [row.id for row in await store.dead_letters.list(PROCESSOR, 1)] == [first.id]
    assert [row.id for row in await store.dead_letters.list("default")] == []
    summaries = await store.dead_letters.summaries(PROCESSOR)
    assert [row.id for row in summaries] == [second.id, first.id]  # by position
    assert [(row.key, row.reason) for row in summaries] == [("k1", DeadLetterReason.HANDLER_FAILED)] * 2
    assert await store.dead_letters.delete(first.id) is True
    assert await store.dead_letters.delete(first.id) is False
    assert await store.dead_letters.count() == 1


async def test_delete_in_joins_the_transaction_and_rolls_back() -> None:
    store = MemoryStore()
    letter = _letter("7")
    await store.dead_letters.park(letter)

    async def delete_twice() -> None:
        async with store.transaction():
            assert await store.dead_letters.delete_in(letter.id) is True
            assert await store.dead_letters.delete_in(letter.id) is False
            raise _Rollback

    with pytest.raises(_Rollback):
        await delete_twice()
    assert await store.dead_letters.get(letter.id) == letter
    async with store.transaction():
        assert await store.dead_letters.delete_in(letter.id) is True
    assert await store.dead_letters.get(letter.id) is None
    assert await store.dead_letters.delete_in(letter.id) is False


async def test_record_failure_updates_the_letter() -> None:
    store = MemoryStore()
    letter = _letter("5", reason=DeadLetterReason.SEQUENCED)
    await store.dead_letters.park(replace(letter, blocked_by="head"))
    error = DeadLetterError.new("IllegalStateException", "still down")
    assert await store.dead_letters.record_failure(letter.id, error, 9_000) is True
    found = await store.dead_letters.get(letter.id)
    assert found is not None
    assert (found.attempts, found.error, found.last_attempt_at_ms) == (2, error, 9_000)
    assert (found.reason, found.blocked_by) == (DeadLetterReason.HANDLER_FAILED, None)
    assert await store.dead_letters.record_failure("missing", error, 9_000) is False


async def test_the_control_document_is_per_processor_and_not_paused_by_default() -> None:
    store = MemoryStore()
    state = await store.control.get(PROCESSOR)
    assert state == ProcessorControlState(PROCESSOR)
    assert state.paused is False
    assert state.operation is None
    assert state.replay == ()

    await store.control.set_paused(PROCESSOR, True, "ctrl-1", 1_000)
    paused = await store.control.get(PROCESSOR)
    assert (paused.paused, paused.paused_at_ms, paused.paused_by, paused.updated_at_ms) == (
        True,
        1_000,
        "ctrl-1",
        1_000,
    )
    await store.control.set_paused(PROCESSOR, False, "ctrl-2", 2_000)
    resumed = await store.control.get(PROCESSOR)
    assert (resumed.paused, resumed.paused_at_ms, resumed.paused_by) == (False, None, None)
    assert await store.control.get("default") == ProcessorControlState("default")


async def test_the_lock_is_exclusive_renewable_and_expires() -> None:
    store = MemoryStore()
    assert await store.control.lock(PROCESSOR, "op-1", 4_000, 1_000) == Locked.was_taken()
    held = await store.control.lock(PROCESSOR, "op-2", 9_000, 1_000)
    assert held.taken is False
    assert held.holder is not None
    assert held.holder.id == "op-1"
    # A renewal of the same operation moves the lease and keeps its prepared_at_ms.
    assert await store.control.lock(PROCESSOR, "op-1", 7_000, 2_000) == Locked.was_taken()
    state = await store.control.get(PROCESSOR)
    assert state.operation == ControlOperation("op-1", "reset", 7_000, 1_000)
    assert await store.control.extend(PROCESSOR, "op-1", 8_000, 3_000) is True
    assert await store.control.extend(PROCESSOR, "op-2", 8_000, 3_000) is False
    assert await store.control.release(PROCESSOR, "op-2", 3_000) is False
    # A passing lease lets another operation take it; the old one cannot renew any more.
    assert await store.control.lock(PROCESSOR, "op-2", 10_000, 9_000) == Locked.was_taken()
    assert await store.control.extend(PROCESSOR, "op-1", 20_000, 9_000) is False
    assert await store.control.expire(PROCESSOR, "op-1", 21_000) is False
    assert await store.control.expire(PROCESSOR, "op-2", 9_999) is False
    assert await store.control.expire(PROCESSOR, "op-2", 10_000) is True
    assert (await store.control.get(PROCESSOR)).operation is None


async def test_replay_ranges_are_replaced_and_removed_exactly() -> None:
    store = MemoryStore()
    first = ReplayRange(SOURCE, "0", "1", "9")
    second = ReplayRange(SOURCE, "1", "3", "4")
    await store.control.set_replay(PROCESSOR, [first, second], 1_000)
    assert (await store.control.get(PROCESSOR)).replay == (first, second)
    assert await store.control.remove_range(PROCESSOR, ReplayRange(SOURCE, "1", "0", "9"), 2_000) is False
    assert await store.control.remove_range(PROCESSOR, first, 2_000) is True
    assert (await store.control.get(PROCESSOR)).replay == (second,)


async def test_delete_markers_removes_only_the_processors_stage_one_in_the_ranges() -> None:
    store = MemoryStore()

    def marker(marker_id: str, processor: str, segment: str, position: str | None) -> ProcessedMarker:
        return ProcessedMarker(
            id=marker_id,
            type_name="orders.PlaceOrder",
            processed_at_ms=1_000,
            processor=processor,
            source=SOURCE,
            segment=segment,
            position=position,
        )

    rows = [
        marker("processor:mail:e-1", "mail", "0", "1"),
        marker("processor:mail:e-2", "mail", "0", "4"),
        marker("processor:mail:e-3", "mail", "1", "4"),
        marker("processor:default:e-4", "default", "0", "4"),
        marker("processor:mail:e-5", "mail", "0", None),
    ]
    for row in rows:
        async with store.transaction():
            assert await store.processed.record(row)
    deleted = await store.processed.delete_markers("mail", [ReplayRange(SOURCE, "0", "1", "4")])
    assert deleted == 1  # position 1; position 4 is until (exclusive), the other segment, the other processor
    assert await store.processed.exists("processor:mail:e-1") is False
    for kept in ("processor:mail:e-2", "processor:mail:e-3", "processor:default:e-4", "processor:mail:e-5"):
        assert await store.processed.exists(kept) is True, kept
