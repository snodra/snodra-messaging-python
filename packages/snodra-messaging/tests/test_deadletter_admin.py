"""The dead-letter admin: list, retry, retry all and discard, and the retry's transaction.

Check: L3-DEAD-LETTERS (collection).

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control", Dead letters) and
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-3-processors (Dead letters).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from eventhelpers import OrderPlaced, dead_letter
from fakehub import FakeHub
from helpers import new_app
from snodra import App, DeadLetterError, DeadLetterReason, MemoryEventStore, MemoryStore
from snodra._deadletters import DeadLetters, RetryAllOutcome
from snodra._events import processor_marker
from snodra._eventstore import ProcessedMarker

CLOCK = 1_793_700_000_000


def _admin(app: App, store: MemoryStore, *, processors: tuple[str, ...] = ("default", "mail")) -> DeadLetters:
    return DeadLetters("collection", store, app._events, app._codec, set(processors), lambda: CLOCK)


async def _app_with(handlers: Callable[[App], None]) -> App:
    """An app, not started, whose event handlers are the ones ``handlers`` registers."""
    hub = await FakeHub.start()
    try:
        app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore())
        handlers(app)
        return app
    finally:
        await hub.stop()


async def test_a_retry_runs_stage_one_and_deletes_the_letter_with_the_marker() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    seen: list[str] = []
    calls = 0

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        seen.append(event.order_id)

    admin = _admin(hubless, store, processors=("default",))
    letter = dead_letter("3")
    await store.dead_letters.park(letter)
    outcome = await admin.retry(letter.id)
    assert (outcome.result, outcome.detail) == ("Retried", "")
    assert calls == 1
    assert seen == ["o-1"]
    assert await store.dead_letters.get(letter.id) is None
    assert await store.processed.exists("processor:default:e-3") is True
    assert await store.outbox.pending() == 0


async def test_a_retry_of_a_letter_whose_marker_exists_is_a_duplicate() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    calls = 0

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1

    admin = _admin(hubless, store, processors=("default",))
    letter = dead_letter("4")
    await store.dead_letters.park(letter)
    async with store.transaction():
        assert await store.processed.record(
            ProcessedMarker(
                id=processor_marker("default", "e-4"),
                type_name="orders.OrderPlaced",
                processed_at_ms=CLOCK,
                processor="default",
                source=letter.source,
                segment=letter.segment,
                position=letter.position,
            )
        )
    outcome = await admin.retry(letter.id)
    assert outcome.result == "Duplicate"
    assert calls == 0
    assert await store.dead_letters.get(letter.id) is None


async def test_a_failed_retry_keeps_the_letter_and_counts_the_attempt() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    calls = 0

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("still down")

    admin = _admin(hubless, store, processors=("default",))
    letter = dead_letter("5", attempts=2)
    await store.dead_letters.park(letter)
    outcome = await admin.retry(letter.id)
    assert (outcome.result, outcome.detail) == ("Failed", "still down")
    assert calls == 1
    kept = await store.dead_letters.get(letter.id)
    assert kept is not None
    assert (kept.attempts, kept.reason, kept.blocked_by) == (3, DeadLetterReason.HANDLER_FAILED, None)
    assert kept.error == DeadLetterError.new("RuntimeError", "still down")
    assert kept.last_attempt_at_ms == CLOCK
    assert await store.processed.exists("processor:default:e-5") is False, "the marker rolled back with the handler"


async def test_a_letter_whose_payload_does_not_fit_its_handler_fails_its_retry() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        raise AssertionError("the handler must not run")

    admin = _admin(hubless, store, processors=("default",))
    letter = dead_letter("6", payload=b'{"orderId":42}')
    await store.dead_letters.park(letter)
    outcome = await admin.retry(letter.id)
    assert outcome.result == "Failed"
    assert (await store.dead_letters.get(letter.id)) is not None


async def test_retry_results_blocked_not_retriable_and_not_found() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    admin = _admin(hubless, store, processors=("default",))
    head = dead_letter("1")
    follower = dead_letter("2", attempts=0)
    invalid = dead_letter("3", reason=DeadLetterReason.INVALID, event_type=None, key=None)
    for letter in (head, follower, invalid):
        await store.dead_letters.park(letter)
    blocked = await admin.retry(follower.id)
    assert (blocked.result, blocked.detail) == ("Blocked", head.id)
    not_retriable = await admin.retry(invalid.id)
    assert not_retriable.result == "NotRetriable"
    assert await store.dead_letters.get(invalid.id) == invalid, "an INVALID letter stays"
    assert (await admin.retry("default:snodra.events:0:99")).result == "NotFound"
    # A letter of a processor that does not run here is refused.
    other = dead_letter("7", processor="audit")
    await store.dead_letters.park(other)
    with pytest.raises(KeyError, match="does not run"):
        await admin.retry(other.id)


async def test_a_letter_with_no_handler_anymore_fails_its_retry() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    admin = _admin(hubless, store, processors=("default",))
    letter = dead_letter("8", event_type="orders.Gone")
    await store.dead_letters.park(letter)
    outcome = await admin.retry(letter.id)
    assert outcome.result == "Failed"
    assert "no handler for orders.Gone" in outcome.detail
    kept = await store.dead_letters.get(letter.id)
    assert kept is not None
    assert kept.error.type_name == "NoHandler"


async def test_retry_all_goes_per_key_in_position_order_and_stops_a_key_at_its_first_failure() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    calls: list[str] = []

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        calls.append(event.order_id)
        if event.order_id == "o-bad":
            raise RuntimeError("bad")

    admin = _admin(hubless, store, processors=("default",))
    base = CLOCK - 10_000
    letters = [
        dead_letter("1", key="k1", event_id="e-1", payload=b'{"orderId":"o-1"}', parked_at_ms=base + 1),
        dead_letter("2", key="k1", event_id="e-2", payload=b'{"orderId":"o-bad"}', parked_at_ms=base + 2),
        dead_letter("3", key="k1", event_id="e-3", payload=b'{"orderId":"o-3"}', parked_at_ms=base + 3),
        dead_letter("4", key="k2", event_id="e-4", payload=b'{"orderId":"o-4"}', parked_at_ms=base + 4),
        dead_letter("5", key=None, event_id="e-5", payload=b'{"orderId":"o-5"}', parked_at_ms=base + 5),
        dead_letter("6", reason=DeadLetterReason.INVALID, key=None, event_id="e-6", event_type=None),
        dead_letter("7", key="k3", event_id="e-7", payload=b'{"orderId":"o-7"}', parked_at_ms=base + 6),
    ]
    for letter in letters:
        await store.dead_letters.park(letter)
    outcome: RetryAllOutcome = await admin.retry_all("default")
    # k1: 1 retried, 2 failed (the key stops), 3 blocked; k2 retried; no key: 5 retried; INVALID skipped; k3 retried.
    assert (outcome.retried, outcome.failed, outcome.blocked, outcome.skipped) == (4, 1, 1, 1)
    assert calls == ["o-1", "o-bad", "o-4", "o-7", "o-5"]
    left = {letter.id: letter.reason for letter in await store.dead_letters.list("default", 100)}
    assert left == {
        letters[1].id: DeadLetterReason.HANDLER_FAILED,
        letters[2].id: DeadLetterReason.HANDLER_FAILED,
        letters[5].id: DeadLetterReason.INVALID,
    }
    assert (await store.dead_letters.get(letters[1].id)).attempts == 2  # type: ignore[union-attr]


async def test_discarding_a_head_unblocks_the_next_letter_of_its_key() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    admin = _admin(hubless, store, processors=("default",))
    head = dead_letter("1")
    follower = dead_letter("2")
    await store.dead_letters.park(head)
    await store.dead_letters.park(follower)
    assert (await admin.retry(follower.id)).result == "Blocked"
    assert await admin.discard(head.id) is True
    assert await admin.discard(head.id) is False
    assert (await admin.retry(follower.id)).result == "Retried"


async def test_list_count_and_a_limit_out_of_range() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    admin = _admin(hubless, store, processors=("default",))
    for position in ("3", "1", "2"):
        await store.dead_letters.park(dead_letter(position, key=None))
    assert [row.position for row in await admin.list("default", 2)] == ["1", "2"]
    assert await admin.count("default") == 3
    with pytest.raises(ValueError, match="limit must be 1 to 100"):
        await admin.list("default", 0)
    with pytest.raises(KeyError, match="does not run"):
        await admin.list("audit")


async def test_outside_collection_every_admin_call_names_the_mode() -> None:
    hubless = await _app_with(lambda app: None)
    for mode in ("event-store", "log"):
        admin = DeadLetters(mode, None, hubless._events, hubless._codec, {"default"}, lambda: CLOCK)
        with pytest.raises(ValueError, match=mode):
            await admin.list("default")
        with pytest.raises(ValueError, match=mode):
            await admin.count("default")
        with pytest.raises(ValueError, match=mode):
            await admin.retry("x")
        with pytest.raises(ValueError, match=mode):
            await admin.retry_all("default")
        with pytest.raises(ValueError, match=mode):
            await admin.discard("x")


async def test_a_retry_whose_letter_vanishes_meanwhile_commits_nothing() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    deleted = False
    calls = 0

    @hubless.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        nonlocal calls, deleted
        calls += 1
        if not deleted:
            deleted = True
            await store.dead_letters.delete(letter.id)
        # The retry's transaction then finds the letter gone and rolls the handling back.

    admin = _admin(hubless, store, processors=("default",))
    letter = dead_letter("9")
    await store.dead_letters.park(letter)
    outcome = await admin.retry(letter.id)
    assert (outcome.result, calls) == ("NotFound", 1)
    assert await store.processed.exists("processor:default:e-9") is False
    assert await store.dead_letters.get(letter.id) is None


async def test_a_replay_letter_skips_the_handlers_marked_for_no_replay() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    calls: list[str] = []

    @hubless.event_handler
    async def every_time(event: OrderPlaced) -> None:
        calls.append("every")

    @hubless.event_handler(replay=False)
    async def once(event: OrderPlaced) -> None:
        calls.append("once")

    admin = _admin(hubless, store, processors=("default",))
    replay_letter = dead_letter("1", replay=True)
    await store.dead_letters.park(replay_letter)
    assert (await admin.retry(replay_letter.id)).result == "Retried"
    assert calls == ["every"], "a replayed letter runs only its replayable handlers"
    assert await store.processed.exists("processor:default:e-1") is True

    calls.clear()
    live_letter = dead_letter("2", event_id="e-2", replay=False)
    await store.dead_letters.park(live_letter)
    assert (await admin.retry(live_letter.id)).result == "Retried"
    assert calls == ["every", "once"], "a letter parked outside a replay runs every handler"


async def test_get_refuses_a_letter_of_a_processor_that_does_not_run_here() -> None:
    hubless = await _app_with(lambda app: None)
    store = MemoryStore()
    admin = _admin(hubless, store, processors=("default", "mail"))
    asked = dead_letter("1", processor="default")
    other = dead_letter("2", processor="audit")
    await store.dead_letters.park(asked)
    await store.dead_letters.park(other)
    assert await admin.get(asked.id) is not None
    with pytest.raises(KeyError, match="does not run"):
        await admin.get(other.id)
