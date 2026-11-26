"""The event-store conformance kit: the nine checks on the memory store, its list and its imports."""

from __future__ import annotations

import ast
import asyncio
import sys
from pathlib import Path

import pytest
from snodra._eventstore import (
    FIRST_ASSIGNMENT_WITHIN,
    Assigned,
    OutgoingEvent,
    ProcessedMarker,
    Rejected,
    SubscriptionSpec,
)
from snodra._eventstore_memory import MemoryEventStore
from snodra.testing import EVENT_STORE_CHECKS, Check, EventStoreKit, run_event_store_conformance

KIT = Path(__file__).resolve().parents[1] / "src" / "snodra" / "testing"

# The Rust kit's ids are authoritative: this literal copies the nine level-2 event-store checks, in order, from
# `src/event_store/conformance.rs`. A missing, renamed or reordered check here fails the test.
RUST_EVENT_STORE_CHECKS = (
    "headers_round_trip",
    "same_key_arrives_in_publish_order",
    "every_processor_gets_every_event",
    "instances_share_a_processor",
    "a_joining_instance_announces_its_first_assignment",
    "an_instance_left_without_segments_closes_its_change",
    "unacked_delivery_is_redelivered",
    "redelivered_after_commit_is_skipped",
    "paused_subscription_receives_nothing_and_resumes_in_place",
)


class Processed:
    """A minimal processed-events port: a record wins once."""

    def __init__(self) -> None:
        self._ids: set[str] = set()

    async def record(self, marker: ProcessedMarker) -> bool:
        if marker.id in self._ids:
            return False
        self._ids.add(marker.id)
        return True


class MarkerStoreStub:
    """A minimal message store: only what the at-least-once check records through."""

    def __init__(self) -> None:
        self.processed = Processed()


def memory_kit(store: MemoryEventStore) -> EventStoreKit:
    return EventStoreKit("conformance.events", lambda: store, MarkerStoreStub())


@pytest.mark.parametrize(
    "check",
    [pytest.param(check, id=check.__name__) for check in EVENT_STORE_CHECKS],
)
async def test_each_check_passes_on_the_memory_store(check: Check) -> None:
    await check(memory_kit(MemoryEventStore()))


async def test_the_runner_runs_every_check_in_order() -> None:
    store = MemoryEventStore()
    await run_event_store_conformance("conformance.events", lambda: store, MarkerStoreStub())


class BrokenMemoryEventStore(MemoryEventStore):
    """A store that refuses every publish, for the runner's failure naming."""

    async def publish(self, event: OutgoingEvent) -> None:
        raise Rejected("broken for the test")


async def test_the_runner_names_a_failing_check_and_the_store() -> None:
    store = BrokenMemoryEventStore()
    with pytest.raises(AssertionError, match=r"headers_round_trip.*memory"):
        await run_event_store_conformance("conformance.events", lambda: store, MarkerStoreStub())


@pytest.mark.repo
def test_the_checked_in_check_list_is_the_reference_list() -> None:
    listed = (KIT / "checks.txt").read_text(encoding="utf-8").splitlines()
    assert listed == list(RUST_EVENT_STORE_CHECKS)
    assert [check.__name__ for check in EVENT_STORE_CHECKS] == list(RUST_EVENT_STORE_CHECKS)
    assert len(EVENT_STORE_CHECKS) == 9


def _imported_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module is not None:
            roots.add(node.module.split(".")[0])
    return roots


@pytest.mark.repo
def test_snodra_testing_imports_only_the_standard_library_and_snodra() -> None:
    allowed = set(sys.stdlib_module_names) | {"snodra"}
    paths = sorted(KIT.glob("*.py"))
    assert paths
    for path in paths:
        roots = _imported_roots(path)
        assert roots <= allowed, f"{path.name}: {sorted(roots - allowed)}"
        assert "pytest" not in roots, f"{path.name}: the kit imports no test framework"


async def test_a_new_subscription_announces_its_first_assignment_within_ten_seconds() -> None:
    assert FIRST_ASSIGNMENT_WITHIN == 10.0
    store = MemoryEventStore()
    subscription = await store.subscribe(SubscriptionSpec("conformance.first", "conformance.events"))
    try:
        first = await asyncio.wait_for(subscription.next(), FIRST_ASSIGNMENT_WITHIN)
        assert isinstance(first, Assigned)
        assert first.segments == ("0", "1", "2", "3")
    finally:
        await subscription.close()


async def test_an_instance_that_holds_no_segment_announces_an_empty_assignment() -> None:
    store = MemoryEventStore(segments=2)
    spec = SubscriptionSpec("conformance.shared", "conformance.events")
    subscriptions = [await store.subscribe(spec) for _ in range(3)]
    try:
        firsts = [
            await asyncio.wait_for(subscription.next(), FIRST_ASSIGNMENT_WITHIN) for subscription in subscriptions
        ]
        assert all(isinstance(first, Assigned) for first in firsts)
        assert firsts == [Assigned(("0",)), Assigned(("1",)), Assigned(())]
    finally:
        for subscription in subscriptions:
            await subscription.close()
