"""The event-store conformance checks: the nine level-2 checks, each named as in the reference kit.

Every check is public and runs alone on an :class:`~snodra.testing.eventstore.EventStoreKit`; the runner in
:mod:`snodra.testing.eventstore` runs them in this order. The ids are the reference kit's, one to one.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable, Sequence
from functools import partial

from snodra._eventstore import FIRST_ASSIGNMENT_WITHIN, Delivery, EventHeaders, ProcessedMarker
from snodra.testing.eventstore import (
    EVENT_TYPE,
    JOIN_WINDOW,
    QUIET,
    SLICE,
    EventStoreKit,
    Reader,
    all_until,
    both_until,
    bounded,
    event,
    fresh,
    id_of,
    ids,
    now_ms,
    publish_all,
)

type Check = Callable[[EventStoreKit], Awaitable[None]]
"""One conformance check: an async function of the kit."""


async def headers_round_trip(kit: EventStoreKit) -> None:
    """The seven headers, any other header, the key (or none) and the payload arrive as published."""
    events = kit.instance()
    keyed = event(kit, fresh("key"), extra={"x-conformance": "kept as given"})
    unkeyed = event(kit, None)
    sent = (keyed, unkeyed)
    await publish_all(events, sent)

    reader = await Reader.join(events, kit.spec())
    try:
        wanted = ids(sent)
        await reader.until("both events delivered", lambda r: r.has_all(wanted))
        for published in sent:
            got = reader.first_delivery(published.id)
            for name in EventHeaders.ALL:
                assert name in got.headers, f"header {name} delivered"
            for name, value in published.headers.items():
                assert got.headers.get(name) == value, f"header {name}"
            assert got.key == published.key, "the key"
            assert got.payload == published.payload, "the payload"
            assert got.timestamp_ms > 0, f"an append time: {got.timestamp_ms}"
    finally:
        await reader.close()


async def same_key_arrives_in_publish_order(kit: EventStoreKit) -> None:
    """Events of one key arrive in publish order, all from one segment, each once; the positions of a segment's
    deliveries increase by :meth:`EventStore.compare`."""
    events = kit.instance()
    keys = [fresh("key") for _ in range(4)]
    sent = [event(kit, key) for _ in range(10) for key in keys]
    await publish_all(events, sent)

    reader = await Reader.join(events, kit.spec())
    try:
        wanted = ids(sent)
        await reader.until("every event delivered", lambda r: r.has_all(wanted))
        got = reader.mine(wanted)
        assert len(got) == len(sent), "each event once"
        for key in keys:
            published = [outgoing.id for outgoing in sent if outgoing.key == key]
            delivered = [delivery for delivery in got if delivery.key == key]
            assert [id_of(delivery) for delivery in delivered] == published, f"key {key}: publish order"
            segments = {delivery.segment for delivery in delivered}
            assert len(segments) == 1, f"key {key}: one segment, got {sorted(segments)}"
        last: dict[str, str] = {}
        for delivery in got:
            before = last.get(delivery.segment)
            if before is not None:
                assert events.compare(before, delivery.position) < 0, (
                    f"segment {delivery.segment}: {before} before {delivery.position}"
                )
            last[delivery.segment] = delivery.position
    finally:
        await reader.close()


async def every_processor_gets_every_event(kit: EventStoreKit) -> None:
    """Two subscription names (two processors) each receive every event."""
    events = kit.instance()
    sent = [event(kit, fresh("key")) for _ in range(10)]
    await publish_all(events, sent)

    a = await Reader.join(events, kit.spec())
    try:
        b = await Reader.join(events, kit.spec())
        try:
            wanted = ids(sent)
            await both_until(
                a,
                b,
                "both processors get every event",
                lambda left, right: left.has_all(wanted) and right.has_all(wanted),
            )
        finally:
            await b.close()
    finally:
        await a.close()


async def instances_share_a_processor(kit: EventStoreKit) -> None:
    """Two instances of one subscription split its segments (adding the second spreads them, every revocation
    closed by an assignment, possibly empty) and share its events: each event goes to one of them, and both get
    some."""
    first, second = kit.instance(), kit.instance()
    spec = kit.spec()
    a = await Reader.join(first, spec)
    try:
        await a.until("the first instance holds segments", lambda r: bool(r.assigned))
        b = await Reader.join(second, spec)
        try:
            await both_until(
                a,
                b,
                "both instances hold segments, none held by both, every revocation closed by an assignment",
                lambda left, right: (
                    bool(left.assigned)
                    and bool(right.assigned)
                    and left.assigned.isdisjoint(right.assigned)
                    and not left.changing
                    and not right.changing
                ),
            )
            sent = [event(kit, fresh("key")) for _ in range(40)]
            await publish_all(first, sent)
            wanted = ids(sent)
            await both_until(
                a,
                b,
                "every event delivered",
                lambda left, right: all(left.has(event_id) or right.has(event_id) for event_id in wanted),
            )
            to_a = {id_of(delivery) for delivery in a.mine(wanted)}
            to_b = {id_of(delivery) for delivery in b.mine(wanted)}
            assert to_a, f"the first instance gets events: {len(to_b)} went to the second"
            assert to_b, f"the second instance gets events: {len(to_a)} went to the first"
            twice = to_a & to_b
            assert not twice, f"delivered to both instances: {sorted(twice)}"
        finally:
            await b.close()
    finally:
        await a.close()


async def a_joining_instance_announces_its_first_assignment(kit: EventStoreKit) -> None:
    """Instances join one subscription one at a time, one more than its segments, each reading the whole time: every
    newcomer's first event is an :class:`Assigned` (an empty one when it holds nothing, as the last one may), within
    :data:`~snodra._eventstore.FIRST_ASSIGNMENT_WITHIN` seconds of joining."""
    spec = kit.spec()
    readers: list[Reader] = []
    try:
        readers.append(await Reader.join(kit.instance(), spec))
        await readers[0].until("the first instance holds segments", lambda r: bool(r.assigned))
        await readers[0].pump(QUIET)
        segments = len(readers[0].assigned)
        while len(readers) <= segments:
            readers.append(await Reader.join(kit.instance(), spec))
            newcomer = readers[-1]
            loop = asyncio.get_running_loop()
            deadline = loop.time() + FIRST_ASSIGNMENT_WITHIN
            while newcomer.first is None:
                assert loop.time() < deadline, (
                    f"instance {len(readers)} of {segments + 1} (assigned {[sorted(r.assigned) for r in readers]}): "
                    f"its first assignment, possibly empty, within {FIRST_ASSIGNMENT_WITHIN:g} s of joining"
                )
                await asyncio.gather(*(reader.pump(SLICE) for reader in readers))
            assert newcomer.first == "Assigned", (
                f"instance {len(readers)}: the first event of a new subscription is an Assigned, got {newcomer.first}"
            )
    finally:
        for reader in readers:
            await reader.close()


async def an_instance_left_without_segments_closes_its_change(kit: EventStoreKit) -> None:
    """More instances than segments: instances join one at a time; after each join the segments are spread (none
    held twice, every one held, and once there are two instances or more no former holder keeps every segment) and
    every instance's last assignment change is closed (a :class:`Revoked` followed by an :class:`Assigned`, an empty
    one for an instance left without segments). How the segments spread beyond that is the event store's. The
    instances join until one that held segments holds none (or there are twice as many instances as segments, two
    more); a run that reaches that cap without such an instance says so on stderr, the check then proved less.
    """
    spec = kit.spec()
    readers: list[Reader] = []
    try:
        readers.append(await Reader.join(kit.instance(), spec))
        await readers[0].until("the first instance holds segments", lambda r: bool(r.assigned))
        # Alone, it holds every segment (in time: an assignment may come in parts).
        await readers[0].pump(QUIET)
        every_segment = set(readers[0].assigned)

        def settled(readers: Sequence[Reader], former: Sequence[int]) -> bool:
            held = [segment for reader in readers for segment in reader.assigned]
            unique = set(held)
            return (
                len(held) == len(unique)
                and unique == every_segment
                and (len(readers) < 2 or all(len(readers[index].assigned) < len(every_segment) for index in former))
                and all(not reader.changing for reader in readers)
            )

        cap = 2 * len(every_segment) + 2
        emptied = False
        while not emptied and len(readers) < cap:
            former = [index for index, reader in enumerate(readers) if reader.assigned]
            revoked_before = sum(reader.revoked for reader in readers)
            readers.append(await Reader.join(kit.instance(), spec))
            newcomer = readers[-1]
            loop = asyncio.get_running_loop()
            deadline = loop.time() + JOIN_WINDOW
            while sum(reader.revoked for reader in readers) == revoked_before and loop.time() < deadline:
                taken = sum(await asyncio.gather(*(reader.pump(SLICE) for reader in readers)))
                if taken == 0 and newcomer.changes > 0:
                    break
            await all_until(
                readers,
                "the segments spread, every revocation closed by an assignment",
                partial(settled, former=former),
            )
            emptied = any(not reader.assigned and reader.revoked > 0 for reader in readers)
        if not emptied:
            print(
                f"event store conformance check `an_instance_left_without_segments_closes_its_change`: {cap} "
                f"instances and none that held segments was left without any (assigned "
                f"{[sorted(reader.assigned) for reader in readers]}): the empty assignment that closes such a change "
                "was not seen",
                file=sys.stderr,
            )
        assert any(not reader.assigned for reader in readers), "more instances than segments leave one without segments"
    finally:
        for reader in readers:
            await reader.close()


async def unacked_delivery_is_redelivered(kit: EventStoreKit) -> None:
    """What a subscription did not acknowledge comes again: to the next subscription of the name after a close, and
    on a live one after :meth:`~snodra._eventstore.EventStoreSubscription.redeliver`; what it acknowledged does
    not."""
    events = kit.instance()
    spec = kit.spec()
    key = fresh("key")
    sent = [event(kit, key) for _ in range(3)]
    wanted = ids(sent)
    unacked = wanted[1:]
    await publish_all(events, sent)

    first = await Reader.join(events, spec, hold=unacked)
    try:
        await first.until("the three events delivered", lambda r: r.has_all(wanted))
    finally:
        await first.close()

    second = await Reader.join(kit.instance(), spec, hold=unacked)
    try:
        await second.until("the unacknowledged events again", lambda r: r.has_all(unacked))
        assert second.ids_in_order(wanted) == unacked, (
            "after a close: the unacknowledged ones, in order, not the acknowledged one"
        )
        again = second.first_delivery(unacked[0])
        await second.redeliver(again)
        await second.until("the unacknowledged events once more", lambda r: len(r.mine(unacked)) == 4)
        assert second.ids_in_order(wanted) == unacked + unacked, (
            "after redeliver: from the redelivered one on, in order"
        )
        last = second.first_delivery(unacked[1])
        await second.ack(last)
    finally:
        await second.close()


async def redelivered_after_commit_is_skipped(kit: EventStoreKit) -> None:
    """A crash after the handler's transaction committed and before the acknowledgement: the event comes again, its
    processed-event marker (recorded as if in that transaction, its id built from the delivered ``id`` header as the
    core builds it) makes it a duplicate, and once acknowledged it does not come again."""
    events = kit.instance()
    spec = kit.spec()
    key = fresh("key")
    handled = event(kit, key)
    await publish_all(events, [handled])
    only = [handled.id]
    processed = kit.store.processed

    def marker(delivery: Delivery) -> ProcessedMarker:
        return ProcessedMarker(
            id=f"processor:{spec.name}:{id_of(delivery)}",
            type_name=EVENT_TYPE,
            processed_at_ms=now_ms(),
            processor=spec.name,
        )

    first = await Reader.join(events, spec, hold=only)
    try:
        await first.until("the event delivered", lambda r: r.has_all(only))
        recorded = await bounded("record the first marker", processed.record(marker(first.first_delivery(handled.id))))
        assert recorded, "the first handling records its marker"
    finally:
        await first.close()  # the crash: committed, never acknowledged

    second = await Reader.join(kit.instance(), spec, hold=only)
    try:
        await second.until("the event again after the crash", lambda r: r.has_all(only))
        again = second.first_delivery(handled.id)
        recorded = await bounded("record the marker again", processed.record(marker(again)))
        assert not recorded, "the redelivered event is a duplicate"
        await second.ack(again)
    finally:
        await second.close()

    fence = event(kit, key)
    await publish_all(events, [fence])
    third = await Reader.join(kit.instance(), spec)
    try:
        await third.until("the next event of the key", lambda r: r.has(fence.id))
        assert not third.has(handled.id), "the skipped event, acknowledged, does not come again"
    finally:
        await third.close()


async def paused_subscription_receives_nothing_and_resumes_in_place(kit: EventStoreKit) -> None:
    """A paused subscription delivers nothing (paused before its first :meth:`next` too) while its assignment goes
    on; resumed, it delivers from where it stopped, each event once, in order."""
    events = kit.instance()
    key = fresh("key")
    before = [event(kit, key) for _ in range(3)]
    during = [event(kit, key) for _ in range(5)]
    before_ids, during_ids = ids(before), ids(during)
    await publish_all(events, before)

    reader = await Reader.join(events, kit.spec())
    try:
        reader.pause()
        await reader.pump(QUIET)
        assert not reader.seen, f"paused from the start: nothing delivered, got {len(reader.seen)}"
        reader.resume()
        await reader.until("the events before the pause", lambda r: r.has_all(before_ids))

        reader.pause()
        delivered = len(reader.seen)
        await publish_all(events, during)
        await reader.pump(QUIET)
        assert len(reader.seen) == delivered, "nothing delivered while paused"
        reader.resume()
        await reader.until("the events published while paused", lambda r: r.has_all(during_ids))
        every = before_ids + during_ids
        assert reader.ids_in_order(every) == every, "each once, in order"
    finally:
        await reader.close()


EVENT_STORE_CHECKS: tuple[Check, ...] = (
    headers_round_trip,
    same_key_arrives_in_publish_order,
    every_processor_gets_every_event,
    instances_share_a_processor,
    a_joining_instance_announces_its_first_assignment,
    an_instance_left_without_segments_closes_its_change,
    unacked_delivery_is_redelivered,
    redelivered_after_commit_is_skipped,
    paused_subscription_receives_nothing_and_resumes_in_place,
)
"""Every check, in the reference kit's order."""
