"""A subscription query this instance serves: its registration when a target request is read, the emitter's updates and
ends, the limit and the duplicate id, the Cancel and the thread rule.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Subscription", "Subscription types").
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from fakehub import FakeHub, FakeStream, ack, cancel, forward
from helpers import (
    ISSUER,
    Jwks,
    Noop,
    WatchArchive,
    WatchInitial,
    WatchOrders,
    WatchUpdate,
    acknowledged,
    eventually,
    new_app,
)
from snodra import App, Updates, authenticated
from snodra._hubpb import hub_pb2

TARGET = b'{"topic":"t"}'


def _target(id: str = "w#1", *, type: str = "orders.WatchOrders", payload: bytes = TARGET) -> hub_pb2.ServerMessage:
    """A subscription target as the hub forwards it: the sender's id with ``#n``, its mode and its payload."""
    return forward(
        hub_pb2.Request(
            id=id,
            kind=hub_pb2.QUERY,
            type=type,
            payload=payload,
            mode=hub_pb2.SUBSCRIPTION,
            initial_credit=1,
        )
    )


def _initial(id: str = "w#0", *, type: str = "orders.WatchOrders", payload: bytes = TARGET) -> hub_pb2.ServerMessage:
    """The initial request of a subscription, as the hub forwards it: a unary query whose id ends ``#0``."""
    return forward(hub_pb2.Request(id=id, kind=hub_pb2.QUERY, type=type, payload=payload))


def _watch(app: App, *, shared: bool = False) -> Updates[WatchOrders, WatchUpdate]:
    """A subscription handler of orders.WatchOrders answering an initial result of value 0."""
    register = app.subscription_handler(update=WatchUpdate, shared=shared)

    @register
    async def watch(q: WatchOrders) -> WatchInitial:
        return WatchInitial(q.topic, 0)

    return watch


async def test_every_register_declares_the_subscription_types(hub: FakeHub) -> None:
    app = new_app(hub)
    _watch(app)
    archive = app.subscription_handler(update=WatchUpdate, shared=True)

    @archive
    async def watch_archive(q: WatchArchive) -> WatchInitial:
        return WatchInitial(q.topic, 0)

    await app.start()
    s = await hub.next_stream()
    first = await s.recv_register()
    await s.ack(ack("hub-1"))
    assert list(first.subscription_query_types) == ["orders.WatchArchive", "orders.WatchOrders"]
    assert list(first.query_types) == ["orders.WatchOrders"]
    assert list(first.shared_query_types) == ["orders.WatchArchive"]

    # A later Register of the same stream declares them too (the fake hub does not ack it by itself).
    app._connections()[0].reregister(empty=False)
    again = await s.recv_register()
    assert list(again.subscription_query_types) == ["orders.WatchArchive", "orders.WatchOrders"]
    assert list(again.query_types) == ["orders.WatchOrders"]
    assert list(again.shared_query_types) == ["orders.WatchArchive"]

    # The empty Register at shutdown lists no types at all, subscription ones included.
    await app.shutdown(timeout=5.0)
    empty = await s.recv_register()
    assert not empty.command_types
    assert not empty.query_types
    assert not empty.shared_query_types
    assert not empty.subscription_query_types
    await s.half_closed(2.0)


async def test_a_subscription_registers_on_its_target_request(hub: FakeHub) -> None:
    app = new_app(hub)
    seen: list[str] = []
    register = app.subscription_handler(update=WatchUpdate)

    @register
    async def updates(q: WatchOrders) -> WatchInitial:
        seen.append(q.topic)
        return WatchInitial(q.topic, 0)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await s.send(_initial("w#0"))  # the initial result runs as a unary request
    reply = await s.recv_reply()
    assert (reply.request_id, json.loads(reply.payload)) == ("w#0", {"topic": "t", "value": 0})
    assert app.subscriptions_served == 1

    # An update emitted right after the initial result was read is written to the target it belongs to.
    assert updates.emit(lambda w: w.topic == "t", WatchUpdate("t", 1)) == 1
    part = await s.recv_part()
    assert (part.request_id, part.seq, json.loads(part.payload)) == ("w#1", 1, {"topic": "t", "value": 1})
    assert updates.complete(lambda w: w.topic == "t") == 1
    end = await s.recv_end()
    assert (end.request_id, end.parts, end.HasField("error")) == ("w#1", 1, False)
    assert app.subscriptions_served == 0
    assert seen == ["t"]


async def test_emits_before_the_admission_are_buffered(hub: FakeHub, jwks: Jwks) -> None:
    jwks.delay = 0.2  # the admission waits for the token's keys
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER)
    register = app.subscription_handler(update=WatchUpdate, guard=authenticated())

    @register
    async def updates(q: WatchOrders) -> WatchInitial:
        return WatchInitial(q.topic, 0)

    await app.start()
    s = await acknowledged(hub)
    await s.send(
        forward(
            hub_pb2.Request(
                id="w#1",
                kind=hub_pb2.QUERY,
                type="orders.WatchOrders",
                payload=TARGET,
                headers={"authorization": f"Bearer {jwks.mint()}"},
                mode=hub_pb2.SUBSCRIPTION,
                initial_credit=1,
            )
        )
    )
    await eventually(lambda: app.subscriptions_served == 1)  # registered before the admission ran
    # The admission still waits on the JWKS: the update is held, matched against the request's payload.
    assert updates.emit(lambda w: w.topic == "t", WatchUpdate("t", 7)) == 1
    await s.nothing(0.1, "part")
    part = await s.recv_part(within=3.0)
    assert (part.request_id, part.seq, json.loads(part.payload)) == ("w#1", 1, {"topic": "t", "value": 7})


async def test_cancel_removes_a_subscription(hub: FakeHub) -> None:
    app = new_app(hub)
    updates = _watch(app)
    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await eventually(lambda: app.subscriptions_served == 1)
    await s.send(cancel("w#1", "conformance"))
    await eventually(lambda: app.subscriptions_served == 0)
    assert updates.emit(lambda w: True, WatchUpdate("t", 1)) == 0
    assert updates.complete(lambda w: True) == 0
    await s.nothing(0.2, "part", "end")


async def test_the_checks_when_the_request_is_read(hub: FakeHub, jwks: Jwks) -> None:
    app = new_app(hub, jwks_url=jwks.url, issuer=ISSUER, max_subscriptions=2)
    _watch(app)
    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await s.send(_target("w#2", payload=b'{"topic":"u"}'))
    await eventually(lambda: app.subscriptions_served == 2)

    # Beyond the limit: decided when the request is read, with the subscription-specific text.
    await s.send(_target("w#3"))
    end = await s.recv_end()
    assert (end.request_id, end.parts) == ("w#3", 0)
    assert (end.error.code, end.error.message) == (hub_pb2.RESOURCE_EXHAUSTED, "subscription limit (2) reached")
    # An id already open: refused, and the open one is kept.
    await s.send(_target("w#1", payload=b'{"topic":"other"}'))
    end = await s.recv_end()
    assert (end.request_id, end.parts) == ("w#1", 0)
    assert (end.error.code, end.error.message) == (hub_pb2.INVALID_REQUEST, "subscription w#1 is already open")
    assert app.subscriptions_served == 2
    # Both are decided before the token check: an expired token still gets the same codes.
    expired = {"authorization": f"Bearer {jwks.mint(exp=time.time() - 60)}"}
    for id, payload, code, message in (
        ("w#4", TARGET, hub_pb2.RESOURCE_EXHAUSTED, "subscription limit (2) reached"),
        ("w#1", b'{"topic":"other"}', hub_pb2.INVALID_REQUEST, "subscription w#1 is already open"),
    ):
        await s.send(
            forward(
                hub_pb2.Request(
                    id=id,
                    kind=hub_pb2.QUERY,
                    type="orders.WatchOrders",
                    payload=payload,
                    headers=expired,
                    mode=hub_pb2.SUBSCRIPTION,
                    initial_credit=1,
                )
            )
        )
        end = await s.recv_end()
        assert (end.request_id, end.error.code, end.error.message) == (id, code, message)
    assert jwks.fetches == 0  # refused before any token was checked
    assert app.subscriptions_served == 2


async def test_fail_ends_with_the_error(hub: FakeHub) -> None:
    app = new_app(hub)
    watch = _watch(app)
    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await eventually(lambda: app.subscriptions_served == 1)
    assert watch.fail(lambda w: w.topic == "t", RuntimeError("feed closed")) == 1
    end = await s.recv_end()
    assert (end.request_id, end.parts) == ("w#1", 0)
    assert (end.error.code, end.error.message) == (hub_pb2.HANDLER_ERROR, "RuntimeError: feed closed")
    assert app.subscriptions_served == 0
    # A text is the message of the same failure.
    await s.send(_target("w#2"))
    await eventually(lambda: app.subscriptions_served == 1)
    assert watch.fail(lambda w: True, "the feed is gone") == 1
    end = await s.recv_end()
    assert (end.error.code, end.error.message) == (hub_pb2.HANDLER_ERROR, "the feed is gone")


async def test_a_subscription_takes_no_permit(hub: FakeHub) -> None:
    app = new_app(hub, max_concurrent=1)
    _watch(app)

    @app.command_handler
    async def noop(m: Noop) -> None:
        return None

    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await eventually(lambda: app.subscriptions_served == 1)
    # A served subscription is outside the permit ledger and holds no handler slot.
    await s.nothing(0.2, "permits")
    await s.send(forward(hub_pb2.Request(id="c-1", kind=hub_pb2.COMMAND, type="orders.Noop", payload=b"{}")))
    reply = await s.recv_reply()
    assert reply.request_id == "c-1"
    permits = await s.recv_permits()
    assert permits.count == 1  # the command's, and only it
    await s.nothing(0.2, "permits")


async def test_emit_from_another_thread_is_refused(hub: FakeHub) -> None:
    app = new_app(hub)
    watch = _watch(app)
    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await eventually(lambda: app.subscriptions_served == 1)
    for call in (
        lambda: watch.emit(lambda w: True, WatchUpdate("t", 1)),
        lambda: watch.complete(lambda w: True),
        lambda: watch.fail(lambda w: True, "gone"),
    ):
        with pytest.raises(RuntimeError, match=r"loop\.call_soon_threadsafe"):
            await asyncio.to_thread(call)
    assert app.subscriptions_served == 1  # nothing was written or ended
    await s.nothing(0.2, "part", "end")


async def test_concurrent_emit_cancel_and_complete(hub: FakeHub) -> None:
    app = new_app(hub)
    stop = asyncio.Event()
    updates = _watch(app)

    @app.command_handler
    async def noop(m: Noop) -> None:
        stop.set()  # the emitters stop when the library handles this request, not when the test reads its reply
        return None

    await app.start()
    s = await acknowledged(hub)
    count = 40
    for n in range(count):
        await s.send(_target(f"w-{n}#1", payload=json.dumps({"topic": f"t{n}"}).encode()))
    await eventually(lambda: app.subscriptions_served == count)

    sent: list[int] = []

    async def emit_for_a_while() -> None:
        reached = 0
        while not stop.is_set() and reached < 200:  # at most 2000 parts unread across the emitters
            reached += updates.emit(lambda w: True, WatchUpdate("t", reached))
            await asyncio.sleep(0.001)  # let the cancels, the completes and the sync request land between rounds
        sent.append(reached)

    emitters = [asyncio.create_task(emit_for_a_while(), name=f"test:emit-{n}") for n in range(8)]
    try:
        await asyncio.sleep(0.05)
        completed = {f"w-{n}#1" for n in range(1, count, 2)}
        assert updates.complete(lambda w: int(w.topic.removeprefix("t")) % 2 == 1) == len(completed)
        for n in range(0, count, 2):  # the other half is cancelled by the hub
            await s.send(cancel(f"w-{n}#1", "conformance"))
        await s.send(forward(hub_pb2.Request(id="c-1", kind=hub_pb2.COMMAND, type="orders.Noop", payload=b"{}")))
        parts, ended, handled = await _read(s, until_reply="c-1")
        assert handled
        await eventually(lambda: stop.is_set())
        await asyncio.wait(emitters, timeout=5.0)
        assert all(t.done() and t.exception() is None for t in emitters)
        await eventually(lambda: app.subscriptions_served == 0)
        more_parts, more_ended, _ = await _read(s)
        for id, seq in more_parts.items():
            parts[id] = max(parts.get(id, 0), seq)
        ended |= more_ended
        assert ended == completed  # every completed target ended once
        assert sum(sent) == sum(parts.values())  # every update an emitter reached was written once
        assert 0 < sum(parts.values()) <= 2000
        await s.nothing(0.3, "part", "end")  # and nothing more followed any of them
    finally:
        stop.set()
        for t in emitters:
            t.cancel()
        await asyncio.gather(*emitters, return_exceptions=True)


async def _read(
    s: FakeStream, *, until_reply: str | None = None, within: float = 0.3
) -> tuple[dict[str, int], set[str], bool]:
    """What the frames already written say about the subscription targets: the last part ``seq`` per id, the ids that
    ended, and whether the reply to ``until_reply`` was among them (then the read stops there).

    Fails when a part follows the end of its own id, when an id ends twice, and when an end counts parts that were not
    all written before it."""
    parts: dict[str, int] = {}
    ended: set[str] = set()
    while True:
        try:
            m = await s.recv(within=within)
        except AssertionError:  # nothing more written
            return parts, ended, False
        kind = m.WhichOneof("message")
        if kind == "reply":
            if until_reply is not None and m.reply.request_id == until_reply:
                return parts, ended, True
            continue
        if kind == "part":
            id_ = m.part.request_id
            assert id_ not in ended, f"a part of {id_} followed its end"
            parts[id_] = max(parts.get(id_, 0), m.part.seq)
        elif kind == "end":
            id_ = m.end.request_id
            assert id_ not in ended, f"{id_} ended twice"
            assert m.end.parts == parts.get(id_, 0), f"{id_} ended with {m.end.parts} parts, {parts.get(id_, 0)} read"
            ended.add(id_)


async def test_shutdown_ends_served_subscriptions(hub: FakeHub) -> None:
    app = new_app(hub)
    _watch(app)
    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1"))
    await eventually(lambda: app.subscriptions_served == 1)
    await app.shutdown(timeout=5.0)
    empty = await s.recv_register()  # the empty Register comes first
    assert not empty.subscription_query_types
    end = await s.recv_end()
    assert (end.request_id, end.parts, end.HasField("error")) == ("w#1", 0, False)
    assert app.subscriptions_served == 0
    await s.half_closed(2.0)  # the end was written before the half-close


async def test_the_stream_stays_up_after_a_subscriptions_mistake(hub: FakeHub) -> None:
    app = new_app(hub)
    _watch(app)
    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("w#1", type="orders.NotServed"))
    end = await s.recv_end()
    assert (end.error.code, end.error.message) == (
        hub_pb2.INVALID_REQUEST,
        "no subscription handler for orders.NotServed on this instance",
    )
    # Another subscription on the same stream is registered and served: the refusal never dropped it.
    await s.send(_target("w#2"))
    await s.send(_initial("w#2#0"))
    assert (await s.recv_reply()).request_id == "w#2#0"
    assert app.subscriptions_served == 1
