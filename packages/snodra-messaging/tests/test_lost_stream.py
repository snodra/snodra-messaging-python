"""A hub stream lost: the streaming handler it carried is cancelled, the subscriptions it carried are removed, and the
streams and subscriptions this instance sent through it fail as an unknown outcome.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("A lost stream").
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator

import grpc
import pytest
from fakehub import FakeHub, forward
from helpers import (
    ListOrders,
    Noop,
    Placed,
    WatchInitial,
    WatchOrders,
    WatchUpdate,
    acknowledged,
    eventually,
    new_app,
)
from snodra import App, OutcomeUnknown
from snodra._hubpb import hub_pb2

WHY = "hub stream closed: UNAVAILABLE (conformance: stream lost)"


def _stream_request(id: str = "lost-1#1") -> hub_pb2.ServerMessage:
    return forward(
        hub_pb2.Request(
            id=id,
            kind=hub_pb2.QUERY,
            type="orders.ListOrders",
            payload=b'{"orderId":"o-1"}',
            mode=hub_pb2.STREAMING,
            initial_credit=1,
        )
    )


def _subscription_target(id: str = "lost-2#1") -> hub_pb2.ServerMessage:
    return forward(
        hub_pb2.Request(
            id=id,
            kind=hub_pb2.QUERY,
            type="orders.WatchOrders",
            payload=b'{"topic":"lost"}',
            mode=hub_pb2.SUBSCRIPTION,
            initial_credit=1,
        )
    )


async def _read_stream(app: App) -> list[Placed]:
    """A streaming query of the app, read to its end."""
    items: list[Placed] = []
    async with app.stream(ListOrders("o-1"), Placed, initial_credit=1) as stream:
        async for item in stream:
            items.append(item)
    return items


async def _read_subscription(app: App) -> list[WatchUpdate]:
    """A subscription of the app, read to its end."""
    updates: list[WatchUpdate] = []
    async with app.subscribe(WatchOrders("t"), WatchInitial, WatchUpdate, initial_credit=1) as sub:
        async for update in sub:
            updates.append(update)
    return updates


async def test_a_lost_stream_ends_what_arrived_on_it(hub: FakeHub) -> None:
    stopped = asyncio.Event()
    app = new_app(hub, max_concurrent=1)  # the streaming handler holds the instance's only handler slot

    @app.stream_handler
    async def stream_orders(q: ListOrders) -> AsyncIterator[Placed]:
        try:
            yield Placed(q.order_id, 1)
            await asyncio.Event().wait()
        finally:
            stopped.set()

    register = app.subscription_handler(update=WatchUpdate)

    @register
    async def updates(q: WatchOrders) -> WatchInitial:
        return WatchInitial(q.topic, 0)

    @app.command_handler
    async def noop(m: Noop) -> None:
        return None

    await app.start()
    s1 = await acknowledged(hub)

    # A streaming handler waiting for credit, and a subscription served, on stream 1.
    await s1.send(_stream_request())
    assert json.loads((await s1.recv_part()).payload) == {"orderId": "o-1", "total": 1}
    await s1.send(_subscription_target())
    await eventually(lambda: app.subscriptions_served == 1)

    # A stream and a subscription this instance sends through stream 1.
    stream_call = asyncio.create_task(_read_stream(app))
    sent_stream = await s1.recv_request()
    part = hub_pb2.ReplyPart(request_id=sent_stream.id, seq=1, payload=b'{"orderId":"o-1","total":1}')
    await s1.send(hub_pb2.ServerMessage(part=part))
    subscription_call = asyncio.create_task(_read_subscription(app))
    sent_subscription = await s1.recv_request()
    initial = hub_pb2.ReplyPart(request_id=sent_subscription.id, seq=1, payload=b'{"topic":"t","value":0}')
    await s1.send(hub_pb2.ServerMessage(part=initial))

    # The hub goes away with everything of stream 1.
    await s1.close(grpc.StatusCode.UNAVAILABLE, "conformance: stream lost")
    await asyncio.wait_for(stopped.wait(), 2.0)  # the waiting streaming handler was cancelled
    await eventually(lambda: app.subscriptions_served == 0)  # and the subscription removed
    assert updates.emit(lambda w: True, WatchUpdate("lost", 1)) == 0

    with pytest.raises(OutcomeUnknown, match=re.escape(WHY)):
        await stream_call
    with pytest.raises(OutcomeUnknown, match=re.escape(WHY)):
        await subscription_call
    assert not app._sender.pending  # neither waits for a frame any more

    # A request on the new stream runs: with capacity 1, the lost stream's handler released its slot.
    s2 = await acknowledged(hub, "hub-2")
    await s2.send(forward(hub_pb2.Request(id="c-1", kind=hub_pb2.COMMAND, type="orders.Noop", payload=b"{}")))
    assert (await s2.recv_reply()).request_id == "c-1"
    # And nothing of the lost stream's work is answers on the new one.
    await s2.nothing(0.2, "part", "end")
