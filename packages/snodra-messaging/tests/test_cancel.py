"""The hub's ``Cancel`` of a request this instance serves: a scatter-gather target past its gather's end.

The one test of a ``Cancel`` (a unary request and a gather target alike): nothing is answered, the handler's task is
cancelled, and the stream stays up.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Cancel", "Scatter-gather").
"""

import asyncio

from fakehub import FakeHub, cancel, forward
from helpers import GetStock, Stock, acknowledged, new_app, next_reply
from snodra._hubpb import hub_pb2


def _target(id: str, sku: str) -> hub_pb2.ServerMessage:
    payload = b'{"sku":"%s"}' % sku.encode()
    return forward(hub_pb2.Request(id=id, kind=hub_pb2.QUERY, type="orders.GetStock", payload=payload))


async def test_a_cancelled_target_sends_nothing(hub: FakeHub) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()
    app = new_app(hub, max_concurrent=1)

    @app.query_handler(shared=True)
    async def get_stock(q: GetStock) -> Stock:
        if q.sku == "slow":
            started.set()
            try:
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return Stock(1)

    await app.start()
    s = await acknowledged(hub)
    await s.send(_target("g#1#0", "slow"))
    await asyncio.wait_for(started.wait(), 1.0)
    await s.send(cancel("g#1#0", "max results"))
    await asyncio.wait_for(cancelled.wait(), 1.0)  # the handler's task is cancelled
    # Past the 2 s the handler would have slept: never answered, neither a reply nor a part or an end.
    await s.nothing(2.5, "reply", "part", "end")
    assert (await s.recv_permits()).count == 1  # its permit goes back
    await s.send(cancel("g#1#0", "max results"))  # a Cancel for an id that ended is ignored
    await s.send(cancel("g#9#0", "deadline"))  # and one for an id never forwarded here
    await s.send(_target("g#2#0", "fast"))
    reply = await next_reply(s)
    assert (reply.request_id, reply.payload) == ("g#2#0", b'{"count":1}')  # the stream is up and served
