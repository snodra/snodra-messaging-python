from events_service.service import Projection, build
from fakehub import FakeHub, ack, forward
from helpers import eventually, next_reply
from snodra._hubpb import hub_pb2

pytest_plugins = ["conftest"]


async def test_the_events_service_publishes_and_projects(hub: FakeHub) -> None:
    import snodra

    projection = Projection()
    app = build(
        snodra.Settings(service="orders-service", hubs=(hub.address,)),
        projection,
    )
    await app.start()
    try:
        stream = await hub.next_stream()
        await stream.recv_register()
        await stream.ack(ack("hub-1"))
        await stream.send(
            forward(
                hub_pb2.Request(
                    id="r-1",
                    kind=hub_pb2.COMMAND,
                    type="orders.PlaceOrder",
                    payload=b'{"orderId":"o-1","total":42}',
                )
            )
        )
        reply = await next_reply(stream)
        assert reply.request_id == "r-1"
        assert reply.payload == b"{}"
        await eventually(lambda: projection.totals == {"o-1": 42})
        await stream.send(
            forward(
                hub_pb2.Request(
                    id="r-2",
                    kind=hub_pb2.QUERY,
                    type="orders.GetOrder",
                    payload=b'{"orderId":"o-1"}',
                )
            )
        )
        reply = await next_reply(stream)
        assert reply.request_id == "r-2"
        assert reply.payload == b'{"orderId":"o-1","total":42}'
    finally:
        await app.shutdown(timeout=5.0)
