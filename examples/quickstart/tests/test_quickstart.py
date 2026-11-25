from fakehub import FakeHub, ack, forward
from helpers import ISSUER, Jwks, next_reply
from orders_quickstart.service import build
from snodra._hubpb import hub_pb2

pytest_plugins = ["conftest"]


async def test_the_quickstart_serves(hub: FakeHub, jwks: Jwks) -> None:
    import snodra

    settings = snodra.Settings(
        service="orders-service",
        hubs=(hub.address,),
        jwks_url=jwks.url,
        issuer=ISSUER,
    )
    app = build(settings)
    await app.start()

    try:
        s = await hub.next_stream()
        r = await s.recv_register()
        assert list(r.command_types) == ["orders.PlaceOrder"]
        assert list(r.query_types) == ["orders.GetOrder"]
        await s.ack(ack("hub-1"))

        # Forward orders.PlaceOrder
        await s.send(
            forward(
                hub_pb2.Request(
                    id="r-1",
                    kind=hub_pb2.COMMAND,
                    type="orders.PlaceOrder",
                    payload=b'{"orderId":"o-1","items":[]}',
                )
            )
        )

        # Handler sends stock.CheckStock query to hub
        nested = await s.recv_request()
        assert nested.type == "stock.CheckStock"
        assert nested.kind == hub_pb2.QUERY
        await s.send(
            hub_pb2.ServerMessage(
                reply=hub_pb2.Reply(
                    request_id=nested.id,
                    payload=b'{"available":true}',
                )
            )
        )

        # PlaceOrder reply
        reply1 = await next_reply(s)
        assert reply1.request_id == "r-1"
        assert reply1.payload == b'{"orderId":"o-1","total":42}'

        # Forward orders.GetOrder with token with role "orders-user"
        token = jwks.mint(roles=("orders-user",))
        await s.send(
            forward(
                hub_pb2.Request(
                    id="r-2",
                    kind=hub_pb2.QUERY,
                    type="orders.GetOrder",
                    payload=b'{"orderId":"o-1"}',
                    headers={"authorization": f"Bearer {token}"},
                )
            )
        )

        # GetOrder reply
        reply2 = await next_reply(s)
        assert reply2.request_id == "r-2"
        assert reply2.payload == b'{"orderId":"o-1","status":"placed"}'
    finally:
        await app.shutdown(timeout=5.0)
