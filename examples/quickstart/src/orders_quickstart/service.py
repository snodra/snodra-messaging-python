import asyncio
from dataclasses import dataclass

import snodra


# Messages are dataclasses (or Pydantic models). The wire name is declared once, on the class.
@dataclass
class Item:
    sku: str
    quantity: int


@snodra.message_type("orders.PlaceOrder")
@dataclass
class PlaceOrder:
    order_id: str  # "orderId" on the wire
    items: list[Item]


@dataclass
class OrderReceipt:  # a result has no wire name of its own
    order_id: str
    total: int


@snodra.message_type("orders.GetOrder")
@dataclass
class GetOrder:
    order_id: str


@dataclass
class Order:
    order_id: str
    status: str


@snodra.message_type("stock.CheckStock")
@dataclass
class CheckStock:
    items: list[Item]


@dataclass
class Stock:
    available: bool


def build(settings: snodra.Settings | None = None) -> snodra.App:
    if settings is None:
        settings = snodra.Settings.from_env(service="orders-service")
    app = snodra.App(settings)

    # snippet:handler begin
    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> OrderReceipt:
        # A request sent while a request is handled carries its correlation id, the caller's token and the trace on.
        stock = await app.query(CheckStock(items=cmd.items), Stock)
        if not stock.available:
            raise snodra.HandlerError("out of stock", details={"orderId": cmd.order_id})
        return OrderReceipt(order_id=cmd.order_id, total=42)

    # snippet:handler end

    @app.query_handler(guard=snodra.role("orders-user"))
    async def get_order(q: GetOrder) -> Order:
        _caller = snodra.current_caller()
        return Order(order_id=q.order_id, status="placed")

    return app


async def run(settings: snodra.Settings | None = None) -> None:
    app = build(settings)
    await app.run()


if __name__ == "__main__":
    asyncio.run(run())
