"""An orders service that publishes an event from a command and projects it in an event handler.

The command writes no database row itself: the outbox entry of ``orders.OrderPlaced`` is the only state it writes, and
the event handler projects it. ``orders.GetOrder`` answers what the projection holds. Both stores are in memory here,
so the example runs with no infrastructure; the service's own database would be an add-on's ``store=`` (for example
``snodra-messaging-mongo`` or ``snodra-messaging-sql``) and its brokers an add-on's ``event_store=``
(``snodra-messaging-kafka`` or ``snodra-messaging-rabbitmq``).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import snodra

log = logging.getLogger("events-service")


@snodra.message_type("orders.PlaceOrder")
@dataclass
class PlaceOrder:
    order_id: str  # "orderId" on the wire
    total: int


@snodra.message_type("orders.OrderPlaced")
@dataclass
class OrderPlaced:
    order_id: str
    total: int


@snodra.message_type("orders.GetOrder")
@dataclass
class GetOrder:
    order_id: str


@dataclass
class Order:
    order_id: str
    total: int


@dataclass
class Projection:
    """What the event handler has projected: the read model of the example."""

    totals: dict[str, int] = field(default_factory=dict)


def build(settings: snodra.Settings | None = None, projection: Projection | None = None) -> snodra.App:
    """The orders service, on the in-memory stores, and the projection it fills."""
    if settings is None:
        settings = snodra.Settings.from_env(service="orders-service")
    if projection is None:
        projection = Projection()
    app = snodra.App(settings, store=snodra.MemoryStore(), event_store=snodra.MemoryEventStore())

    @app.command_handler
    async def place_order(command: PlaceOrder) -> None:
        # The event commits with the command's transaction (in memory, the outbox append does it alone).
        await app.publish(OrderPlaced(order_id=command.order_id, total=command.total), key=command.order_id)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        # At least once: a redelivery is skipped by the event's stage-1 marker, so the projection is exactly once.
        projection.totals[event.order_id] = event.total

    @app.query_handler
    async def get_order(query: GetOrder) -> Order:
        return Order(order_id=query.order_id, total=projection.totals.get(query.order_id, 0))

    return app


async def run(settings: snodra.Settings | None = None) -> None:
    logging.basicConfig(level=logging.INFO)
    app = build(settings)
    await app.run()


if __name__ == "__main__":
    asyncio.run(run())
