"""Registering command and query handlers, and the type lists every ``Register`` carries."""

from __future__ import annotations

from typing import Any

import pytest
from fakehub import FakeHub
from helpers import SCALED, CancelOrder, GetOrder, Placed, PlaceOrder, new_app, start_app
from snodra import App, RegistrationClosed, Settings


def _app() -> App:
    """An app that is never started: registration needs no hub."""
    return App(Settings(service="orders-service", hubs=("127.0.0.1:1",)), _timings=SCALED)


async def test_register_lists_the_handled_types(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        return Placed(cmd.order_id, 42)

    @app.query_handler()
    async def get_order(q: GetOrder) -> None:
        return None

    await app.start()
    r = await (await hub.next_stream()).recv_register()
    assert list(r.command_types) == ["orders.PlaceOrder"]
    assert list(r.query_types) == ["orders.GetOrder"]
    assert list(r.shared_query_types) == []
    assert list(r.subscription_query_types) == []


async def test_register_lists_are_sorted(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None: ...

    @app.command_handler
    async def cancel_order(cmd: CancelOrder) -> None: ...

    await app.start()
    r = await (await hub.next_stream()).recv_register()
    assert list(r.command_types) == ["orders.CancelOrder", "orders.PlaceOrder"]


async def test_every_register_of_every_stream_carries_the_types(hub: FakeHub) -> None:
    app = new_app(hub)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None: ...

    await app.start()
    first = await hub.next_stream()
    assert list((await first.recv_register()).command_types) == ["orders.PlaceOrder"]
    await hub.down()
    await hub.up()
    second = await hub.next_stream()
    assert list((await second.recv_register()).command_types) == ["orders.PlaceOrder"]


def test_the_decorators_return_the_function() -> None:
    app = _app()

    async def place_order(cmd: PlaceOrder) -> None: ...

    async def get_order(q: GetOrder) -> None: ...

    assert app.command_handler(place_order) is place_order
    assert app.query_handler()(get_order) is get_order


async def test_registration_after_start_is_refused(hub: FakeHub) -> None:
    app = await start_app(hub)

    async def place_order(cmd: PlaceOrder) -> None: ...

    with pytest.raises(RegistrationClosed, match="place_order"):
        app.command_handler(place_order)
    with pytest.raises(RegistrationClosed, match="place_order"):
        app.query_handler()(place_order)


async def test_registration_refuses_what_it_cannot_serve() -> None:
    app = _app()

    def not_async(cmd: PlaceOrder) -> None: ...

    with pytest.raises(TypeError, match=r"not_async.*async def"):
        app.command_handler(not_async)  # type: ignore[type-var]

    async def no_annotation(cmd):  # type: ignore[no-untyped-def]
        ...

    with pytest.raises(TypeError, match=r"no_annotation.*annotated"):
        app.command_handler(no_annotation)

    async def no_parameter() -> None: ...

    with pytest.raises(TypeError, match=r"no_parameter.*parameter"):
        app.command_handler(no_parameter)

    async def not_a_message(cmd: Placed) -> None: ...

    with pytest.raises(TypeError, match=r"not_a_message.*Placed.*message_type"):
        app.command_handler(not_a_message)

    async def unresolved(cmd: Missing) -> None:  # type: ignore[name-defined]  # noqa: F821
        ...

    with pytest.raises(TypeError, match=r"unresolved"):
        app.command_handler(unresolved)

    async def two_required(cmd: PlaceOrder, other: Any) -> None: ...

    with pytest.raises(TypeError, match=r"two_required.*other"):
        app.command_handler(two_required)

    async def place_order(cmd: PlaceOrder) -> None: ...

    async def place_order_again(cmd: PlaceOrder) -> None: ...

    app.command_handler(place_order)
    with pytest.raises(TypeError, match=r"place_order_again.*orders\.PlaceOrder.*place_order"):
        app.command_handler(place_order_again)
    with pytest.raises(TypeError, match=r"place_order_again.*orders\.PlaceOrder.*both a command and a query"):
        app.query_handler(place_order_again)

    async def get_order(q: GetOrder) -> None: ...

    app.query_handler(get_order)
    with pytest.raises(TypeError, match=r"get_order.*orders\.GetOrder.*both a command and a query"):
        app.command_handler(get_order)
    with pytest.raises(TypeError, match=r"get_order.*orders\.GetOrder"):
        app.query_handler(get_order)


def test_extra_parameters_with_defaults_are_allowed() -> None:
    app = _app()

    async def place_order(cmd: PlaceOrder, *, note: str = "") -> None: ...

    app.command_handler(place_order)
