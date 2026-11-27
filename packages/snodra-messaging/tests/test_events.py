"""Publishing events: the seven headers, the transaction, and the startup rules."""

from __future__ import annotations

import asyncio
import json
import re
import uuid

import pytest
from eventhelpers import OrderPlaced, OrderShipped, RecordingEventStore, SlowOutbox, delivery
from fakehub import FakeHub
from helpers import TP, library_tasks, new_app, start_app
from snodra import (
    ConfigurationError,
    JsonCodec,
    MemoryEventStore,
    MemoryStore,
    RegistrationClosed,
    ResetContext,
    Settings,
)
from snodra._events import event_handling, event_message, event_timestamp, is_json_object, outgoing_event
from snodra._events import outbox_entry as build_entry
from snodra._eventstore import EventHeaders


async def test_a_published_event_carries_the_seven_headers(hub: FakeHub) -> None:
    store = MemoryStore()
    events = RecordingEventStore()
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    await app.publish(OrderPlaced(order_id="o-1", total=42), key="o-1")
    await asyncio.wait_for(events.published.wait(), 2.0)
    (event,) = events.events
    headers = event.headers
    assert event.source == "snodra.events"
    assert event.key == "o-1"
    assert event.type_name == "orders.OrderPlaced"
    assert json.loads(event.payload) == {"orderId": "o-1", "total": 42}
    assert headers[EventHeaders.TYPE] == "orders.OrderPlaced"
    assert headers[EventHeaders.ID] == event.id
    uuid.UUID(headers[EventHeaders.ID])
    assert headers[EventHeaders.SOURCE] == "orders-service"
    uuid.UUID(headers[EventHeaders.CORRELATION_ID])
    assert headers[EventHeaders.USER_ID] == ""
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", headers[EventHeaders.TIMESTAMP])
    # No handling carries no trace context: the six other headers and no invented traceparent.
    assert EventHeaders.TRACEPARENT not in headers


async def test_inside_an_event_handler_the_headers_carry_its_correlation_user_and_trace() -> None:
    headers = {
        EventHeaders.TYPE: "orders.OrderPlaced",
        EventHeaders.ID: "e-1",
        EventHeaders.CORRELATION_ID: "c-9",
        EventHeaders.USER_ID: "u-7",
        EventHeaders.TRACEPARENT: TP,
    }
    info = event_message("e-1", "orders.OrderPlaced", headers)
    with event_handling(info, "u-7"):
        entry = build_entry(OrderShipped(order_id="o-1"), key=None, seq=1, codec=JsonCodec())
        event = outgoing_event(entry, source="snodra.events", service="orders-service")
    assert entry.headers[EventHeaders.CORRELATION_ID] == "c-9"
    assert entry.headers[EventHeaders.USER_ID] == "u-7"
    assert entry.headers[EventHeaders.TRACEPARENT] == TP
    assert event.headers[EventHeaders.CORRELATION_ID] == "c-9"
    assert event.headers[EventHeaders.USER_ID] == "u-7"
    assert event.headers[EventHeaders.TRACEPARENT] == TP
    assert event.headers[EventHeaders.TYPE] == "orders.OrderShipped"
    assert event.headers[EventHeaders.ID] == entry.id
    assert event.headers[EventHeaders.SOURCE] == "orders-service"


async def test_an_event_without_a_correlation_id_uses_its_own_id() -> None:
    info = event_message("e-1", "orders.OrderPlaced", {EventHeaders.TYPE: "orders.OrderPlaced", EventHeaders.ID: "e-1"})
    assert info.correlation_id == "e-1"
    with event_handling(info, ""):
        entry = build_entry(OrderShipped(order_id="o-1"), key=None, seq=1, codec=JsonCodec())
    assert entry.headers[EventHeaders.CORRELATION_ID] == "e-1"


async def test_an_empty_handled_correlation_id_gets_a_fresh_one() -> None:
    info = event_message(
        "e-1",
        "orders.OrderPlaced",
        {
            EventHeaders.TYPE: "orders.OrderPlaced",
            EventHeaders.ID: "e-1",
            EventHeaders.CORRELATION_ID: "",
        },
    )
    with event_handling(info, ""):
        entry = build_entry(OrderShipped(order_id="o-1"), key=None, seq=1, codec=JsonCodec())
    correlation = entry.headers[EventHeaders.CORRELATION_ID]
    assert correlation != ""
    uuid.UUID(correlation)


async def test_a_published_event_carries_the_callers_user_and_the_requests_correlation(hub: FakeHub) -> None:
    from types import MappingProxyType

    from snodra._context import _CALLER, _CURRENT, MessageInfo
    from snodra._identity import Caller

    store = MemoryStore()
    events = RecordingEventStore()
    app = await start_app(hub, store=store, event_store=events, outbox_poll_interval=0.01)
    info = MessageInfo("r-1", "orders.PlaceOrder", "c-1", MappingProxyType({EventHeaders.TRACEPARENT: TP}))
    current = _CURRENT.set(info)
    calling = _CALLER.set(Caller("u-9", (), {}))
    try:
        await app.publish(OrderPlaced(order_id="o-1"))
    finally:
        _CALLER.reset(calling)
        _CURRENT.reset(current)
    await asyncio.wait_for(events.published.wait(), 2.0)
    headers = events.events[0].headers
    assert headers[EventHeaders.USER_ID] == "u-9"
    assert headers[EventHeaders.CORRELATION_ID] == "c-1"
    assert headers[EventHeaders.TRACEPARENT] == TP


async def test_a_publish_joins_the_transaction_of_the_context(hub: FakeHub) -> None:
    store = MemoryStore()
    app = new_app(hub, store=store, event_store=RecordingEventStore())
    async with store.transaction():
        await app.publish(OrderPlaced(order_id="o-1"))
        assert await store.outbox.pending() == 0
    assert await store.outbox.pending() == 1

    async def fail() -> None:
        async with store.transaction():
            await app.publish(OrderPlaced(order_id="o-2"))
            raise RuntimeError("the handler failed")

    with pytest.raises(RuntimeError):
        await fail()
    assert await store.outbox.pending() == 1


async def test_a_cancellation_of_a_publish_stays_a_cancelled_error(hub: FakeHub) -> None:
    store = MemoryStore()
    store.outbox = SlowOutbox(store.outbox, 0.5)
    app = new_app(hub, store=store, event_store=RecordingEventStore())
    task = asyncio.create_task(app.publish(OrderPlaced(order_id="o-1")))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


async def test_an_event_handler_without_a_message_store_fails_at_start(hub: FakeHub) -> None:
    app = new_app(hub, event_store=MemoryEventStore())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    with pytest.raises(ConfigurationError, match=r"message store.*store="):
        await app.start()


async def test_an_event_handler_without_an_event_store_fails_at_start(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore())

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    with pytest.raises(ConfigurationError, match=r"event store.*event_store="):
        await app.start()


async def test_the_first_publish_names_the_missing_message_store(hub: FakeHub) -> None:
    app = new_app(hub)
    with pytest.raises(ConfigurationError, match=r"message store.*store="):
        await app.publish(OrderPlaced(order_id="o-1"))


async def test_the_first_publish_names_the_missing_event_store(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore())
    with pytest.raises(ConfigurationError, match=r"event store.*event_store="):
        await app.publish(OrderPlaced(order_id="o-1"))


async def test_a_service_with_only_commands_queries_and_subscriptions_starts_without_stores(hub: FakeHub) -> None:
    app = await start_app(hub)
    assert app.hub_ids() == []  # a hub stream is opened in the background
    assert "snodra:outbox" not in library_tasks()
    assert not any(name.startswith("snodra:consumer:") for name in library_tasks())


async def test_turning_the_outbox_off_starts_no_relay(hub: FakeHub) -> None:
    app = await start_app(hub, store=MemoryStore(), event_store=MemoryEventStore(), outbox_enabled=False)
    await asyncio.sleep(0.02)
    assert "snodra:outbox" not in library_tasks()
    assert app.hub_ids() == []


async def test_turning_the_consumer_off_starts_no_consumer(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore(), consumer_enabled=False)

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    await app.start()
    await asyncio.sleep(0.02)
    assert not any(name.startswith("snodra:consumer:") for name in library_tasks())
    assert "snodra:outbox" in library_tasks()


async def test_an_event_handler_registered_after_start_is_refused(hub: FakeHub) -> None:
    app = await start_app(hub, store=MemoryStore(), event_store=MemoryEventStore())
    with pytest.raises(RegistrationClosed, match="registered before the app starts"):

        @app.event_handler
        async def on_placed(event: OrderPlaced) -> None:
            pass


async def test_an_event_handler_must_be_an_async_function_taking_a_message(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore())
    with pytest.raises(TypeError, match="async def"):

        @app.event_handler  # type: ignore[type-var]
        def not_async(event: OrderPlaced) -> None:
            pass

    with pytest.raises(TypeError, match="not a message"):

        @app.event_handler
        async def not_a_message(event: object) -> None:
            pass


async def test_the_processor_name_and_replay_flag_are_checked(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore())
    for processor in ("", "Default", "mail_", "a" * 49):
        with pytest.raises(ValueError, match="processor name"):

            @app.event_handler(processor=processor)
            async def bad(event: OrderPlaced) -> None:
                pass

    with pytest.raises(TypeError, match="replay"):

        @app.event_handler(replay=1)  # type: ignore[call-overload, untyped-decorator]
        async def bad_replay(event: OrderPlaced) -> None:
            pass

    @app.event_handler(processor="default", replay=False)
    async def accepted(event: OrderPlaced) -> None:
        pass

    assert app._events.handlers("default", "orders.OrderPlaced")[0].replay is False


def test_the_timestamp_header_is_rfc3339_in_utc_with_milliseconds() -> None:
    assert event_timestamp(0) == "1970-01-01T00:00:00.000Z"
    assert event_timestamp(1_793_700_000_123) == "2026-11-03T10:00:00.123Z"


def test_the_outbox_lease_must_be_longer_than_a_send() -> None:
    with pytest.raises(ConfigurationError, match="outbox_lease"):
        Settings(service="orders-service", outbox_lease=40.0)
    assert Settings(service="orders-service", outbox_lease=41.0).outbox_lease == 41.0


async def test_an_event_payload_no_store_holds_is_refused_naming_the_type(hub: FakeHub) -> None:
    from snodra._store import InvalidMessage

    app = new_app(hub, store=MemoryStore(), event_store=RecordingEventStore())
    with pytest.raises(InvalidMessage, match=r"orders\.OrderPlaced"):
        await app.publish(OrderPlaced(order_id="o-1", total=2**63))


def test_a_delivery_without_a_correlation_id_names_the_event_in_the_message_info() -> None:
    info = event_message("e-1", "t.E", {})
    assert (info.id, info.type, info.correlation_id) == ("e-1", "t.E", "e-1")
    entry = build_entry(OrderPlaced(order_id="o-1"), key=None, seq=1, codec=JsonCodec())
    # Outside any handling, without a context, a fresh correlation id is written.
    assert entry.headers[EventHeaders.CORRELATION_ID] != ""


def test_a_delivery_helper_omits_none_headers() -> None:
    event = delivery(event_id="e-2", headers={"correlationId": None})
    assert EventHeaders.ID in event.headers
    assert EventHeaders.CORRELATION_ID not in event.headers


def test_is_json_object_refuses_the_constants_json_has_not() -> None:
    assert is_json_object(b'{"n":1}')
    assert is_json_object(b' {"n":{}} ')
    assert not is_json_object(b"[]")
    assert not is_json_object(b'{"n":NaN}')
    assert not is_json_object(b'{"n":Infinity}')
    assert not is_json_object(b'{"n":-Infinity}')
    assert not is_json_object(b"\xef\xbb\xbf{}")


async def test_reset_handlers_run_per_processor_and_are_checked(hub: FakeHub) -> None:
    app = new_app(hub, store=MemoryStore(), event_store=MemoryEventStore())

    @app.reset_handler
    async def bare() -> None:
        pass

    @app.reset_handler()
    async def called(context: ResetContext) -> None:
        pass

    @app.reset_handler("mail")
    async def named(context: ResetContext) -> None:
        pass

    assert [handler.fn for handler in app._events.reset_handlers("default")] == [bare, called]
    assert [handler.fn for handler in app._events.reset_handlers("mail")] == [named]
    assert [handler.wants_context for handler in app._events.reset_handlers("default")] == [False, True]

    with pytest.raises(TypeError, match="ResetContext"):

        @app.reset_handler
        async def wrong(first: int) -> None:
            pass

    with pytest.raises(TypeError, match="ResetContext or nothing"):

        @app.reset_handler
        async def two(first: ResetContext, second: ResetContext) -> None:
            pass

    with pytest.raises(TypeError, match="async def"):

        @app.reset_handler  # type: ignore[type-var]
        def sync() -> None:
            pass

    with pytest.raises(ValueError, match="processor name"):

        @app.reset_handler("Mail")
        async def bad_name() -> None:
            pass


async def test_a_reset_handler_registered_after_start_is_refused(hub: FakeHub) -> None:
    app = await start_app(hub, store=MemoryStore(), event_store=MemoryEventStore(), outbox_enabled=False)
    with pytest.raises(RegistrationClosed, match="registered before the app starts"):

        @app.reset_handler
        async def too_late() -> None:
            pass
