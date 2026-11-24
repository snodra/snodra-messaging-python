"""The service's handlers, written as an application writes them with the library's public API."""

from __future__ import annotations

import asyncio
import time

from snodra import App, HandlerError, RawJson, Settings, current_message

from conformance_service.messages import (
    Done,
    Echo,
    EchoAnswer,
    EchoQuery,
    Fail,
    Missing,
    Outcome,
    Probe,
    Relay,
    RelayAnswer,
    Send,
    Sleep,
    SleepAnswer,
    SleepQuery,
    Strict,
    StrictAnswer,
    View,
    ViewAnswer,
)
from conformance_service.send import Sends


def register_handlers(app: App, settings: Settings) -> None:
    """Registers every type of the contract the service handles so far."""
    instance = settings.instance
    sends = Sends(app)

    def echo(payload: object) -> EchoAnswer:
        info = current_message()
        headers = {} if info is None else dict(info.headers)
        return EchoAnswer(payload, headers, instance, "" if info is None else info.id)

    async def sleep(ms: int) -> SleepAnswer:
        started = time.monotonic()
        await asyncio.sleep(ms / 1000)
        return SleepAnswer(instance, round((time.monotonic() - started) * 1000))

    @app.command_handler
    async def on_echo(m: Echo) -> EchoAnswer:
        return echo(m.value)

    @app.query_handler
    async def on_echo_query(m: EchoQuery) -> EchoAnswer:
        return echo(m.value)

    @app.command_handler
    async def on_done(m: Done) -> None:
        return None

    @app.command_handler
    async def on_sleep(m: Sleep) -> SleepAnswer:
        return await sleep(m.ms)

    @app.query_handler
    async def on_sleep_query(m: SleepQuery) -> SleepAnswer:
        return await sleep(m.ms)

    @app.command_handler
    async def on_fail(m: Fail) -> None:
        raise HandlerError(m.message, details=m.details)

    @app.command_handler
    async def on_strict(m: Strict) -> StrictAnswer:
        return StrictAnswer(m.count)

    @app.query_handler
    async def on_view(m: View) -> ViewAnswer:
        rejection = app.rejection
        return ViewAnswer(
            service=settings.service,
            instance_id=instance,
            hub_ids=app.hub_ids(),
            last_rejection=None if rejection is None else f"{rejection.code}: {rejection.message}",
            subscriptions_served=0,
            retry=settings.default_retry is not None,
        )

    @app.command_handler
    async def on_send(m: Send) -> RawJson:
        return await sends.send(m)

    @app.query_handler
    async def on_outcome(m: Outcome) -> RawJson:
        return await sends.outcome(m)

    @app.command_handler
    async def on_relay(m: Relay) -> RelayAnswer:
        # As an application does: the library's exception propagates and the library answers it.
        target = Missing(m.payload) if m.target == "missing" else Probe(m.payload)
        reply = await app.send(target, RawJson)
        return RelayAnswer(reply.value)
