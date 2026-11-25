"""The service's handlers, written as an application writes them with the library's public API."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator

from snodra import App, HandlerError, RawJson, Settings, caller_matches, current_caller, current_message

from conformance_service.messages import (
    Done,
    Echo,
    EchoAnswer,
    EchoQuery,
    Emit,
    Fail,
    Guarded,
    GuardedAnswer,
    Missing,
    Outcome,
    Probe,
    Relay,
    RelayAnswer,
    Send,
    SharedQuery,
    SharedQueryAnswer,
    Sleep,
    SleepAnswer,
    SleepQuery,
    Stream,
    Strict,
    StrictAnswer,
    View,
    ViewAnswer,
    Watch,
    WatchInitial,
    WatchUpdate,
)
from conformance_service.send import Sends


def register_handlers(app: App, settings: Settings) -> Sends:
    """Registers every type of the contract the service handles so far; the asynchronous sends, to close at
    shutdown."""
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

    @app.query_handler(shared=True)  # other services may own it too
    async def on_shared_query(m: SharedQuery) -> SharedQueryAnswer:
        return SharedQueryAnswer(settings.service, instance)

    @app.stream_handler
    async def on_stream(m: Stream) -> AsyncIterator[dict[str, int]]:
        for n in range(1, m.count + 1):
            if m.delay_ms > 0:
                await asyncio.sleep(m.delay_ms / 1000)
            yield {"n": n}

    @app.subscription_handler(update=WatchUpdate)
    async def watch_updates(m: Watch) -> WatchInitial:
        return WatchInitial(m.topic, 0, instance)

    @app.command_handler
    async def on_emit(m: Emit) -> None:
        # As an application does: the emitter of its subscription handler, on the event loop.
        def matches(w: Watch) -> bool:
            return w.topic == m.topic

        for value in m.values:
            watch_updates.emit(matches, WatchUpdate(m.topic, value))
        if m.fail is not None:
            watch_updates.fail(matches, m.fail)
        elif m.complete:
            watch_updates.complete(matches)
        return None

    @app.command_handler
    async def on_fail(m: Fail) -> None:
        raise HandlerError(m.message, details=m.details)

    @app.command_handler
    async def on_strict(m: Strict) -> StrictAnswer:
        return StrictAnswer(m.count)

    if settings.jwks_url is not None:  # a guard needs the issuer's keys: without them Guarded is not served

        @app.command_handler(guard=caller_matches(lambda c: c.subject != "conformance-refused"))
        async def on_guarded(m: Guarded) -> GuardedAnswer:
            caller = current_caller()
            return GuardedAnswer(m.count, "" if caller is None else caller.subject)

    @app.query_handler
    async def on_view(m: View) -> ViewAnswer:
        rejection = app.rejection
        return ViewAnswer(
            service=settings.service,
            instance_id=instance,
            hub_ids=app.hub_ids(),
            last_rejection=None if rejection is None else f"{rejection.code}: {rejection.message}",
            subscriptions_served=app.subscriptions_served,
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

    return sends
