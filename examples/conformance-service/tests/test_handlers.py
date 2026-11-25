"""The conformance service's handlers answer as the contract's table says, through a fake hub."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import replace

import grpc
import pytest
from conformance_service.env import settings_from_env
from conformance_service.handlers import register_handlers
from fakehub import FakeHub, FakeStream, ack, forward
from helpers import SCALED, Jwks, assert_no_library_tasks
from snodra import App, Settings
from snodra._hubpb import hub_pb2


@pytest.fixture
async def hub() -> AsyncIterator[FakeHub]:
    h = await FakeHub.start()
    try:
        yield h
    finally:
        await h.stop()


@pytest.fixture
async def service(hub: FakeHub) -> AsyncIterator[App]:
    settings = Settings(
        service="conformance-python", instance="i-1", hubs=(hub.address,), reconnect_backoff=(0.05, 0.2)
    )
    app = App(settings, _timings=SCALED)
    register_handlers(app, settings)
    await app.start()
    try:
        yield app
    finally:
        await app.shutdown(timeout=2.0)
        assert_no_library_tasks()


async def _stream(hub: FakeHub, hub_id: str = "hub-1") -> FakeStream:
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack(hub_id))
    return s


async def _ask(
    s: FakeStream, type: str, payload: object, *, kind: int = hub_pb2.COMMAND, headers: dict[str, str] | None = None
) -> hub_pb2.Reply:
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    request = hub_pb2.Request(id=f"{type}-1", kind=kind, type=type, payload=body, headers=headers or {})  # type: ignore[arg-type]
    await s.send(forward(request))
    while True:
        m = await s.recv()
        if m.WhichOneof("message") == "reply":
            assert m.reply.request_id == request.id
            return m.reply


def _answer(r: hub_pb2.Reply) -> object:
    assert r.WhichOneof("result") == "payload", r
    return json.loads(r.payload)


async def test_the_register_lists_the_contracts_unary_types(hub: FakeHub, service: App) -> None:
    r = await (await hub.next_stream()).recv_register()
    assert list(r.command_types) == [  # no conformance.Guarded without a JWKS
        "conformance.Done",
        "conformance.Echo",
        "conformance.Emit",
        "conformance.Fail",
        "conformance.Relay",
        "conformance.Send",
        "conformance.Sleep",
        "conformance.Strict",
    ]
    assert list(r.query_types) == [
        "conformance.EchoQuery",
        "conformance.Outcome",
        "conformance.SleepQuery",
        "conformance.Stream",
        "conformance.View",
        "conformance.Watch",
    ]
    assert list(r.shared_query_types) == ["conformance.SharedQuery"]
    assert list(r.subscription_query_types) == ["conformance.Watch"]


async def _frames_until(s: FakeStream, reply_id: str) -> tuple[list[hub_pb2.ReplyPart], list[hub_pb2.ReplyEnd]]:
    """Every part and end written before the reply to reply_id (Permits and the like are skipped)."""
    parts: list[hub_pb2.ReplyPart] = []
    ends: list[hub_pb2.ReplyEnd] = []
    while True:
        m = await s.recv()
        match m.WhichOneof("message"):
            case "part":
                parts.append(m.part)
            case "end":
                ends.append(m.end)
            case "reply" if m.reply.request_id == reply_id:
                return parts, ends


async def _send(s: FakeStream, id: str, type: str, payload: object, *, kind: int = hub_pb2.COMMAND) -> str:
    await s.send(forward(hub_pb2.Request(id=id, kind=kind, type=type, payload=json.dumps(payload).encode())))  # type: ignore[arg-type]
    return id


async def test_watch_emits_updates_then_ends(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    # A subscription target, and its initial result as the hub's separate unary request ending in #0.
    await s.send(
        forward(
            hub_pb2.Request(
                id="watch-1#1",
                kind=hub_pb2.QUERY,
                type="conformance.Watch",
                payload=b'{"topic":"a"}',
                mode=hub_pb2.SUBSCRIPTION,
                initial_credit=1,
            )
        )
    )
    await _send(s, "watch-1#0", "conformance.Watch", {"topic": "a"}, kind=hub_pb2.QUERY)
    initial = await s.recv_reply()
    assert (initial.request_id, json.loads(initial.payload)) == (
        "watch-1#0",
        {"topic": "a", "value": 0, "instanceId": "i-1"},
    )
    assert service.subscriptions_served == 1

    emit = await _send(s, "emit-1", "conformance.Emit", {"topic": "a", "values": [1, 2]})
    parts, ends = await _frames_until(s, emit)
    assert not ends
    assert [(p.request_id, p.seq) for p in parts] == [("watch-1#1", 1), ("watch-1#1", 2)]
    assert [json.loads(p.payload)["value"] for p in parts] == [1, 2]

    emit = await _send(s, "emit-2", "conformance.Emit", {"topic": "a", "complete": True})
    _, ends = await _frames_until(s, emit)
    assert [(e.request_id, e.parts, e.HasField("error")) for e in ends] == [("watch-1#1", 2, False)]
    assert service.subscriptions_served == 0

    # Another subscription fails with the emitter's failure: HANDLER_ERROR with the text.
    await s.send(
        forward(
            hub_pb2.Request(
                id="watch-2#1",
                kind=hub_pb2.QUERY,
                type="conformance.Watch",
                payload=b'{"topic":"b"}',
                mode=hub_pb2.SUBSCRIPTION,
                initial_credit=1,
            )
        )
    )
    emit = await _send(s, "emit-3", "conformance.Emit", {"topic": "b", "fail": "the feed closed"})
    _, ends = await _frames_until(s, emit)
    assert [(e.request_id, e.error.code, e.error.message) for e in ends] == [
        ("watch-2#1", hub_pb2.HANDLER_ERROR, "the feed closed")
    ]


async def test_view_reports_the_subscriptions_served(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    for id, topic in (("watch-1#1", "x"), ("watch-2#1", "y")):
        await s.send(
            forward(
                hub_pb2.Request(
                    id=id,
                    kind=hub_pb2.QUERY,
                    type="conformance.Watch",
                    payload=json.dumps({"topic": topic}).encode(),
                    mode=hub_pb2.SUBSCRIPTION,
                    initial_credit=1,
                )
            )
        )
    view = _answer(await _ask(s, "conformance.View", {}, kind=hub_pb2.QUERY))
    assert isinstance(view, dict)
    assert view["subscriptionsServed"] == 2


async def test_stream_answers_count_items_one_part_at_a_time(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    request = hub_pb2.Request(
        id="stream-1#1",
        kind=hub_pb2.QUERY,
        type="conformance.Stream",
        payload=b'{"count":3,"delayMs":10}',
        mode=hub_pb2.STREAMING,
        initial_credit=2,
    )
    await s.send(forward(request))
    one, two = await s.recv_part(), await s.recv_part()
    assert [(p.request_id, p.seq) for p in (one, two)] == [("stream-1#1", 1), ("stream-1#1", 2)]
    assert [json.loads(p.payload) for p in (one, two)] == [{"n": 1}, {"n": 2}]
    await s.send(hub_pb2.ServerMessage(flow=hub_pb2.Flow(request_id="stream-1#1", credits=1)))
    three = await s.recv_part()
    end = await s.recv_end()
    assert (three.seq, json.loads(three.payload)) == (3, {"n": 3})
    assert (end.request_id, end.parts, end.HasField("error")) == ("stream-1#1", 3, False)


async def test_echo_answers_what_it_got(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    payload = {"anything": [1, "two", None]}
    for type, kind in (("conformance.Echo", hub_pb2.COMMAND), ("conformance.EchoQuery", hub_pb2.QUERY)):
        answer = _answer(await _ask(s, type, payload, kind=kind, headers={"x-probe": "p-1"}))
        assert answer == {
            "payload": payload,
            "headers": {"x-probe": "p-1"},
            "instanceId": "i-1",
            "requestId": f"{type}-1",
        }
    assert _answer(await _ask(s, "conformance.Echo", [1, 2])) == {
        "payload": [1, 2],
        "headers": {},
        "instanceId": "i-1",
        "requestId": "conformance.Echo-1",
    }


async def test_done_answers_an_empty_object(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    assert (await _ask(s, "conformance.Done", {"x": 1})).payload == b"{}"


async def test_sleep_sleeps_then_answers(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    for type, kind in (("conformance.Sleep", hub_pb2.COMMAND), ("conformance.SleepQuery", hub_pb2.QUERY)):
        answer = _answer(await _ask(s, type, {"ms": 30}, kind=kind))
        assert isinstance(answer, dict)
        assert answer["instanceId"] == "i-1"
        assert 30 <= answer["sleptMs"] < 200
    answer = _answer(await _ask(s, "conformance.Sleep", {}))
    assert isinstance(answer, dict)
    assert answer["sleptMs"] < 100


async def test_shared_query_answers_who_answered(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    answer = _answer(await _ask(s, "conformance.SharedQuery", {}, kind=hub_pb2.QUERY))
    assert answer == {"service": "conformance-python", "instanceId": "i-1"}


async def test_fail_raises_an_application_error(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    r = await _ask(s, "conformance.Fail", {"message": "it broke", "details": {"reason": "test", "n": [1]}})
    assert r.WhichOneof("result") == "error"
    assert (r.error.code, r.error.message) == (hub_pb2.HANDLER_ERROR, "it broke")
    assert json.loads(r.error.details) == {"reason": "test", "n": [1]}
    r = await _ask(s, "conformance.Fail", {"message": "no details"})
    assert (r.error.code, r.error.message, r.error.details) == (hub_pb2.HANDLER_ERROR, "no details", b"")


async def test_strict_takes_only_an_integer_count(hub: FakeHub, service: App) -> None:
    s = await _stream(hub)
    assert _answer(await _ask(s, "conformance.Strict", {"count": 3})) == {"count": 3}
    for bad in ({}, None, {"count": True}, {"count": 1.5}, b"not json"):
        r = await _ask(s, "conformance.Strict", bad)
        assert r.WhichOneof("result") == "error", bad
        assert r.error.code == hub_pb2.INVALID_REQUEST, (bad, r.error)


async def test_view_answers_the_instances_view(hub: FakeHub, service: App) -> None:
    s = await hub.next_stream()
    await s.recv_register()
    before_ack = _answer(await _ask(s, "conformance.View", {}, kind=hub_pb2.QUERY))
    assert before_ack == {
        "service": "conformance-python",
        "instanceId": "i-1",
        "hubIds": [],
        "lastRejection": None,
        "subscriptionsServed": 0,
        "retry": False,
    }
    await s.close(grpc.StatusCode.ALREADY_EXISTS, "type conformance.Echo is owned by another-service")
    s = await _stream(hub, "hub-7")
    view = _answer(await _ask(s, "conformance.View", {}, kind=hub_pb2.QUERY))
    assert isinstance(view, dict)
    assert view["hubIds"] == ["hub-7"]
    assert view["lastRejection"] == "ALREADY_EXISTS: type conformance.Echo is owned by another-service"


@pytest.fixture
async def issuer() -> AsyncIterator[Jwks]:
    jwks = await asyncio.to_thread(Jwks)
    try:
        yield jwks
    finally:
        await asyncio.to_thread(jwks.stop)


async def test_guarded_needs_a_verified_caller_other_than_the_refused_one(hub: FakeHub, issuer: Jwks) -> None:
    settings = settings_from_env(
        {
            "CONFORMANCE_SERVICE_NAME": "conformance-python",
            "CONFORMANCE_INSTANCE_ID": "i-1",
            "CONFORMANCE_HUBS": hub.address,
            "CONFORMANCE_JWKS_URL": issuer.url,
            "CONFORMANCE_ISSUER": "https://issuer.test",
        }
    )
    app = App(replace(settings, reconnect_backoff=(0.05, 0.2)), _timings=SCALED)
    register_handlers(app, settings)
    await app.start()
    try:
        s = await hub.next_stream()
        assert "conformance.Guarded" in (await s.recv_register()).command_types
        await s.ack(ack("hub-1"))

        def bearer(sub: str = "u-1") -> dict[str, str]:
            return {"authorization": f"Bearer {issuer.mint(sub)}"}

        assert _answer(await _ask(s, "conformance.Guarded", {"count": 2}, headers=bearer())) == {
            "count": 2,
            "user": "u-1",
        }
        for payload, headers, code in (
            ({"count": 2}, {}, hub_pb2.UNAUTHENTICATED),
            (b"not json", {}, hub_pb2.UNAUTHENTICATED),  # decided before decoding
            (b"not json", bearer(), hub_pb2.INVALID_REQUEST),
            ({"count": True}, bearer(), hub_pb2.INVALID_REQUEST),
            ({"count": 2}, bearer("conformance-refused"), hub_pb2.PERMISSION_DENIED),
            ({"count": 2}, {"authorization": "Bearer not-a-jwt"}, hub_pb2.UNAUTHENTICATED),
        ):
            r = await _ask(s, "conformance.Guarded", payload, headers=headers)
            assert r.WhichOneof("result") == "error", (payload, headers)
            assert r.error.code == code, (payload, headers, r.error)
    finally:
        await app.shutdown(timeout=2.0)
        assert_no_library_tasks()
