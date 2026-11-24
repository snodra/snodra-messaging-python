"""The conformance service's handlers answer as the contract's table says, through a fake hub."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import grpc
import pytest
from conformance_service.handlers import register_handlers
from fakehub import FakeHub, FakeStream, ack, forward
from helpers import SCALED, assert_no_library_tasks
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
    assert list(r.command_types) == [
        "conformance.Done",
        "conformance.Echo",
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
        "conformance.View",
    ]


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
