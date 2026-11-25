"""conformance.Send, conformance.Outcome and conformance.Relay: the service sends through the library as an application
does and reports the outcome in the contract's shape."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import replace
from typing import Any, cast

import pytest
from conformance_service import send as send_module
from conformance_service.env import settings_from_env
from conformance_service.handlers import register_handlers
from conformance_service.messages import Outcome, Send
from conformance_service.send import Sends, classify
from fakehub import FakeHub, FakeStream, ack, forward, hub_error
from helpers import SCALED, assert_no_library_tasks
from snodra import (
    App,
    ConfigurationError,
    ErrorCode,
    HubError,
    LostParts,
    NeverSent,
    OutcomeUnknown,
    RawJson,
    ReservedHeader,
    RetryPolicy,
)
from snodra._hubpb import hub_pb2

_Service = Callable[..., Coroutine[Any, Any, tuple[App, FakeStream]]]


@pytest.fixture
async def hub() -> AsyncIterator[FakeHub]:
    h = await FakeHub.start()
    try:
        yield h
    finally:
        await h.stop()


@pytest.fixture
async def service(hub: FakeHub) -> AsyncIterator[_Service]:
    apps: list[App] = []

    async def start(**env: str) -> tuple[App, FakeStream]:
        settings = settings_from_env(
            {"CONFORMANCE_SERVICE_NAME": "conformance-python", "CONFORMANCE_INSTANCE_ID": "i-1"}
            | {"CONFORMANCE_HUBS": hub.address}
            | env
        )
        app = App(settings, _timings=SCALED)
        register_handlers(app, settings)
        apps.append(app)
        await app.start()
        s = await hub.next_stream()
        await s.recv_register()
        await s.ack(ack("hub-1"))
        await app.ready(timeout=2.0)
        return app, s

    try:
        yield start
    finally:
        for app in apps:
            await app.shutdown(timeout=2.0)
        assert_no_library_tasks()


_count = 0


async def _ask(s: FakeStream, type: str, payload: object, *, kind: int = hub_pb2.COMMAND) -> str:
    """Forwards a request to the service; its id."""
    global _count
    _count += 1
    request = hub_pb2.Request(id=f"ask-{_count}", kind=kind, type=type, payload=json.dumps(payload).encode())  # type: ignore[arg-type]
    await s.send(forward(request))
    return request.id


async def _frames(s: FakeStream, answer: str) -> tuple[list[hub_pb2.Request], hub_pb2.Reply]:
    """The requests the service sends until its reply to ``answer``, and that reply."""
    sent: list[hub_pb2.Request] = []
    while True:
        m = await s.recv()
        match m.WhichOneof("message"):
            case "request":
                sent.append(m.request)
            case "reply" if m.reply.request_id == answer:
                return sent, m.reply


def _payload(r: hub_pb2.Reply) -> Any:
    assert r.WhichOneof("result") == "payload", r
    return json.loads(r.payload)


async def _next_request(s: FakeStream) -> hub_pb2.Request:
    while True:
        m = await s.recv()
        if m.WhichOneof("message") == "request":
            return m.request


def _reply(id: str, payload: object) -> hub_pb2.ServerMessage:
    return hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=id, payload=json.dumps(payload).encode()))


def _value(r: RawJson) -> Any:
    return r.value


def _part(id: str, seq: int, instance: str, payload: object = None, error: tuple[int, str] | None = None) -> Any:
    part = hub_pb2.ReplyPart(
        request_id=id, seq=seq, responder_service="conformance-runner", responder_instance=instance
    )
    if error is None:
        part.payload = json.dumps(payload).encode()
    else:
        part.error.CopyFrom(hub_pb2.Error(request_id=id, code=error[0], message=error[1]))  # type: ignore[arg-type]
    return hub_pb2.ServerMessage(part=part)


def test_classify() -> None:
    def kinds(e: BaseException) -> dict[str, Any]:
        return classify(e)

    assert kinds(NeverSent("no hub connected within 5000 ms")) == {
        "kind": "never-sent",
        "code": None,
        "message": "no hub connected within 5000 ms",
        "details": None,
    }
    assert kinds(OutcomeUnknown("x"))["kind"] == "outcome-unknown"
    assert kinds(LostParts("x"))["kind"] == "lost-parts"
    assert kinds(ReservedHeader("x"))["kind"] == "refused"
    assert kinds(ValueError("x")) == {"kind": "local", "code": None, "message": "x", "details": None}
    assert kinds(HubError(ErrorCode.HANDLER_ERROR, "insufficient stock", b'{"sku":"a"}')) == {
        "kind": "code",
        "code": "HANDLER_ERROR",
        "message": "insufficient stock",
        "details": '{"sku":"a"}',
    }
    assert kinds(HubError(ErrorCode.TIMEOUT, "late"))["details"] is None


async def test_a_unary_send_reports_the_decoded_reply(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {
            "op": "unary",
            "target": "probe",
            "payload": {"x": 1},
            "routingKey": "k-1",
            "timeoutMs": 1500,
            "headers": {"x-probe": "p-1"},
        },
    )
    probe = await _next_request(s)
    assert (probe.type, probe.kind, probe.routing_key, probe.timeout_ms, dict(probe.headers)) == (
        "conformance.runner.Probe",
        hub_pb2.COMMAND,
        "k-1",
        1500,
        {"x-probe": "p-1", "correlationId": ask},  # the handled request's id stands in for its correlation id
    )
    assert json.loads(probe.payload) == {"x": 1}
    await s.send(_reply(probe.id, {"ok": True}))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert outcome.pop("elapsedMs") >= 0
    assert outcome == {"result": "ok", "payload": {"ok": True}}

    ask = await _ask(s, "conformance.Send", {"op": "unary", "target": "probe-query"})
    probe = await _next_request(s)
    assert (probe.type, probe.kind, probe.timeout_ms) == ("conformance.runner.ProbeQuery", hub_pb2.QUERY, 0)
    await s.send(_reply(probe.id, {}))
    _, reply = await _frames(s, ask)
    assert _payload(reply)["payload"] == {}


async def test_a_failed_send_is_classified(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(s, "conformance.Send", {"op": "unary", "target": "missing"})
    probe = await _next_request(s)
    assert probe.type == "conformance.runner.Missing"
    await s.send(hub_error(probe.id, ErrorCode.NO_HANDLER, "no handler"))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert outcome["result"] == "error"
    assert outcome["error"] == {
        "kind": "code",
        "code": "NO_HANDLER",
        "message": "No handler registered for 'conformance.runner.Missing'",
        "details": None,
    }


async def test_an_async_send_and_its_outcome(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(s, "conformance.Send", {"op": "unary", "target": "probe", "async": True})
    sent, reply = await _frames(s, ask)
    op = _payload(reply)["opId"]
    assert isinstance(op, str)
    assert op
    ask = await _ask(s, "conformance.Outcome", {"opId": op, "waitMs": 0}, kind=hub_pb2.QUERY)
    more, reply = await _frames(s, ask)
    assert _payload(reply) == {"pending": True}
    (probe,) = sent + more
    await s.send(_reply(probe.id, {"done": 1}))
    ask = await _ask(s, "conformance.Outcome", {"opId": op, "waitMs": 2000}, kind=hub_pb2.QUERY)
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert (outcome["result"], outcome["payload"]) == ("ok", {"done": 1})
    ask = await _ask(s, "conformance.Outcome", {"opId": "nope"}, kind=hub_pb2.QUERY)
    _, reply = await _frames(s, ask)
    assert _payload(reply) == {"unknownOp": "nope"}


async def test_the_other_ops_are_not_supported_yet(service: _Service) -> None:
    _, s = await service()
    for op, target in (("scatter", "probe"), ("frobnicate", "probe")):
        ask = await _ask(s, "conformance.Send", {"op": op, "target": target})
        sent, reply = await _frames(s, ask)
        assert not sent
        outcome = _payload(reply)
        assert outcome["result"] == "unsupported"
        assert outcome["reason"]


async def test_scatter_goes_through_the_librarys_gather(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {"op": "scatter", "target": "probe-query", "payload": {"k": 1}, "maxResults": 2, "timeoutMs": 3000},
    )
    gather = await _next_request(s)
    assert (gather.type, gather.mode, gather.kind, gather.max_results, gather.timeout_ms, gather.routing_key) == (
        "conformance.runner.ProbeQuery",
        hub_pb2.SCATTER_GATHER,
        hub_pb2.QUERY,
        2,
        3000,
        "",
    )
    assert json.loads(gather.payload) == {"k": 1}
    await s.send(_part(gather.id, 0, "p1", {"target": "p1"}))
    await s.send(_part(gather.id, 1, "p2", error=(hub_pb2.HANDLER_ERROR, "p2 fails")))
    await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=gather.id, parts=2, failed=1, timed_out=1)))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert outcome.pop("elapsedMs") >= 0
    assert outcome == {
        "result": "ok",
        "parts": [
            {"service": "conformance-runner", "instance": "p1", "ok": True, "payload": {"target": "p1"}},
            {
                "service": "conformance-runner",
                "instance": "p2",
                "ok": False,
                "error": {"kind": "code", "code": "HANDLER_ERROR", "message": "p2 fails", "details": None},
            },
        ],
        "failed": 1,
        "timedOut": 1,
    }

    # No target at all: the gather fails as a whole.
    ask = await _ask(s, "conformance.Send", {"op": "scatter", "target": "echo-query"})
    gather = await _next_request(s)
    assert (gather.type, gather.mode) == ("conformance.EchoQuery", hub_pb2.SCATTER_GATHER)  # its own type too
    await s.send(hub_error(gather.id, ErrorCode.NO_HANDLER, "nobody"))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert (outcome["result"], outcome["error"]["code"]) == ("error", "NO_HANDLER")


async def test_shared_query_sends(service: _Service) -> None:
    _, s = await service()
    for op, mode in (("unary", hub_pb2.UNARY), ("scatter", hub_pb2.SCATTER_GATHER)):
        ask = await _ask(s, "conformance.Send", {"op": op, "target": "shared-query"})
        sent = await _next_request(s)
        assert (sent.type, sent.mode) == ("conformance.SharedQuery", mode)
        if op == "unary":
            await s.send(_reply(sent.id, {"service": "other", "instanceId": "o-1"}))
        else:
            await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=sent.id)))
        _, reply = await _frames(s, ask)
        assert _payload(reply)["result"] == "ok"


async def _reply_to(s: FakeStream, answer: str) -> hub_pb2.Reply:
    """The reply to the request answer, skipping the frames of other requests (a Cancel, a Flow, Permits)."""
    while True:
        m = await s.recv()
        if m.WhichOneof("message") == "reply" and m.reply.request_id == answer:
            return m.reply


async def _next_cancel(s: FakeStream) -> hub_pb2.Cancel:
    """The next Cancel on a stream, skipping heartbeats, statuses, Permits and Flows."""
    while True:
        m = await s.recv()
        if m.WhichOneof("message") == "cancel":
            return m.cancel


async def test_a_stream_send_reports_its_items(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {
            "op": "stream",
            "target": "probe-stream",
            "payload": {"case": "s1"},
            "initialCredit": 4,
            "maxResults": 2,
            "timeoutMs": 1500,
        },
    )
    probe = await _next_request(s)
    assert (
        probe.type,
        probe.mode,
        probe.kind,
        probe.initial_credit,
        probe.max_results,
        probe.timeout_ms,
    ) == (
        "conformance.runner.ProbeStream",
        hub_pb2.STREAMING,
        hub_pb2.QUERY,
        4,
        2,
        1500,
    )
    assert json.loads(probe.payload) == {"case": "s1"}
    await s.send(_part(probe.id, 1, "runner", {"n": 1}))
    await s.send(_part(probe.id, 2, "runner", {"n": 2}))
    await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=probe.id, parts=2)))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert outcome.pop("elapsedMs") >= 0
    assert outcome == {"result": "ok", "items": [{"n": 1}, {"n": 2}], "end": "completed"}


async def test_a_stream_send_stops_after_read_and_cancels(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {"op": "stream", "target": "probe-stream", "payload": {"case": "s2"}, "initialCredit": 4, "read": 2},
    )
    probe = await _next_request(s)
    for n in range(1, 4):
        await s.send(_part(probe.id, n, "runner", {"n": n}))
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (probe.id, "collector cancelled")
    outcome = _payload(await _reply_to(s, ask))
    assert (outcome["result"], outcome["items"], outcome["end"]) == ("ok", [{"n": 1}, {"n": 2}], "stopped")


async def test_a_subscription_send_reports_the_initial_and_the_updates(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {
            "op": "subscribe",
            "target": "probe-watch",
            "payload": {"case": "u1"},
            "initialCredit": 2,
            "maxResults": 3,
            "timeoutMs": 1500,
        },
    )
    r = await _next_request(s)
    assert (r.type, r.mode, r.kind, r.initial_credit, r.max_results, r.timeout_ms) == (
        "conformance.runner.ProbeWatch",
        hub_pb2.SUBSCRIPTION,
        hub_pb2.QUERY,
        2,
        3,
        1500,
    )
    assert json.loads(r.payload) == {"case": "u1"}
    for n in range(1, 5):  # the initial result, then three updates
        await s.send(_part(r.id, n, "runner", {"n": n}))
    await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=r.id, parts=4)))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert outcome.pop("elapsedMs") >= 0
    assert outcome == {"result": "ok", "items": [{"n": n} for n in range(1, 5)], "end": "completed"}


async def test_a_subscription_send_stops_after_read_and_cancels(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {"op": "subscribe", "target": "probe-watch", "payload": {"case": "u2"}, "initialCredit": 4, "read": 2},
    )
    r = await _next_request(s)
    for n in (1, 2):
        await s.send(_part(r.id, n, "runner", {"n": n}))
    cancel = await _next_cancel(s)
    assert (cancel.request_id, cancel.reason) == (r.id, "collector cancelled")
    outcome = _payload(await _reply_to(s, ask))
    assert (outcome["result"], outcome["items"], outcome["end"]) == (
        "ok",
        [{"n": 1}, {"n": 2}],
        "stopped",
    )


async def test_a_subscription_send_reports_lost_parts(service: _Service) -> None:
    _, s = await service()
    for case, seqs, parts in (("u5", [1, 2, 4], 4), ("u6", [1, 2], 4)):
        ask = await _ask(
            s,
            "conformance.Send",
            {"op": "subscribe", "target": "probe-watch", "payload": {"case": case}, "initialCredit": 10},
        )
        r = await _next_request(s)
        for n in seqs:
            await s.send(_part(r.id, n, "runner", {"n": n}))
        await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=r.id, parts=parts)))
        _, reply = await _frames(s, ask)
        outcome = _payload(reply)
        assert (outcome["result"], outcome["error"]["kind"]) == ("error", "lost-parts"), case


async def test_a_subscription_send_of_the_services_own_type(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(s, "conformance.Send", {"op": "subscribe", "target": "watch", "payload": {"topic": "x"}})
    r = await _next_request(s)
    assert (r.type, r.mode) == ("conformance.Watch", hub_pb2.SUBSCRIPTION)  # its own type goes through the hub too
    assert json.loads(r.payload) == {"topic": "x"}
    await s.send(hub_error(r.id, ErrorCode.NO_HANDLER, "nobody"))
    _, reply = await _frames(s, ask)
    assert _payload(reply)["error"]["code"] == "NO_HANDLER"


async def test_a_stream_send_of_the_services_own_type(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s,
        "conformance.Send",
        {"op": "stream", "target": "stream", "payload": {"count": 2, "delayMs": 5}},
    )
    r = await _next_request(s)
    assert (r.type, r.mode) == ("conformance.Stream", hub_pb2.STREAMING)  # its own type goes through the hub too
    assert json.loads(r.payload) == {"count": 2, "delayMs": 5}
    await s.send(_part(r.id, 1, "i-1", {"n": 1}))
    await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=r.id, parts=1)))
    _, reply = await _frames(s, ask)
    outcome = _payload(reply)
    assert (outcome["result"], outcome["items"], outcome["end"]) == ("ok", [{"n": 1}], "completed")


async def test_a_stream_send_reports_lost_parts(service: _Service) -> None:
    _, s = await service()
    for case, seqs, parts, items in (
        ("gap", [1, 2, 4], 4, [1, 2]),
        ("short", [1, 2, 3], 5, [1, 2, 3]),
    ):
        ask = await _ask(
            s,
            "conformance.Send",
            {"op": "stream", "target": "probe-stream", "payload": {"case": case}, "initialCredit": 10},
        )
        probe = await _next_request(s)
        for n in seqs:
            await s.send(_part(probe.id, n, "runner", {"n": n}))
        await s.send(hub_pb2.ServerMessage(end=hub_pb2.ReplyEnd(request_id=probe.id, parts=parts)))
        _, reply = await _frames(s, ask)
        outcome = _payload(reply)
        assert (outcome["result"], outcome["error"]["kind"]) == ("error", "lost-parts"), case
        assert [item["n"] for item in outcome["items"]] == items, case


async def test_relay_sends_on_and_rethrows(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(s, "conformance.Relay", {"target": "probe", "payload": {"a": 1}})
    probe = await _next_request(s)
    assert (probe.type, probe.timeout_ms, json.loads(probe.payload)) == ("conformance.runner.Probe", 0, {"a": 1})
    await s.send(_reply(probe.id, {"b": 2}))
    _, reply = await _frames(s, ask)
    assert _payload(reply) == {"reply": {"b": 2}}

    ask = await _ask(s, "conformance.Relay", {"target": "missing", "payload": {}})
    probe = await _next_request(s)
    assert probe.type == "conformance.runner.Missing"
    await s.send(hub_error(probe.id, ErrorCode.NO_HANDLER, "no handler"))
    _, reply = await _frames(s, ask)
    assert reply.WhichOneof("result") == "error"
    assert (reply.error.code, reply.error.message) == (
        hub_pb2.HANDLER_ERROR,
        "HubError: No handler registered for 'conformance.runner.Missing'",
    )


async def test_the_retry_policy_of_every_send(service: _Service) -> None:
    _, s = await service(CONFORMANCE_RETRY_MAX_ATTEMPTS="3")
    ask = await _ask(s, "conformance.View", {}, kind=hub_pb2.QUERY)
    _, reply = await _frames(s, ask)
    assert _payload(reply)["retry"] is True
    ask = await _ask(s, "conformance.Send", {"op": "unary", "target": "probe", "timeoutMs": 5000})
    first = await _next_request(s)
    await s.send(hub_error(first.id, ErrorCode.NO_HANDLER, "no handler"))
    second = await _next_request(s)
    assert second.id != first.id
    assert 0 < second.timeout_ms < 5000
    await s.send(_reply(second.id, {}))
    _, reply = await _frames(s, ask)
    assert _payload(reply)["result"] == "ok"


async def test_a_per_call_retry_policy(service: _Service) -> None:
    _, s = await service()
    ask = await _ask(
        s, "conformance.Send", {"op": "unary", "target": "probe", "retry": {"maxAttempts": 2, "initialBackoffMs": 10}}
    )
    first = await _next_request(s)
    await s.send(hub_error(first.id, ErrorCode.NO_HANDLER, "no handler"))
    second = await _next_request(s)
    await s.send(hub_error(second.id, ErrorCode.NO_HANDLER, "no handler"))
    _, reply = await _frames(s, ask)
    assert _payload(reply)["error"]["code"] == "NO_HANDLER"
    await s.nothing(0.2, "request")  # two attempts in all


def test_the_retry_variables() -> None:
    base = {"CONFORMANCE_SERVICE_NAME": "conformance-python"}
    assert settings_from_env(base).default_retry is None
    assert settings_from_env(base | {"CONFORMANCE_RETRY_MAX_ATTEMPTS": "3"}).default_retry == RetryPolicy(3, 0.1)
    assert settings_from_env(
        base | {"CONFORMANCE_RETRY_MAX_ATTEMPTS": "2", "CONFORMANCE_RETRY_INITIAL_BACKOFF_MS": "250"}
    ).default_retry == RetryPolicy(2, 0.25)
    for name, bad in (("CONFORMANCE_RETRY_MAX_ATTEMPTS", "0"), ("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS", "soon")):
        with pytest.raises(ConfigurationError, match=name):
            settings_from_env(base | {"CONFORMANCE_RETRY_MAX_ATTEMPTS": "3", name: bad})


async def test_a_send_before_any_ack_is_never_sent(hub: FakeHub) -> None:
    settings = settings_from_env({"CONFORMANCE_SERVICE_NAME": "conformance-python", "CONFORMANCE_HUBS": hub.address})
    app = App(settings, _timings=replace(SCALED, handshake_timeout=5.0))  # the stream waits for its ack
    register_handlers(app, settings)
    await app.start()
    try:
        s = await hub.next_stream()
        await s.recv_register()
        ask = await _ask(s, "conformance.Send", {"op": "unary", "target": "probe", "timeoutMs": 300})
        sent, reply = await _frames(s, ask)  # served before the ack
        assert not sent
        outcome = _payload(reply)
        assert outcome["error"]["kind"] == "never-sent"
        assert 280 <= outcome["elapsedMs"] < 600
    finally:
        await app.shutdown(timeout=2.0)
    assert_no_library_tasks()


async def test_old_outcomes_are_forgotten(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(send_module, "KEPT_OUTCOMES", 2)
    sends = Sends(cast("App", None))

    async def run(m: Send) -> dict[str, Any]:
        return {"result": "ok", "payload": m.payload, "elapsedMs": 0}

    monkeypatch.setattr(sends, "_run", run)
    ops: list[str] = []
    for n in range(4):
        answer = await sends.send(Send(op="unary", target="probe", payload=n, async_=True))
        ops.append(_value(answer)["opId"])
        await asyncio.sleep(0)  # the send runs to its end
    first = await sends.outcome(Outcome(op_id=ops[0]))
    assert first.value == {"unknownOp": ops[0]}  # the oldest finished one is forgotten
    for n in (1, 2, 3):
        assert _value(await sends.outcome(Outcome(op_id=ops[n])))["payload"] == n


async def test_close_cancels_the_sends_still_running(hub: FakeHub) -> None:
    settings = settings_from_env({"CONFORMANCE_SERVICE_NAME": "conformance-python", "CONFORMANCE_HUBS": hub.address})
    app = App(settings, _timings=SCALED)
    sends = register_handlers(app, settings)
    await app.start()
    try:
        s = await hub.next_stream()
        await s.recv_register()
        await s.ack(ack("hub-1"))
        ask = await _ask(s, "conformance.Send", {"op": "unary", "target": "probe", "timeoutMs": 5000, "async": True})
        _, reply = await _frames(s, ask)
        op = _payload(reply)["opId"]
        await _next_request(s)  # the send is in flight, never answered
        await sends.close()
        outcome = _value(await sends.outcome(Outcome(op_id=op)))
        assert (outcome["result"], outcome["error"]["message"]) == ("error", "the send was cancelled")
        assert not [t for t in asyncio.all_tasks() if t.get_name().startswith("conformance:")]
    finally:
        await app.shutdown(timeout=2.0)
    assert_no_library_tasks()
