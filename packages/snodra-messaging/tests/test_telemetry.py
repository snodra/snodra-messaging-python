"""Tests of the telemetry interface and its integration with App."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import snodra
from fakehub import FakeHub, ack, hub_error
from helpers import TokenAnswer, eventually, new_app, next_reply, start_app
from snodra import JsonCodec, NoTelemetry, RetryPolicy, TokenFile, message_type
from snodra._credentials import ClientCredentialsSource, TokenError
from snodra._errors import HubError, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._telemetry import request_outcome


def coded(code: int) -> HubError:
    return error_from_wire(hub_pb2.Error(code=code), "x.Y")  # type: ignore[arg-type]


def test_request_outcome_labels() -> None:
    cases = {
        "ok": None,
        "no_handler": coded(1),
        "timeout": coded(2),
        "handler_disconnected": coded(3),
        "handler_error": coded(4),
        "denied": coded(5),
        "invalid_request": coded(6),
        "resource_exhausted": coded(8),
        "cancelled": coded(9),
        "unavailable": snodra.OutcomeUnknown("x"),
        "error": RuntimeError("x"),
    }
    for want, exc in cases.items():
        assert request_outcome(exc) == want
    assert request_outcome(coded(7)) == "denied"
    assert request_outcome(snodra.NeverSent("x")) == "unavailable"
    assert request_outcome(asyncio.CancelledError()) is None


@message_type("orders.PlaceOrder")
@dataclass
class PlaceOrder:
    order_id: str


@message_type("orders.OrderPlaced")
@dataclass
class OrderPlaced:
    order_id: str


@message_type("orders.GetOrder")
@dataclass
class GetOrder:
    order_id: str


@dataclass
class RequestRecord:
    kind: str
    type_: str
    outcome: str
    seconds: float


class TelemetryRecorder(NoTelemetry):
    def __init__(self) -> None:
        self.requests: list[RequestRecord] = []
        self.token_fetches: list[str] = []
        self.certificate_rotations: int = 0
        self.credentials_refreshes: int = 0
        self.gauge_reader: Any = None
        self.injected: list[dict[str, str]] = []
        self.inject_callback: Any = None

    def request_done(self, kind: str, type_: str, outcome: str, seconds: float) -> None:
        self.requests.append(RequestRecord(kind, type_, outcome, seconds))

    def token_fetched(self, result: str) -> None:
        self.token_fetches.append(result)

    def certificate_rotated(self) -> None:
        self.certificate_rotations += 1

    def credentials_refresh_sent(self) -> None:
        self.credentials_refreshes += 1

    def observe_gauges(self, read: Any) -> None:
        self.gauge_reader = read

    def inject(self, headers: dict[str, str]) -> None:
        self.injected.append(dict(headers))
        if self.inject_callback:
            self.inject_callback(headers)


async def test_one_request_record_per_call(hub: FakeHub) -> None:
    rec = TelemetryRecorder()
    app = await start_app(hub, telemetry=rec)
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))

    # Send a request with retry policy; first two attempts get error, third succeeds
    policy = RetryPolicy(max_attempts=3, initial_backoff=0.01)

    async def answer() -> None:
        # First attempt -> RESOURCE_EXHAUSTED
        m1 = await s.recv_request()
        await s.send(hub_error(m1.id, 8, "busy"))
        # Second attempt -> RESOURCE_EXHAUSTED
        m2 = await s.recv_request()
        await s.send(hub_error(m2.id, 8, "busy"))
        # Third attempt -> Success
        m3 = await s.recv_request()
        await s.send(
            hub_pb2.ServerMessage(
                reply=hub_pb2.Reply(
                    request_id=m3.id,
                    payload=JsonCodec().encode(OrderPlaced("o-1")),
                )
            )
        )

    task = asyncio.create_task(answer())
    result = await app.send(PlaceOrder("o-1"), OrderPlaced, retry=policy)
    await task
    assert result == OrderPlaced("o-1")

    # Retried twice, recorded once
    assert len(rec.requests) == 1
    r = rec.requests[0]
    assert r.kind == "command"
    assert r.type_ == "orders.PlaceOrder"
    assert r.outcome == "ok"
    assert r.seconds > 0


async def test_token_fetches_are_counted(hub: FakeHub, token_endpoint: Any) -> None:
    rec = TelemetryRecorder()
    token_endpoint.script = [TokenAnswer(status=200, token="jwt-1")]
    creds = snodra.ClientCredentials(token_url=token_endpoint.url, client_id="orders-service", client_secret="sec")

    # App start fetches token and calls token_fetched("ok")
    await start_app(hub, credentials=creds, telemetry=rec)
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))
    assert "ok" in rec.token_fetches

    # Test all 5 results through ClientCredentialsSource wired to rec.token_fetched
    source = ClientCredentialsSource(creds, timeout=0.5, on_token_fetched=rec.token_fetched)

    # 1. ok
    token_endpoint.script = [TokenAnswer(status=200, token="jwt-2")]
    tok = await source.token()
    assert tok.value == "jwt-2"
    assert rec.token_fetches.count("ok") == 2

    # 2. http_error (500)
    token_endpoint.script = [TokenAnswer(status=500, body=b"internal error")]
    with pytest.raises(TokenError):
        await source.token()
    assert "http_error" in rec.token_fetches

    # 3. oauth_error ({"error": "invalid_client"})
    token_endpoint.script = [TokenAnswer(status=401, error="invalid_client")]
    with pytest.raises(TokenError):
        await source.token()
    assert "oauth_error" in rec.token_fetches

    # 4. bad_response (no access_token)
    token_endpoint.script = [TokenAnswer(status=200, body=b"{}")]
    with pytest.raises(TokenError):
        await source.token()
    assert "bad_response" in rec.token_fetches

    # 5. io_error (refused connection)
    bad_creds = snodra.ClientCredentials(
        token_url="http://127.0.0.1:1/token", client_id="orders-service", client_secret="sec"
    )
    bad_source = ClientCredentialsSource(bad_creds, timeout=0.5, on_token_fetched=rec.token_fetched)
    with pytest.raises(TokenError):
        await bad_source.token()
    assert "io_error" in rec.token_fetches


async def test_gauges_read_the_live_state(hub: FakeHub, hub2: FakeHub) -> None:
    rec = TelemetryRecorder()
    app = new_app([hub, hub2], telemetry=rec)

    started_handler = asyncio.Event()
    release_handler = asyncio.Event()

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        started_handler.set()
        await release_handler.wait()

    await app.start()
    s1 = await hub.next_stream()
    await s1.recv_register()
    await s1.ack(ack("hub-1"))

    s2 = await hub2.next_stream()
    await s2.recv_register()
    await s2.ack(ack("hub-2"))

    assert rec.gauge_reader is not None
    await eventually(lambda: rec.gauge_reader().connected_hubs == 2)

    # Before any handler runs: connected_hubs 2, handlers_in_flight 0
    g0 = rec.gauge_reader()
    assert g0.connected_hubs == 2
    assert g0.handlers_in_flight == 0

    # observe_gauges may call app.hub_ids() without a deadlock
    ids = app.hub_ids()
    assert sorted(ids) == ["hub-1", "hub-2"]

    # Send request to trigger handler
    await s1.send(
        hub_pb2.ServerMessage(
            request=hub_pb2.Request(
                id="req-1",
                kind=hub_pb2.COMMAND,
                type="orders.PlaceOrder",
                payload=JsonCodec().encode(PlaceOrder("o-1")),
            )
        )
    )
    await started_handler.wait()

    # While one handler sleeps: handlers_in_flight 1, connected_hubs 2
    g1 = rec.gauge_reader()
    assert g1.connected_hubs == 2
    assert g1.handlers_in_flight == 1

    # Release handler
    release_handler.set()
    await next_reply(s1)

    # After shutdown: read() returns zeros; a start() racing shutdown() registers nothing after stop
    await app.shutdown()
    g2 = rec.gauge_reader()
    assert g2.connected_hubs == 0
    assert g2.handlers_in_flight == 0

    rec2 = TelemetryRecorder()
    app_racing = new_app(hub, telemetry=rec2)
    await app_racing.shutdown()
    # start() after shutdown raises RuntimeError and does not call observe_gauges
    with pytest.raises(RuntimeError):
        await app_racing.start()
    assert rec2.gauge_reader is None


async def test_inject_may_replace_the_forwarded_traceparent(hub: FakeHub) -> None:
    rec = TelemetryRecorder()
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    parent_id = "00f067aa0ba902b7"
    incoming_tp = f"00-{trace_id}-{parent_id}-01"

    app = new_app(hub, telemetry=rec)

    # Handler receives a request and sends an outbound command
    child_span_id = "1122334455667788"
    valid_child_tp = f"00-{trace_id}-{child_span_id}-01"

    sent_headers: dict[str, str] = {}

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        # Outbound call
        await app.send(GetOrder("o-1"), None)

    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))

    # Test 1: inject replaces the traceparent with a valid child traceparent
    def inject_valid(headers: dict[str, str]) -> None:
        headers["traceparent"] = valid_child_tp
        headers["tracestate"] = "v=1"

    rec.inject_callback = inject_valid

    # Send incoming request to handler
    await s.send(
        hub_pb2.ServerMessage(
            request=hub_pb2.Request(
                id="req-in",
                kind=hub_pb2.COMMAND,
                type="orders.PlaceOrder",
                payload=JsonCodec().encode(PlaceOrder("o-1")),
                headers={"traceparent": incoming_tp},
            )
        )
    )
    m = await s.recv_request()
    assert m.type == "orders.GetOrder"
    sent_headers.update(dict(m.headers))
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=m.id, payload=b"{}")))

    reply = await next_reply(s)
    assert reply.request_id == "req-in"

    # Valid child traceparent traveled
    assert sent_headers["traceparent"] == valid_child_tp
    assert sent_headers["tracestate"] == "v=1"

    # Test 2: an invalid injected traceparent is dropped, original kept
    sent_headers.clear()

    def inject_invalid(headers: dict[str, str]) -> None:
        headers["traceparent"] = "invalid-traceparent"

    rec.inject_callback = inject_invalid

    await s.send(
        hub_pb2.ServerMessage(
            request=hub_pb2.Request(
                id="req-in-2",
                kind=hub_pb2.COMMAND,
                type="orders.PlaceOrder",
                payload=JsonCodec().encode(PlaceOrder("o-1")),
                headers={"traceparent": incoming_tp},
            )
        )
    )
    m2 = await s.recv_request()
    assert m2.type == "orders.GetOrder"
    sent_headers.update(dict(m2.headers))
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=m2.id, payload=b"{}")))

    reply2 = await next_reply(s)
    assert reply2.request_id == "req-in-2"

    # Invalid was dropped, incoming traceparent was forwarded
    assert sent_headers["traceparent"] == incoming_tp


async def test_certificate_rotation_is_counted(hub: FakeHub) -> None:
    rec = TelemetryRecorder()
    app = new_app(hub, telemetry=rec)
    # First certificate presented: not counted as rotation
    app._cert_presented("fp-1")
    assert rec.certificate_rotations == 0

    # Renewed certificate presented: counted as 1 rotation
    app._cert_presented("fp-2")
    assert rec.certificate_rotations == 1

    # A second hub rolls over to it (same fingerprint): not counted again
    app._cert_presented("fp-2")
    assert rec.certificate_rotations == 1

    # An older pair is presented (reconnect with old): not counted
    app._cert_presented("fp-1")
    assert rec.certificate_rotations == 1

    # A third renewed pair is presented: counted as second rotation
    app._cert_presented("fp-3")
    assert rec.certificate_rotations == 2


async def test_credentials_refreshes_are_counted(hub: FakeHub, tmp_path: Path) -> None:
    rec = TelemetryRecorder()
    file = tmp_path / "hub-token"
    file.write_text("token-initial\n")

    await start_app(hub, credentials=TokenFile(str(file)), telemetry=rec)
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", interval_ms=100))

    assert rec.credentials_refreshes == 0

    # Modify token file
    file.write_text("token-renewed\n")
    c = await s.recv_credentials(within=2.0)
    assert c.token == "token-renewed"

    await eventually(lambda: rec.credentials_refreshes == 1)
    assert rec.credentials_refreshes == 1


class BuggyTelemetry(NoTelemetry):
    def request_done(self, kind: str, type_: str, outcome: str, seconds: float) -> None:
        raise RuntimeError("boom: request_done")

    def token_fetched(self, result: str) -> None:
        raise RuntimeError("boom: token_fetched")

    def certificate_rotated(self) -> None:
        raise RuntimeError("boom: certificate_rotated")

    def credentials_refresh_sent(self) -> None:
        raise RuntimeError("boom: credentials_refresh_sent")

    def observe_gauges(self, read: Any) -> None:
        raise RuntimeError("boom: observe_gauges")

    def inject(self, headers: dict[str, str]) -> None:
        raise RuntimeError("boom: inject")

    def handling(self, h: Any) -> Any:
        class BuggyContext:
            def __enter__(self) -> None:
                raise RuntimeError("boom: __enter__")

            def __exit__(self, *args: Any) -> None:
                raise RuntimeError("boom: __exit__")

        return BuggyContext()


async def test_buggy_telemetry_does_not_crash(hub: FakeHub, token_endpoint: Any) -> None:
    token_endpoint.script = [TokenAnswer(status=200, token="jwt-1")]
    creds = snodra.ClientCredentials(token_url=token_endpoint.url, client_id="orders-service", client_secret="sec")

    telemetry = BuggyTelemetry()
    app = new_app(hub, credentials=creds, telemetry=telemetry)

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> OrderPlaced:
        # Outbound call within handler
        await app.send(GetOrder("o-out"), None)
        return OrderPlaced("o-1")

    # 1. start() must not crash even if observe_gauges raises
    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1"))

    # 2. certificate_rotated must not crash
    app._cert_presented("fp-1")
    app._cert_presented("fp-2")  # triggers certificate_rotated() which raises

    # 3. Request dispatch must not crash even if handling.__enter__ / __exit__ raises
    await s.send(
        hub_pb2.ServerMessage(
            request=hub_pb2.Request(
                id="req-buggy",
                kind=hub_pb2.COMMAND,
                type="orders.PlaceOrder",
                payload=JsonCodec().encode(PlaceOrder("o-1")),
            )
        )
    )

    # Answering the outbound send from handler (inject raises, request_done raises)
    m = await s.recv_request()
    assert m.type == "orders.GetOrder"
    await s.send(hub_pb2.ServerMessage(reply=hub_pb2.Reply(request_id=m.id, payload=b"{}")))

    # Reply arrives back to hub from handler
    reply = await next_reply(s)
    assert reply.request_id == "req-buggy"
    assert JsonCodec().decode(reply.payload, OrderPlaced) == OrderPlaced("o-1")
