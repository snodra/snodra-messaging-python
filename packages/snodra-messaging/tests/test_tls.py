"""TLS to the hubs and the client certificate: the operator's CA and server name, the certificate checked at start and
presented, a renewed pair rolled over to one hub at a time, and the hub's close at the certificate's end.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Credentials").
"""

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import grpc
import pytest
from certs import Ca, KeyPair, ServerTls, make_ca
from cryptography import x509
from fakehub import FakeHub, FakeStream, ack, forward
from helpers import SCALED, Placed, PlaceOrder, eventually, new_app, next_reply, start_app
from snodra import ClientCertificate, ConfigurationError, TLSConfig
from snodra._hubpb import hub_pb2
from snodra._tls import roll_delay_bound

HUB_NAME = "hub.test"


@pytest.fixture
def ca() -> Ca:
    return make_ca("operators")


@pytest.fixture
async def tls_hub(ca: Ca) -> AsyncIterator[FakeHub]:
    """A fake hub serving TLS with a certificate of ``ca`` for hub.test (mutual TLS: client certificates of ``ca``)."""
    h = await FakeHub.start(tls=ServerTls(ca.issue_server(HUB_NAME), ca))
    try:
        yield h
    finally:
        await h.stop()


@pytest.fixture
async def tls_hub2(ca: Ca) -> AsyncIterator[FakeHub]:
    h = await FakeHub.start(tls=ServerTls(ca.issue_server(HUB_NAME), ca))
    try:
        yield h
    finally:
        await h.stop()


def _ca_file(tmp_path: Path, ca: Ca) -> str:
    path = tmp_path / f"{ca.name}.pem"
    path.write_bytes(ca.cert_pem)
    return str(path)


def _write_pair(tmp_path: Path, pair: KeyPair) -> ClientCertificate:
    cert, key = tmp_path / "client.pem", tmp_path / "client-key.pem"
    key.write_bytes(pair.key_pem)
    cert.write_bytes(pair.cert_pem)
    return ClientCertificate(str(cert), str(key))


def _write_key(files: ClientCertificate, pair: KeyPair) -> None:
    Path(files.key_file).write_bytes(pair.key_pem)


def _uri_sans(c: x509.Certificate) -> list[str]:
    return c.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(
        x509.UniformResourceIdentifier
    )


async def _registered(hub: FakeHub, interval_ms: int = 3000) -> FakeStream:
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", interval_ms=interval_ms))
    return s


async def test_tls_with_the_operators_ca(tmp_path: Path) -> None:
    ca = make_ca("operators")
    hub = await FakeHub.start(tls=ServerTls(ca.issue_server(HUB_NAME), None))
    try:
        app = await start_app(hub, tls=TLSConfig(_ca_file(tmp_path, ca), HUB_NAME))
        s = await hub.next_stream()
        assert (await s.recv_register()).service_name == "orders-service"
        assert s.peer_certificates == []  # no client certificate asked for, none given
        await s.ack(ack("hub-1"))
        await app.ready(timeout=2.0)
    finally:
        await hub.stop()


async def test_tls_refuses_another_ca_or_name(tmp_path: Path) -> None:
    ca, other = make_ca("operators"), make_ca("another")
    hub = await FakeHub.start(tls=ServerTls(ca.issue_server(HUB_NAME), None))
    try:
        app = await start_app(hub, tls=TLSConfig(_ca_file(tmp_path, other), HUB_NAME))
        await hub.no_stream_within(2.0)  # the hub's certificate is not of the trusted CA
        await app.shutdown()
        app = await start_app(hub, tls=TLSConfig(_ca_file(tmp_path, ca), "other.test"))
        await hub.no_stream_within(2.0)  # nor for the name to verify
        await app.shutdown()
    finally:
        await hub.stop()


async def test_the_client_certificate_is_presented(tls_hub: FakeHub, ca: Ca, tmp_path: Path) -> None:
    await start_app(
        tls_hub,
        tls=TLSConfig(_ca_file(tmp_path, ca), HUB_NAME),
        client_certificate=_write_pair(tmp_path, ca.issue_client("orders-service")),
    )
    s = await tls_hub.next_stream()
    register = await s.recv_register()
    assert not register.HasField("credentials")  # the TLS handshake is the credential
    assert _uri_sans(s.peer_certificates[0]) == ["snodra://service/orders-service"]


async def test_a_certificate_for_another_service_is_refused_at_start(tls_hub: FakeHub, ca: Ca, tmp_path: Path) -> None:
    tls = TLSConfig(_ca_file(tmp_path, ca), HUB_NAME)
    with pytest.raises(ConfigurationError, match="billing-service") as e:
        new_app(tls_hub, tls=tls, client_certificate=_write_pair(tmp_path, ca.issue_client("billing-service")))
    assert "orders-service" in str(e.value)
    expired = ca.issue_client("orders-service", not_after=datetime.now(UTC) - timedelta(minutes=1))
    with pytest.raises(ConfigurationError, match="expired"):
        new_app(tls_hub, tls=tls, client_certificate=_write_pair(tmp_path, expired))
    pair, stranger = ca.issue_client("orders-service"), ca.issue_client("orders-service")
    mixed = _write_pair(tmp_path, pair)
    _write_key(mixed, stranger)
    with pytest.raises(ConfigurationError, match="does not match"):
        new_app(tls_hub, tls=tls, client_certificate=mixed)
    with pytest.raises(ConfigurationError, match="SNODRA_SERVER_TLS_CLIENT_CERT"):
        new_app(tls_hub, tls=tls, client_certificate=ClientCertificate(str(tmp_path / "none.pem"), mixed.key_file))


async def test_a_renewed_certificate_is_presented_before_not_after(
    tls_hub: FakeHub, tls_hub2: FakeHub, ca: Ca, tmp_path: Path
) -> None:
    first = ca.issue_client("orders-service")
    files = _write_pair(tmp_path, first)
    app = new_app(
        [tls_hub, tls_hub2],
        tls=TLSConfig(_ca_file(tmp_path, ca), HUB_NAME),
        client_certificate=files,
        _timings=replace(SCALED, cert_check_interval=0.05, roll_delay_max=0.0),
    )

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> None:
        return None

    await app.start()
    old1, old2 = await _registered(tls_hub, 100), await _registered(tls_hub2, 100)
    assert old1.peer_certificates[0].serial_number == first.certificate.serial_number

    renewed = ca.issue_client("orders-service")
    _write_pair(tmp_path, renewed)
    started = time.monotonic()
    # One hub at a time: the first hub is told this instance leaves it, then the stream is half-closed...
    register = await old1.recv_register(within=1.0)
    assert list(register.command_types) == []
    assert (await old1.recv()).WhichOneof("message") == "heartbeat"
    await old1.half_closed(within=1.0)
    # ...and a new stream presents the renewed pair, while the second hub keeps its stream as it is.
    new1 = await tls_hub.next_stream(within=1.0)
    assert time.monotonic() - started < 1.0
    assert new1.peer_certificates[0].serial_number == renewed.certificate.serial_number
    assert list((await new1.recv_register()).command_types) == ["orders.PlaceOrder"]
    await old2.nothing(0.2, "register")  # not before the first hub's new stream is registered
    await new1.ack(ack("hub-1", interval_ms=100))
    # Then the second hub.
    assert list((await old2.recv_register(within=1.0)).command_types) == []
    await old2.half_closed(within=1.0)
    new2 = await tls_hub2.next_stream(within=1.0)
    assert new2.peer_certificates[0].serial_number == renewed.certificate.serial_number
    await new2.recv_register()
    await new2.ack(ack("hub-2", interval_ms=100))
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    await new1.nothing(0.3, "register")  # nothing more to roll
    assert app.rejection is None


async def test_no_request_is_lost_while_a_hub_rolls(tls_hub: FakeHub, ca: Ca, tmp_path: Path) -> None:
    release = asyncio.Event()
    started = asyncio.Event()
    app = new_app(
        tls_hub,
        tls=TLSConfig(_ca_file(tmp_path, ca), HUB_NAME),
        client_certificate=_write_pair(tmp_path, ca.issue_client("orders-service")),
        _timings=replace(SCALED, cert_check_interval=0.05, roll_delay_max=0.0),
    )

    @app.command_handler
    async def place_order(cmd: PlaceOrder) -> Placed:
        started.set()
        await release.wait()
        return Placed(cmd.order_id, 1)

    await app.start()
    old = await _registered(tls_hub, 100)
    request = hub_pb2.Request(
        id="c-1", kind=hub_pb2.COMMAND, type="orders.PlaceOrder", payload=b'{"orderId":"o-1","lines":[]}'
    )
    await old.send(forward(request))
    await asyncio.wait_for(started.wait(), 1.0)
    _write_pair(tmp_path, ca.issue_client("orders-service"))
    assert list((await old.recv_register(within=1.0)).command_types) == []  # the roll began
    await old.nothing(0.3, "reply")  # the request is still served: the stream is not half-closed under it
    release.set()
    assert (await next_reply(old)).request_id == "c-1"  # answered on the stream it came on
    await old.half_closed(within=1.0)  # then the roll goes on
    new = await tls_hub.next_stream(within=1.0)
    await new.recv_register()


async def test_certificate_expired_is_an_ordinary_reconnect(tls_hub: FakeHub, ca: Ca, tmp_path: Path) -> None:
    app = await start_app(
        tls_hub,
        tls=TLSConfig(_ca_file(tmp_path, ca), HUB_NAME),
        client_certificate=_write_pair(tmp_path, ca.issue_client("orders-service")),
        reconnect_backoff=(0.05, 1.0),
        _timings=replace(SCALED, cert_check_interval=60.0),  # only a connect reads the files here
    )
    s = await _registered(tls_hub)
    renewed = ca.issue_client("orders-service")
    _write_pair(tmp_path, renewed)
    closed = time.monotonic()
    await s.close(grpc.StatusCode.UNAUTHENTICATED, "certificate expired")
    s = await tls_hub.next_stream()
    assert time.monotonic() - closed < 0.5  # at the floor (0.05 s), not a refusal's 0.8 to 1 s
    assert s.peer_certificates[0].serial_number == renewed.certificate.serial_number  # the files read again
    assert app.rejection is None
    await s.recv_register()
    await s.ack(ack("hub-1"))
    closed = time.monotonic()
    await s.close(grpc.StatusCode.UNAUTHENTICATED, "client revoked")  # any other UNAUTHENTICATED close is a refusal
    s = await tls_hub.next_stream()
    assert time.monotonic() - closed >= 0.75
    assert app.rejection is not None
    assert (app.rejection.code, app.rejection.message) == ("UNAUTHENTICATED", "client revoked")


def test_roll_delay_bound() -> None:
    # The random delay before a roll: at most 30 s, at most a tenth of what the presented certificate has left.
    assert (roll_delay_bound(30, 100), roll_delay_bound(30, 3600)) == (10, 30)
    assert roll_delay_bound(30, -5) == 0
