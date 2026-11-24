"""One HTTP exchange of the library: its answer, its bounds, and the deadline of the thread that runs it."""

import asyncio
import http.client
import json
import socket
import ssl
from typing import Any
from urllib.parse import urlsplit

import pytest
from helpers import Jwks, TokenAnswer, TokenEndpoint, eventually
from snodra import _http


def _answer(r: _http.Response) -> _http.Response:
    return r


async def test_an_exchange_returns_the_answer(token_endpoint: TokenEndpoint) -> None:
    token_endpoint.script = [TokenAnswer(status=401, body=b'{"error":"invalid_client"}')]
    r = await _http.exchange(
        token_endpoint.url,
        read=_answer,
        timeout=2.0,
        max_bytes=1024,
        method="POST",
        data=b"a=1",
        headers={"X-Probe": "p-1"},
    )
    assert (r.status, json.loads(r.body)) == (401, {"error": "invalid_client"})  # an error status is an answer
    (request,) = token_endpoint.requests
    assert (request.method, request.body, request.headers["X-Probe"]) == ("POST", b"a=1", "p-1")


async def test_a_redirect_is_an_answer(jwks: Jwks) -> None:
    jwks.redirect = "/elsewhere"
    r = await _http.exchange(jwks.url, read=_answer, timeout=2.0, max_bytes=1024)
    assert r.status == 302
    assert jwks.paths == ["/jwks"]  # never followed


async def test_a_body_over_the_bound_is_too_large(token_endpoint: TokenEndpoint) -> None:
    token_endpoint.script = [TokenAnswer(body=b"x" * 101)]
    with pytest.raises(_http.TooLarge):
        await _http.exchange(token_endpoint.url, read=_answer, timeout=2.0, max_bytes=100)
    token_endpoint.script = [TokenAnswer(body=b"x" * 100)]
    assert (await _http.exchange(token_endpoint.url, read=_answer, timeout=2.0, max_bytes=100)).body == b"x" * 100


async def test_another_scheme_is_refused() -> None:
    with pytest.raises(ValueError, match="no handler"):
        await _http.exchange("ftp://127.0.0.1/x", read=_answer, timeout=2.0, max_bytes=100)


async def test_the_thread_stops_reading_at_the_deadline_by_itself(jwks: Jwks, monkeypatch: pytest.MonkeyPatch) -> None:
    jwks.dribble = True  # every byte in time for a per-read timeout, the whole far beyond the deadline
    monkeypatch.setattr(_http._Cut, "cut", lambda self: None)  # without the event loop's cut
    with pytest.raises(TimeoutError):
        await _http.exchange(jwks.url, read=_answer, timeout=0.3, max_bytes=1 << 20)
    await eventually(lambda: jwks.dribble_cut, within=1.0)  # the thread's own deadline ended the download


def test_the_connections_use_only_public_http_client_api() -> None:
    cut = _http._Cut()
    for conn in (
        _http._HTTPConnection("127.0.0.1", cut=cut),
        _http._HTTPSConnection("127.0.0.1", cut=cut, context=ssl.create_default_context()),
    ):
        assert "_create_connection" not in vars(conn)
        assert "connect" in vars(_http._Attach)  # the socket is taken in the documented connect()


def test_the_tls_socket_is_taken_before_the_handshake(monkeypatch: pytest.MonkeyPatch) -> None:
    """A peer that stalls the handshake can be cut: the TCP socket reaches the cut before the TLS wrap."""
    listener = socket.create_server(("127.0.0.1", 0))
    cut = _http._Cut()
    conn = _http._HTTPSConnection(
        "127.0.0.1", listener.getsockname()[1], cut=cut, context=ssl.create_default_context(), timeout=2.0
    )
    taken: list[bool] = []

    def spy(self: ssl.SSLContext, sock: socket.socket, *args: Any, **kwargs: Any) -> Any:
        taken.append(cut._socket is sock)  # the socket the cut holds, before the handshake
        raise OSError("the handshake is not performed here")

    monkeypatch.setattr(ssl.SSLContext, "wrap_socket", spy)
    try:
        with pytest.raises(OSError, match="the handshake is not performed here"):
            conn.connect()
    finally:
        conn.close()
        listener.close()
    assert taken == [True]


async def test_a_deadline_cut_ends_the_exchange_at_once(jwks: Jwks) -> None:
    jwks.delay = 0.35  # the answer comes after the deadline
    started = asyncio.get_running_loop().time()
    with pytest.raises(TimeoutError, match=r"^no answer within 0\.2 s$"):
        await _http.exchange(jwks.url, read=_answer, timeout=0.2, max_bytes=1024)
    assert asyncio.get_running_loop().time() - started < 0.6
    # A read nothing else bounds ends at the cut itself, and the answer written after it cannot reach the socket: the
    # server's own write fails. Without the cut this read would return that answer and no failure would be recorded.
    jwks.write_failed = False
    cut = _http._Cut()
    parts = urlsplit(jwks.url)
    conn = _http._HTTPConnection(parts.hostname or "", parts.port, cut=cut, timeout=5.0)
    try:
        await asyncio.to_thread(conn.connect)
        await asyncio.to_thread(conn.request, "GET", parts.path)
        cut.cut()  # what the exchange's deadline does
        with pytest.raises(http.client.RemoteDisconnected):
            await asyncio.to_thread(conn.getresponse)
    finally:
        conn.close()
    await eventually(lambda: jwks.write_failed, within=0.5)
