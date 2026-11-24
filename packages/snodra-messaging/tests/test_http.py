"""One HTTP exchange of the library: its answer, its bounds, and the deadline of the thread that runs it."""

import asyncio
import json
import ssl

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


async def test_a_deadline_cut_ends_the_exchange_at_once(jwks: Jwks) -> None:
    jwks.delay = 1.0
    started = asyncio.get_running_loop().time()
    with pytest.raises(TimeoutError, match=r"^no answer within 0\.2 s$"):
        await _http.exchange(jwks.url, read=_answer, timeout=0.2, max_bytes=1024)
    assert asyncio.get_running_loop().time() - started < 0.6
