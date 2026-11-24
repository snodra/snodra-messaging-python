"""One HTTP exchange of the library (the JWKS, the token endpoint): run off the event loop, never following a
redirect, bounded by one deadline for the whole exchange and by a number of bytes.

``urllib`` bounds each socket operation only, so a server that sends a byte now and then could keep a thread for
minutes. Here the deadline is the event loop's: when it passes (or the caller is cancelled) the exchange's socket is
shut down, and the thread's read ends at once. The proxy environment variables (``https_proxy``, ``http_proxy``,
``no_proxy``) apply, as for any ``urllib`` request.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import http.client
import socket
import ssl
import threading
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Response:
    """What the server answered: its status (a redirect too, never followed) and at most ``max_bytes`` of its body."""

    status: int
    body: bytes


class TooLarge(ValueError):
    """The body is larger than the exchange takes."""


async def exchange[T](
    url: str,
    *,
    read: Callable[[Response], T],
    timeout: float,  # noqa: ASYNC109 (the deadline of the whole exchange)
    max_bytes: int,
    method: str = "GET",
    data: bytes | None = None,
    headers: Mapping[str, str] | None = None,
    ca_file: str | None = None,
) -> T:
    """``read`` of the answer to a request at url (``http://`` or ``https://``), all of it (connecting, the request,
    the answer and ``read``) in a thread, within ``timeout`` seconds: ``TimeoutError`` after it. A redirect is an answer
    like any other (``read`` sees its status). ``https://`` trusts the system's certificate authorities and those of
    ``ca_file`` (PEM), read for this exchange. A body over ``max_bytes`` is :class:`TooLarge`."""
    cut = _Cut()
    try:
        async with asyncio.timeout(timeout):
            return await asyncio.to_thread(
                _exchange, url, method, data, dict(headers or {}), timeout, max_bytes, ca_file, cut, read
            )
    except TimeoutError:
        raise TimeoutError(f"no answer within {timeout:g} s") from None
    finally:
        cut.cut()  # the thread's socket, when the exchange did not end by itself


class _Cut:
    """The socket of one exchange, shut down from the event loop when its deadline passes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._socket: socket.socket | None = None
        self._cut = False

    def attach(self, sock: socket.socket) -> None:
        with self._lock:
            if self._cut:
                sock.close()
                raise TimeoutError("the exchange's deadline passed")
            self._socket = sock

    def release(self) -> None:
        with self._lock:
            self._socket = None

    def cut(self) -> None:
        with self._lock:
            self._cut = True
            sock, self._socket = self._socket, None
        if sock is not None:
            with contextlib.suppress(OSError):  # already closed
                sock.shutdown(socket.SHUT_RDWR)


def _exchange[T](
    url: str,
    method: str,
    data: bytes | None,
    headers: dict[str, str],
    timeout: float,
    max_bytes: int,
    ca_file: str | None,
    cut: _Cut,
    read: Callable[[Response], T],
) -> T:
    context = ssl.create_default_context()
    if ca_file is not None:
        context.load_verify_locations(cafile=ca_file)
    opener = urllib.request.OpenerDirector()  # no redirect handler, no error processor: every answer is returned
    for handler in (urllib.request.ProxyHandler(), _HTTPHandler(cut), _HTTPSHandler(cut, context)):
        opener.add_handler(handler)
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        response = opener.open(request, timeout=timeout)
        if response is None:
            raise ValueError(f"no handler for the URL {url}")
        with response:
            body: bytes = response.read(max_bytes + 1)
            status: int = response.status
    finally:
        cut.release()
    if len(body) > max_bytes:
        raise TooLarge(f"the answer is larger than {max_bytes} bytes")
    return read(Response(status, body))


def _attached(create: Callable[..., socket.socket], cut: _Cut) -> Callable[..., socket.socket]:
    def create_connection(*args: Any, **kwargs: Any) -> socket.socket:
        sock = create(*args, **kwargs)
        cut.attach(sock)  # the plain socket: shutting it down ends a TLS handshake or read on it too
        return sock

    return create_connection


class _HTTPConnection(http.client.HTTPConnection):
    def __init__(self, *args: Any, cut: _Cut, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._create_connection = _attached(socket.create_connection, cut)


class _HTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args: Any, cut: _Cut, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._create_connection = _attached(socket.create_connection, cut)


class _HTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, cut: _Cut) -> None:
        super().__init__()
        self._cut = cut

    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(functools.partial(_HTTPConnection, cut=self._cut), req)


class _HTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, cut: _Cut, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._cut = cut
        self._ssl = context

    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        return self.do_open(functools.partial(_HTTPSConnection, cut=self._cut), req, context=self._ssl)
