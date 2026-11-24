"""One HTTP exchange of the library (the JWKS, the token endpoint): run off the event loop, never following a
redirect, bounded by one deadline for the whole exchange and by a number of bytes.

``urllib`` bounds each socket operation only, so a server that sends a byte now and then could keep a thread for
minutes. Here the deadline belongs to the exchange, and both the event loop and the thread enforce it: the thread
reads in short slices (the socket's timeout is shortened for it) and, once the deadline passed, shuts the socket down
and stops reading by itself, whether or not the loop is still there to cut it; the loop, when its ``asyncio.timeout``
passes first, cuts the same socket at once. The proxy environment variables (``https_proxy``, ``http_proxy``,
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
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final

_SLICE: Final = 0.1
"""How long a socket read of the exchange's thread may block before it looks at its deadline again."""

_CHUNK: Final = 1 << 16
"""How much of the body the exchange's thread reads at a time."""


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
    ``ca_file`` (PEM), read for this exchange. A body over ``max_bytes`` is :class:`TooLarge`.

    A URL of another scheme is refused here, before any handler or proxy sees it: ``ftp_proxy`` and ``all_proxy`` must
    not turn it into an HTTP proxy request."""
    scheme = urllib.parse.urlsplit(url).scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"no handler for a URL of scheme {scheme!r}: an exchange is http:// or https://")
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
    """The socket of one exchange, shut down by the event loop when its deadline passes, and the deadline the thread
    reads against."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._socket: socket.socket | None = None
        self._cut = False
        self.deadline = float("inf")

    def attach(self, sock: socket.socket) -> None:
        with self._lock:
            if self._cut:
                sock.close()
                raise TimeoutError("the exchange's deadline passed")
            self._socket = sock

    def release(self) -> None:
        with self._lock:
            self._socket = None

    def retime(self, seconds: float) -> None:
        """The attached socket's read timeout (how long one read blocks before the thread looks at the deadline)."""
        with self._lock:
            sock = self._socket
        if sock is not None:
            with contextlib.suppress(OSError):  # already closed
                sock.settimeout(max(0.001, seconds))

    @property
    def cut_off(self) -> bool:
        """The deadline passed: the socket was shut down, or is to be."""
        return self._cut

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
    cut.deadline = time.monotonic() + timeout
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
            status: int = response.status
            body = _read_body(response, max_bytes, cut)
    finally:
        cut.release()
    return read(Response(status, body))


def _read_body(response: http.client.HTTPResponse, max_bytes: int, cut: _Cut) -> bytes:
    """At most ``max_bytes`` + 1 bytes of the answer's body, read in slices so that the thread stops at the deadline
    by itself (:class:`TimeoutError`, the socket shut down); :class:`TooLarge` once the bound is passed."""
    body = bytearray()
    while len(body) <= max_bytes:
        left = cut.deadline - time.monotonic()
        if left <= 0 or cut.cut_off:
            cut.cut()
            raise TimeoutError("the exchange's deadline passed")
        cut.retime(min(_SLICE, left))
        try:
            chunk = response.read1(min(_CHUNK, max_bytes + 1 - len(body)))
        except TimeoutError:  # this slice elapsed (or the loop cut the socket): look at the clocks again
            if cut.cut_off:
                raise TimeoutError("the exchange's deadline passed") from None
            continue
        if not chunk:
            return bytes(body)
        body += chunk
    raise TooLarge(f"the answer is larger than {max_bytes} bytes")


class _Attach:
    """Takes the socket a connection opened, in the documented ``connect()``, for the deadline's cut, and keeps its
    reads short so the thread checks that deadline often.

    ``http.client`` keeps its socket factory as an instance attribute (``_create_connection``); here it stays the
    documented default, on the class, so an instance carries none and nothing of it is replaced: the socket is taken
    as ``connect()`` made it, a proxy tunnel included."""

    def __init__(self, *args: Any, cut: _Cut, **kwargs: Any) -> None:
        self._cut = cut
        super().__init__(*args, **kwargs)

    sock: socket.socket | None

    @property
    def _create_connection(self) -> Callable[..., socket.socket]:
        return socket.create_connection

    @_create_connection.setter
    def _create_connection(self, factory: Callable[..., socket.socket]) -> None:
        pass  # ``http.client`` setting its documented default: the class's own, unchanged

    def connect(self) -> None:
        super().connect()  # type: ignore[misc]  # the http.client connection this is mixed into
        sock = self.sock
        if sock is not None:
            self._cut.attach(sock)  # a TLS socket too: shutting it down ends a read or a handshake on it


class _HTTPConnection(_Attach, http.client.HTTPConnection):
    pass


class _HTTPSConnection(_Attach, http.client.HTTPSConnection):
    """The TCP socket is taken before the TLS handshake, so a peer that stalls the handshake is cut at the deadline,
    and the proxy tunnel (plaintext, on that same socket) is set up as ``http.client`` does it."""

    _tunnel_host: str | None  # set by http.client when a proxy CONNECT tunnel is needed
    _context: ssl.SSLContext  # the SSL context of the connection

    def connect(self) -> None:
        http.client.HTTPConnection.connect(self)  # the TCP socket, and the proxy tunnel on it
        sock = self.sock
        assert sock is not None  # http.client's connect() left it there
        self._cut.attach(sock)  # before the handshake: a peer that stalls it is cut at the deadline
        server_hostname = self._tunnel_host if self._tunnel_host else self.host
        # The wrap ``http.client.HTTPSConnection.connect`` does once its TCP connect returned.
        self.sock = self._context.wrap_socket(sock, server_hostname=server_hostname)


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
