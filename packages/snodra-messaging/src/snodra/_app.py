"""The application: one service instance and its hub connections."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, TypeVar, overload

from snodra._codec import Codec, JsonCodec
from snodra._connection import HubConnection, Rejection
from snodra._dispatch import Dispatcher
from snodra._pool import Pool
from snodra._registry import Kind, Registry
from snodra._retry import RetryPolicy
from snodra._send import Sender
from snodra._settings import Settings
from snodra._timings import DEFAULT_TIMINGS, Timings

log = logging.getLogger(__name__)

H = TypeVar("H", bound=Callable[..., Awaitable[Any]])
R = TypeVar("R")


class App:
    """One service instance: its handlers and its hub connections.

    Handlers are registered before ``start`` (``command_handler``, ``query_handler``); ``start`` opens a connection
    per hub; ``shutdown`` leaves the hubs in order (https://snodra.damoreira.com/library-authors/protocol/, "Shutting
    down"). ``run`` does both around the process's life, and ``async with app:`` around a block. ``codec`` reads and
    writes the payloads (:class:`JsonCodec` by default).
    """

    def __init__(self, settings: Settings, *, codec: Codec | None = None, _timings: Timings | None = None) -> None:
        self._settings = settings
        self._timings = DEFAULT_TIMINGS if _timings is None else _timings
        self._codec: Codec = JsonCodec() if codec is None else codec
        self._registry = Registry()
        self._dispatcher = Dispatcher(self._registry, self._codec, settings.max_concurrent, self._timings)
        self._sender = Sender(self._codec, settings, self._timings)
        self._pool = Pool(settings, self._timings, self._dispatcher, on_answer=self._sender.on_answer)
        self._sender.pool = self._pool
        self._started = False
        self._shutting_down = False
        self._stop = asyncio.Event()  # a signal, or shutdown: run() stops waiting
        self._down = asyncio.Event()  # shutdown is over

    @property
    def instance_id(self) -> str:
        """The instance id this app registers (``Register.instance_id``)."""
        return self._settings.instance

    @property
    def rejection(self) -> Rejection | None:
        """The latest refusal of this instance's registration by any hub; never cleared."""
        return self._pool.rejection

    @overload
    def command_handler(self, fn: H, /) -> H: ...
    @overload
    def command_handler(self, fn: None = None, /) -> Callable[[H], H]: ...
    def command_handler(self, fn: H | None = None, /) -> H | Callable[[H], H]:
        """Registers a command handler, bare (``@app.command_handler``) or called (``@app.command_handler()``): an
        ``async def`` whose first parameter is annotated with a ``@message_type`` class, the type it handles; what it
        returns is the reply (``None`` answers ``{}``). Raises ``TypeError`` for what cannot be served and
        :class:`RegistrationClosed` after ``start``, naming the handler. Returns the function unchanged."""
        return self._handler("command", fn)

    @overload
    def query_handler(self, fn: H, /) -> H: ...
    @overload
    def query_handler(self, fn: None = None, /) -> Callable[[H], H]: ...
    def query_handler(self, fn: H | None = None, /) -> H | Callable[[H], H]:
        """Registers a point query handler, as :meth:`command_handler` registers a command handler."""
        return self._handler("query", fn)

    def _handler(self, kind: Kind, fn: H | None) -> H | Callable[[H], H]:
        def register(f: H) -> H:
            self._registry.add(kind, f)
            return f

        return register if fn is None else register(fn)

    @overload
    async def send(
        self,
        command: object,
        result: None = None,
        *,
        routing_key: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 (the API takes a timeout)
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> None: ...
    @overload
    async def send(
        self,
        command: object,
        result: type[R],
        *,
        routing_key: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 (the API takes a timeout)
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> R: ...
    async def send(
        self,
        command: object,
        result: type[R] | None = None,
        *,
        routing_key: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 (the API takes a timeout)
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> R | None:
        """Sends a command (an instance of a ``@message_type`` class) through a hub to the service that handles its
        type, and returns the answer decoded as ``result`` (None without a ``result``: the answer is not read).

        ``routing_key``: commands of a type with the same key go to the same instance. ``timeout`` (seconds) is sent
        as ``Request.timeout_ms`` (else ``Settings.default_timeout``; without one, or with ``timeout=0``, the hub
        applies its own), and bounds how long the call waits for a hub: without one, ``Settings.default_timeout``,
        else 5 s. ``priority``: ``Request.priority`` (else
        ``Settings.priorities``, else 0). ``retry``: a :class:`RetryPolicy` (else ``Settings.default_retry``;
        ``False``: none).

        Raises :class:`NeverSent` when no hub took the request (the handler never ran: sending it again is safe),
        :class:`OutcomeUnknown` when the hub stream broke after the request was written (the handler may have run),
        :class:`HubError` for an error code from the hub or the handler (``TIMEOUT`` also when no answer came
        within the hub's bound for the request plus 1 s), ``DecodeError`` when the answer is not a ``result``. A
        cancellation of the caller (``asyncio.timeout``, a task group) stays ``asyncio.CancelledError``."""
        return await self._sender.call(  # type: ignore[return-value]
            command,
            result,
            "command",
            routing_key=routing_key,
            timeout=timeout,
            priority=priority,
            headers=headers,
            retry=retry,
        )

    async def query(
        self,
        query: object,
        result: type[R],
        *,
        routing_key: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 (the API takes a timeout)
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> R:
        """Sends a point query through a hub to an instance that handles its type and returns the answer decoded as
        ``result``; the options and the errors are those of :meth:`send` (the hub ignores the routing key of a
        query). A retry policy also retries a query after an unknown outcome."""
        return await self._sender.call(  # type: ignore[return-value]
            query,
            result,
            "query",
            routing_key=routing_key,
            timeout=timeout,
            priority=priority,
            headers=headers,
            retry=retry,
        )

    def hub_ids(self) -> list[str]:
        """``RegisterAck.hub_id`` of every acknowledged stream (its address when empty), sorted."""
        return self._pool.hub_ids()

    async def start(self) -> None:
        """Opens a connection per hub and returns at once; the connections register and reconnect on their own."""
        if self._shutting_down:
            raise RuntimeError("the app was shut down")
        if self._started:
            raise RuntimeError("the app is already started")
        self._registry.close()
        self._pool.declare(self._registry.types("command"), self._registry.types("query"))
        self._started = True
        self._pool.start()

    async def ready(self, timeout: float | None = None) -> None:  # noqa: ASYNC109 (the API takes a timeout)
        """Returns once a hub has acknowledged this instance's registration; ``TimeoutError`` after ``timeout``."""
        async with asyncio.timeout(timeout):
            await self._pool.ready()

    async def run(self) -> None:
        """Starts, serves until SIGTERM or SIGINT (where the event loop takes signal handlers), a ``shutdown`` from
        elsewhere or the cancellation of the task, then shuts down (``shutdown(15.0)``). A cancelled ``run`` ends
        cancelled: the unexpected error that crashed a connection is then only logged (at error level, when it
        happened); after a signal it is raised, as by ``shutdown``."""
        await self.start()
        loop = asyncio.get_running_loop()
        previous: dict[signal.Signals, Any] = {}
        for sig in (signal.SIGTERM, signal.SIGINT):
            before = signal.getsignal(sig)
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError, ValueError):  # Windows, or not the main thread
                continue
            previous[sig] = before
        propagating = False
        try:
            await self._stop.wait()
        except BaseException:
            propagating = True
            raise
        finally:
            for sig, before in previous.items():
                loop.remove_signal_handler(sig)
                # The handler found is restored, the default one too; None is a handler installed outside Python,
                # which signal.signal cannot reinstall: the default stays then.
                if before is not None:
                    signal.signal(sig, before)
            await self._shutdown(15.0, raise_crash=not propagating)
            await self._down.wait()  # a shutdown that another task started

    async def shutdown(self, timeout: float = 15.0) -> None:  # noqa: ASYNC109 (the API takes a timeout)
        """Leaves the hubs in order and stops every connection, within ``timeout``: a ``Register`` with no types on
        every live stream (its ack awaited for at most 1 s), then the handlers in flight drained (awaited for at most
        5 s, then cancelled and never awaited again), then each stream half-closed and its end awaited (at most 2 s).
        A handler still running when ``timeout`` is reached is cancelled and left behind: one warning names it. A
        second call returns at once. The handlers being drained can still send commands and queries; once the drain
        is over nothing is sent: a call fails as :class:`NeverSent`, and one still waiting for its answer as
        :class:`OutcomeUnknown` ("hub connection stopped").

        The unexpected error that crashed a connection (a library bug, logged at error level when it happened) is
        raised here once everything is stopped, unless another exception or a cancellation is already propagating
        out of the shutdown: that one is never replaced."""
        await self._shutdown(timeout, raise_crash=True)

    async def _shutdown(self, budget: float, *, raise_crash: bool) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        self._stop.set()
        propagating = False
        try:
            if self._started:
                try:
                    async with asyncio.timeout(budget):
                        await self._pool.leave(drain=lambda: self._dispatcher.drain(self._timings.handler_drain))
                except TimeoutError:
                    log.warning("Leaving the hubs took longer than %.2f s: the streams are cut", budget)
        except BaseException:
            propagating = True
            raise
        finally:
            self._dispatcher.abandon()  # the handlers the drain did not reach: cancelled, never awaited
            try:
                await self._pool.close()
            except Exception:
                if raise_crash and not propagating:
                    raise
                log.debug("A connection crashed (logged when it happened); not raised over what propagates")
            finally:
                self._down.set()

    async def __aenter__(self) -> App:
        await self.start()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, *exc: object) -> None:
        """Shuts down (``shutdown(15.0)``); the unexpected error that crashed a connection is raised only when the
        block ended without an exception: the block's own exception is never replaced (the crash was logged at error
        level when it happened)."""
        await self._shutdown(15.0, raise_crash=exc_type is None)

    def _connections(self) -> list[HubConnection]:
        """The hub connections (for tests)."""
        return list(self._pool.connections)
