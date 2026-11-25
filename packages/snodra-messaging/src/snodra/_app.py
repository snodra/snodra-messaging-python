"""The application: one service instance and its hub connections."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import replace
from typing import Any, Literal, TypeVar, overload

from snodra import _credentials, _tls
from snodra._codec import Codec, JsonCodec
from snodra._connection import HubConnection, Rejection
from snodra._dispatch import Dispatcher
from snodra._errors import ConfigurationError
from snodra._gather import Gathered, gather
from snodra._identity import Guard, IdentityResolver
from snodra._jwt import JwksVerifier
from snodra._local import LocalCalls
from snodra._pool import Pool
from snodra._registry import Kind, Registry, label_of
from snodra._retry import RetryPolicy
from snodra._send import Sender
from snodra._settings import Settings, hub_targets
from snodra._stream_client import Stream, stream
from snodra._subscribe import Subscription, subscribe
from snodra._subscription_handler import Updates
from snodra._telemetry import Gauges, NoTelemetry, Telemetry
from snodra._timings import DEFAULT_TIMINGS, Timings

log = logging.getLogger(__name__)

H = TypeVar("H", bound=Callable[..., Awaitable[Any]])
IN = TypeVar("IN")
Q = TypeVar("Q")
R = TypeVar("R")
S = TypeVar("S", bound=Callable[..., AsyncIterator[Any]])
U = TypeVar("U")


class App:
    """One service instance: its handlers and its hub connections.

    Handlers are registered before ``start`` (``command_handler``, ``query_handler``, ``stream_handler``,
    ``subscription_handler``); ``start`` opens a connection per hub; ``shutdown`` leaves the hubs in order
    (https://snodra.damoreira.com/docs/library-authors/protocol/, "Shutting down"). ``run`` does both around the
    process's life, and ``async with app:`` around a block. ``codec`` reads and writes the payloads
    (:class:`JsonCodec` by default).

    Building it checks the credential files of the settings (a token file, a client secret file, the CA files, the
    client certificate pair): one that cannot be used is a :class:`ConfigurationError`.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        codec: Codec | None = None,
        telemetry: Telemetry | None = None,
        _timings: Timings | None = None,
    ) -> None:
        self._settings = settings
        self._telemetry: Telemetry = NoTelemetry() if telemetry is None else telemetry
        # The status period is the app's own setting: the shipped timings as given, with it applied.
        self._timings = (
            replace(DEFAULT_TIMINGS, status_interval=settings.status_interval) if _timings is None else _timings
        )
        if settings.tls is not None:
            _tls.check_ca(settings.tls)
        self._credentials = _credentials.build(
            settings.credentials,
            service=settings.service,
            timings=self._timings,
            on_token_fetched=self._telemetry.token_fetched,
        )
        certificates = None
        if settings.client_certificate is not None:
            certificates = _tls.Certificates(settings.client_certificate, settings.service)
        if self._credentials is not None and settings.tls is None:
            log.warning(
                "The hub %s of %s goes to %s without TLS: it travels unencrypted "
                "(configure tls, SNODRA_SERVER_TLS=true)",
                self._credentials.kind,
                settings.service,
                ", ".join(hub_targets(settings.hubs)),
            )
        self._codec: Codec = JsonCodec() if codec is None else codec
        self._registry = Registry()
        verifier = None
        if settings.jwks_url is not None and settings.issuer is not None:
            t = self._timings
            verifier = JwksVerifier(
                settings.jwks_url,
                settings.issuer,
                min_refresh=t.jwks_min_refresh,
                fetch_timeout=t.jwks_fetch_timeout,
                leeway=t.jwt_leeway,
            )
        self._identity = IdentityResolver(verifier, settings.roles_claim)
        self._dispatcher = Dispatcher(
            self._registry,
            self._codec,
            settings.max_concurrent,
            self._timings,
            identity=self._identity,
            stream_credit_timeout=settings.stream_credit_timeout,
            max_subscriptions=settings.max_subscriptions,
            telemetry=self._telemetry,
        )
        self._sender = Sender(self._codec, settings, self._timings, telemetry=self._telemetry)
        self._cert_seen: set[str] = set()
        self._pool = Pool(
            settings,
            self._timings,
            self._dispatcher,
            on_answer=self._sender.on_answer,
            credentials=self._credentials,
            certificates=certificates,
            telemetry=self._telemetry,
            on_cert_presented=self._cert_presented,
        )
        self._sender.pool = self._pool
        self._sender.local = LocalCalls(self._registry, self._dispatcher, self._codec, settings)
        self._sender.local.pool = self._pool
        self._started = False
        self._shutting_down = False
        self._stop = asyncio.Event()  # a signal, or shutdown: run() stops waiting
        self._down = asyncio.Event()  # shutdown is over

    def _cert_presented(self, fp: str) -> None:
        seen = fp in self._cert_seen
        first = len(self._cert_seen) == 0
        if not seen:
            self._cert_seen.add(fp)
        if not seen and not first:
            self._telemetry.certificate_rotated()

    def _read_gauges(self) -> Gauges:
        if self._shutting_down:
            return Gauges(handlers_in_flight=0, connected_hubs=0)
        return Gauges(
            handlers_in_flight=self._dispatcher.handlers_in_flight,
            connected_hubs=len(self._pool._live()),
        )

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
    def command_handler(self, fn: None = None, /, *, guard: Guard | None = None) -> Callable[[H], H]: ...
    def command_handler(self, fn: H | None = None, /, *, guard: Guard | None = None) -> H | Callable[[H], H]:
        """Registers a command handler, bare (``@app.command_handler``) or called (``@app.command_handler()``): an
        ``async def`` whose first parameter is annotated with a ``@message_type`` class, the type it handles; what it
        returns is the reply (``None`` answers ``{}``). Raises ``TypeError`` for what cannot be served and
        :class:`RegistrationClosed` after ``start``, naming the handler. Returns the function unchanged.

        ``guard`` (:func:`authenticated`, :func:`role`, :func:`caller_matches`) says who may call it: a request
        without a verified caller is answered ``UNAUTHENTICATED`` before its payload is decoded, a caller the rule
        refuses ``PERMISSION_DENIED`` once it decoded. A guard needs ``Settings.jwks_url``: without it, a guarded
        handler is a :class:`ConfigurationError` naming it."""
        return self._handler("command", fn, guard)

    @overload
    def query_handler(self, fn: H, /) -> H: ...
    @overload
    def query_handler(
        self, fn: None = None, /, *, shared: bool = False, guard: Guard | None = None
    ) -> Callable[[H], H]: ...
    def query_handler(
        self, fn: H | None = None, /, *, shared: bool = False, guard: Guard | None = None
    ) -> H | Callable[[H], H]:
        """Registers a point query handler, as :meth:`command_handler` registers a command handler.

        ``shared=True`` registers its type as a shared query type (``Register.shared_query_types``): many services may
        own it, a point query goes to one live instance of any owner, a gather asks all of them. Every owner must
        register it shared (the hub refuses a type registered exclusive by one owner and shared by another). A point
        query of a shared type this instance handles still goes through the hub, never in-process, so that the hub
        spreads it over every owner."""
        if not isinstance(shared, bool):
            raise TypeError(f"shared must be True or False, got {shared!r}")
        return self._handler("query", fn, guard, shared=shared)

    def _handler(self, kind: Kind, fn: H | None, guard: Guard | None, *, shared: bool = False) -> H | Callable[[H], H]:
        if guard is not None and not isinstance(guard, Guard):
            raise TypeError(f"guard must be made by authenticated(), role() or caller_matches(), got {guard!r}")

        def register(f: H) -> H:
            self._check_guard(f, guard)
            self._registry.add(kind, f, guard, shared=shared)
            return f

        return register if fn is None else register(fn)

    @overload
    def stream_handler(self, fn: S, /) -> S: ...
    @overload
    def stream_handler(self, fn: None = None, /, *, guard: Guard | None = None) -> Callable[[S], S]: ...
    def stream_handler(self, fn: S | None = None, /, *, guard: Guard | None = None) -> S | Callable[[S], S]:
        """Registers a streaming query handler, bare (``@app.stream_handler``) or called
        (``@app.stream_handler()``): an ``async def`` generator whose first parameter is annotated with a
        ``@message_type`` class, the query type it serves, and whose items are the stream's parts. A query of its type
        is answered in mode ``STREAMING`` only (``App.stream``); a unary query of it is ``INVALID_REQUEST``. ``guard``
        is that of :meth:`command_handler`. Returns the function unchanged."""
        if guard is not None and not isinstance(guard, Guard):
            raise TypeError(f"guard must be made by authenticated(), role() or caller_matches(), got {guard!r}")

        def register(f: S) -> S:
            self._check_guard(f, guard)
            self._registry.add("query", f, guard, stream=True)
            return f

        return register if fn is None else register(fn)

    def _check_guard(self, fn: object, guard: Guard | None) -> None:
        if guard is not None and not self._identity.verifies:
            raise ConfigurationError(
                f"handler {label_of(fn)} requires an authenticated caller (guard={guard!r}) but no JWKS is "
                "configured to verify forwarded tokens: set jwks_url (SNODRA_JWKS_URL) and issuer (SNODRA_ISSUER); "
                "without them every caller is anonymous"
            )

    def subscription_handler(
        self, *, update: type[U], shared: bool = False, guard: Guard | None = None
    ) -> Callable[[Callable[[Q], Awaitable[Any]]], Updates[Q, U]]:
        """Registers a subscription query handler and returns the :class:`Updates` that pushes its updates.

        The decorated ``async def`` takes the query (a ``@message_type`` class) and returns the subscription's *initial
        result*; the type is served in mode ``SUBSCRIPTION`` only (``App.subscribe``), and a unary query of it is
        ``INVALID_REQUEST`` unless its id is the hub's initial request ``<id>#0``. ``update`` is the class of the
        updates the returned :class:`Updates` emits (``emit(match, update)``, ``complete(match)``,
        ``fail(match, error)``, all on the app's event loop). ``shared=True`` registers it as a shared subscription type
        (``Register.shared_query_types`` as well as ``Register.subscription_query_types``); ``guard`` is that of
        :meth:`command_handler`."""
        if not isinstance(update, type):
            raise TypeError(f"update must be the class of the updates, got {update!r}")
        if not isinstance(shared, bool):
            raise TypeError(f"shared must be True or False, got {shared!r}")
        if guard is not None and not isinstance(guard, Guard):
            raise TypeError(f"guard must be made by authenticated(), role() or caller_matches(), got {guard!r}")

        def register(fn: Callable[[Q], Awaitable[Any]]) -> Updates[Q, U]:
            self._check_guard(fn, guard)
            handler = self._registry.add("query", fn, guard, shared=shared, subscription=True)
            return Updates(self._dispatcher.subscriptions, self._codec, handler.type, handler.message, update)

        return register

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
        as ``Request.timeout_ms`` (else ``Settings.default_timeout``; without one, or with a ``timeout`` of 0 or less,
        the hub applies its own), and bounds how long the call waits for a hub: without one,
        ``Settings.default_timeout``, else 5 s. ``priority``: ``Request.priority`` (else ``Settings.priorities``, else
        0). ``retry``: a :class:`RetryPolicy` (else ``Settings.default_retry``; ``False``: none). ``headers``: the
        call's own request headers; the names the library sets itself (``authorization``, ``traceparent``,
        ``tracestate``, ``correlationId``, ``correlation-id``, in any letter case) raise :class:`ReservedHeader` before
        anything is sent. The request carries the ``correlationId`` of the request being handled (its id when it has
        none, a new UUID outside any handling), its valid ``traceparent``, and its ``authorization`` unless
        ``Settings.forward_jwt`` is off.

        A command of a type this instance has a command handler for is answered in-process, without a hub (unless
        ``Settings.always_route_through_hub``): through the handler's guard, with the headers and the caller a request
        through the hub would carry, and the same errors; its routing key, priority, timeout and retry do not apply,
        and a cancellation of the caller cancels the handler.

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
        query). A retry policy also retries a query after an unknown outcome. A query of a type this instance has a
        query handler for is answered in-process, as :meth:`send` answers a command."""
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

    async def gather(
        self,
        query: object,
        result: type[R],
        *,
        max_results: int = 0,
        timeout: float | None = None,  # noqa: ASYNC109 (the API takes a timeout)
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> Gathered[R]:
        """Sends a scatter-gather: the query goes to every live instance of every service that owns its type (a shared
        type; on an exclusive type every instance of its owner), and every answer comes back as a :class:`Part`, in
        arrival order, until the hub's end. Partial answers are answers: a gather where some targets failed or did
        not answer in time succeeds, and :class:`Gathered` says what was missing.

        ``max_results`` ends the gather after that many successful parts (0: no limit; the hub cancels the targets
        still working). ``timeout`` is the deadline of the whole gather: the targets that have not answered by then
        are counted in ``timed_out``. ``timeout``, ``priority``, ``headers`` and ``retry`` are those of :meth:`send`;
        a retry sends a new gather, which collects its own parts. A gather always goes through a hub, also when this
        instance handles the type.

        A target's failure is a part with an ``error`` (:class:`HubError`), and so is an answer that does not decode as
        ``result`` (:class:`DecodeError`). The gather itself fails with :class:`HubError` (``NO_HANDLER`` when the
        type has no live target, ``INVALID_REQUEST`` for a command type or a subscription type, ``TIMEOUT`` when the
        hub's end did not come within its bound for the gather plus 1 s), :class:`NeverSent` or
        :class:`OutcomeUnknown` (the hub stream broke before the end: the parts received are dropped). A cancellation
        of the caller stays ``asyncio.CancelledError``."""
        return await gather(
            self._sender,
            query,
            result,
            max_results=max_results,
            timeout=timeout,
            priority=priority,
            headers=headers,
            retry=retry,
        )

    def stream(
        self,
        query: object,
        item: type[R],
        *,
        initial_credit: int | None = None,
        max_results: int = 0,
        timeout: float | None = None,
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> Stream[R]:
        """A streaming query: ``async with app.stream(query, Item) as items: async for item in items`` reads the items a
        streaming handler produces, one at a time. Nothing is sent until the block is entered; leaving it early (a
        ``break``, an exception, a cancellation) cancels the stream on the hub.

        ``initial_credit`` is the window: how many parts the handler may send ahead of the caller (None or 0: 64); the
        library grants ``max(1, window // 2)`` more each time that many items were consumed. ``max_results`` ends the
        stream after that many parts at the hub (0: until the handler completes). ``timeout`` is the stream's idle
        timeout (``Request.timeout_ms``): the longest the hub waits for the next part, not a bound on the whole stream;
        ``priority``, ``headers`` and ``retry`` are those of :meth:`send`, and a retry applies only before the first
        part, sending the caller's idle timeout whole on every attempt. A stream always goes through a hub, also when
        this instance handles the type.

        The items are the handler's parts decoded as ``item`` (``DecodeError`` for one that does not). A stream the hub
        ends with an error fails as :meth:`send` fails with that code; one that lost parts fails as :class:`LostParts`;
        one whose hub stream broke after a part as :class:`OutcomeUnknown` (:class:`NeverSent` when no hub took the
        request); one that waited for a frame longer than the hub's bound for the stream plus 1 s (twice the bound for
        the first part, which the hub may hold in its queue) as :class:`HubError` (``TIMEOUT``), and the stream is
        cancelled. A cancellation of the caller stays ``asyncio.CancelledError``."""
        return stream(
            self._sender,
            query,
            item,
            initial_credit=initial_credit,
            max_results=max_results,
            timeout=timeout,
            priority=priority,
            headers=headers,
            retry=retry,
        )

    def subscribe(
        self,
        query: object,
        initial: type[IN],
        update: type[U],
        *,
        initial_credit: int | None = None,
        max_results: int = 0,
        timeout: float | None = None,
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> Subscription[IN, U]:
        """A subscription query: ``async with app.subscribe(query, Initial, Update) as sub`` waits for the initial
        result, kept as ``sub.initial``, and ``async for update in sub`` reads the updates the handler instance(s) push.
        Nothing is sent until the block is entered; leaving it early (a ``break``, an exception, a cancellation) cancels
        the subscription on the hub. A subscription always goes through a hub, also when this instance handles the type.

        ``initial_credit`` is the window: how many parts (the initial result included) the hub may relay before the
        caller consumed any (None or 0: 64); the library grants ``max(1, window // 2)`` more each time that many parts
        were consumed, the initial result counting as one. ``max_results`` ends the subscription after that many updates
        at the hub (0: until the handlers complete; the initial result does not count). ``timeout`` bounds the initial
        result only (queue wait plus handler): a quiet subscription is never timed out. ``priority``, ``headers`` and
        ``retry`` are those of :meth:`send`, and a retry applies only until the initial result, each attempt sending
        what is left of the caller's budget.

        The updates are the parts decoded as ``update`` (``DecodeError`` for one that does not). A subscription the hub
        ends with an error fails as :meth:`send` fails with that code; one that lost parts fails as :class:`LostParts`;
        one whose hub stream broke after its initial result fails as :class:`OutcomeUnknown` (:class:`NeverSent` when no
        hub took the request); one whose initial result did not come within the hub's bound for the request plus 1 s
        fails as :class:`HubError` (``TIMEOUT``) and is cancelled. A cancellation of the caller stays
        ``asyncio.CancelledError``."""
        return subscribe(
            self._sender,
            query,
            initial,
            update,
            initial_credit=initial_credit,
            max_results=max_results,
            timeout=timeout,
            priority=priority,
            headers=headers,
            retry=retry,
        )

    def hub_ids(self) -> list[str]:
        """``RegisterAck.hub_id`` of every hub this instance holds a live stream to (its address when empty), one per
        hub id, sorted."""
        return self._pool.hub_ids()

    @property
    def subscriptions_served(self) -> int:
        """How many subscription targets this instance serves now (every open subscription, pending admissions
        included)."""
        return self._dispatcher.subscriptions_served

    async def start(self) -> None:
        """Opens a connection per hub and returns at once; the connections register and reconnect on their own."""
        if self._shutting_down:
            raise RuntimeError("the app was shut down")
        if self._started:
            raise RuntimeError("the app is already started")
        self._registry.close()
        self._dispatcher.subscriptions.bind(asyncio.get_running_loop())
        registry = self._registry
        self._pool.declare(
            registry.types("command"),
            registry.types("query"),
            registry.shared_types(),
            registry.subscription_types(),
        )
        self._started = True
        if not self._shutting_down:
            self._telemetry.observe_gauges(self._read_gauges)
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
        every live stream (its ack awaited for at most 1 s), then a clean ``ReplyEnd`` on every subscription target this
        instance serves (so their subscribers hear them end before the stream is gone), then the handlers in flight
        drained (awaited for at most 5 s, then cancelled and never awaited again), then each stream half-closed and its
        end awaited (at most 2 s). A handler still running when ``timeout`` is reached is cancelled and left behind: one
        warning names it. A second call returns at once. The handlers being drained can still send commands and queries,
        and a stream lost meanwhile is opened again with no types (no new work arrives on it); once the drain is over
        nothing is sent: a call fails as :class:`NeverSent`, and one still waiting for its answer as
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
                        await self._pool.leave(
                            drain=lambda: self._dispatcher.drain(self._timings.handler_drain),
                            end_subscriptions=self._dispatcher.end_subscriptions,
                        )
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
                try:
                    await self._identity.close()  # a JWKS fetch still running ends here
                finally:
                    try:
                        if self._credentials is not None:
                            await self._credentials.close()  # and a token fetch or renewal
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
