"""A request the hub forwarded, end to end: its task, its handler, its one answer and its permit.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Handling a request", "Flow control", "Cancel").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final, Protocol, cast

from snodra._codec import Codec
from snodra._context import _CALLER, _CURRENT, MessageInfo
from snodra._errors import DecodeError, ErrorCode, Unauthenticated, error_to_wire
from snodra._hubpb import hub_pb2
from snodra._identity import AUTHORIZATION, Caller, IdentityResolver
from snodra._permits import PermitLedger
from snodra._registry import Handler, Kind, Registry
from snodra._stream_handler import StreamCredit, end_of_stream, failure, serve
from snodra._subscription_handler import ServedSubscription, Subscriptions
from snodra._timings import Timings

log = logging.getLogger(__name__)

CORRELATION_ID: Final = "correlationId"


class Arrival(Protocol):
    """The stream a request arrived on: its answers and its permits go back on it, and only on it."""

    @property
    def writable(self) -> bool:
        """False once the stream ended: what is put on it is dropped."""
        ...

    @property
    def permits(self) -> PermitLedger: ...

    def put(self, m: hub_pb2.ClientMessage) -> bool:
        """Queues m for the stream's writer; False when the stream takes nothing more."""
        ...


@dataclass(eq=False)
class _Running:
    """A forwarded request being served."""

    arrival: Arrival
    request: hub_pb2.Request
    label: str
    """The handler's name, else the type (for the logs)."""
    task: asyncio.Task[None] | None = None
    credit: StreamCredit | None = None
    """The credit of a streaming request (``Request.initial_credit`` and its ``Flow``s); None for any other."""
    cancelled: bool = False
    """By the hub's ``Cancel``: nothing at all is answered."""
    finished: bool = False
    """Its permit was accounted."""
    abandoned: bool = False
    """Cancelled by the shutdown and left behind."""


def _error_reply(request_id: str, code: ErrorCode, message: str) -> hub_pb2.Reply:
    error = hub_pb2.Error(request_id=request_id, code=int(code), message=message)  # type: ignore[arg-type]
    return hub_pb2.Reply(request_id=request_id, error=error)


def _kind_name(kind: int) -> str:
    try:
        return hub_pb2.RequestKind.Name(kind)
    except ValueError:
        return "UNRECOGNIZED"


class Dispatcher:
    """Serves the requests of every hub stream of an app: each one in a task of its own, ``snodra:handler:<id>``,
    at most ``max_concurrent`` at once over every hub (a request beyond waits for a local permit)."""

    def __init__(
        self,
        registry: Registry,
        codec: Codec,
        max_concurrent: int,
        timings: Timings,
        identity: IdentityResolver | None = None,
        *,
        stream_credit_timeout: float = 600.0,
        max_subscriptions: int = 1024,
    ) -> None:
        self._registry = registry
        self._codec = codec
        self._identity = IdentityResolver(None, "roles") if identity is None else identity
        self._max_concurrent = max_concurrent
        self._slots: asyncio.Semaphore | None = None
        self._timings = timings
        self._stream_credit_timeout = stream_credit_timeout
        self._running: dict[tuple[object, str], _Running] = {}
        self._tasks: dict[asyncio.Task[None], _Running] = {}
        self._subscriptions = Subscriptions(max_subscriptions)
        self._subscription_tasks: set[asyncio.Task[None]] = set()
        """The admissions of subscription targets still running: no permit, no handler slot."""
        self._timers: set[asyncio.Task[None]] = set()
        self._closed = False
        """Set by :meth:`abandon`: no permit timer is armed any more (the shutdown is under way)."""

    @property
    def subscriptions(self) -> Subscriptions:
        """The subscription targets this instance serves, for the emitters of its subscription handlers."""
        return self._subscriptions

    @property
    def subscriptions_served(self) -> int:
        """How many subscription targets this instance serves now."""
        return len(self._subscriptions)

    @property
    def handlers_in_flight(self) -> int:
        """The handlers of every hub running now (a request waiting for a local permit included)."""
        return sum(1 for task in self._tasks if not task.done())

    # Called by the stream's reader: never raises, runs no user code, never waits.

    def on_request(self, arrival: Arrival, request: hub_pb2.Request) -> None:
        """A ``ServerMessage.request``, also before the stream's ``RegisterAck``."""
        if request.mode == hub_pb2.SUBSCRIPTION:
            self._open_subscription(arrival, request)  # outside flow control: no permit
            return
        arrival.permits.arrived()
        key = (arrival, request.id)
        run = _Running(arrival, request, self._label(request))
        if request.mode == hub_pb2.STREAMING:
            # Before its task starts: a Flow of the hub may land before the handler's first item.
            run.credit = StreamCredit(request.initial_credit or self._timings.default_credit)
        run.task = asyncio.create_task(self._serve(run), name=f"snodra:handler:{request.id}")
        self._tasks[run.task] = run
        self._running[key] = run
        run.task.add_done_callback(lambda t: self._ended(key, run, t))

    def on_flow(self, arrival: Arrival, flow: hub_pb2.Flow) -> None:
        """A ``Flow`` from the hub: credit for a streaming request this instance serves. One for an id that ended here,
        or never arrived here, is dropped."""
        run = self._running.get((arrival, flow.request_id))
        if run is not None and run.credit is not None:
            run.credit.add(flow.credits)

    def serving(self, arrival: Arrival) -> bool:
        """True while a request that arrived on arrival is being served."""
        return any(a is arrival for a, _ in self._running)

    def on_cancel(self, arrival: Arrival, cancel: hub_pb2.Cancel) -> None:
        """The hub gave up on a request this instance serves: its handler is cancelled and nothing is answered; a
        ``Cancel`` for a request that already ended is ignored. A subscription target it names is removed, nothing is
        written for it, and its admission stops."""
        run = self._running.pop((arrival, cancel.request_id), None)
        if run is None or run.task is None:
            log.debug("Cancel for request %s ignored: not running here (%s)", cancel.request_id, cancel.reason)
        else:
            log.debug("The hub cancelled request %s (%s): %s", cancel.request_id, run.label, cancel.reason)
            run.cancelled = True
            run.task.cancel()
        if self._subscriptions.cancel(arrival, cancel.request_id):
            log.debug("The hub cancelled subscription %s (%s)", cancel.request_id, cancel.reason)

    def on_stream_lost(self, arrival: Arrival) -> None:
        """The stream arrival is gone: its streaming handlers are cancelled (their flows would never move again) and its
        subscriptions are removed, since the hub forgot them with the session. A unary handler may run on, its reply
        dropped."""
        for (stream, _), run in list(self._running.items()):
            if stream is arrival and run.credit is not None and run.task is not None:
                run.task.cancel()
        dropped = self._subscriptions.lost(arrival)
        if dropped:
            log.info("%d subscription(s) of a lost hub stream dropped", dropped)

    # Shutdown.

    async def drain(self, timeout: float) -> None:  # noqa: ASYNC109 (a bound of the shutdown)
        """Waits for the handlers in flight, at most ``timeout`` (also when new requests arrive meanwhile); then
        cancels those still running, and never waits for them again. A cancellation of the wait does the same."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while left := list(self._tasks) + list(self._subscription_tasks):
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                await asyncio.wait(left, timeout=remaining)
        finally:
            self.abandon()

    def abandon(self) -> None:
        """Cancels every handler still running, without waiting for it, and the permit timers; one warning names the
        handlers it cancels. From then on no permit timer is armed: a handler it cancels ends after it, and its
        permit, when still due, is written at once or dropped with its stream. The subscriptions it still serves are
        dropped without writing anything."""
        self._closed = True
        for timer in list(self._timers):
            timer.cancel()
        left = [r for t, r in self._tasks.items() if not t.done() and not r.abandoned]
        for run in left:
            run.abandoned = True
            if run.task is not None:
                run.task.cancel()
        for task in list(self._subscription_tasks):
            task.cancel()
        self._subscriptions.close_all()
        if left:
            names = ", ".join(f"{r.label} ({r.request.type} {r.request.id})" for r in left)
            log.warning(
                "%d handler(s) still running at the end of the drain, cancelled and not awaited: %s", len(left), names
            )

    def end_subscriptions(self) -> int:
        """Ends every served subscription with a clean ``ReplyEnd`` (the shutdown, before the handlers drain): its
        subscriber hears that this target completed once every other target has ended too. Returns how many."""
        return self._subscriptions.end_all()

    # A request's task.

    async def _serve(self, run: _Running) -> None:
        try:
            if self._slots is None:
                self._slots = asyncio.Semaphore(self._max_concurrent)
            async with self._slots:  # held until the handler really ended, a cancelled one too
                if run.request.mode == hub_pb2.STREAMING:
                    await self._stream(run)
                    return
                reply = await self._answer(run.request)
                if run.cancelled:
                    return  # the hub's Cancel: nothing at all
                if not run.arrival.put(hub_pb2.ClientMessage(reply=reply)):
                    log.debug(
                        "Reply to %s request %s dropped: the stream it arrived on is gone",
                        run.request.type,
                        run.request.id,
                    )
        finally:
            self._finish_run(run)  # before the next request takes the local permit: the order on the wire

    async def _stream(self, run: _Running) -> None:
        """A streaming request: the token check, then the handler that owns the type in mode ``STREAMING``, then its
        items as parts and the one ``ReplyEnd``. Every other outcome is a ``ReplyEnd`` with an error, and no part."""
        request = run.request
        id_ = request.id
        try:
            caller = await self._identity.resolve(request.headers.get(AUTHORIZATION))
        except Unauthenticated as e:
            log.debug("Refused %s request %s: %s", request.type, id_, e)
            self._end_stream(run, failure(id_, ErrorCode.UNAUTHENTICATED, str(e)))
            return
        except Exception:  # a bug of the check is the caller's refusal, never an unanswered request
            log.warning("Checking the token of %s request %s failed", request.type, id_, exc_info=True)
            self._end_stream(run, failure(id_, ErrorCode.UNAUTHENTICATED, "invalid token"))
            return
        # A streaming request always looks for the query handler, whatever its kind.
        handler = self._registry.find("query", request.type)
        if handler is None:
            self._end_stream(run, failure(id_, ErrorCode.NO_HANDLER, f"no handler for type {request.type}"))
            return
        if not handler.stream:
            self._end_stream(
                run,
                failure(
                    id_,
                    ErrorCode.INVALID_REQUEST,
                    f"handler {handler.label} for {request.type} is not a streaming query handler "
                    "(it answers mode UNARY)",
                ),
            )
            return
        message, refused = self._prepare(handler, request, caller)
        if refused is not None:
            self._end_stream(run, refused.error)
            return
        credit = run.credit
        assert credit is not None  # a streaming request opens its credit when it arrives
        items = cast("AsyncIterator[object]", handler.fn(message))  # a streaming handler is an async generator
        current = _CURRENT.set(self._info(request))
        calling = _CALLER.set(caller)
        try:
            await serve(
                items,
                request,
                arrival=run.arrival,
                codec=self._codec,
                credit=credit,
                credit_timeout=self._stream_credit_timeout,
            )
        finally:
            _CALLER.reset(calling)
            _CURRENT.reset(current)

    def _end_stream(self, run: _Running, error: hub_pb2.Error) -> None:
        """A stream over before its handler produced anything: one ``ReplyEnd`` with error, and no part."""
        if not run.cancelled:  # the hub's Cancel: nothing at all
            run.arrival.put(end_of_stream(run.request.id, 0, error))

    async def _answer(self, request: hub_pb2.Request) -> hub_pb2.Reply:
        """The one ``Reply`` to a unary request; every outcome is a message."""
        id_ = request.id
        # The caller's identity: the forwarded token is checked first, before the handler lookup.
        try:
            caller = await self._identity.resolve(request.headers.get(AUTHORIZATION))
        except Unauthenticated as e:
            log.debug("Refused %s request %s: %s", request.type, id_, e)
            return _error_reply(id_, ErrorCode.UNAUTHENTICATED, str(e))
        except Exception:  # a bug of the check is the caller's refusal, never an unanswered request
            log.warning("Checking the token of %s request %s failed", request.type, id_, exc_info=True)
            return _error_reply(id_, ErrorCode.UNAUTHENTICATED, "invalid token")
        if request.kind == hub_pb2.COMMAND:
            kind: Kind = "command"
        elif request.kind == hub_pb2.QUERY:
            kind = "query"
        else:
            return _error_reply(id_, ErrorCode.INVALID_REQUEST, f"unsupported request kind {_kind_name(request.kind)}")
        handler = self._registry.find(kind, request.type)
        if handler is None:
            return _error_reply(id_, ErrorCode.NO_HANDLER, f"no handler for type {request.type}")
        return await self.answer_checked(handler, request, caller)

    async def answer_checked(self, handler: Handler, request: hub_pb2.Request, caller: Caller | None) -> hub_pb2.Reply:
        """The ``Reply`` of handler to request from caller, once its checks pass: a guarded handler refuses an
        anonymous caller before the payload is decoded, then the payload is decoded, then the guard's rule applies.
        The handler runs in the calling task, with ``current_message()`` and ``current_caller()`` set for it only.
        Every outcome is a message; a cancellation of the calling task is raised."""
        if handler.subscription:
            # A subscription handler's type is served in mode SUBSCRIPTION only; its one unary request is the hub's
            # initial request, whose id ends "#0".
            if not request.id.endswith("#0"):
                return _error_reply(
                    request.id,
                    ErrorCode.INVALID_REQUEST,
                    f"{handler.type} is a subscription query: send it with App.subscribe (mode SUBSCRIPTION)",
                )
        elif handler.stream:
            # A streaming handler's type is served in mode STREAMING only: a unary request of it (a point query this
            # instance answers itself, or one the hub forwarded) is a contract mismatch.
            return _error_reply(
                request.id,
                ErrorCode.INVALID_REQUEST,
                f"{handler.type} is a streaming query: send it with App.stream (mode STREAMING)",
            )
        message, refused = self._prepare(handler, request, caller)
        if refused is not None:
            return refused
        return await self._call(handler, request, message, caller)

    def _prepare(
        self, handler: Handler, request: hub_pb2.Request, caller: Caller | None
    ) -> tuple[object | None, hub_pb2.Reply | None]:
        """The message request carries, once handler's checks pass, and None; or None and the ``Reply`` that refuses
        it. The caller's token was checked before this; a guarded handler refuses an anonymous caller before the payload
        is decoded, so that an anonymous caller cannot probe the payload shapes of a guarded handler."""
        id_ = request.id
        guard = handler.guard
        if guard is not None and caller is None:
            # TODO(spec-gap): SG-1
            return None, _error_reply(
                id_, ErrorCode.UNAUTHENTICATED, f"handler {handler.label} requires authentication"
            )
        try:
            message: object = self._codec.decode(request.payload, handler.message)
        except DecodeError as e:
            return None, _error_reply(id_, ErrorCode.INVALID_REQUEST, str(e))
        except Exception as e:  # a codec's bug is the handler's failure, never the stream's
            log.warning("Decoding %s request %s failed", request.type, id_, exc_info=True)
            return None, hub_pb2.Reply(request_id=id_, error=error_to_wire(id_, e, self._codec))
        if guard is not None and caller is not None:
            # The rule on the caller (a role, a given caller) applies once the payload decoded.
            try:
                allowed = guard.allows(caller)
            except Exception as e:
                log.warning("The guard of %s refused a caller by raising", handler.label, exc_info=True)
                denied = f"handler {handler.label}: access denied ({type(e).__name__})"
                return None, _error_reply(id_, ErrorCode.PERMISSION_DENIED, denied)
            if not allowed:
                return None, _error_reply(id_, ErrorCode.PERMISSION_DENIED, f"handler {handler.label}: access denied")
        return message, None

    @staticmethod
    def _info(request: hub_pb2.Request) -> MessageInfo:
        """What a handler of request reads as ``current_message()``."""
        headers = dict(request.headers)
        # An empty correlationId is one the request carries: kept as it is.
        # TODO(spec-gap): SG-9
        return MessageInfo(request.id, request.type, headers.get(CORRELATION_ID, request.id), MappingProxyType(headers))

    async def _call(
        self, handler: Handler, request: hub_pb2.Request, message: object, caller: Caller | None
    ) -> hub_pb2.Reply:
        id_ = request.id
        # Set for this request only, and restored after it: an in-process call runs in its caller's task.
        current = _CURRENT.set(self._info(request))
        calling = _CALLER.set(caller)
        try:
            result: Any = await handler.fn(message)
            payload = self._codec.encode(result)
        except asyncio.CancelledError as e:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise  # this request was cancelled (the hub, the shutdown, an in-process caller): never answered
            # A CancelledError the handler got from something it awaited, not a cancellation of its own task: a
            # failure like any other.
            return self._failed(handler, request, e)
        except Exception as e:
            return self._failed(handler, request, e)
        finally:
            _CALLER.reset(calling)
            _CURRENT.reset(current)
        return hub_pb2.Reply(request_id=id_, payload=payload)

    def _failed(self, handler: Handler, request: hub_pb2.Request, e: BaseException) -> hub_pb2.Reply:
        log.debug("Handler %s of %s request %s failed: %r", handler.label, request.type, request.id, e)
        return hub_pb2.Reply(request_id=request.id, error=error_to_wire(request.id, e, self._codec))

    # Permits.

    def _ended(self, key: tuple[object, str], run: _Running, task: asyncio.Task[None]) -> None:
        if self._running.get(key) is run:
            del self._running[key]
        self._tasks.pop(task, None)
        self._finish_run(run)  # a task cancelled before it ever ran
        if not task.cancelled() and (e := task.exception()) is not None:
            log.error("Serving %s request %s failed", run.request.type, run.request.id, exc_info=e)

    def _finish_run(self, run: _Running) -> None:
        if not run.finished:
            run.finished = True
            self._finish(run.arrival)

    def _finish(self, arrival: Arrival) -> None:
        """One request of the stream finished: its permit is pending, written now or by the flush timer."""
        n, arm = arrival.permits.finished()
        self._grant(arrival, n)
        if arm and arrival.writable and not self._closed:
            timer = asyncio.create_task(self._flush_later(arrival), name="snodra:permits")
            self._timers.add(timer)
            timer.add_done_callback(self._timers.discard)

    async def _flush_later(self, arrival: Arrival) -> None:
        await asyncio.sleep(self._timings.permit_flush_after)
        self._grant(arrival, arrival.permits.flush())

    @staticmethod
    def _grant(arrival: Arrival, n: int) -> None:
        # A stream that is gone drops them: the hub forgot its permits with the session.
        if n > 0:
            arrival.put(hub_pb2.ClientMessage(permits=hub_pb2.Permits(count=n)))

    # Subscription targets: registered when the request is read, admitted in a task of their own.

    def _open_subscription(self, arrival: Arrival, request: hub_pb2.Request) -> None:
        """A ``SUBSCRIPTION`` request (``<id>#n``): registered here, in the reader, before the admission runs, so that
        an update emitted after the subscription's initial request was read reaches it. The duplicate id and the
        instance's limit are decided here; the token check, the handler lookup and the payload follow in a task."""
        handler = self._registry.find("query", request.type)
        query_type = type(None) if handler is None else handler.message
        opened = self._subscriptions.open(arrival, request, query_type)
        if isinstance(opened, hub_pb2.Error):
            self._end(arrival, request.id, ErrorCode(opened.code), opened.message)
            return
        task = asyncio.create_task(self._admit_subscription(opened, request), name=f"snodra:subscription:{request.id}")
        opened.task = task
        self._subscription_tasks.add(task)
        task.add_done_callback(self._subscription_ended)

    async def _admit_subscription(self, served: ServedSubscription, request: hub_pb2.Request) -> None:
        """The subscription's admission: the forwarded token, then the handler that owns the type in mode
        ``SUBSCRIPTION``, then the guards and the payload. A refusal ends it with ``ReplyEnd{error}``; an admitted one
        goes live and what was held is written."""
        try:
            await self._admit_or_refuse(served, request)
        except asyncio.CancelledError:
            raise  # a hub's Cancel, a lost stream or the shutdown: nothing is written
        except Exception as e:  # a bug of the admission is the subscription's failure, never an unanswered target
            log.warning("Admitting subscription %s (%s) failed", request.id, request.type, exc_info=True)
            self._subscriptions.refuse(served, error_to_wire(request.id, e, self._codec))

    async def _admit_or_refuse(self, served: ServedSubscription, request: hub_pb2.Request) -> None:
        id_ = request.id
        try:
            caller = await self._identity.resolve(request.headers.get(AUTHORIZATION))
        except Unauthenticated as e:
            log.debug("Refused subscription %s: %s", id_, e)
            self._subscriptions.refuse(served, failure(id_, ErrorCode.UNAUTHENTICATED, str(e)))
            return
        except Exception:
            log.warning("Checking the token of subscription %s (%s) failed", id_, request.type, exc_info=True)
            self._subscriptions.refuse(served, failure(id_, ErrorCode.UNAUTHENTICATED, "invalid token"))
            return
        # A subscription target looks for the query handler, whatever its kind (the hub's id scheme only).
        handler = self._registry.find("query", request.type)
        if handler is None:
            self._subscriptions.refuse(
                served,
                failure(id_, ErrorCode.INVALID_REQUEST, f"no subscription handler for {request.type} on this instance"),
            )
            return
        if not handler.subscription:
            self._subscriptions.refuse(
                served,
                failure(
                    id_,
                    ErrorCode.INVALID_REQUEST,
                    f"handler {handler.label} for {request.type} is not a subscription query handler "
                    "(it answers mode UNARY)",
                ),
            )
            return
        message, refused = self._prepare(handler, request, caller)
        if refused is not None:
            assert refused.HasField("error")  # _prepare answers a refusal with an Error
            self._subscriptions.refuse(served, refused.error)
            return
        self._subscriptions.activate(served, message)

    def _subscription_ended(self, task: asyncio.Task[None]) -> None:
        self._subscription_tasks.discard(task)
        if not task.cancelled() and (e := task.exception()) is not None:
            log.error("Serving a subscription target failed", exc_info=e)

    @staticmethod
    def _end(arrival: Arrival, request_id: str, code: ErrorCode, message: str) -> None:
        error = hub_pb2.Error(request_id=request_id, code=int(code), message=message)  # type: ignore[arg-type]
        arrival.put(hub_pb2.ClientMessage(end=hub_pb2.ReplyEnd(request_id=request_id, error=error)))

    def _label(self, request: hub_pb2.Request) -> str:
        kind: Kind | None = (
            "command" if request.kind == hub_pb2.COMMAND else "query" if request.kind == hub_pb2.QUERY else None
        )
        handler = None if kind is None else self._registry.find(kind, request.type)
        return request.type if handler is None else handler.label
