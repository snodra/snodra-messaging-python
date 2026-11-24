"""A request the hub forwarded, end to end: its task, its handler, its one answer and its permit.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Handling a request", "Flow control", "Cancel").
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final, Protocol

from snodra._codec import Codec
from snodra._context import _CALLER, _CURRENT, MessageInfo
from snodra._errors import DecodeError, ErrorCode, Unauthenticated, error_to_wire
from snodra._hubpb import hub_pb2
from snodra._identity import AUTHORIZATION, Caller, IdentityResolver
from snodra._permits import PermitLedger
from snodra._registry import Handler, Kind, Registry
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
    ) -> None:
        self._registry = registry
        self._codec = codec
        self._identity = IdentityResolver(None, "roles") if identity is None else identity
        self._max_concurrent = max_concurrent
        self._slots: asyncio.Semaphore | None = None
        self._timings = timings
        self._running: dict[tuple[object, str], _Running] = {}
        self._tasks: dict[asyncio.Task[None], _Running] = {}
        self._timers: set[asyncio.Task[None]] = set()
        self._closed = False
        """Set by :meth:`abandon`: no permit timer is armed any more (the shutdown is under way)."""

    # Called by the stream's reader: never raises, runs no user code, never waits.

    def on_request(self, arrival: Arrival, request: hub_pb2.Request) -> None:
        """A ``ServerMessage.request``, also before the stream's ``RegisterAck``."""
        if request.mode == hub_pb2.SUBSCRIPTION:
            self._refuse_subscription(arrival, request)  # outside flow control: no permit
            return
        arrival.permits.arrived()
        if request.mode == hub_pb2.STREAMING:
            self._refuse_stream(arrival, request)
            self._finish(arrival)
            return
        key = (arrival, request.id)
        run = _Running(arrival, request, self._label(request))
        run.task = asyncio.create_task(self._serve(run), name=f"snodra:handler:{request.id}")
        self._tasks[run.task] = run
        self._running[key] = run
        run.task.add_done_callback(lambda t: self._ended(key, run, t))

    def on_cancel(self, arrival: Arrival, cancel: hub_pb2.Cancel) -> None:
        """The hub gave up on a request this instance serves: its handler is cancelled and nothing is answered; a
        ``Cancel`` for a request that already ended is ignored."""
        run = self._running.pop((arrival, cancel.request_id), None)
        if run is None or run.task is None:
            log.debug("Cancel for request %s ignored: not running here (%s)", cancel.request_id, cancel.reason)
            return
        log.debug("The hub cancelled request %s (%s): %s", cancel.request_id, run.label, cancel.reason)
        run.cancelled = True
        run.task.cancel()

    # Shutdown.

    async def drain(self, timeout: float) -> None:  # noqa: ASYNC109 (a bound of the shutdown)
        """Waits for the handlers in flight, at most ``timeout`` (also when new requests arrive meanwhile); then
        cancels those still running, and never waits for them again. A cancellation of the wait does the same."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            while self._tasks and (left := deadline - loop.time()) > 0:
                await asyncio.wait(set(self._tasks), timeout=left)
        finally:
            self.abandon()

    def abandon(self) -> None:
        """Cancels every handler still running, without waiting for it, and the permit timers; one warning names the
        handlers it cancels. From then on no permit timer is armed: a handler it cancels ends after it, and its
        permit, when still due, is written at once or dropped with its stream."""
        self._closed = True
        for timer in list(self._timers):
            timer.cancel()
        left = [r for t, r in self._tasks.items() if not t.done() and not r.abandoned]
        for run in left:
            run.abandoned = True
            if run.task is not None:
                run.task.cancel()
        if left:
            names = ", ".join(f"{r.label} ({r.request.type} {r.request.id})" for r in left)
            log.warning(
                "%d handler(s) still running at the end of the drain, cancelled and not awaited: %s", len(left), names
            )

    # A request's task.

    async def _serve(self, run: _Running) -> None:
        try:
            if self._slots is None:
                self._slots = asyncio.Semaphore(self._max_concurrent)
            async with self._slots:  # held until the handler really ended, a cancelled one too
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
        id_ = request.id
        guard = handler.guard
        if guard is not None and caller is None:
            # Before decoding, so that an anonymous caller cannot probe the payload shapes of a guarded handler.
            # TODO(spec-gap): SG-1
            return _error_reply(id_, ErrorCode.UNAUTHENTICATED, f"handler {handler.label} requires authentication")
        try:
            message: object = self._codec.decode(request.payload, handler.message)
        except DecodeError as e:
            return _error_reply(id_, ErrorCode.INVALID_REQUEST, str(e))
        except Exception as e:  # a codec's bug is the handler's failure, never the stream's
            log.warning("Decoding %s request %s failed", request.type, id_, exc_info=True)
            return hub_pb2.Reply(request_id=id_, error=error_to_wire(id_, e, self._codec))
        if guard is not None and caller is not None:
            # The rule on the caller (a role, a given caller) applies once the payload decoded.
            try:
                allowed = guard.allows(caller)
            except Exception as e:
                log.warning("The guard of %s refused a caller by raising", handler.label, exc_info=True)
                denied = f"handler {handler.label}: access denied ({type(e).__name__})"
                return _error_reply(id_, ErrorCode.PERMISSION_DENIED, denied)
            if not allowed:
                return _error_reply(id_, ErrorCode.PERMISSION_DENIED, f"handler {handler.label}: access denied")
        return await self._call(handler, request, message, caller)

    async def _call(
        self, handler: Handler, request: hub_pb2.Request, message: object, caller: Caller | None
    ) -> hub_pb2.Reply:
        id_ = request.id
        headers = dict(request.headers)
        # Set for this request only, and restored after it: an in-process call runs in its caller's task.
        # An empty correlationId is one the request carries: kept as it is.
        # TODO(spec-gap): SG-9
        info = MessageInfo(id_, request.type, headers.get(CORRELATION_ID, id_), MappingProxyType(headers))
        current = _CURRENT.set(info)
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

    # Modes that have no handler here yet.

    def _refuse_stream(self, arrival: Arrival, request: hub_pb2.Request) -> None:
        query = self._registry.find("query", request.type)
        if query is None:
            error = (ErrorCode.NO_HANDLER, f"no handler for type {request.type}")
        else:
            error = (
                ErrorCode.INVALID_REQUEST,
                f"handler {query.label} for {request.type} is not a streaming query handler (it answers mode UNARY)",
            )
        self._end(arrival, request.id, *error)

    def _refuse_subscription(self, arrival: Arrival, request: hub_pb2.Request) -> None:
        query = self._registry.find("query", request.type)
        if query is None:
            message = f"no subscription handler for {request.type} on this instance"
        else:
            message = (
                f"handler {query.label} for {request.type} is not a subscription query handler (it answers mode UNARY)"
            )
        self._end(arrival, request.id, ErrorCode.INVALID_REQUEST, message)

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
