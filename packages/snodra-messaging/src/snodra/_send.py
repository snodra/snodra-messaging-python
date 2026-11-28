"""Sending a command, a point query or a scatter-gather through the hubs: the request, its timing, the hubs tried in
turn, retries, and the calls waiting for their answer.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Requests", "Several hubs").
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Final, Literal

from snodra._codec import Codec
from snodra._connection import HubConnection, Lost, OutboundRequest, _Stream
from snodra._errors import ErrorCode, HubError, NeverSent, OutcomeUnknown, SnodraError, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._ids import new_id
from snodra._local import LocalCalls
from snodra._messages import type_name
from snodra._pool import Pool
from snodra._propagation import outgoing_headers
from snodra._retry import RetryPolicy, retryable
from snodra._settings import Settings
from snodra._telemetry import Telemetry, request_outcome
from snodra._timings import DEFAULT_TIMINGS, Timings

log = logging.getLogger(__name__)

_UINT32_MAX: Final = 2**32 - 1
_INT32: Final = range(-(2**31), 2**31)


def request_timing(
    ack: hub_pb2.RegisterAck | None,
    type_: str,
    chosen: float | None,
    *,
    margin: float = DEFAULT_TIMINGS.guard_margin,
    fallback_max: float = DEFAULT_TIMINGS.fallback_max_timeout,
) -> tuple[int, float]:
    """``Request.timeout_ms`` and the local guard (seconds) of a request of ``type_`` to the hub that sent ``ack``,
    when the call chose ``chosen`` (None: no timeout). It sends the chosen timeout (at least 1 ms) or 0, so that the
    hub applies its per-type override or its default; the guard is the hub's bound for the request plus ``margin``:
    ``min(chosen, override or max)``, or the override or max when 0 is sent (https://snodra.damoreira.com/docs/library-authors/protocol/,
    "Timeouts")."""
    timeout_ms = 0 if chosen is None else min(max(1, round(chosen * 1000)), _UINT32_MAX)
    ack = hub_pb2.RegisterAck() if ack is None else ack
    override = ack.type_timeouts_ms.get(type_, 0)
    if override > 0:
        cap_ms: float = override
    elif ack.max_timeout_ms > 0:
        cap_ms = ack.max_timeout_ms
    else:
        cap_ms = fallback_max * 1000
    bound_ms = min(timeout_ms, cap_ms) if timeout_ms > 0 else cap_ms
    return timeout_ms, bound_ms / 1000 + margin


def call_deadline(chosen: float | None, *, default: float = DEFAULT_TIMINGS.default_deadline) -> float:
    """How long a call waits for a hub to take its request, and the latest a hop to another hub starts: the chosen
    timeout, else ``default`` (5 s)."""
    return chosen if chosen is not None else default


def retry_budget(
    ack: hub_pb2.RegisterAck | None,
    type_: str,
    chosen: float | None,
    *,
    default: float = DEFAULT_TIMINGS.default_deadline,
) -> float:
    """The time every attempt of a retried call shares: the chosen timeout, else the hub's override for the type,
    else its default timeout, else ``default`` (5 s)."""
    if chosen is not None:
        return chosen
    if ack is not None:
        if (override := ack.type_timeouts_ms.get(type_, 0)) > 0:
            return override / 1000
        if ack.default_timeout_ms > 0:
            return ack.default_timeout_ms / 1000
    return default


@dataclass(frozen=True)
class _NotTaken:
    """The hub did not take the request: no live stream when chosen, or it ended before the request was queued."""


@dataclass(frozen=True)
class _LostInBuffer:
    """The request was queued, and its stream ended before the writer took it: a move."""


@dataclass(frozen=True)
class Answered:
    """A request a hub answered: the frame that answered it (a ``Reply``, an ``Error`` or a ``ReplyEnd``) and its
    entry."""

    message: hub_pb2.ServerMessage
    entry: OutboundRequest


@dataclass(frozen=True)
class Prepared:
    """A call's request before its id, and how it is sent."""

    template: hub_pb2.Request
    chosen: float | None
    """The call's timeout (None: the hub decides)."""
    policy: RetryPolicy | None


_Hop = Answered | _NotTaken | _LostInBuffer


class Sender:
    """Sends commands, point queries and scatter-gathers through the hubs and keeps the calls waiting for an answer:
    one entry per request id, buffered while in a stream's outbound buffer, written once the writer took it."""

    def __init__(self, codec: Codec, settings: Settings, timings: Timings, telemetry: Telemetry | None = None) -> None:
        self._codec = codec
        self._settings = settings
        self._timings = timings
        self._telemetry = telemetry
        self.pool: Pool | None = None
        """The hubs; set by the app once built (they deliver the answers to :meth:`on_answer`)."""
        self.pending: dict[str, OutboundRequest] = {}
        """The requests waiting for their answer, by id."""
        self.local: LocalCalls | None = None
        """Answers the calls of the instance's own types in-process; set by the app."""

    @property
    def codec(self) -> Codec:
        """Reads and writes the payloads."""
        return self._codec

    @property
    def has_configured_hubs(self) -> bool:
        """Whether the app has hub addresses; without one a command must be answered by a local handler."""
        return bool(self._settings.hubs)

    @property
    def default_credit(self) -> int:
        """The window of a stream whose sender asked for none (``Request.initial_credit`` 0 reads as 64 at the hub)."""
        return self._timings.default_credit

    def guard_for(
        self, ack: hub_pb2.RegisterAck | None, type_: str, chosen: float | None, *, queue_wait: bool = False
    ) -> float:
        """The local guard (seconds) of a request of type_ to the hub that sent ack, when the call chose chosen
        (None: no timeout): the hub's bound plus the margin, and twice the bound plus the margin with ``queue_wait``
        (a streaming handler's first part, which the hub forwards after a queue wait)."""
        _, guard = request_timing(
            ack, type_, chosen, margin=self._timings.guard_margin, fallback_max=self._timings.fallback_max_timeout
        )
        if queue_wait:
            bound = guard - self._timings.guard_margin
            guard = 2 * bound + self._timings.guard_margin
        return guard

    def on_answer(self, stream: _Stream, m: hub_pb2.ServerMessage) -> None:
        """A ``Reply``, an ``Error``, a ``ReplyPart`` or a ``ReplyEnd`` from a hub (called by the stream's reader): the
        request of that id sent on that stream takes it; any other is dropped (its caller gave up, it came from
        another stream, or it is no answer to that request)."""
        kind = m.WhichOneof("message")
        id_ = getattr(m, kind).request_id if kind is not None else ""
        request = self.pending.get(id_)
        if request is None or request.stream is not stream:
            log.debug("A %s for request %s dropped: nobody waits for it on this stream", kind, id_)
            return
        if not request.take(m):
            log.debug("A %s for request %s dropped: no answer to that request", kind, id_)

    async def call(
        self,
        message: object,
        result: type | None,
        kind: Literal["command", "query"],
        *,
        routing_key: str | None,
        timeout: float | None,  # noqa: ASYNC109 (the call's timeout)
        priority: int | None,
        headers: Mapping[str, str] | None,
        retry: RetryPolicy | Literal[False] | None,
    ) -> object:
        """One call: the request built, sent (retried by the policy), its answer decoded as ``result`` (None: the
        answer is not read). A call of a type this instance handles is answered in-process (see :mod:`snodra._local`):
        its options are checked, and its timeout, retry, routing key and priority do not apply."""
        p = self.prepare(
            message,
            kind,
            routing_key=routing_key,
            timeout=timeout,
            priority=priority,
            headers=headers,
            retry=retry,
        )
        t = p.template
        if self.local is not None and (local := self.local.handler(kind, t.type)) is not None:
            return await self.local.call(local, kind, t.payload, dict(t.headers), result)
        started = time.monotonic()
        exc: BaseException | None = None
        try:
            payload = await self.with_retry(p, self._attempt)
            if result is None:
                return None
            return self._codec.decode(payload or b"{}", result)
        except BaseException as e:
            exc = e
            raise
        finally:
            if self._telemetry is not None:
                outcome = request_outcome(exc)
                if outcome is not None:
                    self._telemetry.request_done(kind, t.type, outcome, time.monotonic() - started)

    async def call_payload(
        self,
        type_: str,
        payload: bytes,
        kind: Literal["command", "query"] = "command",
        *,
        routing_key: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109 (the call's timeout)
        priority: int | None = None,
        headers: Mapping[str, str] | None = None,
        retry: RetryPolicy | Literal[False] | None = None,
    ) -> None:
        """Sends an already encoded message by its wire type, with headers the caller built itself.

        The saga relay uses it: the entry's payload and headers (its correlation id, idempotency key and saga id) are
        already on disk, and there is no message object to encode. A type this instance handles is answered in-process;
        the headers are passed as they are (the reserved names included), since the relay owns them. The answer's
        payload is not read.
        """
        chosen = self._chosen(timeout)
        policy = self._policy(retry)
        if priority is not None and (
            isinstance(priority, bool) or not isinstance(priority, int) or priority not in _INT32
        ):
            raise ValueError(f"the priority of {type_} must be a 32-bit integer, got {priority!r}")
        template = hub_pb2.Request(
            kind=hub_pb2.COMMAND if kind == "command" else hub_pb2.QUERY,
            type=type_,
            routing_key=routing_key or "",
            payload=payload,
            headers=dict(headers or {}),
            priority=self._settings.priorities.get(type_, 0) if priority is None else priority,
            mode=hub_pb2.UNARY,
        )
        p = Prepared(template, chosen, policy)
        if self.local is not None and (local := self.local.handler(kind, type_)) is not None:
            await self.local.call(local, kind, payload, dict(template.headers), None)
            return
        await self.with_retry(p, self._attempt, whole_timeout=False)

    def prepare(
        self,
        message: object,
        kind: Literal["command", "query"],
        *,
        routing_key: str | None,
        timeout: float | None,
        priority: int | None,
        headers: Mapping[str, str] | None,
        retry: RetryPolicy | Literal[False] | None,
        mode: hub_pb2.RequestMode = hub_pb2.UNARY,
    ) -> Prepared:
        """The call's request (without its id) and its timeout and retry policy, once its options are checked; raises
        before anything is sent for an option it refuses."""
        type_ = type_name(message)
        # The own headers are checked before anything else is built: a reserved name sends nothing.
        request_headers = outgoing_headers(headers, forward_jwt=self._settings.forward_jwt, telemetry=self._telemetry)
        chosen = self._chosen(timeout)
        policy = self._policy(retry)
        if priority is not None and (
            isinstance(priority, bool) or not isinstance(priority, int) or priority not in _INT32
        ):
            raise ValueError(f"the priority of {type_} must be a 32-bit integer, got {priority!r}")
        if routing_key is not None and not isinstance(routing_key, str):
            raise TypeError(f"the routing key of {type_} must be a text, got {routing_key!r}")
        template = hub_pb2.Request(
            kind=hub_pb2.COMMAND if kind == "command" else hub_pb2.QUERY,
            type=type_,
            routing_key=routing_key or "",
            payload=self._codec.encode(message),
            headers=request_headers,
            priority=self._settings.priorities.get(type_, 0) if priority is None else priority,
            mode=mode,
        )
        return Prepared(template, chosen, policy)

    def _chosen(self, timeout: float | None) -> float | None:
        """The call's timeout: ``timeout``, else ``default_timeout``. 0 (or less) is none, also over the default: the
        hub decides."""
        if timeout is None:
            return self._settings.default_timeout
        if isinstance(timeout, bool) or not isinstance(timeout, int | float):
            raise TypeError(f"timeout must be a number of seconds or None, got {timeout!r}")
        if not math.isfinite(timeout):
            raise ValueError(f"timeout must be a finite number of seconds, got {timeout!r}")
        return float(timeout) if timeout > 0 else None

    def _policy(self, retry: RetryPolicy | Literal[False] | None) -> RetryPolicy | None:
        if retry is None:
            return self._settings.default_retry
        if retry is False:
            return None
        if not isinstance(retry, RetryPolicy):
            raise TypeError(f"retry must be a RetryPolicy, False or None, got {retry!r}")
        return retry

    async def with_retry[T](
        self,
        p: Prepared,
        run: Callable[[hub_pb2.Request, float | None], Awaitable[T]],
        *,
        whole_timeout: bool = False,
    ) -> T:
        """What ``run(template, timeout)`` returns; attempts follow the call's policy within one budget, each a fresh
        id (``run`` routes the template) sending what is left of the budget (at least 1 ms). With ``whole_timeout``
        they send the caller's chosen timeout as it is, and the budget only bounds when an attempt may start (a
        stream's idle timeout, which the hub restarts at every part)."""
        template, chosen, policy = p.template, p.chosen, p.policy
        if policy is None or policy.max_attempts <= 1:
            return await run(template, chosen)
        t = self._timings
        loop = asyncio.get_running_loop()
        ack = None if self.pool is None else self.pool.first_ack()
        budget = retry_budget(ack, template.type, chosen, default=t.default_deadline)
        deadline = loop.time() + budget
        attempt = 1
        while True:
            remaining = max(deadline - loop.time(), 0.001)
            try:
                return await run(template, chosen if whole_timeout else remaining)
            except SnodraError as e:
                if attempt >= policy.max_attempts or not retryable(template.kind, e) or self._leaving():
                    raise
                wait = policy.wait_before(attempt + 1)
                # Too little of the budget left after the wait for a useful attempt: this error is the answer, now.
                if deadline - loop.time() - wait < max(policy.initial_backoff, t.retry_min_remaining):
                    raise
                log.debug("Retrying %s in %.3f s after: %s", template.type, wait, e)
                await asyncio.sleep(wait)
                attempt += 1

    async def _attempt(self, template: hub_pb2.Request, chosen: float | None) -> bytes:
        """One attempt: the answer's payload, or the error it maps to."""
        answer = (await self.route(template, chosen)).message
        if answer.WhichOneof("message") == "error":
            raise error_from_wire(answer.error, template.type)
        reply = answer.reply
        if reply.WhichOneof("result") == "error":
            raise error_from_wire(reply.error, template.type)
        return reply.payload

    def _leaving(self) -> bool:
        return self.pool is not None and self.pool.leaving

    async def route(
        self,
        template: hub_pb2.Request,
        chosen: float | None,
        *,
        entry: Callable[[hub_pb2.ClientMessage], OutboundRequest] = OutboundRequest,
        walk: bool = True,
        queue_wait: bool = False,
    ) -> Answered:
        """Sends the request, with one fresh id, to the hubs in turn until one answers it, within one deadline (the
        chosen timeout, else 5 s): a hub that did not take the request is not tried again, nor, with ``walk`` (a unary
        request), one that answers ``NO_HANDLER``; when every live hub declined, the last ``NO_HANDLER`` is the answer
        at once. With no hub to take it, the call waits for the live hubs to change until the deadline; no hop starts
        after it. Every hop sends the chosen timeout whole. ``entry`` makes the request's entry, which takes the hub's
        frames for its id; ``queue_wait`` gives its first answer the hub's queue wait too (a streaming handler's first
        part, which the hub forwards after a queue wait and then times out on its own)."""
        t = self._timings
        type_ = template.type
        loop = asyncio.get_running_loop()
        # Without a chosen timeout (also a call's explicit 0) the configured default still bounds the wait for a hub.
        wait = (
            chosen if chosen is not None else call_deadline(self._settings.default_timeout, default=t.default_deadline)
        )
        deadline = loop.time() + wait
        request = hub_pb2.Request()
        request.CopyFrom(template)
        request.id = new_id()
        tried: set[HubConnection] = set()
        moves = 0
        declined: Answered | None = None
        while True:
            pool = self.pool
            if pool is None or pool.leaving:
                raise NeverSent(f"'{type_}' not sent: the app is shutting down")
            changed = pool.change_signal()  # taken before the choice: a change after it wakes the wait below
            conn = pool.pick(request.routing_key, tried)
            if conn is None and declined is not None:
                return declined  # every live hub declined: that is the answer
            left = deadline - loop.time()
            if conn is None or left <= 0:
                if conn is None and left > 0 and await _woken(changed, left):
                    tried.clear()  # the live hubs changed: a hub that did not take it may have a new stream
                    continue
                # TODO(spec-gap): SG-3
                text = "no hub connected within" if not tried else "no hub took the request within"
                raise NeverSent(f"{text} {round(wait * 1000)} ms")
            hop = await self._hop(conn, request, chosen, deadline, entry, queue_wait)
            if isinstance(hop, _NotTaken):
                tried.add(conn)
            elif isinstance(hop, _LostInBuffer):
                moves += 1
                if moves > t.max_unwritten_moves:
                    raise NeverSent(f"'{type_}' not written: {moves} hub streams lost before it was")
                log.debug("Request %s (%s) lost in a stream's buffer: moving it (%d)", request.id, type_, moves)
                tried.add(conn)
            elif walk and _no_handler(hop.message):
                log.debug(
                    "Hub %s has no handler for request %s (%s): trying another hub", conn.address, request.id, type_
                )
                declined = hop
                tried.add(conn)
            else:
                return hop

    async def _hop(
        self,
        conn: HubConnection,
        request: hub_pb2.Request,
        chosen: float | None,
        deadline: float,
        entry: Callable[[hub_pb2.ClientMessage], OutboundRequest],
        queue_wait: bool = False,
    ) -> _Hop:
        """The request on one hub's live stream, and what came of it; ``TIMEOUT`` after the local guard (for a gather,
        the guard of the whole gather; for a stream's first part, its queue wait included), outcome unknown when the
        stream ended after the write."""
        stream = conn.live_stream()
        if stream is None:
            return _NotTaken()
        timeout_ms = request_timing(
            stream.ack,
            request.type,
            chosen,
            margin=self._timings.guard_margin,
            fallback_max=self._timings.fallback_max_timeout,
        )[0]
        # The hub may hold the request in its queue for the whole idle timeout before forwarding it, and starts its
        # own bound again at the forward: for a stream's first part the local guard is two bounds and one margin.
        guard = self.guard_for(stream.ack, request.type, chosen, queue_wait=queue_wait)
        request.timeout_ms = timeout_ms
        # TODO(spec-gap): SG-8
        while stream.buffered >= self._timings.outbound_limit:  # wait for room, within the call's deadline
            room = stream.room()
            try:
                async with asyncio.timeout_at(deadline):
                    await room
            except TimeoutError:
                return _NotTaken()
            if self._leaving():
                raise NeverSent(f"'{request.type}' not sent: the app is shutting down")
            if not stream.writable:
                return _NotTaken()
        outbound = entry(hub_pb2.ClientMessage(request=request))
        if not stream.put_request(outbound):
            return _NotTaken()
        self.pending[outbound.id] = outbound
        keep = False
        try:
            async with asyncio.timeout(guard):
                outcome = await outbound.answer
            keep = outbound.keeps_frames and not isinstance(outcome, Lost) and outcome.WhichOneof("message") == "part"
        except TimeoutError:
            # The hub answers TIMEOUT itself; this guard covers a hub that never answers.
            raise HubError(ErrorCode.TIMEOUT, f"Timed out waiting for reply to '{request.type}'") from None
        finally:
            if self.pending.get(outbound.id) is outbound:
                del self.pending[outbound.id]
            stream.forget(outbound)
            if keep:  # a stream's parts keep coming: its entry stays registered, with no window in between
                stream.regain(outbound)
                self.pending[outbound.id] = outbound
        if isinstance(outcome, Lost):
            if outcome.written:
                raise OutcomeUnknown(outcome.why)
            return _LostInBuffer()
        return Answered(outcome, outbound)


def _no_handler(m: hub_pb2.ServerMessage) -> bool:
    """The hub's ``Error{NO_HANDLER}``: it delivered the request to no handler."""
    return m.WhichOneof("message") == "error" and m.error.code == hub_pb2.NO_HANDLER


async def _woken(changed: asyncio.Event, within: float) -> bool:
    """True when ``changed`` is set within ``within`` seconds."""
    try:
        async with asyncio.timeout(within):
            await changed.wait()
    except TimeoutError:
        return False
    return True
