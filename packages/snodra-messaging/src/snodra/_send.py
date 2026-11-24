"""Sending a command or a point query through the hubs: the request, its timing, the hubs tried in turn, retries, and
the calls waiting for their answer.

See https://snodra.damoreira.com/library-authors/protocol/ ("Requests", "Several hubs").
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from snodra._codec import Codec
from snodra._connection import HubConnection, Lost, OutboundRequest, _Stream
from snodra._errors import ErrorCode, HubError, NeverSent, OutcomeUnknown, SnodraError, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._ids import new_id
from snodra._messages import type_name
from snodra._pool import Pool
from snodra._retry import RetryPolicy, retryable
from snodra._settings import Settings
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
    ``min(chosen, override or max)``, or the override or max when 0 is sent (https://snodra.damoreira.com/library-authors/protocol/,
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


_Hop = hub_pb2.ServerMessage | _NotTaken | _LostInBuffer


class Sender:
    """Sends commands and point queries through the hubs and keeps the calls waiting for an answer: one entry per
    request id, buffered while in a stream's outbound buffer, written once the writer took it."""

    def __init__(self, codec: Codec, settings: Settings, timings: Timings) -> None:
        self._codec = codec
        self._settings = settings
        self._timings = timings
        self.pool: Pool | None = None
        """The hubs; set by the app once built (they deliver the answers to :meth:`on_answer`)."""
        self.pending: dict[str, OutboundRequest] = {}
        """The requests waiting for their answer, by id."""

    def on_answer(self, stream: _Stream, m: hub_pb2.ServerMessage) -> None:
        """A ``Reply`` or an ``Error`` from a hub (called by the stream's reader): it answers the request of that id
        sent on that stream; any other is dropped (its caller gave up, or it came from another stream)."""
        id_ = m.reply.request_id if m.WhichOneof("message") == "reply" else m.error.request_id
        request = self.pending.get(id_)
        if request is None or request.stream is not stream:
            log.debug("Answer to request %s dropped: nobody waits for it on this stream", id_)
            return
        request.resolve(m)

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
        answer is not read)."""
        type_ = type_name(message)
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
            headers=dict(headers or {}),
            priority=self._settings.priorities.get(type_, 0) if priority is None else priority,
            mode=hub_pb2.UNARY,
        )
        payload = await self._with_retry(template, chosen, policy)
        if result is None:
            return None
        return self._codec.decode(payload or b"{}", result)

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

    async def _with_retry(self, template: hub_pb2.Request, chosen: float | None, policy: RetryPolicy | None) -> bytes:
        """The answer's payload; attempts follow ``policy`` within one budget, each a fresh id sending what is left of
        the budget (at least 1 ms)."""
        if policy is None or policy.max_attempts <= 1:
            return await self._attempt(template, chosen)
        t = self._timings
        loop = asyncio.get_running_loop()
        ack = None if self.pool is None else self.pool.first_ack()
        budget = retry_budget(ack, template.type, chosen, default=t.default_deadline)
        deadline = loop.time() + budget
        attempt = 1
        while True:
            remaining = max(deadline - loop.time(), 0.001)
            try:
                return await self._attempt(template, remaining)
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
        answer = await self._route(template, chosen)
        if answer.WhichOneof("message") == "error":
            raise error_from_wire(answer.error, template.type)
        reply = answer.reply
        if reply.WhichOneof("result") == "error":
            raise error_from_wire(reply.error, template.type)
        return reply.payload

    def _leaving(self) -> bool:
        return self.pool is not None and self.pool.leaving

    async def _route(self, template: hub_pb2.Request, chosen: float | None) -> hub_pb2.ServerMessage:
        """Sends the request, with one fresh id, to the hubs in turn until one answers it, within one deadline (the
        chosen timeout, else 5 s): a hub that answers ``NO_HANDLER`` or did not take the request is not tried again;
        when every live hub declined, the last ``NO_HANDLER`` is the answer at once. With no hub to take it, the call
        waits for the live hubs to change until the deadline; no hop starts after it. Every hop sends the chosen
        timeout whole."""
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
        declined: hub_pb2.ServerMessage | None = None
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
            hop = await self._hop(conn, request, chosen, deadline)
            if isinstance(hop, _NotTaken):
                tried.add(conn)
            elif isinstance(hop, _LostInBuffer):
                moves += 1
                if moves > t.max_unwritten_moves:
                    raise NeverSent(f"'{type_}' not written: {moves} hub streams lost before it was")
                log.debug("Request %s (%s) lost in a stream's buffer: moving it (%d)", request.id, type_, moves)
                tried.add(conn)
            elif hop.WhichOneof("message") == "error" and hop.error.code == hub_pb2.NO_HANDLER:
                log.debug(
                    "Hub %s has no handler for request %s (%s): trying another hub", conn.address, request.id, type_
                )
                declined = hop
                tried.add(conn)
            else:
                return hop

    async def _hop(self, conn: HubConnection, request: hub_pb2.Request, chosen: float | None, deadline: float) -> _Hop:
        """The request on one hub's live stream, and what came of it; ``TIMEOUT`` after the local guard, outcome
        unknown when the stream ended after the write."""
        stream = conn.live_stream()
        if stream is None:
            return _NotTaken()
        timeout_ms, guard = request_timing(
            stream.ack,
            request.type,
            chosen,
            margin=self._timings.guard_margin,
            fallback_max=self._timings.fallback_max_timeout,
        )
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
        outbound = OutboundRequest(hub_pb2.ClientMessage(request=request))
        if not stream.put_request(outbound):
            return _NotTaken()
        self.pending[outbound.id] = outbound
        try:
            async with asyncio.timeout(guard):
                outcome = await outbound.answer
        except TimeoutError:
            # The hub answers TIMEOUT itself; this guard covers a hub that never answers.
            raise HubError(ErrorCode.TIMEOUT, f"Timed out waiting for reply to '{request.type}'") from None
        finally:
            if self.pending.get(outbound.id) is outbound:
                del self.pending[outbound.id]
            stream.forget(outbound)
        if isinstance(outcome, Lost):
            if outcome.written:
                raise OutcomeUnknown(outcome.why)
            return _LostInBuffer()
        return outcome


async def _woken(changed: asyncio.Event, within: float) -> bool:
    """True when ``changed`` is set within ``within`` seconds."""
    try:
        async with asyncio.timeout(within):
            await changed.wait()
    except TimeoutError:
        return False
    return True
