"""A streaming query this instance sends: its request, its credit, its parts and its end.

The caller opens the stream with ``async with`` and reads its items with ``async for``; nothing is sent before the block
is entered, and leaving it early (a ``break``, an exception, a cancellation) cancels the stream on the hub and releases
everything of it here.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Streaming").
"""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from typing import Final, Literal, cast

from snodra._connection import Lost, OutboundRequest
from snodra._errors import ErrorCode, HubError, LostParts, OutcomeUnknown, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._messages import type_name
from snodra._retry import RetryPolicy
from snodra._send import Prepared, Sender

log = logging.getLogger(__name__)

CANCEL_REASON: Final = "collector cancelled"
"""Why the caller's own end of a stream (a ``break``, an exception, a cancellation) cancels it."""

NO_PART_GUARD: Final = "no part within the local guard"
"""Why the local guard of a stream that waited too long for its next frame cancels it."""

_UINT32_MAX: Final = 2**32 - 1

_LOST: Final = "stream {type} lost {n} part(s)"
_OUT_OF_ORDER: Final = "stream {type} part seq {seq} out of order (expected {expected})"
_TIMEOUT: Final = "Timed out waiting for reply to '{type}'"


class _State(enum.Enum):
    NEW = "new"
    """Nothing was sent; ``async with`` sends the request."""
    OPEN = "open"
    """Entered: the request is on the wire and its items may be read."""
    CLOSED = "closed"
    """Left: the stream ended or was cancelled, and nothing of it is pending here."""


class _StreamEntry(OutboundRequest):
    """A stream or a subscription waiting for its frames: its parts, its end (or the hub's error for the request
    itself) and the loss of the stream it was sent on, in arrival order.

    Every frame is queued in the order the hub relayed it; the reader decides what a part out of seq or an end that
    counts more parts than it received means when it reaches that frame, so a part that arrived before a loss is still
    handed to the consumer. A gap in the seq the target gave is a part the hub dropped while the stream is still open:
    leaving it cancels the stream. An end short of the parts it counts arrives after the stream ended: nothing is left
    to cancel."""

    keeps_frames = True
    """Its frames keep coming after the first answer (its parts): the entry stays registered for them."""

    def __init__(self, message: hub_pb2.ClientMessage) -> None:
        super().__init__(message)
        self.frames: asyncio.Queue[hub_pb2.ServerMessage | Lost] = asyncio.Queue()

    def take(self, m: hub_pb2.ServerMessage) -> bool:
        if m.WhichOneof("message") not in ("part", "end", "error"):
            return False  # a Reply is no answer to a stream or a subscription
        self.frames.put_nowait(m)
        self.resolve(m)  # the first frame opens the stream; a later one changes nothing
        return True

    def resolve(self, outcome: hub_pb2.ServerMessage | Lost) -> None:
        super().resolve(outcome)
        if isinstance(outcome, Lost):
            self.frames.put_nowait(outcome)  # its caller waits for the next part: the stream is gone


class Stream[T]:
    """The items of a streaming query, read inside ``async with``.

    ``stream = app.stream(query, Item)`` sends nothing; ``async with stream as items`` sends the request and waits for
    the stream to open (its first part, or its end), and ``async for item in items`` yields the items as the hub relays
    them, under the credit this library grants. Leaving the block cancels the stream on the hub when it has not ended,
    whether the caller broke out of the loop, an exception left the block or its task was cancelled: nothing is left
    running here, and the credits stop.

    A stream is entered once (a second ``async with`` is a ``RuntimeError``), and its items are read inside the block
    only (``async for`` outside it is a ``RuntimeError``).
    """

    def __init__(
        self,
        sender: Sender,
        prepared: Prepared,
        item: type[T],
        *,
        credit: int,
        type_: str,
    ) -> None:
        self._sender = sender
        self._prepared = prepared
        self._item = item
        self._credit = credit
        self._refill = max(1, credit // 2)
        self._type = type_
        self._state = _State.NEW
        self._entry: _StreamEntry | None = None
        self._items: AsyncIterator[T] | None = None
        self._consumed = 0
        self._ended = False
        """Its terminal frame arrived (a ``ReplyEnd``, or the loss of the hub's stream): nothing is cancelled then."""

    async def __aenter__(self) -> Stream[T]:
        """Sends the request and returns once the stream is open: its first part, or its end."""
        if self._state is not _State.NEW:
            raise RuntimeError(
                f"a stream of {self._type} is entered once: `async with app.stream(...)` is the one block that sends it"
            )
        self._state = _State.OPEN
        try:
            await self._sender.with_retry(self._prepared, self._attempt, whole_timeout=True)
        except BaseException:
            self._state = _State.CLOSED
            raise
        self._items = self._read()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Cancels the stream when it has not ended, and releases what this library holds of it: no task is left."""
        try:
            await self._close_items()
        finally:
            if self._ended:
                self._release(cancel=False)
            else:
                self._release(cancel=True, reason=_cancel_reason(exc))
            self._state = _State.CLOSED

    def __aiter__(self) -> AsyncIterator[T]:
        if self._state is not _State.OPEN or self._items is None:
            raise RuntimeError(
                f"the items of {self._type} are read inside `async with app.stream(...) as items`, with `async for`"
            )
        return self._items

    async def _attempt(self, template: hub_pb2.Request, chosen: float | None) -> None:
        """One attempt: the request on one hub, and the wait for its first frame; a retryable failure of it is the
        retry's, and a stream that opened keeps its entry for the items that follow."""
        entry: _StreamEntry | None = None

        def make(message: hub_pb2.ClientMessage) -> OutboundRequest:
            nonlocal entry
            entry = _StreamEntry(message)
            return entry

        try:
            answered = await self._sender.route(template, chosen, entry=make, walk=False, queue_wait=True)
        except BaseException as e:
            # Only this library's own guard, or a cancellation, gives up a stream the hub may still be serving: the
            # hub's own errors and a never-sent request need no Cancel.
            local_timeout = _is_local_timeout(e)
            if entry is not None and (local_timeout or isinstance(e, asyncio.CancelledError)):
                self._let_go(entry, cancel=True, reason=NO_PART_GUARD if local_timeout else CANCEL_REASON)
            raise
        message = answered.message
        kind = message.WhichOneof("message")
        if kind == "error":
            raise error_from_wire(message.error, self._type)
        if message.end.HasField("error"):
            raise error_from_wire(message.end.error, self._type)
        opened = cast("_StreamEntry", answered.entry)
        self._entry = opened

    async def _read(self) -> AsyncGenerator[T, None]:
        """The items, in the seq the handler gave them, refilling the credit as the caller consumes them: a part out of
        seq or an end that counts more parts than were delivered is reported when the reader reaches that frame, so the
        parts already delivered stay.

        An end carrying an error is that error whatever its ``parts`` count, also when the count is above the parts
        received: the short-end rule is the specification's for a *clean* end ("a clean ``ReplyEnd`` whose
        ``ReplyEnd.parts`` is more than the parts received"), and a failed stream reports the failure that ended it
        rather than a loss it may also have suffered (the reference checks the count first; this library answers the
        error, deliberately)."""
        entry = self._entry
        assert entry is not None  # a stream is open only once its entry is here
        codec = self._sender.codec
        delivered = 0
        while True:
            frame = await self._next(entry)
            if isinstance(frame, Lost):
                self._ended = True
                raise OutcomeUnknown(frame.why)
            kind = frame.WhichOneof("message")
            if kind == "part":
                seq = frame.part.seq
                if seq != delivered + 1:
                    reason = (
                        _LOST.format(type=self._type, n=seq - delivered - 1)
                        if seq > delivered + 1
                        else _OUT_OF_ORDER.format(type=self._type, seq=seq, expected=delivered + 1)
                    )
                    self._ended = True
                    self._let_go(entry, cancel=True, reason=reason)
                    raise LostParts(reason)
                delivered += 1
                item = codec.decode(frame.part.payload or b"{}", self._item)
                self._consumed += 1
                if self._consumed >= self._refill:
                    self._consumed = 0
                    self._flow(entry, self._refill)
                yield item
                continue
            self._ended = True
            if kind == "error":
                raise error_from_wire(frame.error, self._type)
            if frame.end.HasField("error"):
                raise error_from_wire(frame.end.error, self._type)
            if frame.end.parts > delivered:  # the stream ended, short of the parts its end counted
                raise LostParts(_LOST.format(type=self._type, n=frame.end.parts - delivered))
            return

    async def _next(self, entry: _StreamEntry) -> hub_pb2.ServerMessage | Lost:
        """The next frame of the stream in arrival order, at most the hub's bound for the request plus the local margin
        after the one before it; a hub stream that ends before that frame fails it as an unknown outcome."""
        try:
            async with asyncio.timeout(self._guard(entry)):
                return await entry.frames.get()
        except TimeoutError:
            self._ended = True
            self._let_go(entry, cancel=True, reason=NO_PART_GUARD)
            raise HubError(ErrorCode.TIMEOUT, _TIMEOUT.format(type=self._type)) from None

    def _guard(self, entry: _StreamEntry) -> float:
        """The local guard of the wait for the next frame: the hub's bound for the request plus the margin."""
        stream = entry.stream
        return self._sender.guard_for(None if stream is None else stream.ack, self._type, self._prepared.chosen)

    def _flow(self, entry: _StreamEntry, credits: int) -> None:
        """Grants credits more parts for the stream, on the stream it was sent on."""
        grant_flow(entry, credits)

    def _release(self, *, cancel: bool, reason: str = CANCEL_REASON) -> None:
        """Drops what this library holds of the stream, and with cancel writes the ``Cancel`` (when the stream has not
        ended): its entry waits for no frame, and the credits stop."""
        entry, self._entry = self._entry, None
        if entry is not None:
            self._let_go(entry, cancel=cancel, reason=reason)

    def _let_go(self, entry: _StreamEntry, *, cancel: bool, reason: str) -> None:
        """Forgets a stream's entry here and, with cancel, tells the hub to stop it on the stream it was sent on."""
        release_entry(self._sender, entry, cancel=cancel, reason=reason)

    async def _close_items(self) -> None:
        """Closes the generator the caller was reading, so that nothing of it stays suspended."""
        items, self._items = self._items, None
        if items is not None:
            await cast("AsyncGenerator[T, None]", items).aclose()


def grant_flow(entry: OutboundRequest, credits: int) -> None:
    """Grants credits more parts for a stream or a subscription, on the stream it was sent on."""
    stream = entry.stream
    if stream is not None:
        stream.put(hub_pb2.ClientMessage(flow=hub_pb2.Flow(request_id=entry.id, credits=credits)))


def release_entry(sender: Sender, entry: OutboundRequest, *, cancel: bool, reason: str) -> None:
    """Forgets a stream's or a subscription's entry here and, with cancel, tells the hub to stop it on the stream it was
    sent on."""
    stream = entry.stream
    if cancel and stream is not None:
        stream.put(hub_pb2.ClientMessage(cancel=hub_pb2.Cancel(request_id=entry.id, reason=reason)))
    if sender.pending.get(entry.id) is entry:
        del sender.pending[entry.id]
    if stream is not None:
        stream.forget(entry)


def stream[T](
    sender: Sender,
    query: object,
    item: type[T],
    *,
    initial_credit: int | None = None,
    max_results: int = 0,
    timeout: float | None = None,
    priority: int | None = None,
    headers: Mapping[str, str] | None = None,
    retry: RetryPolicy | Literal[False] | None = None,
) -> Stream[T]:
    """A stream of query's items: its request built here, once its options are checked (nothing sent yet), and sent by
    ``async with``. ``initial_credit`` is the sender's window (None or 0: the protocol's 64); ``max_results`` ends the
    stream after that many parts at the hub (0: until the handler completes)."""
    credit = _credit(initial_credit, sender)
    _check_max_results(max_results)
    prepared = sender.prepare(
        query,
        "query",
        routing_key=None,
        timeout=timeout,
        priority=priority,
        headers=headers,
        retry=retry,
        mode=hub_pb2.STREAMING,
    )
    prepared.template.initial_credit = credit
    prepared.template.max_results = max_results
    return Stream(sender, prepared, item, credit=credit, type_=type_name(query))


def _credit(initial_credit: int | None, sender: Sender) -> int:
    if initial_credit is None:
        return sender.default_credit
    if isinstance(initial_credit, bool) or not isinstance(initial_credit, int):
        raise TypeError(f"initial_credit must be an integer or None, got {initial_credit!r}")
    if not 0 <= initial_credit <= _UINT32_MAX:
        raise ValueError(f"initial_credit must be from 0 to {_UINT32_MAX}, got {initial_credit!r}")
    # 0 reads as 64 at the hub; this library sends the window it will refill from instead.
    return initial_credit or sender.default_credit


def _check_max_results(max_results: int) -> None:
    if isinstance(max_results, bool) or not isinstance(max_results, int):
        raise TypeError(f"max_results must be an integer, got {max_results!r}")
    if not 0 <= max_results <= _UINT32_MAX:
        raise ValueError(f"max_results must be from 0 (no limit) to {_UINT32_MAX}, got {max_results!r}")


def _is_local_timeout(exc: BaseException) -> bool:
    """Whether exc is this library's own guard giving up a request (``HubError`` ``TIMEOUT`` from the wait, not from
    the hub): only that one leaves a stream the hub may still be serving."""
    return isinstance(exc, HubError) and exc.code == ErrorCode.TIMEOUT


def _cancel_reason(exc: tuple[object, ...]) -> str:
    """Why the caller's end of the stream cancels it: a break or a cancellation, or the exception that left the body."""
    failure = exc[1] if len(exc) > 1 else None
    if failure is None or isinstance(failure, asyncio.CancelledError):
        return CANCEL_REASON
    if isinstance(failure, BaseException):
        return f"collector failed: {type(failure).__name__}: {failure}"
    return CANCEL_REASON
