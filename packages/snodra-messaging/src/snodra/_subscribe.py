"""A subscription query this instance sends: its initial result, its updates, its credit and its end.

The caller opens the subscription with ``async with``, which sends the request and waits for the initial result, then
reads the updates with ``async for``; nothing is sent before the block is entered, and leaving it early (a ``break``, an
exception, a cancellation) cancels the subscription on the hub and releases everything of it here. There is no idle
timeout: a quiet subscription is not a broken one.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Subscription").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from typing import Literal, cast

from snodra._connection import Lost, OutboundRequest
from snodra._errors import ErrorCode, HubError, LostParts, OutcomeUnknown, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._messages import type_name
from snodra._retry import RetryPolicy
from snodra._send import Prepared, Sender
from snodra._stream_client import (
    CANCEL_REASON,
    _cancel_reason,
    _check_max_results,
    _credit,
    _is_local_timeout,
    _State,
    _StreamEntry,
    grant_flow,
    release_entry,
)

log = logging.getLogger(__name__)

NO_INITIAL_GUARD = "no initial result within the local guard"
"""Why the local guard of a subscription that waited too long for its initial result cancels it."""

_UNEXPECTED = "unexpected answer to a subscription to {type}"


class Subscription[I, U]:
    """The initial result and the updates of a subscription query, read inside ``async with``.

    ``subscription = app.subscribe(query, Initial, Update)`` sends nothing; ``async with subscription as sub`` sends
    the request and waits for the initial result, kept as ``sub.initial``, and ``async for update in sub`` yields the
    updates as the hub relays them, under the credit this library grants (the initial result counts as consumed).
    Leaving the block cancels the subscription on the hub when it has not ended, whether the caller broke out of the
    loop, an exception left the block or its task was cancelled: nothing is left running here.

    A subscription is entered once (a second ``async with`` is a ``RuntimeError``), and its updates are read inside the
    block only (``async for`` outside it is a ``RuntimeError``)."""

    def __init__(
        self,
        sender: Sender,
        prepared: Prepared,
        initial: type[I],
        update: type[U],
        *,
        credit: int,
        type_: str,
    ) -> None:
        self._sender = sender
        self._prepared = prepared
        self._initial_type = initial
        self._update = update
        self._credit = credit
        self._refill = max(1, credit // 2)
        self._type = type_
        self._state = _State.NEW
        self._entry: _StreamEntry | None = None
        self._items: AsyncIterator[U] | None = None
        self._consumed = 0
        self._ended = False
        """Its terminal frame arrived (a ``ReplyEnd``, or the loss of the hub's stream): nothing is cancelled then."""
        self.initial: I
        """The initial result, set by ``async with`` (it waits for it)."""

    async def __aenter__(self) -> Subscription[I, U]:
        """Sends the request and returns once the initial result arrived, kept as :attr:`initial`."""
        if self._state is not _State.NEW:
            raise RuntimeError(
                f"a subscription to {self._type} is entered once: `async with app.subscribe(...)` is the one block "
                "that sends it"
            )
        self._state = _State.OPEN
        try:
            await self._sender.with_retry(self._prepared, self._attempt)
        except BaseException:
            self._state = _State.CLOSED
            raise
        entry = self._entry
        assert entry is not None  # an open subscription has its entry
        self._consumed_one(entry)  # the initial result counts as consumed as soon as it was handed over
        self._items = self._read()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Cancels the subscription when it has not ended, and releases what this library holds of it: no task is
        left."""
        try:
            await self._close_items()
        finally:
            if self._ended:
                self._release(cancel=False)
            else:
                self._release(cancel=True, reason=_cancel_reason(exc))
            self._state = _State.CLOSED

    def __aiter__(self) -> AsyncIterator[U]:
        if self._state is not _State.OPEN or self._items is None:
            raise RuntimeError(
                f"the updates of {self._type} are read inside `async with app.subscribe(...) as sub`, with `async for`"
            )
        return self._items

    async def _attempt(self, template: hub_pb2.Request, chosen: float | None) -> None:
        """One attempt: the request on one hub, and the wait for its initial result; a retryable failure of it is the
        retry's, and a subscription that opened keeps its entry for the updates that follow."""
        entry: _StreamEntry | None = None

        def make(message: hub_pb2.ClientMessage) -> OutboundRequest:
            nonlocal entry
            entry = _StreamEntry(message)
            return entry

        try:
            answered = await self._sender.route(template, chosen, entry=make, walk=False, queue_wait=False)
        except BaseException as e:
            # Only this library's own guard, or a cancellation, gives up a subscription the hub may still be serving:
            # the hub's own errors and a never-sent request need no Cancel.
            local_timeout = _is_local_timeout(e)
            if entry is not None and (local_timeout or isinstance(e, asyncio.CancelledError)):
                release_entry(
                    self._sender,
                    entry,
                    cancel=True,
                    reason=NO_INITIAL_GUARD if local_timeout else CANCEL_REASON,
                )
            raise
        message = answered.message
        kind = message.WhichOneof("message")
        if kind == "error":
            raise error_from_wire(message.error, self._type)
        if kind == "end":
            if message.end.HasField("error"):
                raise error_from_wire(message.end.error, self._type)
            # TODO(spec-gap): SG-12
            raise HubError(ErrorCode.INVALID_REQUEST, f"subscription {self._type} ended before its initial result")
        if kind != "part":
            raise HubError(ErrorCode.INVALID_REQUEST, _UNEXPECTED.format(type=self._type))
        part = message.part
        opened = cast("_StreamEntry", answered.entry)
        if part.seq != 1:  # the hub numbers a subscription's parts itself, from 1: another is a part it dropped
            reason = (
                f"subscription {self._type} lost {part.seq - 1} part(s)"
                if part.seq > 1
                else f"subscription {self._type} part seq {part.seq} out of order (expected 1)"
            )
            release_entry(self._sender, opened, cancel=True, reason=reason)
            raise LostParts(reason)
        if part.HasField("error"):
            release_entry(self._sender, opened, cancel=True, reason="initial result unusable")
            raise error_from_wire(part.error, self._type)
        try:
            initial: I = self._sender.codec.decode(part.payload or b"{}", self._initial_type)
        except BaseException as e:
            release_entry(self._sender, opened, cancel=True, reason=f"initial result unusable: {type(e).__name__}")
            raise
        opened.frames.get_nowait()  # the initial part was read here: the updates start after it
        self.initial = initial
        self._entry = opened

    async def _read(self) -> AsyncGenerator[U, None]:
        """The updates, in the seq the hub gave them, refilling the credit as the caller consumes them: a part out of
        seq, or a clean end that counts more parts than were delivered, is reported when the reader reaches that frame,
        so the updates already delivered stay. A subscription may stay quiet: there is no local deadline."""
        entry = self._entry
        assert entry is not None  # a subscription is open only once its entry is here
        codec = self._sender.codec
        delivered = 1  # the initial result
        while True:
            frame = await entry.frames.get()
            if isinstance(frame, Lost):
                self._ended = True
                raise OutcomeUnknown(frame.why)
            kind = frame.WhichOneof("message")
            if kind == "part":
                seq = frame.part.seq
                if seq != delivered + 1:
                    reason = (
                        f"subscription {self._type} lost {seq - delivered - 1} part(s)"
                        if seq > delivered + 1
                        else f"subscription {self._type} part seq {seq} out of order (expected {delivered + 1})"
                    )
                    self._ended = True
                    release_entry(self._sender, entry, cancel=True, reason=reason)
                    raise LostParts(reason)
                delivered += 1
                if frame.part.HasField("error"):  # a hub that relays a target's failure as a part, not as the end
                    self._ended = True
                    raise error_from_wire(frame.part.error, self._type)
                update: U = codec.decode(frame.part.payload or b"{}", self._update)
                self._consumed_one(entry)
                yield update
                continue
            self._ended = True
            if kind == "error":
                raise error_from_wire(frame.error, self._type)
            if frame.end.HasField("error"):
                raise error_from_wire(frame.end.error, self._type)
            if frame.end.parts > delivered:  # a clean end short of the parts it counts
                raise LostParts(f"subscription {self._type} lost {frame.end.parts - delivered} part(s)")
            return

    def _consumed_one(self, entry: _StreamEntry) -> None:
        """Counts one part the caller consumed (the initial result included) and refills every half window."""
        self._consumed += 1
        if self._consumed >= self._refill:
            self._consumed = 0
            grant_flow(entry, self._refill)

    def _release(self, *, cancel: bool, reason: str = CANCEL_REASON) -> None:
        """Drops what this library holds of the subscription, and with cancel writes the ``Cancel`` (when it has not
        ended): its entry waits for no frame, and the credits stop."""
        entry, self._entry = self._entry, None
        if entry is not None:
            release_entry(self._sender, entry, cancel=cancel, reason=reason)

    async def _close_items(self) -> None:
        """Closes the generator the caller was reading, so that nothing of it stays suspended."""
        items, self._items = self._items, None
        if items is not None:
            await cast("AsyncGenerator[U, None]", items).aclose()


def subscribe[I, U](
    sender: Sender,
    query: object,
    initial: type[I],
    update: type[U],
    *,
    initial_credit: int | None = None,
    max_results: int = 0,
    timeout: float | None = None,
    priority: int | None = None,
    headers: Mapping[str, str] | None = None,
    retry: RetryPolicy | Literal[False] | None = None,
) -> Subscription[I, U]:
    """A subscription to query's updates: its request built here, once its options are checked (nothing sent yet), and
    sent by ``async with``. ``initial_credit`` is the sender's window (None or 0: the protocol's 64), the initial
    result counted among the parts it consumes; ``max_results`` ends the subscription after that many updates at the hub
    (0: until the handler completes); ``timeout`` bounds the initial result only."""
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
        mode=hub_pb2.SUBSCRIPTION,
    )
    prepared.template.initial_credit = credit
    prepared.template.max_results = max_results
    return Subscription(sender, prepared, initial, update, credit=credit, type_=type_name(query))
