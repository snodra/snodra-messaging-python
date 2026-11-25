"""A streaming request this instance serves: its handler's items as parts numbered from 1, under the sender's credit,
and the one ``ReplyEnd`` that ends it.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Streaming").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from snodra._codec import Codec
from snodra._errors import ErrorCode, error_to_wire
from snodra._hubpb import hub_pb2

if TYPE_CHECKING:
    from snodra._dispatch import Arrival

log = logging.getLogger(__name__)


class StreamCredit:
    """The credit a hub granted for one inbound stream: the request's ``initial_credit``, then every ``Flow`` for it.

    :meth:`take` waits for one at most the handler's bound: a hub that forgot the stream (a lost ``Flow`` or ``Cancel``)
    would otherwise hold the handler and its permit for ever. The request's credit and a ``Flow``'s are ``uint32``;
    ``0`` on the request reads as the protocol's 64, which the caller of this class decides."""

    def __init__(self, initial: int) -> None:
        self._available = initial
        self._wake = asyncio.Event()
        if initial > 0:
            self._wake.set()

    @property
    def available(self) -> int:
        """The parts the hub relays without another ``Flow``."""
        return self._available

    def add(self, credits: int) -> None:
        """A ``Flow`` for the stream; a count of 0 adds nothing, as the hub ignores one."""
        if credits > 0:
            self._available += credits
            self._wake.set()

    async def take(self, timeout: float) -> None:  # noqa: ASYNC109 (the credit bound)
        """Spends one credit, waiting for one at most ``timeout`` seconds: ``TimeoutError`` when none came."""
        if self._available > 0:
            self._available -= 1
            return
        self._wake.clear()
        async with asyncio.timeout(timeout):
            await self._wake.wait()
        self._available -= 1


def end_of_stream(request_id: str, parts: int, error: hub_pb2.Error | None = None) -> hub_pb2.ClientMessage:
    """The one ``ReplyEnd`` of a stream of ``parts`` parts, with ``error`` when it failed."""
    end = hub_pb2.ReplyEnd(request_id=request_id, parts=parts)
    if error is not None:
        end.error.CopyFrom(error)
    return hub_pb2.ClientMessage(end=end)


def failure(request_id: str, code: ErrorCode, message: str) -> hub_pb2.Error:
    """The ``Error`` that ends a stream refused or failed before its handler produced anything."""
    return hub_pb2.Error(request_id=request_id, code=int(code), message=message)  # type: ignore[arg-type]


async def serve[R](
    items: AsyncIterator[R],
    request: hub_pb2.Request,
    *,
    arrival: Arrival,
    codec: Codec,
    credit: StreamCredit,
    credit_timeout: float,
) -> None:
    """Sends the items of a streaming handler to the hub: each as a part with the next ``seq``, once the sender's credit
    for it is there (at most ``credit_timeout``), then exactly one ``ReplyEnd``.

    The handler's generator runs in this task: an item it yields is held until the credit for its part is there, so the
    hub's window is never exceeded and the generator is asked for one item beyond the window, not more. A handler that
    fails ends the stream with the error the library maps its exception to (``HANDLER_ERROR`` for an application's
    exception); one that waits for credit past the bound ends it with ``TIMEOUT`` ``no credit within <ms> ms``. A
    cancellation of this task (the hub's ``Cancel``, the shutdown) is raised, and nothing at all is answered: the
    generator is closed, so its ``finally`` runs and its flow stops where it was."""
    id_ = request.id
    seq = 0
    error: hub_pb2.Error | None = None
    try:
        while True:
            try:
                item = await items.__anext__()
            except StopAsyncIteration:
                break
            try:
                await credit.take(credit_timeout)
            except TimeoutError:
                error = failure(id_, ErrorCode.TIMEOUT, f"no credit within {round(credit_timeout * 1000)} ms")
                log.warning("Streaming request %s (%s) ended: %s", id_, request.type, error.message)
                break
            seq += 1
            part = hub_pb2.ReplyPart(request_id=id_, payload=codec.encode(item), seq=seq)
            if not arrival.put(hub_pb2.ClientMessage(part=part)):
                # The stream it arrived on is gone, and the hub with its credits: the handler's flow stops here.
                log.debug("Part %d of streaming request %s dropped: its stream is gone", seq, id_)
                await _close(items)
                return
    except asyncio.CancelledError as e:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            await _close(items)  # a Cancel: nothing at all is answered for the request
            raise
        # A CancelledError the handler got from something it awaited, not a cancellation of its own task.
        error = error_to_wire(id_, e, codec)
    except Exception as e:
        log.debug("Streaming handler of request %s (%s) failed: %r", id_, request.type, e)
        error = error_to_wire(id_, e, codec)
    if error is not None:
        await _close(items)
    arrival.put(end_of_stream(id_, seq, error))  # dropped by a stream that is gone, which is all it can do


async def _close[R](items: AsyncIterator[R]) -> None:
    """Closes a handler's generator, so that its ``finally`` runs once the stream is over (a ``Cancel``, a failure or a
    credit timeout). What it does while closing is refused by the generator itself; a failure of it is logged."""
    try:
        await items.aclose()  # type: ignore[attr-defined]
    except Exception:
        log.warning("Closing a streaming handler's generator failed", exc_info=True)
