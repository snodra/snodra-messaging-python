"""The subscription queries this instance serves: their registration when a request is read, the updates an emitter
pushes to them, and their ends.

A subscription target (``Request.id`` ``<id>#n``) is registered synchronously, in the hub stream's reader, before its
admission runs (:class:`~snodra._dispatch.Dispatcher`): the hub writes it before the subscription's initial request
``<id>#0`` to the same instance, so an update emitted after the initial result was read is never missed. Updates emitted
until the admission is decided are held in order and written once it succeeds; a refusal, a hub ``Cancel`` or a lost
stream drops them. A served subscription takes no handler permit: it is a standing interest, not work in progress.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Subscription", "A lost stream").
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Final

from snodra._errors import ErrorCode, error_to_wire
from snodra._hubpb import hub_pb2

if TYPE_CHECKING:
    from snodra._codec import Codec
    from snodra._dispatch import Arrival

log = logging.getLogger(__name__)

_UINT32_MAX: Final = 2**32 - 1


def _error(request_id: str, code: ErrorCode, message: str) -> hub_pb2.Error:
    return hub_pb2.Error(request_id=request_id, code=int(code), message=message)  # type: ignore[arg-type]


class _End:
    """An end held back while the subscription's admission runs."""

    __slots__ = ("error",)

    def __init__(self, error: hub_pb2.Error | None) -> None:
        self.error = error


class ServedSubscription:
    """One subscription target this instance serves: its arrival stream, its query once admitted (else the request's
    payload, decoded by the emitter to match), the ``seq`` of its next part and what is held until the admission.

    Every method runs on the app's event loop: the reader registers and drops, the admission task activates or refuses,
    an emitter writes, and none of them awaits between reading and writing this state."""

    def __init__(
        self,
        arrival: Arrival,
        request_id: str,
        type_: str,
        query_type: type,
        payload: bytes,
    ) -> None:
        self.arrival = arrival
        self.request_id = request_id
        self.type = type_
        self.query_type = query_type
        self.query: object | None = None
        """The decoded query once admitted; None while pending."""
        self.payload: bytes | None = payload
        """The request's payload while pending (the emitter matches it by decoding this); None once admitted."""
        self.task: asyncio.Task[None] | None = None
        """The admission's task, cancelled by a hub ``Cancel`` or a lost stream."""
        self._seq = 0
        self._ended = False
        """Nothing more is accepted (it ended, was refused or was dropped)."""
        self._dropped = False
        """Nothing more is written, whatever is held (a ``Cancel``, a lost stream, a shutdown)."""
        self._held: list[bytes | _End] | None = [] if payload is not None else None
        """The updates and the end held while pending; None once the admission decided."""

    @property
    def pending(self) -> bool:
        """True while its admission runs."""
        return self._held is not None

    def update(self, payload: bytes) -> bool:
        """Writes one update (holds it while pending); False when the subscription ended or its stream is gone."""
        if self._ended:
            return False
        if self._held is not None:
            self._held.append(payload)
            return True
        return self._write(payload)

    def end(self, error: hub_pb2.Error | None) -> bool:
        """Writes the one end (holds it while pending); False when it had ended already."""
        if self._ended:
            return False
        self._ended = True
        if self._held is not None:
            self._held.append(_End(error))
            return True
        self._write_end(error)
        return True

    def close(self) -> None:
        """Ends it without writing anything (the hub's ``Cancel``, a lost stream, the shutdown)."""
        self._ended = True
        self._dropped = True
        self._held = None

    def activate(self, query: object) -> bool:
        """The admission succeeded: records the query, writes what was held in order and lets later calls write
        directly. False when it was dropped meanwhile."""
        if self._dropped:
            return False
        self.query = query
        self.payload = None
        held, self._held = self._held, None
        if held is None:
            return True
        for item in held:
            if isinstance(item, _End):
                self._write_end(item.error)
            elif not self._write(item):
                return True  # its stream is gone: nothing more can be written
        return True

    def refuse(self, error: hub_pb2.Error) -> bool:
        """The admission failed: what was held is dropped and the refusal is the only thing written (with no part).
        False when it was dropped meanwhile."""
        if self._dropped:
            return False
        self._ended = True
        self._dropped = True
        self._held = None
        self._write_end(error)
        return True

    def _write(self, payload: bytes) -> bool:
        """Writes one part with the next ``seq``; False when its stream is gone (the subscription is then over)."""
        self._seq += 1
        part = hub_pb2.ReplyPart(request_id=self.request_id, payload=payload, seq=self._seq)
        if self.arrival.put(hub_pb2.ClientMessage(part=part)):
            return True
        self._ended = True
        return False

    def _write_end(self, error: hub_pb2.Error | None) -> None:
        end = hub_pb2.ReplyEnd(request_id=self.request_id, parts=min(self._seq, _UINT32_MAX))
        if error is not None:
            end.error.CopyFrom(error)
        self.arrival.put(hub_pb2.ClientMessage(end=end))


class Subscriptions:
    """The subscription targets this instance serves, over every hub, up to ``max_subscriptions`` at once.

    A subscription counts against the limit from the moment its request is read. The limit and an id already open are
    decided there, before the token check, as the specification requires."""

    def __init__(self, max_subscriptions: int) -> None:
        self._max = max_subscriptions
        self._open: dict[tuple[object, str], ServedSubscription] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        """Records the app's loop: the emitters refuse any other thread (``loop.call_soon_threadsafe``)."""
        self._loop = loop

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        return self._loop

    def __len__(self) -> int:
        return len(self._open)

    def all(self) -> list[ServedSubscription]:
        """Every open subscription, in no particular order."""
        return list(self._open.values())

    def of(self, type_: str) -> list[ServedSubscription]:
        """The open subscriptions of type_, in no particular order."""
        return [s for s in self._open.values() if s.type == type_]

    def open(self, arrival: Arrival, request: hub_pb2.Request, query_type: type) -> ServedSubscription | hub_pb2.Error:
        """Registers the subscription target request (the hub's ``<id>#n``) that arrived on arrival; the ``Error`` that
        ends it when its id is already open (``INVALID_REQUEST``) or when the instance serves its limit already
        (``RESOURCE_EXHAUSTED``), decided here, when the request is read."""
        key = (arrival, request.id)
        if key in self._open:
            return _error(request.id, ErrorCode.INVALID_REQUEST, f"subscription {request.id} is already open")
        if len(self._open) >= self._max:
            return _error(request.id, ErrorCode.RESOURCE_EXHAUSTED, f"subscription limit ({self._max}) reached")
        served = ServedSubscription(arrival, request.id, request.type, query_type, request.payload)
        self._open[key] = served
        return served

    def discard(self, served: ServedSubscription) -> None:
        """Forgets it here (it ended, was refused or its stream is gone)."""
        key = (served.arrival, served.request_id)
        if self._open.get(key) is served:
            del self._open[key]

    def activate(self, served: ServedSubscription, query: object) -> bool:
        """The admission succeeded; False when a ``Cancel`` or a lost stream dropped it meanwhile."""
        return served.activate(query)

    def refuse(self, served: ServedSubscription, error: hub_pb2.Error) -> bool:
        """The admission failed: it is forgotten here and the refusal ends it."""
        self.discard(served)
        return served.refuse(error)

    def cancel(self, arrival: Arrival, request_id: str) -> bool:
        """The hub cancelled request_id: it is removed, nothing is written and its admission stops."""
        served = self._open.pop((arrival, request_id), None)
        if served is None:
            return False
        self._drop(served)
        return True

    def lost(self, arrival: Arrival) -> int:
        """arrival's stream is gone (the hub forgot its subscriptions): they are all removed and nothing is written.
        Returns how many."""
        dropped = [s for s in self._open.values() if s.arrival is arrival]
        for served in dropped:
            self.discard(served)
            self._drop(served)
        return len(dropped)

    def end_all(self) -> int:
        """Ends every served subscription with a clean ``ReplyEnd`` (the shutdown): to the hub this target's emitter
        completed, and the subscriber hears it once its other targets have ended too. Returns how many."""
        ended = list(self._open.values())
        for served in ended:
            self.discard(served)
            served.end(None)
        return len(ended)

    def close_all(self) -> None:
        """Drops every subscription without writing anything (the handlers are abandoned)."""
        for served in list(self._open.values()):
            self.discard(served)
            self._drop(served)

    @staticmethod
    def _drop(served: ServedSubscription) -> None:
        served.close()
        if served.task is not None and not served.task.done():
            served.task.cancel()


class Updates[Q, U]:
    """The updates of one subscription query handler (:meth:`snodra.App.subscription_handler`): ``emit`` pushes one to
    every served subscription of its type whose query matches, and ``complete``/``fail`` end them.

    Every method runs on the app's event loop only: an emitter that updates a subscription from a thread of its own is a
    ``RuntimeError`` naming ``loop.call_soon_threadsafe``. What each returns is how many subscriptions it reached. A
    filter that raises skips that subscription (logged), never the others."""

    def __init__(
        self,
        subscriptions: Subscriptions,
        codec: Codec,
        type_: str,
        query_type: type,
        update_type: type,
    ) -> None:
        self._subscriptions = subscriptions
        self._codec = codec
        self._type = type_
        self._query_type = query_type
        self._update_type = update_type

    def emit(self, match: Callable[[Q], bool], update: U) -> int:
        """Sends update to every served subscription of this type whose query match accepts (a pending subscription
        included: the emitter decodes its request's payload to match it). Returns how many it reached."""
        self._check_loop("emit")
        reached = self._matching(match)
        if not reached:
            return 0
        payload = self._codec.encode(update)
        sent = 0
        for served in reached:
            if served.update(payload):
                sent += 1
            else:
                self._subscriptions.discard(served)
        return sent

    def complete(self, match: Callable[[Q], bool]) -> int:
        """Ends every served subscription of this type whose query match accepts with a clean ``ReplyEnd``; the
        subscriber hears the end once every other target has ended too. Returns how many it ended."""
        self._check_loop("complete")
        ended = self._matching(match)
        for served in ended:
            self._subscriptions.discard(served)
            served.end(None)
        return len(ended)

    def fail(self, match: Callable[[Q], bool], error: BaseException | str) -> int:
        """Ends every served subscription of this type whose query match accepts with ``ReplyEnd{error}``: a handler
        failure (``HANDLER_ERROR``, ``<ClassName>: <message>``), or a text as the failure's message. Returns how many it
        ended."""
        self._check_loop("fail")
        ended = self._matching(match)
        for served in ended:
            self._subscriptions.discard(served)
            if isinstance(error, str):
                served.end(_error(served.request_id, ErrorCode.HANDLER_ERROR, error))
            else:
                served.end(error_to_wire(served.request_id, error, self._codec))
        return len(ended)

    def _matching(self, match: Callable[[Q], bool]) -> list[ServedSubscription]:
        found: list[ServedSubscription] = []
        for served in self._subscriptions.of(self._type):
            query: Any = served.query
            if query is None:
                # The admission still runs: its request's payload is the query (when it does not decode, the admission
                # refuses it, so no update of it is written).
                if served.payload is None:  # pragma: no cover (a query of every message is never None)
                    continue
                try:
                    query = self._codec.decode(served.payload, self._query_type)
                except Exception:
                    continue
            try:
                if match(query):
                    found.append(served)
            except Exception:
                log.warning(
                    "The filter of an update for %s raised; subscription %s skipped",
                    served.type,
                    served.request_id,
                    exc_info=True,
                )
        return found

    def _check_loop(self, method: str) -> None:
        """Refuses a call from another thread: it must be handed to the app's loop with ``call_soon_threadsafe``."""
        loop = self._subscriptions.loop
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if loop is None or current is not loop:
            raise RuntimeError(
                f"Updates.{method} runs on the app's event loop only: from another thread, use "
                f"loop.call_soon_threadsafe(updates.{method}, ...)"
            )
