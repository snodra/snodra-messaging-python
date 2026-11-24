"""Requests the instance answers itself, in-process, without a hub.

By default a command of a type this instance has a command handler for, and a point query of a type it has a query
handler for, run here: through the same checks as a forwarded request (the guards, the decoding, the error mapping),
with the headers a request through the hub would carry, and the caller the hub path would carry. Nothing goes on the
wire: no permit, no routing key, no timeout of the library's own, no retry. ``Settings.always_route_through_hub``
sends them through a hub instead.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Requests a service answers itself").
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from snodra._codec import Codec
from snodra._context import current_caller
from snodra._dispatch import Dispatcher
from snodra._errors import NeverSent, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._ids import new_id
from snodra._registry import Handler, Registry
from snodra._settings import Settings

if TYPE_CHECKING:
    from snodra._pool import Pool


class LocalCalls:
    """Answers in-process the calls of the types this instance handles."""

    def __init__(self, registry: Registry, dispatcher: Dispatcher, codec: Codec, settings: Settings) -> None:
        self._registry = registry
        self._dispatcher = dispatcher
        self._codec = codec
        self._settings = settings
        self.pool: Pool | None = None
        """The hubs, set by the app: once its shutdown drained the handlers, nothing is answered in-process either."""

    def handler(self, kind: Literal["command", "query"], type_: str) -> Handler | None:
        """The handler of this instance that answers a call of kind and type_ in-process; None when the call goes
        through a hub (always with ``always_route_through_hub``)."""
        if self._settings.always_route_through_hub:
            return None
        return self._registry.find(kind, type_)

    async def call(
        self,
        handler: Handler,
        kind: Literal["command", "query"],
        payload: bytes,
        headers: dict[str, str],
        result: type | None,
    ) -> object:
        """Runs handler on payload in the calling task, as a request with a fresh id and headers: its answer decoded as
        result (None: not read), or the error a caller of it through the hub would get. Its caller is the one the
        request being handled has when ``forward_jwt`` is on (its token is in headers), else anonymous. A
        cancellation of the calling task cancels the handler."""
        type_ = handler.type
        if self.pool is not None and self.pool.leaving:
            raise NeverSent(f"'{type_}' not sent: the app is shutting down")
        request = hub_pb2.Request(
            id=new_id(),
            kind=hub_pb2.COMMAND if kind == "command" else hub_pb2.QUERY,
            type=type_,
            payload=payload,
            headers=headers,
            mode=hub_pb2.UNARY,
        )
        caller = current_caller() if self._settings.forward_jwt else None
        reply = await self._dispatcher.answer_checked(handler, request, caller)
        if reply.WhichOneof("result") == "error":
            raise error_from_wire(reply.error, type_)
        if result is None:
            return None
        return self._codec.decode(reply.payload or b"{}", result)
