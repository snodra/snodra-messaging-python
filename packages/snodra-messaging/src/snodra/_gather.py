"""Scatter-gather: a query asked of every live instance of every owner of its type, through one hub, its answers
collected as parts until the hub's end.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Scatter-gather").
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, cast

from snodra._codec import Codec
from snodra._connection import OutboundRequest
from snodra._errors import DecodeError, ErrorCode, HubError, SnodraError, error_from_wire
from snodra._hubpb import hub_pb2
from snodra._messages import type_name
from snodra._retry import RetryPolicy
from snodra._send import Sender

_UINT32_MAX: Final = 2**32 - 1


@dataclass(frozen=True)
class Part[R]:
    """One target's answer to a gather: the service and instance that answered, and its answer decoded (``value``) or
    its failure (``error``).

    ``error`` is a :class:`HubError` with the target's code and message (its handler's error, or a failure the hub saw
    for that target, such as ``HANDLER_DISCONNECTED`` or ``RESOURCE_EXHAUSTED``), or a :class:`DecodeError` when the
    target answered in a shape the result type does not read."""

    service: str
    """``ReplyPart.responder_service``."""
    instance: str
    """``ReplyPart.responder_instance``."""
    value: R | None
    """The answer; None when ``error`` is set."""
    error: SnodraError | None
    """The target's failure; None when it answered."""


@dataclass(frozen=True)
class Gathered[R]:
    """Everything a gather got by the hub's end."""

    parts: list[Part[R]]
    """One per answer, in arrival order, failed ones included."""
    targets: int
    """The parts the hub sent plus ``timed_out``: the targets of a gather that ended at its deadline or with every
    target answered. When ``max_results`` ended it, the targets the hub cancelled are counted nowhere: five targets
    and ``max_results=2`` give 2."""
    failed: int
    """The parts whose ``error`` is set (an answer that does not decode included)."""
    timed_out: int
    """The targets that had not answered by the deadline (``ReplyEnd.timed_out``): no part is there for them."""


class _GatherEntry(OutboundRequest):
    """A gather waiting for its end: it collects the ``ReplyPart`` frames of its id until the ``ReplyEnd``, or the
    hub's ``Error`` for the gather itself; a frame after that is dropped."""

    def __init__(self, message: hub_pb2.ClientMessage) -> None:
        super().__init__(message)
        self.parts: list[hub_pb2.ReplyPart] = []

    def take(self, m: hub_pb2.ServerMessage) -> bool:
        kind = m.WhichOneof("message")
        if kind == "part":
            if not self.answer.done():  # a part after the end, before its caller resumed, is no part of it
                self.parts.append(m.part)
            return True
        if kind in ("end", "error"):
            self.resolve(m)
            return True
        return False  # a Reply is no answer to a gather


async def gather[R](
    sender: Sender,
    query: object,
    result: type[R],
    *,
    max_results: int,
    timeout: float | None,  # noqa: ASYNC109 (the call's timeout)
    priority: int | None,
    headers: Mapping[str, str] | None,
    retry: RetryPolicy | Literal[False] | None,
) -> Gathered[R]:
    """A scatter-gather of query through one hub (never answered in-process): the parts decoded as ``result``, until
    the hub's ``ReplyEnd``. Each attempt of a retried gather is a new gather, with a fresh id and its own parts."""
    if isinstance(max_results, bool) or not isinstance(max_results, int):
        raise TypeError(f"max_results must be an integer, got {max_results!r}")
    if not 0 <= max_results <= _UINT32_MAX:
        raise ValueError(f"max_results must be from 0 (no limit) to {_UINT32_MAX}, got {max_results!r}")
    p = sender.prepare(
        query,
        "query",
        routing_key=None,
        timeout=timeout,
        priority=priority,
        headers=headers,
        retry=retry,
        mode=hub_pb2.SCATTER_GATHER,
    )
    p.template.max_results = max_results
    type_ = type_name(query)

    async def attempt(template: hub_pb2.Request, chosen: float | None) -> Gathered[R]:
        # A gather is no unary request: a hub's NO_HANDLER means no live target there, and is the answer.
        # TODO(spec-gap): SG-10
        answered = await sender.route(template, chosen, entry=_GatherEntry, walk=False)
        m = answered.message
        if m.WhichOneof("message") == "error":
            raise error_from_wire(m.error, type_)
        end = m.end
        if end.HasField("error"):  # the hub never ends a gather with an error; one that does fails it whole
            raise error_from_wire(end.error, type_)
        entry = cast("_GatherEntry", answered.entry)
        return _assemble(sender.codec, type_, entry.parts, end, result)

    return await sender.with_retry(p, attempt)


def _assemble[R](
    codec: Codec, type_: str, parts: list[hub_pb2.ReplyPart], end: hub_pb2.ReplyEnd, result: type[R]
) -> Gathered[R]:
    """The gather's result from its parts, in arrival order, and its end."""
    out: list[Part[R]] = []
    for p in parts:
        value: R | None = None
        error: SnodraError | None = None
        if p.HasField("error"):
            error = _part_error(p.error)
        else:
            try:
                value = codec.decode(p.payload or b"{}", result)
            except DecodeError as e:
                # One owner answering in another shape fails its own part, not the gather.
                text = f"the answer of {p.responder_service}/{p.responder_instance} to '{type_}' does not decode: {e}"
                error = DecodeError(text)
                error.__cause__ = e
        out.append(Part(p.responder_service, p.responder_instance, value, error))
    failed = sum(1 for part in out if part.error is not None)
    return Gathered(out, end.parts + end.timed_out, failed, end.timed_out)


def _part_error(e: hub_pb2.Error) -> HubError:
    """A target's failure as the caller reads it: its code (an unknown one reads as ``HANDLER_ERROR``) and the message
    the target or the hub gave, unchanged; the details only with ``HANDLER_ERROR``."""
    try:
        code = ErrorCode(int(e.code))
    except ValueError:
        code = ErrorCode.HANDLER_ERROR
    return HubError(code, e.message, bytes(e.details) if code == ErrorCode.HANDLER_ERROR else b"")
