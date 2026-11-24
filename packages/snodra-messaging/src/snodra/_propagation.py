"""The headers of a request the library sends: the caller's own, then what it carries on from the request being
handled (the correlation id, the caller's token, the trace context).

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Request headers", "Correlation id",
"Trace context", "Forwarded identity").
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from snodra import _traceparent
from snodra._context import current_message
from snodra._errors import ReservedHeader
from snodra._identity import AUTHORIZATION, bearer_token
from snodra._ids import new_id

CORRELATION_ID: Final = "correlationId"
TRACEPARENT: Final = "traceparent"

RESERVED: Final = frozenset({"authorization", "traceparent", "tracestate", "correlationid", "correlation-id"})
"""The header names the library sets itself, in lower case: a call may not set them, in any letter case."""


def check_own(own: Mapping[str, str] | None) -> dict[str, str]:
    """A call's own headers, as a new dict: :class:`ReservedHeader` for a name the library sets itself (compared
    ignoring letter case), ``TypeError`` for a name or a value that is not a text."""
    if own is None:
        return {}
    if not isinstance(own, Mapping):
        raise TypeError(f"headers must be a mapping of texts, got {own!r}")
    headers: dict[str, str] = {}
    for name, value in own.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError(f"a header's name and value are texts, got {name!r}: {value!r}")
        if name.lower() in RESERVED:
            raise ReservedHeader(
                f"header {name!r} is set by the library and cannot be given with headers= "
                "(authorization, traceparent, tracestate, correlationId and correlation-id, in any letter case)"
            )
        headers[name] = value
    return headers


def outgoing_headers(own: Mapping[str, str] | None, *, forward_jwt: bool) -> dict[str, str]:
    """The headers of a request sent now: the caller's own (checked by :func:`check_own`), then the ``correlationId``
    of the request being handled (its id when it carries none; a new UUID outside any handling), its
    ``authorization`` (``Bearer <token>``) when ``forward_jwt``, and its ``traceparent`` when it is a valid one."""
    headers = check_own(own)
    info = current_message()
    if info is None:
        headers[CORRELATION_ID] = new_id()
        return headers
    headers[CORRELATION_ID] = info.correlation_id or info.id
    if forward_jwt and (authorization := info.headers.get(AUTHORIZATION)) is not None:
        token = bearer_token(authorization)
        if token is not None:
            headers[AUTHORIZATION] = f"Bearer {token}"
    if (traceparent := info.headers.get(TRACEPARENT)) is not None and _traceparent.valid(traceparent):
        headers[TRACEPARENT] = traceparent
    return headers
