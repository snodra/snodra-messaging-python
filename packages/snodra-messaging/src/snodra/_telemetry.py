"""The telemetry interface and the default no-op telemetry."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from typing import Protocol

from snodra._errors import (
    DecodeError,
    ErrorCode,
    HubError,
    InvalidRequest,
    NeverSent,
    OutcomeUnknown,
    ReservedHeader,
)


@dataclass(frozen=True)
class Gauges:
    """The gauges of an app at one moment."""

    handlers_in_flight: int
    """Incoming requests from the hubs currently being handled or waiting for a handler permit."""

    connected_hubs: int
    """Hubs with an acknowledged stream."""


@dataclass(frozen=True)
class Handling:
    """Describes a request a handler is about to handle."""

    type: str
    """The message type, for example ``orders.PlaceOrder``."""

    kind: str
    """``command`` or ``query``."""

    traceparent: str | None
    """The request's incoming traceparent header when valid, else None."""

    tracestate: str | None
    """The request's incoming tracestate header, only when traceparent is valid."""


class Telemetry(Protocol):
    """Where the library reports request timings, token fetches, certificate rotations, credentials refreshes,
    gauges and request handling."""

    def request_done(self, kind: str, type_: str, outcome: str, seconds: float) -> None:
        """One call: from the call to its result with every retry included.

        Not reported for in-process calls, calls whose options refuse, or calls cancelled by the caller.
        """

    def token_fetched(self, result: str) -> None:
        """One access token fetch by client credentials: ``ok``, ``http_error``, ``oauth_error``, ``io_error``, or
        ``bad_response``."""

    def certificate_rotated(self) -> None:
        """A client certificate pair presented to a hub after another one before it."""

    def credentials_refresh_sent(self) -> None:
        """A credentials refresh frame written to a hub stream."""

    def observe_gauges(self, read: Callable[[], Gauges]) -> None:
        """Called once by :meth:`snodra.App.start`, under no library lock. read returns the current gauges."""

    def handling(self, h: Handling) -> AbstractContextManager[None]:
        """Context manager around the execution of a handler."""

    def inject(self, headers: dict[str, str]) -> None:
        """Injects trace context into headers about to be sent (may set ``traceparent`` and ``tracestate``)."""


class NoTelemetry:
    """The default :class:`Telemetry`: reports nothing, allocates nothing and reads no clock.

    Subclass it to implement only some methods.
    """

    def request_done(self, kind: str, type_: str, outcome: str, seconds: float) -> None:
        pass

    def token_fetched(self, result: str) -> None:
        pass

    def certificate_rotated(self) -> None:
        pass

    def credentials_refresh_sent(self) -> None:
        pass

    def observe_gauges(self, read: Callable[[], Gauges]) -> None:
        pass

    def handling(self, h: Handling) -> AbstractContextManager[None]:
        return nullcontext()

    def inject(self, headers: dict[str, str]) -> None:
        pass


def request_outcome(exc: BaseException | None) -> str | None:
    """The outcome label of a request that ended with exc (None on success -> ``ok``).

    Returns None when the caller cancelled the request: a caller cancellation records nothing.
    """
    if exc is None:
        return "ok"
    if isinstance(exc, asyncio.CancelledError):
        return None
    if isinstance(exc, HubError):
        match exc.code:
            case ErrorCode.NO_HANDLER:
                return "no_handler"
            case ErrorCode.TIMEOUT:
                return "timeout"
            case ErrorCode.HANDLER_DISCONNECTED:
                return "handler_disconnected"
            case ErrorCode.RESOURCE_EXHAUSTED:
                return "resource_exhausted"
            case ErrorCode.HANDLER_ERROR:
                return "handler_error"
            case ErrorCode.UNAUTHENTICATED | ErrorCode.PERMISSION_DENIED:
                return "denied"
            case ErrorCode.INVALID_REQUEST:
                return "invalid_request"
            case ErrorCode.CANCELLED:
                return "cancelled"
            case _:
                return "handler_error"
    if isinstance(exc, (DecodeError, InvalidRequest, ReservedHeader)):
        return "invalid_request"
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, (NeverSent, OutcomeUnknown)):
        return "unavailable"
    return "error"
