"""Error codes, the exceptions the library raises, and the mapping to and from the wire.

See https://snodra.damoreira.com/docs/library-authors/messages/ (Handler errors).
"""

import enum
import json
import re
from typing import TYPE_CHECKING, Any, Final, cast

from snodra._hubpb import hub_pb2

if TYPE_CHECKING:
    from snodra._codec import Codec


class ErrorCode(enum.IntEnum):
    """The hub's error codes (``ErrorCode`` of hub.proto, without the unspecified value)."""

    NO_HANDLER = 1
    TIMEOUT = 2
    HANDLER_DISCONNECTED = 3
    HANDLER_ERROR = 4
    UNAUTHENTICATED = 5
    INVALID_REQUEST = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    CANCELLED = 9


class SnodraError(Exception):
    """The base of every exception the library raises."""


RETRYABLE: Final[bytes] = b'{"snodra":"retryable"}'
"""The reserved ``Error.details`` value of a failure a later attempt may cure (``L4-ADAPTER-MARKERS``)."""

UNKNOWN_OUTCOME: Final[bytes] = b'{"snodra":"unknown-outcome"}'
"""The reserved ``Error.details`` value of an external call whose outcome cannot be known."""


class Retryable(SnodraError):
    """Raised by a handler (an idempotent adapter) to answer ``HANDLER_ERROR`` with the retryable marker.

    The saga relay treats it as transient and owns the backoff: do not retry inside the handler. It is not
    :class:`OutcomeUnknown`, which is a sender's transport failure.
    """


class UnknownOutcome(SnodraError):
    """Raised by an idempotent adapter that cannot tell whether its external call happened.

    The answer is ``HANDLER_ERROR`` with the unknown-outcome marker; the relay records ``UNKNOWN_OUTCOME`` and does not
    run the body again automatically.
    """


def is_retryable(error: BaseException | None) -> bool:
    """Whether ``error`` carries the retryable marker, locally or from a hub, with surrounding whitespace trimmed.

    The marker is the exception :class:`Retryable`, a :class:`HandlerError` or :class:`HubError` whose details are the
    reserved bytes, or one of those reached through the explicit cause chain (``raise ... from ...``).
    """
    return _has_marker(error, Retryable, RETRYABLE)


def is_unknown_outcome(error: BaseException | None) -> bool:
    """Whether ``error`` carries the unknown-outcome marker, read as :func:`is_retryable` reads the other one."""
    return _has_marker(error, UnknownOutcome, UNKNOWN_OUTCOME)


def _has_marker(error: BaseException | None, kind: type[BaseException], marker: bytes) -> bool:
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, kind):
            return True
        if isinstance(error, HandlerError | HubError) and _details_bytes(error.details) == marker:
            return True
        if isinstance(error, BaseExceptionGroup) and any(_has_marker(part, kind, marker) for part in error.exceptions):
            return True
        error = error.__cause__
    return False


def _details_bytes(details: object) -> bytes:
    if details is None:
        return b""
    if isinstance(details, bytes):
        return details.strip()
    if isinstance(details, str):
        return details.encode().strip()
    try:
        return json.dumps(details, separators=(",", ":")).encode().strip()
    except Exception:
        return b""


class HubError(SnodraError):
    """An error code that reached the caller, from the hub, a handler or the library itself.

    ``details`` is the handler's machine-readable JSON, only ever set with ``HANDLER_ERROR``.
    """

    def __init__(self, code: ErrorCode, message: str, details: bytes = b"") -> None:
        super().__init__(code, message, details)
        self.code = code
        self.message = message
        self.details = details

    def __str__(self) -> str:
        return self.message


class NeverSent(SnodraError):
    """The request was never written to a hub: the handler did not run, and sending it again is safe."""


class OutcomeUnknown(SnodraError):
    """The request was written and the hub stream broke before an answer: the handler may have run."""


class LostParts(SnodraError):
    """A gather lost some of its parts: they were not answered."""


class ReservedHeader(SnodraError, ValueError):
    """The call set a header the library owns."""


class RegistrationClosed(SnodraError, RuntimeError):
    """A handler was registered after the app started."""


class ConfigurationError(SnodraError, ValueError):
    """A setting the library refuses."""


class DecodeError(SnodraError, ValueError):
    """A payload the codec cannot read."""


class Unauthenticated(SnodraError):
    """Raised by a handler to answer ``UNAUTHENTICATED``."""


class PermissionDenied(SnodraError):
    """Raised by a handler to answer ``PERMISSION_DENIED``."""


class InvalidRequest(SnodraError):
    """Raised by a handler to answer ``INVALID_REQUEST``."""


class HandlerError(SnodraError):
    """Raised by a handler to answer ``HANDLER_ERROR`` with machine-readable JSON ``details``."""

    def __init__(self, message: str, *, details: object = None) -> None:
        super().__init__(message)
        self.details = details

    def __reduce__(self) -> tuple[object, ...]:
        return (_rebuild_handler_error, (str(self), self.details))


def _rebuild_handler_error(message: str, details: object) -> HandlerError:
    return HandlerError(message, details=details)


# TODO(spec-gap): SG-1
def error_from_wire(e: hub_pb2.Error, type_: str) -> HubError:
    """The error a caller reads for a hub ``Error``; a code this library does not know reads as ``HANDLER_ERROR``."""
    try:
        code = ErrorCode(int(e.code))
    except ValueError:
        code = ErrorCode.HANDLER_ERROR
    message = e.message
    match code:
        case ErrorCode.NO_HANDLER:
            message = f"No handler registered for '{type_}'"
        case ErrorCode.TIMEOUT:
            message = f"Timed out waiting for reply to '{type_}'"
        case ErrorCode.HANDLER_DISCONNECTED:
            message = f"Handler for '{type_}' disconnected"
        case ErrorCode.RESOURCE_EXHAUSTED:
            message = f"Handler for '{type_}' is busy: its hub queue or session buffer is full"
        case ErrorCode.CANCELLED:
            message = message or "cancelled by the hub"
    details = bytes(e.details) if code == ErrorCode.HANDLER_ERROR else b""
    return HubError(code, message, details)


_LINE_BREAK = re.compile(r"\r\n|\n|\r")


def _one_line(text: str) -> str:
    return _LINE_BREAK.sub(" ", text)


# TODO(spec-gap): SG-1
def error_to_wire(request_id: str, exc: BaseException, codec: "Codec") -> hub_pb2.Error:
    """The ``Error`` a handler's exception is answered with: a message of one line, never a traceback.

    Details are set only with ``HANDLER_ERROR``. An exception raised by the call of another service is the handler's
    own ``HANDLER_ERROR``, never passed through with its code.
    """
    text = str(exc)
    code = ErrorCode.HANDLER_ERROR
    details = b""
    if isinstance(exc, Unauthenticated):
        code, message = ErrorCode.UNAUTHENTICATED, text or "unauthenticated"
    elif isinstance(exc, PermissionDenied):
        code, message = ErrorCode.PERMISSION_DENIED, text or "access denied"
    elif isinstance(exc, InvalidRequest):
        code, message = ErrorCode.INVALID_REQUEST, text or "invalid request"
    elif is_retryable(exc):
        message = text or "the external call failed and may be retried"
        details = RETRYABLE
    elif is_unknown_outcome(exc):
        message = text or "the outcome of the external call is unknown"
        details = UNKNOWN_OUTCOME
    elif isinstance(exc, HandlerError):
        message = text
        details = _encode_details(codec, exc.details) if exc.details is not None else b""
    elif isinstance(exc, ValueError):
        message = text or type(exc).__name__
        details = _encode_details(codec, [])
    else:
        message = f"{type(exc).__name__}: {text}"
    if code != ErrorCode.HANDLER_ERROR:
        details = b""
    return hub_pb2.Error(request_id=request_id, code=cast("Any", code), message=_one_line(message), details=details)


def _encode_details(codec: "Codec", details: object) -> bytes:
    try:
        return codec.encode(details)
    except Exception:
        return b""
