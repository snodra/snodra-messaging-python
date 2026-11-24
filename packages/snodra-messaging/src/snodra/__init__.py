"""Snodra messaging client library."""

from importlib.metadata import version

from snodra._app import App
from snodra._codec import Codec, JsonCodec, RawJson
from snodra._connection import Rejection
from snodra._context import MessageInfo, current_message
from snodra._errors import (
    ConfigurationError,
    DecodeError,
    ErrorCode,
    HandlerError,
    HubError,
    InvalidRequest,
    LostParts,
    NeverSent,
    OutcomeUnknown,
    PermissionDenied,
    RegistrationClosed,
    ReservedHeader,
    SnodraError,
    Unauthenticated,
)
from snodra._messages import message_type
from snodra._retry import RetryPolicy
from snodra._settings import Settings

__version__: str = version("snodra-messaging")

__all__ = [
    "App",
    "Codec",
    "ConfigurationError",
    "DecodeError",
    "ErrorCode",
    "HandlerError",
    "HubError",
    "InvalidRequest",
    "JsonCodec",
    "LostParts",
    "MessageInfo",
    "NeverSent",
    "OutcomeUnknown",
    "PermissionDenied",
    "RawJson",
    "RegistrationClosed",
    "Rejection",
    "ReservedHeader",
    "RetryPolicy",
    "Settings",
    "SnodraError",
    "Unauthenticated",
    "__version__",
    "current_message",
    "message_type",
]
