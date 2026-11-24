"""Snodra messaging client library."""

from importlib.metadata import version

from snodra._codec import Codec, JsonCodec, RawJson
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

__version__: str = version("snodra-messaging")

__all__ = [
    "Codec",
    "ConfigurationError",
    "DecodeError",
    "ErrorCode",
    "HandlerError",
    "HubError",
    "InvalidRequest",
    "JsonCodec",
    "LostParts",
    "NeverSent",
    "OutcomeUnknown",
    "PermissionDenied",
    "RawJson",
    "RegistrationClosed",
    "ReservedHeader",
    "SnodraError",
    "Unauthenticated",
    "__version__",
    "message_type",
]
