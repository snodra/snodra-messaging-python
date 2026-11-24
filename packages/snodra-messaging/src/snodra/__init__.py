"""Snodra messaging client library."""

from importlib.metadata import version

from snodra._app import App
from snodra._codec import Codec, JsonCodec, RawJson
from snodra._connection import Rejection
from snodra._context import MessageInfo, current_caller, current_message
from snodra._credentials import AccessToken, ClientCredentials, Token, TokenFile, TokenSource
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
from snodra._gather import Gathered, Part
from snodra._identity import Caller, Guard, authenticated, caller_matches, role
from snodra._messages import message_type
from snodra._retry import RetryPolicy
from snodra._settings import Settings
from snodra._tls import ClientCertificate, TLSConfig

__version__: str = version("snodra-messaging")

__all__ = [
    "AccessToken",
    "App",
    "Caller",
    "ClientCertificate",
    "ClientCredentials",
    "Codec",
    "ConfigurationError",
    "DecodeError",
    "ErrorCode",
    "Gathered",
    "Guard",
    "HandlerError",
    "HubError",
    "InvalidRequest",
    "JsonCodec",
    "LostParts",
    "MessageInfo",
    "NeverSent",
    "OutcomeUnknown",
    "Part",
    "PermissionDenied",
    "RawJson",
    "RegistrationClosed",
    "Rejection",
    "ReservedHeader",
    "RetryPolicy",
    "Settings",
    "SnodraError",
    "TLSConfig",
    "Token",
    "TokenFile",
    "TokenSource",
    "Unauthenticated",
    "__version__",
    "authenticated",
    "caller_matches",
    "current_caller",
    "current_message",
    "message_type",
    "role",
]
