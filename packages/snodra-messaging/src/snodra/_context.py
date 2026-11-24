"""What the request being handled carries, in context variables: tasks a handler starts see it too."""

from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from snodra._identity import Caller


@dataclass(frozen=True)
class MessageInfo:
    """The request a handler is handling."""

    id: str
    """``Request.id`` as it was forwarded (``<id>#<n>`` for a scatter-gather target)."""
    type: str
    """The wire type name, for example ``orders.PlaceOrder``."""
    correlation_id: str
    """The request's ``correlationId`` header (an empty one as it is); its id when it carries none."""
    headers: Mapping[str, str]
    """Every request header, read-only."""

    def __hash__(self) -> int:
        # The headers are a mapping: hashed by their items, so that equal infos hash alike.
        return hash((self.id, self.type, self.correlation_id, frozenset(self.headers.items())))


_CURRENT: Final[ContextVar[MessageInfo | None]] = ContextVar("snodra.current_message", default=None)


def current_message() -> MessageInfo | None:
    """The request being handled; None outside a handler."""
    return _CURRENT.get()


_CALLER: Final[ContextVar[Caller | None]] = ContextVar("snodra.current_caller", default=None)


def current_caller() -> Caller | None:
    """The verified caller of the request being handled; None outside a handler, and for an anonymous caller (no
    token, or no JWKS to verify it with: ``Settings.jwks_url``)."""
    return _CALLER.get()
