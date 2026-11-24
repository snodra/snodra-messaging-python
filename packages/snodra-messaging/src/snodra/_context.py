"""What the request being handled carries, in context variables: tasks a handler starts see it too."""

from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class MessageInfo:
    """The request a handler is handling."""

    id: str
    """``Request.id`` as it was forwarded (``<id>#<n>`` for a scatter-gather target)."""
    type: str
    """The wire type name, for example ``orders.PlaceOrder``."""
    correlation_id: str
    """The request's ``correlationId`` header; its id when it carries none."""
    headers: Mapping[str, str]
    """Every request header, read-only."""


_CURRENT: Final[ContextVar[MessageInfo | None]] = ContextVar("snodra.current_message", default=None)


def current_message() -> MessageInfo | None:
    """The request being handled; None outside a handler."""
    return _CURRENT.get()
