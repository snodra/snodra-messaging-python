"""The W3C ``traceparent`` values the library carries on.

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Trace context").
"""

from __future__ import annotations

import re
from typing import Final

_FORMAT: Final = re.compile(r"00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}")
_ZERO_TRACE: Final = "0" * 32
_ZERO_PARENT: Final = "0" * 16


def valid(s: str) -> bool:
    """True when s is a valid W3C ``traceparent`` of version ``00``: ``00-<32 hex>-<16 hex>-<2 hex>`` in lower case,
    neither the trace id nor the parent id all zeros."""
    m = _FORMAT.fullmatch(s) if isinstance(s, str) else None
    return m is not None and m.group(1) != _ZERO_TRACE and m.group(2) != _ZERO_PARENT


def trace_id(s: str) -> str:
    """The trace id (32 hex digits) of a valid ``traceparent``; ``ValueError`` for any other value."""
    if not valid(s):
        raise ValueError(f"not a valid traceparent: {s!r}")
    return s[3:35]
