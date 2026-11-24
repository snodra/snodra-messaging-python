"""The retry policy of a call, and which failures it retries.

See https://snodra.damoreira.com/library-authors/protocol/ ("Never sent or outcome unknown").
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from snodra._backoff import jitter
from snodra._errors import ErrorCode, HubError, NeverSent, OutcomeUnknown
from snodra._hubpb import hub_pb2


@dataclass(frozen=True)
class RetryPolicy:
    """Sends a failed call again, as a new request (a fresh id), when the handler never ran (never sent,
    ``NO_HANDLER``, ``RESOURCE_EXHAUSTED``); a query also after an unknown outcome (``TIMEOUT``,
    ``HANDLER_DISCONNECTED``, a stream broken after the write). Every attempt shares the call's time budget and
    sends what is left of it as its timeout. Durations are seconds.
    """

    max_attempts: int = 3
    """The attempts in all, the first one included (1: no retry)."""
    initial_backoff: float = 0.1
    """The wait before the second attempt."""
    multiplier: float = 2.0
    """How much each further wait grows (at least 1)."""
    max_backoff: float = 2.0
    """The longest wait."""

    def __post_init__(self) -> None:
        if not _is_int(self.max_attempts) or self.max_attempts < 1:
            raise ValueError(f"RetryPolicy.max_attempts must be an integer of at least 1, got {self.max_attempts!r}")
        for name in ("initial_backoff", "max_backoff"):
            value = getattr(self, name)
            if not _is_number(value) or not math.isfinite(value) or value < 0:
                raise ValueError(f"RetryPolicy.{name} must be a finite number of seconds, not negative, got {value!r}")
        if not _is_number(self.multiplier) or not math.isfinite(self.multiplier) or self.multiplier < 1:
            raise ValueError(f"RetryPolicy.multiplier must be a finite number of at least 1, got {self.multiplier!r}")

    def backoff_before(self, attempt: int) -> float:
        """The wait before attempt number ``attempt`` (2 for the first retry), without jitter:
        ``initial_backoff * multiplier ** (attempt - 2)``, at most ``max_backoff``."""
        if attempt < 2:
            raise ValueError(f"attempt {attempt} has no wait before it: attempt 1 is the first")
        try:
            base = self.initial_backoff * self.multiplier ** (attempt - 2)
        except OverflowError:
            return self.max_backoff
        return min(base, self.max_backoff)

    def wait_before(self, attempt: int, unit: float | None = None) -> float:
        """The wait before attempt number ``attempt``: :meth:`backoff_before` +-20 %, at the maximum 80-100 % of it
        (``unit`` uniform in [0, 1], random by default)."""
        return jitter(self.backoff_before(attempt), self.max_backoff, random.random() if unit is None else unit)


def retryable(kind: int, exc: BaseException) -> bool:
    """Whether a call of ``kind`` (``RequestKind``) that failed with ``exc`` may be sent again: always when the handler
    never ran, and for a query also when the outcome is unknown."""
    if isinstance(exc, NeverSent):
        return True
    if isinstance(exc, HubError):
        if exc.code in (ErrorCode.NO_HANDLER, ErrorCode.RESOURCE_EXHAUSTED):
            return True
        if exc.code in (ErrorCode.TIMEOUT, ErrorCode.HANDLER_DISCONNECTED):
            return kind == hub_pb2.QUERY
        return False
    if isinstance(exc, OutcomeUnknown):
        return kind == hub_pb2.QUERY
    return False


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)
