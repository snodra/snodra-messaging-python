"""The library's fixed timings (seconds), in one place so that the tests can scale them down."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class Timings:
    """The client-side timings of https://snodra.damoreira.com/library-authors/protocol/ ("Timing summary")."""

    heartbeat_before_ack: float
    """The heartbeat period of a stream before its ``RegisterAck`` (after it: ``RegisterAck.heartbeat_interval_ms``)."""
    handshake_timeout: float
    """A stream without a ``RegisterAck`` within this is closed and reopened."""
    silent_intervals: int
    """A hub is silent on a tick more than this many heartbeat intervals after its last echo, with at least this many
    heartbeats unanswered."""
    keepalive_time: float
    """HTTP/2 keepalive ping period, also while idle."""
    keepalive_timeout: float
    """How long a keepalive ping may go unanswered."""
    deregister_wait: float
    """Shutdown: the longest wait for the acks of the empty ``Register``."""
    subscription_end: float
    """Shutdown: the longest wait for the served subscriptions to end."""
    handler_drain: float
    """Shutdown: the longest wait for the handlers in flight."""
    close_wait: float
    """Shutdown: the longest wait for the hub to finish a stream after the half-close."""
    permit_batch: int
    """Permits go back at once when this many are pending (or while the hub holds few of the stream's)."""
    permit_flush_after: float
    """Pending permits go back at the latest this long after the first of them."""
    guard_margin: float
    """A request is given up locally this long after the hub's bound for it, so the hub's ``TIMEOUT`` comes first."""
    fallback_max_timeout: float
    """The hub's bound for a request when its ``RegisterAck`` names no ``max_timeout_ms``."""
    default_deadline: float
    """A call's deadline when neither the call nor the settings chose a timeout."""
    retry_min_remaining: float
    """A retry starts only with at least this much of the budget left after its wait (or the initial backoff)."""
    outbound_limit: int
    """Requests waiting in a stream's outbound buffer at most; a call beyond waits for room."""
    max_unwritten_moves: int
    """How often a request lost in a stream's buffer moves to another stream before it fails as never sent."""


DEFAULT_TIMINGS: Final = Timings(
    heartbeat_before_ack=3.0,
    handshake_timeout=10.0,
    silent_intervals=3,
    keepalive_time=10.0,
    keepalive_timeout=5.0,
    deregister_wait=1.0,
    subscription_end=2.0,
    handler_drain=5.0,
    close_wait=2.0,
    permit_batch=16,
    permit_flush_after=0.05,
    guard_margin=1.0,
    fallback_max_timeout=60.0,
    default_deadline=5.0,
    retry_min_remaining=0.25,
    outbound_limit=256,
    max_unwritten_moves=8,
)
"""The shipped timings."""
