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
)
"""The shipped timings."""
