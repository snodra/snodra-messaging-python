"""The library's fixed timings (seconds), in one place so that the tests can scale them down."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class Timings:
    """The client-side timings of https://snodra.damoreira.com/docs/library-authors/protocol/ ("Timing summary")."""

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
    """Shutdown: the phase the served subscriptions end in. Their ``ReplyEnd``\\ s are queued before the half-close, so
    nothing waits on this (the shipped value is part of the published timings)."""
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
    default_credit: int
    """The window of a stream or subscription whose sender asked for none (``Request.initial_credit`` 0 reads as 64)."""
    max_unwritten_moves: int
    """How often a request lost in a stream's buffer moves to another stream before it fails as never sent."""
    jwks_min_refresh: float
    """A token whose key id the cached JWKS does not know fetches the JWKS again at most once per this."""
    jwks_fetch_timeout: float
    """The longest a JWKS fetch takes."""
    jwt_leeway: float
    """The clock skew a token's ``exp`` and ``nbf`` are allowed."""
    token_fetch_timeout: float
    """The longest a fetch of the instance's access token takes."""
    default_token_lifetime: float
    """The lifetime of an access token that names no expiry."""
    min_refresh_margin: float
    """An access token is renewed when a fifth of its lifetime is left, at least this long before its expiry (and at
    most half of its lifetime before it)."""
    token_retry: tuple[float, float]
    """A failed renewal of the access token is retried after the first, doubling up to the second."""
    cert_check_interval: float
    """How often the client certificate files are read."""
    roll_delay_max: float
    """The longest random delay before a renewed client certificate is rolled over to (also at most a tenth of what
    the presented certificate has left)."""
    roll_idle_limit: float
    """A hub being rolled over: the longest wait for the requests of its stream to finish before the half-close."""
    roll_reconnect_limit: float
    """A hub being rolled over: the longest wait for its new stream's ``RegisterAck`` before the roll stops."""
    status_interval: float
    """How often every hub stream gets an ``InstanceStatus`` (also once on every change of the acknowledged hubs)."""
    event_handler_backoff: float
    """The wait between two invocations of an event handler on one record."""
    event_resubscribe_min: float
    """After a failed event subscription, the first wait before subscribing again."""
    event_resubscribe_max: float
    """The cap of that wait (the wait doubles until it, and starts over once a subscription delivers or lived
    :attr:`event_resubscribe_stable`)."""
    event_resubscribe_stable: float
    """A subscription that lived this long before it failed counts as a good one: the next wait starts over."""
    dns_refresh: float
    """How often a ``dns:///`` target is resolved again."""
    dns_drain_limit: float
    """An address that left the DNS answer: the longest wait for nothing of the instance to be in flight on it before
    its stream is closed."""


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
    default_credit=64,
    max_unwritten_moves=8,
    jwks_min_refresh=60.0,
    jwks_fetch_timeout=5.0,
    jwt_leeway=60.0,
    token_fetch_timeout=10.0,
    default_token_lifetime=60.0,
    min_refresh_margin=5.0,
    token_retry=(1.0, 30.0),
    cert_check_interval=3.0,
    roll_delay_max=30.0,
    roll_idle_limit=10.0,
    roll_reconnect_limit=30.0,
    status_interval=5.0,
    event_handler_backoff=1.0,
    event_resubscribe_min=1.0,
    event_resubscribe_max=30.0,
    event_resubscribe_stable=30.0,
    dns_refresh=30.0,
    dns_drain_limit=65.0,
)
"""The shipped timings."""
