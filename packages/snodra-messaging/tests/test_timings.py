from snodra._timings import DEFAULT_TIMINGS


def test_default_timings() -> None:
    t = DEFAULT_TIMINGS
    assert (t.heartbeat_before_ack, t.handshake_timeout, t.silent_intervals, t.keepalive_time, t.keepalive_timeout) == (
        3.0,
        10.0,
        3,
        10.0,
        5.0,
    )
    assert (t.deregister_wait, t.subscription_end, t.handler_drain, t.close_wait) == (1.0, 2.0, 5.0, 2.0)
    assert (t.permit_batch, t.permit_flush_after) == (16, 0.05)
    assert (t.guard_margin, t.fallback_max_timeout, t.default_deadline, t.retry_min_remaining) == (1.0, 60.0, 5.0, 0.25)
    assert (t.outbound_limit, t.max_unwritten_moves) == (256, 8)
    assert (t.jwks_min_refresh, t.jwks_fetch_timeout, t.jwt_leeway) == (60.0, 5.0, 60.0)
    assert (t.token_fetch_timeout, t.default_token_lifetime, t.min_refresh_margin, t.token_retry) == (
        10.0,
        60.0,
        5.0,
        (1.0, 30.0),
    )
    assert (t.cert_check_interval, t.roll_delay_max, t.roll_idle_limit, t.roll_reconnect_limit) == (
        3.0,
        30.0,
        10.0,
        30.0,
    )
    assert t.default_credit == 64  # Request.initial_credit 0 reads as 64 at the hub


def test_default_timings_of_several_hubs() -> None:
    t = DEFAULT_TIMINGS
    assert (t.status_interval, t.dns_refresh) == (5.0, 30.0)
    assert (t.dns_drain_limit, t.max_unwritten_moves) == (65.0, 8)


def test_the_event_timings() -> None:
    t = DEFAULT_TIMINGS
    assert (t.event_handler_backoff, t.event_resubscribe_min, t.event_resubscribe_max) == (1.0, 1.0, 30.0)
    assert t.event_resubscribe_stable == 30.0
