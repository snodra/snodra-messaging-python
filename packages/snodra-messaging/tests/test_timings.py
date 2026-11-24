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
