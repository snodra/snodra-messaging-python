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
