"""The retry policy: which failures are retried, and the waits between attempts.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Never sent or outcome unknown").
"""

import math

import pytest
from snodra import ErrorCode, HubError, NeverSent, OutcomeUnknown, RetryPolicy, SnodraError
from snodra._hubpb import hub_pb2
from snodra._retry import retryable


def _hub(code: ErrorCode) -> HubError:
    return HubError(code, "x")


def test_retryable() -> None:
    never_ran: list[BaseException] = [NeverSent("x"), _hub(ErrorCode.NO_HANDLER), _hub(ErrorCode.RESOURCE_EXHAUSTED)]
    may_have_run: list[BaseException] = [
        _hub(ErrorCode.TIMEOUT),
        _hub(ErrorCode.HANDLER_DISCONNECTED),
        OutcomeUnknown("x"),
    ]
    never: list[BaseException] = [
        _hub(ErrorCode.HANDLER_ERROR),
        _hub(ErrorCode.UNAUTHENTICATED),
        _hub(ErrorCode.PERMISSION_DENIED),
        _hub(ErrorCode.INVALID_REQUEST),
        _hub(ErrorCode.CANCELLED),
        SnodraError("x"),
        ValueError("x"),
    ]
    for e in never_ran:
        assert retryable(hub_pb2.COMMAND, e), e
        assert retryable(hub_pb2.QUERY, e), e
    for e in may_have_run:
        assert not retryable(hub_pb2.COMMAND, e), e
        assert retryable(hub_pb2.QUERY, e), e
    for e in never:
        assert not retryable(hub_pb2.COMMAND, e), e
        assert not retryable(hub_pb2.QUERY, e), e


def test_the_policy_defaults() -> None:
    p = RetryPolicy()
    assert (p.max_attempts, p.initial_backoff, p.multiplier, p.max_backoff) == (3, 0.1, 2.0, 2.0)


def test_the_waits_grow_by_the_multiplier_up_to_the_maximum() -> None:
    p = RetryPolicy(max_attempts=10, initial_backoff=0.1, multiplier=3.0, max_backoff=1.0)
    assert [round(p.backoff_before(n), 6) for n in range(2, 6)] == [0.1, 0.3, 0.9, 1.0]
    assert RetryPolicy(max_attempts=5000, multiplier=10.0).backoff_before(4999) == 2.0  # no overflow
    # +-20 % below the maximum, 80-100 % of it at the maximum.
    assert (p.wait_before(2, 0.0), p.wait_before(2, 1.0)) == pytest.approx((0.08, 0.12))
    assert (p.wait_before(5, 0.0), p.wait_before(5, 1.0)) == pytest.approx((0.8, 1.0))
    with pytest.raises(ValueError, match="attempt 1"):
        p.backoff_before(1)


@pytest.mark.parametrize(
    ("fields", "text"),
    [
        ({"max_attempts": 0}, "max_attempts"),
        ({"max_attempts": True}, "max_attempts"),
        ({"max_attempts": 1.5}, "max_attempts"),
        ({"initial_backoff": -0.1}, "initial_backoff"),
        ({"initial_backoff": math.nan}, "initial_backoff"),
        ({"max_backoff": math.inf}, "max_backoff"),
        ({"multiplier": 0.5}, "multiplier"),
        ({"multiplier": math.inf}, "multiplier"),
        ({"multiplier": "2"}, "multiplier"),
    ],
)
def test_a_policy_refuses_bad_values(fields: dict[str, object], text: str) -> None:
    with pytest.raises(ValueError, match=text):
        RetryPolicy(**fields)  # type: ignore[arg-type]
