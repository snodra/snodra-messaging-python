import pytest
from snodra._backoff import jitter, next_wait


def test_backoff() -> None:
    seq = [0.5]
    for _ in range(5):
        seq.append(next_wait(seq[-1], 0.5, 5.0))
    assert seq == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0]
    assert jitter(1.0, 5.0, 0) == 0.8
    assert jitter(1.0, 5.0, 1) == pytest.approx(1.2)
    assert jitter(5.0, 5.0, 0) == 4.0
    assert jitter(4.5, 5.0, 1) == 5.0  # never above the maximum


def test_at_the_maximum_the_jitter_only_goes_down() -> None:
    # 80 to 100 % of the maximum, spread over the whole range: clamping a +-20 % wait would put half of a
    # reconnecting fleet on exactly the maximum.
    assert jitter(5.0, 5.0, 0.5) == pytest.approx(4.5)
    assert jitter(5.0, 5.0, 1) == pytest.approx(5.0)
    assert jitter(0.2, 0.2, 0.25) == pytest.approx(0.17)


def test_next_wait_never_goes_below_the_floor() -> None:
    assert next_wait(0.0, 0.5, 5.0) == 0.5
