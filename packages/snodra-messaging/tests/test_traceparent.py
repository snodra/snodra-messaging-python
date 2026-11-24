"""The W3C ``traceparent`` values the library forwards.

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Trace context").
"""

import pytest
from helpers import INVALID_TRACEPARENTS as INVALID
from helpers import TP
from snodra._traceparent import trace_id, valid


def test_only_a_valid_traceparent_is_forwarded() -> None:
    assert valid(TP)
    assert valid("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
    for bad in INVALID:
        assert not valid(bad), bad


def test_the_trace_id_of_a_traceparent() -> None:
    assert trace_id(TP) == "4bf92f3577b34da6a3ce929d0e0e4736"
    with pytest.raises(ValueError, match="traceparent"):
        trace_id(INVALID[1])
