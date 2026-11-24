"""Reconnect waits: doubling from a floor to a maximum, each jittered.

See https://snodra.damoreira.com/library-authors/protocol/ ("Reconnect and backoff").
"""

from __future__ import annotations


def next_wait(prev: float, lo: float, hi: float) -> float:
    """The wait after ``prev``: twice it, at least ``lo``, at most ``hi``."""
    return min(max(prev * 2, lo), hi)


def jitter(base: float, hi: float, unit: float) -> float:
    """``base`` jittered by +-20 % (``unit`` uniform in [0, 1]), never above ``hi``.

    At the maximum the wait goes down only, to 80-100 % of it: clamping a +-20 % wait there would put half of a
    reconnecting fleet on exactly the maximum. A refused ``Register`` waits ``jitter(hi, hi, unit)``.
    """
    if base >= hi:
        return hi * (0.8 + 0.2 * unit)
    return min(base * (0.8 + 0.4 * unit), hi)
