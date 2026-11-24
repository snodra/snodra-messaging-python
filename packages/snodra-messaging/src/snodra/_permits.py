"""The permit accounting of one hub stream: when the permits of finished requests go back to the hub.

See https://snodra.damoreira.com/library-authors/protocol/ ("Flow control").
"""

from __future__ import annotations

from snodra._timings import DEFAULT_TIMINGS


class PermitLedger:
    """What one stream offered (its ``Register.initial_permits``), the requests that arrived on it, the permits written
    back and those pending. The hub still holds ``offered - arrived + returned`` permits of the stream.

    On each finished request one grant is pending; then all the pending grants are written at once while the hub holds
    ``min(batch, offered // 2)`` or fewer (with one permit: every completion), or once ``batch`` are pending; else they
    wait for the flush timer (50 ms), armed by the first one.
    """

    def __init__(self, offered: int, *, batch: int = DEFAULT_TIMINGS.permit_batch) -> None:
        self._offered = offered
        self._batch = batch
        self._low_water = min(batch, offered // 2)
        self._arrived = 0
        self._returned = 0
        self._pending = 0
        self._armed = False

    def arrived(self) -> None:
        """A request arrived on the stream: it took one of the hub's permits."""
        self._arrived += 1

    def finished(self) -> tuple[int, bool]:
        """A request of the stream is finished: the grants to write now (0 for none), and whether to arm the flush
        timer (which then calls :meth:`flush`)."""
        self._pending += 1
        held = self._offered - self._arrived + self._returned
        if held <= self._low_water or self._pending >= self._batch:
            return self._take(), False
        if self._armed:
            return 0, False
        self._armed = True
        return 0, True

    def flush(self) -> int:
        """The flush timer fired: the grants to write now (0 for none)."""
        self._armed = False
        return self._take()

    def _take(self) -> int:
        n, self._pending = self._pending, 0
        self._returned += n
        return n
