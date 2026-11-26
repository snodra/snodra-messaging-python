"""What the probe's consumer did, in memory.

The stage-1 marker records come from the observed processed port; the handled events come from the
event handler. The real state is the stores: the driver asserts the markers through each instance's
``processed.exists``.
"""

from __future__ import annotations

from dataclasses import dataclass

from snodra import ProcessedMarker

from store_interop.messages import Delivery, OrderPlaced
from store_interop.settings import processor_name


@dataclass(frozen=True, slots=True)
class _Marker:
    event_id: str
    segment: str
    position: str
    duplicate: bool


class Ledger:
    """The deliveries of one probe instance."""

    def __init__(self) -> None:
        self._markers: list[_Marker] = []
        self._handled: dict[str, OrderPlaced] = {}

    def observe_marker(self, family: str, marker: ProcessedMarker, recorded: bool) -> None:
        """Record one processed port call, when it is this family's stage-1 marker."""
        prefix = f"processor:{processor_name(family)}:"
        if not marker.id.startswith(prefix):
            return
        self._markers.append(
            _Marker(
                event_id=marker.id.removeprefix(prefix),
                segment=marker.segment or "",
                position=marker.position or "",
                duplicate=not recorded,
            )
        )

    def observe_handled(self, event: OrderPlaced) -> None:
        """Record one event handler run."""
        self._handled[event.id] = event

    def snapshot(self) -> tuple[list[Delivery], int, int]:
        """The deliveries as the driver reads them, and the counts of handled and duplicate ones."""
        deliveries: list[Delivery] = []
        handled = duplicates = 0
        for marker in self._markers:
            if marker.duplicate:
                deliveries.append(
                    Delivery(
                        id=marker.event_id,
                        segment=marker.segment,
                        position=marker.position,
                        outcome="duplicate",
                    )
                )
                duplicates += 1
                continue
            event = self._handled.get(marker.event_id)
            deliveries.append(
                Delivery(
                    id=marker.event_id,
                    segment=marker.segment,
                    position=marker.position,
                    outcome="handled",
                    publisher=None if event is None else event.publisher,
                    key=None if event is None else event.key,
                )
            )
            handled += 1
        return deliveries, handled, duplicates
