"""The RabbitMQ Streams routing rules: which partition a key goes to, and how a position compares.

The stream clients' hash routing is murmur3 x86_32 of the routing key's UTF-8 bytes, seed 104729,
taken **unsigned** and modulo the partitions in the broker's order. A signed modulo lands
``order-7`` on partition 5 instead of 3, so the unsigned value is the rule every Snodra add-on
follows. The routing key is the event's key, or its id when it has none.

A segment is the partition stream's suffix after ``<source>-`` (``snodra.events-3`` is ``3``), and
``0`` for a source that is one plain stream (the dead-letter stream). A position is the stream
offset in decimal; the stored offset is the next one to read.

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#rabbitmq-streams-wire-rules.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from snodra import Invalid

HASH_SEED: Final = 104_729
"""The murmur3 seed of the stream clients' hash routing."""

SINGLE_SEGMENT: Final = "0"
"""The segment of a source that is one plain stream (a dead-letter stream)."""

MAX_OFFSET: Final = 2**64 - 1
"""The largest stream offset (the protocol's uint64)."""


def murmur3(data: bytes) -> int:
    """Murmur3 x86_32 of ``data`` with :data:`HASH_SEED`, as an unsigned 32-bit integer."""
    length = len(data)
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    mask = 0xFFFFFFFF
    value = HASH_SEED
    rounds = length & ~3
    for index in range(0, rounds, 4):
        word = data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24)
        word = (word * c1) & mask
        word = ((word << 15) | (word >> 17)) & mask
        word = (word * c2) & mask
        value ^= word
        value = ((value << 13) | (value >> 19)) & mask
        value = (value * 5 + 0xE6546B64) & mask
    tail = length & 3
    if tail:
        word = 0
        if tail >= 3:
            word ^= data[rounds + 2] << 16
        if tail >= 2:
            word ^= data[rounds + 1] << 8
        word ^= data[rounds]
        word = (word * c1) & mask
        word = ((word << 15) | (word >> 17)) & mask
        word = (word * c2) & mask
        value ^= word
    value ^= length
    value ^= value >> 16
    value = (value * 0x85EBCA6B) & mask
    value ^= value >> 13
    value = (value * 0xC2B2AE35) & mask
    value ^= value >> 16
    return value


def partition_for(routing_key: str, partitions: Sequence[str]) -> int:
    """The index of ``routing_key``'s partition among ``partitions`` (in the broker's order)."""
    if not partitions:
        raise Invalid("the source has no partitions")
    return murmur3(routing_key.encode("utf-8")) % len(partitions)


def routing_key(key: str | None, event_id: str) -> str:
    """The key an event routes by: its own, or its id when it has none."""
    return event_id if key is None else key


def segment_of(source: str, stream: str) -> str:
    """The segment of partition stream ``stream`` of ``source``: its suffix after ``<source>-``,
    else the stream's whole name."""
    prefix = source + "-"
    if stream.startswith(prefix):
        suffix = stream[len(prefix) :]
        if suffix:
            return suffix
    return stream


def parse_position(position: str) -> int:
    """``position`` as a stream offset: a decimal uint64; anything else is :class:`~snodra.Invalid`."""
    if not position or not all("0" <= char <= "9" for char in position):
        raise Invalid(f"position {position!r} is not an offset")
    offset = int(position)
    if offset > MAX_OFFSET:
        raise Invalid(f"position {position!r} is not an offset")
    return offset


def committed_offset(position: str) -> int:
    """The offset stored for an acknowledgement of ``position``: the next one to read."""
    offset = parse_position(position)
    if offset == MAX_OFFSET:
        raise Invalid(f"position {position!r} has no next offset")
    return offset + 1


def position_of(offset: int) -> str:
    """``offset`` as a position: its decimal number."""
    return str(offset)


def compare_positions(a: str, b: str) -> int:
    """The order of two offsets: decimal offsets by value, text that is not an offset after every
    offset (and by its UTF-8 bytes among themselves). The order is total, as
    :meth:`snodra.EventStore.compare` needs."""
    first = _offset(a)
    second = _offset(b)
    if first is not None and second is not None:
        return (first > second) - (first < second)
    if first is not None:
        return -1
    if second is not None:
        return 1
    return _bytes_compare(a.encode("utf-8", "surrogatepass"), b.encode("utf-8", "surrogatepass"))


def _offset(position: str) -> int | None:
    if not position or not all("0" <= char <= "9" for char in position):
        return None
    offset = int(position)
    return offset if offset <= MAX_OFFSET else None


def _bytes_compare(a: bytes, b: bytes) -> int:
    return (a > b) - (a < b)
