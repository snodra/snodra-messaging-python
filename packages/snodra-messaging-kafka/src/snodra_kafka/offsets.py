"""Kafka's segments, positions and key placement, read and written exactly as the wire rules say.

On Kafka a segment is a partition number and a position an offset, both in decimal; a subscription's
committed offset is the next offset to read. A keyed record is placed as the Java client's default
partitioner places it: murmur2 of the key's UTF-8 bytes (seed ``0x9747b28c``), masked positive and
taken modulo the partitions. One key therefore lands on one partition whatever Snodra library
published it, and its events keep their order there. An empty key is a key, hashed like any other;
a record without a key has no placement rule.

See https://snodra.damoreira.com/docs/library-authors/event-store-add-ons/#kafka-wire-rules.
"""

from __future__ import annotations

from typing import Final

from snodra import Invalid

MAX_PARTITION: Final = 2**31 - 1
"""The largest Kafka partition number (the protocol's int32)."""

MAX_OFFSET: Final = 2**63 - 1
"""The largest Kafka offset (the protocol's int64)."""


def parse_segment(segment: str) -> int:
    """``segment`` as a partition number: a decimal int32 that is not negative.

    The forms the Kafka clients accept are accepted (an optional sign and leading zeros); anything
    else is :class:`~snodra.Invalid`.
    """
    partition = _decimal(segment, MAX_PARTITION)
    if partition is None:
        raise Invalid(f"segment {segment!r} is not a partition")
    return partition


def parse_position(position: str) -> int:
    """``position`` as an offset: a decimal int64 that is not negative; anything else is
    :class:`~snodra.Invalid`."""
    offset = _decimal(position, MAX_OFFSET)
    if offset is None:
        raise Invalid(f"position {position!r} is not an offset")
    return offset


def committed_offset(position: str) -> int:
    """The offset a group commits for an acknowledgement of ``position``: the next one to read.

    Kafka always stores the next offset, never the acknowledged one; a position with no next offset
    is :class:`~snodra.Invalid`.
    """
    offset = parse_position(position)
    if offset == MAX_OFFSET:
        raise Invalid(f"position {position!r} has no next offset")
    return offset + 1


def segment_of(partition: int) -> str:
    """``partition`` as a segment name: its decimal number."""
    return str(partition)


def position_of(offset: int) -> str:
    """``offset`` as a position: its decimal number."""
    return str(offset)


def segment_names(partitions: list[int]) -> tuple[str, ...]:
    """The segment names of ``partitions``, in segment order (partitions by number)."""
    return tuple(segment_of(partition) for partition in sorted(partitions))


def compare_positions(a: str, b: str) -> int:
    """The order of two offsets: decimal offsets by value, text that is not an offset after every
    offset (and by its UTF-8 bytes among themselves).

    :func:`snodra.EventStore.compare` reports a difference for any two positions, also
    for text that is not an offset, so the comparison is total.
    """
    first = _decimal(a, MAX_OFFSET)
    second = _decimal(b, MAX_OFFSET)
    if first is not None and second is not None:
        return (first > second) - (first < second)
    if first is not None:
        return -1
    if second is not None:
        return 1
    return _bytes_compare(a.encode("utf-8", "surrogatepass"), b.encode("utf-8", "surrogatepass"))


def murmur2(data: bytes) -> int:
    """MurmurHash2 of ``data`` with the Java client's seed, as a signed 32-bit integer."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    mask = 0xFFFFFFFF

    value = (seed ^ length) & mask
    for index in range(length // 4):
        at = index * 4
        word = (
            (data[at] & 0xFF)
            | ((data[at + 1] & 0xFF) << 8)
            | ((data[at + 2] & 0xFF) << 16)
            | ((data[at + 3] & 0xFF) << 24)
        )
        word = (word * m) & mask
        word ^= word >> r
        word = (word * m) & mask
        value = (value * m) & mask
        value ^= word
    extra = length % 4
    if extra >= 3:
        value ^= (data[(length & ~3) + 2] & 0xFF) << 16
    if extra >= 2:
        value ^= (data[(length & ~3) + 1] & 0xFF) << 8
    if extra >= 1:
        value ^= data[length & ~3] & 0xFF
        value = (value * m) & mask
    value ^= value >> 13
    value = (value * m) & mask
    value ^= value >> 15
    return value - 0x100000000 if value & 0x80000000 else value


def partition_for(key: bytes, partitions: int) -> int:
    """The partition of ``key`` among ``partitions``: murmur2 of its bytes, masked positive, modulo
    the partitions (the Java client's default partitioner)."""
    if partitions < 1:
        raise Invalid(f"the topic has no partitions, got {partitions}")
    return (murmur2(key) & 0x7FFFFFFF) % partitions


def _decimal(text: str, maximum: int) -> int | None:
    """``text`` as a decimal number in the forms the Kafka clients accept (an optional ``+`` or
    ``-``, ASCII digits, leading zeros), when it is not negative and at most ``maximum``."""
    if not text:
        return None
    digits = text[1:] if text[0] in "+-" else text
    if not digits or not all("0" <= char <= "9" for char in digits):
        return None
    value = int(text)
    if value < 0 or value > maximum:
        return None
    return value


def _bytes_compare(a: bytes, b: bytes) -> int:
    return (a > b) - (a < b)
