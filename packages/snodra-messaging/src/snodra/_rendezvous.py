"""The hub choice of a keyed request: the rendezvous score of a hub id and a routing key, and the ranking of hubs by it.

Every official library uses exactly this function, so that a key goes through the same hub whatever library its sender
uses (https://snodra.damoreira.com/docs/library-authors/protocol/, "Several hubs").
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

_FNV_BASIS: Final = 0xCBF29CE484222325
_FNV_PRIME: Final = 0x100000001B3
_MASK: Final = 2**64 - 1


def score(hub_id: str, key: str) -> int:
    """The rendezvous score of (hub_id, key): 64-bit FNV-1a over the UTF-8 bytes of ``hub_id``, one zero byte and the
    UTF-8 bytes of ``key``, then MurmurHash3's ``fmix64`` finalizer. The zero byte keeps ("ab", "c") and ("a", "bc")
    apart."""
    h = _FNV_BASIS
    for b in hub_id.encode():
        h = ((h ^ b) * _FNV_PRIME) & _MASK
    h = (h * _FNV_PRIME) & _MASK  # the zero byte: h ^ 0 is h
    for b in key.encode():
        h = ((h ^ b) * _FNV_PRIME) & _MASK
    return _fmix64(h)


def _fmix64(k: int) -> int:
    """MurmurHash3's 64-bit finalizer: every input bit affects every output bit."""
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & _MASK
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & _MASK
    return k ^ (k >> 33)


def rank(hub_ids: Sequence[str], key: str) -> list[str]:
    """The hub ids from the preferred to the least preferred hub for key (a new list): the highest score as an unsigned
    64-bit number first, equal scores won by the smaller hub id, compared as bytes (hub ids are ASCII)."""
    scored = [(hub_id, score(hub_id, key)) for hub_id in hub_ids]
    scored.sort(key=lambda s: (-s[1], s[0].encode()))
    return [hub_id for hub_id, _ in scored]
