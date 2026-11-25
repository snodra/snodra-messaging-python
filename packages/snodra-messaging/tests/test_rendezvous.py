"""The rendezvous choice of a keyed request: the shared score vectors, unsigned comparison, the bytes tie-break.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Several hubs").
"""

from __future__ import annotations

import pytest
from snodra import _rendezvous
from snodra._rendezvous import rank, score


def test_score_vectors() -> None:
    assert score("", "") == 0xB9034AD37056F5FB
    assert (score("hub-1", "order-42"), score("hub-2", "order-42")) == (0x5C855509F5AD7C8D, 0xD109CA177E04AE14)
    assert rank(["hub-1", "hub-2"], "order-42")[0] == "hub-2"


def test_score_is_a_64_bit_unsigned_number() -> None:
    for hub_id, key in (("hub-1", "order-42"), ("", ""), ("x" * 200, "y" * 200)):
        assert 0 <= score(hub_id, key) < 2**64


def test_rank_compares_unsigned() -> None:
    # The shared ids k-0..k-999 under order-42: the winning score is above 1 << 63, which a signed comparison would read
    # as negative and rank last.
    hubs = [f"k-{n}" for n in range(1000)]
    scores = {h: score(h, "order-42") for h in hubs}
    assert scores["k-759"] >= 1 << 63
    assert min(scores.values()) < 1 << 63
    assert rank(hubs, "order-42")[0] == "k-759"
    signed = sorted(hubs, key=lambda h: scores[h] - (2**64 if scores[h] >= 1 << 63 else 0))
    assert signed[-1] != "k-759"  # a signed comparison sends the key elsewhere


def test_equal_scores_go_to_the_smaller_hub_id(monkeypatch: pytest.MonkeyPatch) -> None:
    # Hub ids are ASCII and compared as bytes: "hub-10" sorts before "hub-2", which a text comparison would not do.
    monkeypatch.setattr(_rendezvous, "score", lambda hub_id, key: 7)
    assert rank(["hub-2", "hub-10", "hub-1"], "order-42") == ["hub-1", "hub-10", "hub-2"]


def test_rank_returns_every_id_once() -> None:
    hubs = ["hub-1", "hub-2", "hub-3"]
    assert sorted(rank(hubs, "order-42")) == sorted(hubs)
    assert rank([], "order-42") == []
