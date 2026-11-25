"""The hub addresses: the permit share of a stream, a hub's id, the parsing of a static list or one dns:/// target, and
the one address family of a DNS answer.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Several hubs", "Addresses").
"""

from __future__ import annotations

import pytest
from snodra import ConfigurationError
from snodra._targets import Target, hub_id_of, one_family, parse_targets, share_of


def test_share_of() -> None:
    assert (share_of(5, 2), share_of(5, 1), share_of(4, 3), share_of(1, 2)) == (3, 5, 2, 1)
    assert (share_of(256, 3), share_of(7, 8)) == (86, 1)  # ceil, and a hub always offers one


def test_hub_id_of() -> None:
    assert hub_id_of("hub-1", "h:7000") == "hub-1"
    assert hub_id_of("", "static://h:7000") == "h:7000"
    assert hub_id_of("", "[::1]:7000") == "[::1]:7000"


def test_one_family() -> None:
    assert one_family(["10.0.0.1", "::1", "10.0.0.1"], 7000) == ["10.0.0.1:7000"]  # IPv4 when any, no repeats
    assert one_family(["::1"], 7000) == ["[::1]:7000"]
    assert one_family(["10.0.0.2", "10.0.0.1"], 7000) == ["10.0.0.2:7000", "10.0.0.1:7000"]  # the answer's order
    assert one_family(["fe80::1%en0"], 7000) == ["[fe80::1]:7000"]  # a zone is dropped
    assert one_family([], 7000) == []


def test_parse_a_static_list() -> None:
    targets = parse_targets(("hub-1:7000", " static://hub-2:7000 ", "hub-1:7000"))
    assert targets == [Target("hub-1:7000"), Target("hub-2:7000")]
    assert [t.address for t in targets] == ["hub-1:7000", "hub-2:7000"]
    assert not any(t.dns for t in targets)


def test_parse_one_dns_target() -> None:
    (target,) = parse_targets(("dns:///hubs.internal:7000",))
    assert (target.address, target.name, target.port, target.dns) == ("hubs.internal:7000", "hubs.internal", 7000, True)
    (v6,) = parse_targets(("dns:///[::1]:7000",))
    assert (v6.address, v6.name, v6.port) == ("[::1]:7000", "::1", 7000)


@pytest.mark.parametrize(
    "addresses",
    [
        (),
        ("",),
        ("static://",),
        ("dns:///hubs.internal:7000", "hub-1:7000"),
        ("dns:///hubs.internal",),
        ("dns:///hubs.internal:0",),
        ("dns:///hubs.internal:65536",),
        ("dns://resolver/hubs.internal:7000",),
        ("http://hub-1:7000",),
    ],
)
def test_parse_refuses_what_is_not_accepted(addresses: tuple[str, ...]) -> None:
    with pytest.raises(ConfigurationError, match="SNODRA_SERVER_ADDRESS"):
        parse_targets(addresses)
