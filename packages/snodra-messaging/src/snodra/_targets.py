"""The hub addresses of an instance: parsing ``SNODRA_SERVER_ADDRESS``, the id of a hub, the share of the permits of
one stream, and the addresses of one DNS answer.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Several hubs", "Addresses").
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from snodra._errors import ConfigurationError

STATIC: Final = "static://"
DNS: Final = "dns:///"

_DNS_TARGET: Final = re.compile(r"dns:///([^/:]+|\[[^/\]]+\]):(\d+)")
_ACCEPTED_FORMS: Final = "host:port, static://host:port, or one dns:///name:port"


@dataclass(frozen=True)
class Target:
    """One entry of the hub addresses: a hub at ``host:port``, or a ``dns:///name:port`` whose every address (on its
    port) is a hub."""

    address: str
    """``host:port``: the gRPC target, and the hub's id when its ``RegisterAck.hub_id`` is empty."""
    name: str | None = None
    """The host name a ``dns:///`` target resolves; None for a static target."""
    port: int = 0
    """The port of a ``dns:///`` target; 0 for a static one."""

    @property
    def dns(self) -> bool:
        """True for a ``dns:///name:port`` target, resolved every ``Timings.dns_refresh``."""
        return self.name is not None


def share_of(maximum: int, n: int) -> int:
    """The permits one of n hubs offers: ``ceil(maximum / n)``, at least 1 (a hub always offers a permit)."""
    return max(1, -(-maximum // n))


def hub_id_of(ack_id: str, target: str) -> str:
    """The id of a hub: its ``RegisterAck.hub_id``, or, when that is empty (a real hub always names itself), its target
    as ``host:port`` without a ``static://`` prefix."""
    return ack_id or target.removeprefix(STATIC)


def parse_targets(addresses: Sequence[str]) -> list[Target]:
    """The targets of the hub addresses, in order, repeats dropped: a list of ``host:port`` (``static://`` optional),
    or exactly one ``dns:///name:port``. Raises :class:`ConfigurationError` for anything else."""
    entries = [a.strip() if isinstance(a, str) else a for a in addresses]
    if not entries or any(not isinstance(e, str) or not e for e in entries):
        raise ConfigurationError(
            f"the hub addresses (hubs, SNODRA_SERVER_ADDRESS) name no hub, or an empty one: {addresses!r}"
        )
    if any(e.startswith("dns:") for e in entries):
        if len(entries) > 1:
            raise ConfigurationError(
                f"the hub addresses (hubs, SNODRA_SERVER_ADDRESS) {addresses!r}: a dns:/// address must be the only one"
            )
        return [_dns_target(entries[0])]
    targets: list[Target] = []
    for e in entries:
        target = e.removeprefix(STATIC).strip()
        if not target or "://" in target:
            # TODO(spec-gap): SG-2
            raise ConfigurationError(
                f"hub address {e!r} (hubs, SNODRA_SERVER_ADDRESS) is not accepted: "
                f"the accepted forms are {_ACCEPTED_FORMS}"
            )
        if all(t.address != target for t in targets):
            targets.append(Target(target))
    return targets


def _dns_target(entry: str) -> Target:
    m = _DNS_TARGET.fullmatch(entry)
    if m is None or not 1 <= int(m.group(2)) <= 65535:
        raise ConfigurationError(
            f"hub address {entry!r} (hubs, SNODRA_SERVER_ADDRESS) is not dns:///name:port with a port from 1 to 65535"
        )
    host, port = m.group(1), int(m.group(2))
    return Target(f"{host}:{port}", name=host.strip("[]"), port=port)


def one_family(addresses: Sequence[str], port: int) -> list[str]:
    """The addresses of one DNS answer as gRPC targets on port: its IPv4 addresses when it has any, else its IPv6 ones
    (written ``[ip]:port``, without a zone), in the answer's order, without repeats. A dual-stack name lists every hub
    under both families, and two streams of one instance id to one hub would replace each other."""
    parsed = [ipaddress.ip_address(a.split("%", 1)[0]) for a in addresses]
    chosen: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = [a for a in parsed if a.version == 4]
    if not chosen:
        chosen = [a for a in parsed if a.version == 6]
    out: list[str] = []
    for a in chosen:
        target = f"{a}:{port}" if isinstance(a, ipaddress.IPv4Address) else f"[{a}]:{port}"
        if target not in out:
            out.append(target)
    return out
