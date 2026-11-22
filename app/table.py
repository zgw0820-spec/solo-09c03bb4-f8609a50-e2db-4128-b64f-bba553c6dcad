"""Immutable classroom subscription tables."""

from __future__ import annotations

from dataclasses import dataclass
from ipaddress import IPv4Address
from types import MappingProxyType
from typing import Mapping

MAX_SOURCES_PER_VERSION = 2


def validate_source(value: str) -> str:
    """Return ``value`` as a normalised IPv4 source address.

    Loopback addresses are deliberately allowed: the local demo uses
    ``127.0.0.0/8`` aliases to simulate several up-link source IPs.
    """

    try:
        address = IPv4Address(value)
    except ValueError as exc:  # ipaddress raises AddressValueError (ValueError)
        raise ValueError(f"invalid IPv4 source address: {value!r}") from exc
    if address.is_unspecified:
        raise ValueError("source address 0.0.0.0 is not a valid stream source")
    if address.is_multicast:
        raise ValueError(
            f"source address {address} is multicast; a stream source must be unicast"
        )
    if address == IPv4Address("255.255.255.255"):
        raise ValueError("broadcast address is not a valid stream source")
    return str(address)


@dataclass(frozen=True)
class SubscriptionTable:
    """A complete version of the classroom subscription table.

    ``entries`` maps a classroom UDP port to the source IP that feeds it.
    Instances are immutable so the relay thread can take a reference without
    locking while the control plane swaps in a new version.
    """

    entries: Mapping[int, str]
    by_source: Mapping[str, tuple[int, ...]]
    port_min: int
    port_max: int

    @classmethod
    def build(
        cls,
        mapping: Mapping[int, str],
        *,
        port_min: int,
        port_max: int,
        require_entries: bool = True,
    ) -> "SubscriptionTable":
        normalised: dict[int, str] = {}
        for raw_port, raw_source in mapping.items():
            port = int(raw_port)
            if port < 1 or port > 65535:
                raise ValueError(f"classroom port out of UDP range: {port}")
            if not (port_min <= port <= port_max):
                raise ValueError(
                    f"classroom port {port} is outside the configured range "
                    f"{port_min}-{port_max}"
                )
            if port in normalised:
                raise ValueError(f"duplicate classroom port: {port}")
            source = validate_source(str(raw_source))
            normalised[port] = source

        if require_entries and not normalised:
            raise ValueError("a subscription table must contain at least one classroom")

        sources = sorted(set(normalised.values()))
        if len(sources) > MAX_SOURCES_PER_VERSION:
            raise ValueError(
                "a subscription table may use at most "
                f"{MAX_SOURCES_PER_VERSION} distinct sources, got {len(sources)}: "
                f"{', '.join(sources)}"
            )

        by_source: dict[str, list[int]] = {source: [] for source in sources}
        for port in sorted(normalised):
            by_source[normalised[port]].append(port)

        return cls(
            entries=MappingProxyType(dict(sorted(normalised.items()))),
            by_source=MappingProxyType(
                {source: tuple(ports) for source, ports in by_source.items()}
            ),
            port_min=port_min,
            port_max=port_max,
        )

    def ports_for(self, source: str) -> tuple[int, ...]:
        return self.by_source.get(source, ())

    def sources(self) -> frozenset[str]:
        return frozenset(self.by_source)

    def as_dict(self) -> dict[str, str]:
        return {str(port): source for port, source in self.entries.items()}

    def ports_by_source(self) -> dict[str, list[int]]:
        return {source: list(ports) for source, ports in self.by_source.items()}

    def __len__(self) -> int:
        return len(self.entries)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SubscriptionTable):
            return NotImplemented
        return dict(self.entries) == dict(other.entries)

    def __hash__(self) -> int:
        return hash(tuple(self.entries.items()))
