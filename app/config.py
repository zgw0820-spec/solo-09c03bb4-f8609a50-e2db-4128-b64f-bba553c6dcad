"""Runtime configuration (all overridable through environment variables)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from ipaddress import IPv4Address


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """Settings for the relay service.

    The relay receives the up-link multicast group and re-emits every accepted
    datagram to the classroom ports that are currently subscribed to the
    datagram's source IP.
    """

    # Address the up-link receiver binds to.
    listen_address: str = "0.0.0.0"
    # Group the up-link sends to (a single group, several source IPs).
    uplink_group: str = "239.255.42.10"
    uplink_port: int = 5004
    # Interface (local IP) used for IP_ADD_MEMBERSHIP on the up-link group.
    uplink_interface: str = "127.0.0.1"
    # Group that classroom receivers expect.  Defaults to the same group the
    # up-link uses: the relay takes over emission of the original group on the
    # classroom-facing interface, while the up-link socket stays bound to the
    # up-link port, so the relay does not hear its own output.
    multicast_group: str = "239.255.42.10"
    # Interface used for outgoing classroom multicast.
    multicast_interface: str = "127.0.0.1"
    multicast_ttl: int = 1
    # Classroom ports are re-emitted on ``multicast_group``.
    classroom_port_min: int = 16000
    classroom_port_max: int = 16999
    # Optional local allow-list for source IPs.  Empty means "accept what the
    # up-link delivers"; the up-link simulator itself enforces its filter by
    # only transmitting allowed source IPs.
    allowed_sources: tuple[str, ...] = ()
    # Up-link simulator base URL; empty means the filter is unmanaged.
    uplink_url: str = "http://127.0.0.1:9100"
    uplink_timeout: float = 2.0
    # Fail startup when the simulator cannot be reached (used by compose).
    uplink_required: bool = False
    # Initial (old) subscription table, "port=source;port=source".
    initial_table: str = ""
    # Receive buffer for the up-link socket.
    recv_buffer: int = 4 * 1024 * 1024
    # Number of events kept in the in-memory audit trail.
    event_log_size: int = 100


def load_settings() -> Settings:
    allowed = tuple(
        part.strip()
        for part in _env("RELAY_ALLOWED_SOURCES", "").split(",")
        if part.strip()
    )
    return Settings(
        listen_address=_env("RELAY_LISTEN_ADDRESS", Settings.listen_address),
        uplink_group=_env("RELAY_UPLINK_GROUP", Settings.uplink_group),
        uplink_port=_env_int("RELAY_UPLINK_PORT", Settings.uplink_port),
        uplink_interface=_env(
            "RELAY_UPLINK_INTERFACE", Settings.uplink_interface
        ),
        multicast_group=_env("RELAY_MULTICAST_GROUP", Settings.multicast_group),
        multicast_interface=_env(
            "RELAY_MULTICAST_INTERFACE", Settings.multicast_interface
        ),
        multicast_ttl=_env_int("RELAY_MULTICAST_TTL", Settings.multicast_ttl),
        classroom_port_min=_env_int(
            "RELAY_CLASSROOM_PORT_MIN", Settings.classroom_port_min
        ),
        classroom_port_max=_env_int(
            "RELAY_CLASSROOM_PORT_MAX", Settings.classroom_port_max
        ),
        allowed_sources=allowed,
        uplink_url=os.environ.get("RELAY_UPLINK_URL", Settings.uplink_url)
        or "",
        uplink_timeout=_env_float("RELAY_UPLINK_TIMEOUT", Settings.uplink_timeout),
        uplink_required=_env_bool(
            "RELAY_UPLINK_REQUIRED", Settings.uplink_required
        ),
        initial_table=_env("RELAY_INITIAL_TABLE", Settings.initial_table),
        recv_buffer=_env_int("RELAY_RECV_BUFFER", Settings.recv_buffer),
        event_log_size=_env_int("RELAY_EVENT_LOG_SIZE", Settings.event_log_size),
    )


def parse_initial_table(raw: str) -> dict[int, str]:
    """Parse ``port=source;port=source`` into ``{port: source}``."""
    result: dict[int, str] = {}
    for chunk in raw.replace("\n", ";").split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" not in chunk:
            raise ValueError(f"invalid initial table entry: {chunk!r}")
        port_text, source_text = chunk.split("=", 1)
        port = int(port_text.strip())
        source = str(IPv4Address(source_text.strip()))
        if port in result:
            raise ValueError(f"duplicate classroom port in initial table: {port}")
        result[port] = source
    return result
