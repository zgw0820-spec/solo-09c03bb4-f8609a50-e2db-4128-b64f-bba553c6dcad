"""Multicast receive/forward engine.

A single up-link socket joins the up-link group.  Every datagram is forwarded
to the classroom multicast group on each classroom port that is currently
subscribed to the datagram's *source IP*.  The subscription table is an
immutable object, swapped atomically, so the data plane never blocks on the
control plane.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import socket
import struct
import time
from dataclasses import dataclass
from ipaddress import IPv4Address
from typing import Any

from .config import Settings
from .table import SubscriptionTable

log = logging.getLogger(__name__)


def resolve_interface_ip(value: str) -> str:
    """Accept either an IPv4 literal or a Linux interface name."""

    if not value:
        return "0.0.0.0"
    try:
        return str(IPv4Address(value))
    except ValueError:
        pass
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            packed = fcntl.ioctl(
                probe.fileno(),
                0x8915,  # SIOCGIFADDR
                struct.pack("256s", value.encode()[:15]),
            )
            return socket.inet_ntoa(packed[20:24])
        finally:
            probe.close()
    except OSError as exc:
        raise ValueError(
            f"cannot resolve multicast interface {value!r} to an IPv4 address"
        ) from exc


@dataclass
class RelayStats:
    packets_in: int = 0
    bytes_in: int = 0
    packets_forwarded: int = 0
    copies_forwarded: int = 0
    source_not_allowed: int = 0
    no_subscribers: int = 0
    relay_echo_ignored: int = 0
    send_errors: int = 0
    last_source: str | None = None
    last_payload_bytes: int = 0

    def as_view(self) -> dict[str, Any]:
        return {
            "packets_in": self.packets_in,
            "bytes_in": self.bytes_in,
            "packets_forwarded": self.packets_forwarded,
            "copies_forwarded": self.copies_forwarded,
            "source_not_allowed": self.source_not_allowed,
            "no_subscribers": self.no_subscribers,
            "relay_echo_ignored": self.relay_echo_ignored,
            "send_errors": self.send_errors,
            "last_source": self.last_source,
            "last_payload_bytes": self.last_payload_bytes,
        }


class _UplinkProtocol(asyncio.DatagramProtocol):
    def __init__(self, engine: "RelayEngine") -> None:
        self._engine = engine

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self._engine._handle_datagram(data, addr[0])

    def error_received(self, exc: Exception) -> None:  # pragma: no cover
        log.warning("up-link socket error: %s", exc)


class EchoGuard:
    """Detects our own re-emitted datagrams coming back on the up-link socket.

    When the classroom group and port mapping coincide with the up-link
    group/port (a valid deployment when the same host both receives and
    emits), IP_MULTICAST_LOOP delivers our own copies back.  Forwarding those
    again would multiply traffic, so recently forwarded payloads are
    remembered for a short window and dropped on re-entry.
    """

    def __init__(self, max_entries: int = 4096, window: float = 0.5) -> None:
        self._seen: dict[bytes, float] = {}
        self._max = max_entries
        self._window = window
        self._last_prune = 0.0

    def mark(self, payload: bytes, now: float) -> None:
        if len(self._seen) >= self._max:
            self._prune(now)
            if len(self._seen) >= self._max:
                # Hard cap: forget the oldest half.
                for key in list(self._seen)[: self._max // 2]:
                    self._seen.pop(key, None)
        self._seen[payload] = now

    def is_echo(self, payload: bytes, now: float) -> bool:
        stamp = self._seen.get(payload)
        if stamp is None:
            return False
        if now - stamp <= self._window:
            return True
        self._seen.pop(payload, None)
        return False

    def _prune(self, now: float) -> None:
        if now - self._last_prune < self._window:
            return
        self._last_prune = now
        cutoff = now - self._window
        for key in [k for k, ts in self._seen.items() if ts < cutoff]:
            self._seen.pop(key, None)


class RelayEngine:
    """Owns the sockets and the live subscription table."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._table = SubscriptionTable.build(
            {}, port_min=settings.classroom_port_min,
            port_max=settings.classroom_port_max, require_entries=False,
        )
        self._stats = RelayStats()
        self._per_source: dict[str, int] = {}
        self._per_port: dict[str, int] = {}
        self._allowed_sources = frozenset(settings.allowed_sources)
        self._recv_transport: asyncio.DatagramTransport | None = None
        self._send_transport: asyncio.DatagramTransport | None = None
        self._started = False
        self._uplink_iface_ip: str | None = None
        self._egress_iface_ip: str | None = None
        self._loop_guard: EchoGuard | None = None
        # Only needed when our own classroom emission can land on the up-link
        # socket: same group and a classroom port equal to the up-link port.
        if (
            settings.multicast_group == settings.uplink_group
            and settings.classroom_port_min
            <= settings.uplink_port
            <= settings.classroom_port_max
        ):
            self._loop_guard = EchoGuard()
            log.info(
                "classroom output shares the up-link group/port; "
                "enabling self-echo suppression"
            )
        self.last_error: str | None = None

    # ------------------------------------------------------------------ API
    @property
    def started(self) -> bool:
        return self._started

    @property
    def table(self) -> SubscriptionTable:
        return self._table

    def set_table(self, table: SubscriptionTable) -> None:
        # Atomic swap: the receive callback reads this attribute once.
        self._table = table

    def reset_stats(self) -> None:
        self._stats = RelayStats()
        self._per_source.clear()
        self._per_port.clear()

    def stats_view(self) -> dict[str, Any]:
        view = self._stats.as_view()
        view["per_source"] = dict(sorted(self._per_source.items()))
        view["per_classroom_port"] = {
            key: value for key, value in sorted(self._per_port.items())
        }
        return view

    async def start(self) -> None:
        if self._started:
            return
        settings = self._settings
        loop = asyncio.get_running_loop()

        self._uplink_iface_ip = resolve_interface_ip(settings.uplink_interface)
        self._egress_iface_ip = resolve_interface_ip(
            settings.multicast_interface or settings.uplink_interface
        )

        recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            recv_sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_RCVBUF, settings.recv_buffer
            )
        except OSError:  # pragma: no cover - kernel may cap it
            pass
        recv_sock.bind((settings.listen_address, settings.uplink_port))
        membership = socket.inet_aton(settings.uplink_group) + socket.inet_aton(
            self._uplink_iface_ip if self._uplink_iface_ip != "0.0.0.0" else "0.0.0.0"
        )
        recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        recv_sock.setblocking(False)

        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        send_sock.setsockopt(
            socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, settings.multicast_ttl
        )
        send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        if self._egress_iface_ip and self._egress_iface_ip != "0.0.0.0":
            send_sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self._egress_iface_ip),
            )
        send_sock.bind(
            (
                self._egress_iface_ip
                if self._egress_iface_ip != "0.0.0.0"
                else "",
                0,
            )
        )
        send_sock.setblocking(False)

        try:
            self._recv_transport, _ = await loop.create_datagram_endpoint(
                lambda: _UplinkProtocol(self), sock=recv_sock
            )
            self._send_transport, _ = await loop.create_datagram_endpoint(
                asyncio.DatagramProtocol, sock=send_sock
            )
        except BaseException:
            recv_sock.close()
            send_sock.close()
            raise

        self._started = True
        log.info(
            "relay started: up-link %s:%s (iface %s) -> group %s ports %s-%s",
            settings.uplink_group,
            settings.uplink_port,
            self._uplink_iface_ip,
            settings.multicast_group,
            settings.classroom_port_min,
            settings.classroom_port_max,
        )

    async def stop(self) -> None:
        if self._recv_transport is not None:
            self._recv_transport.close()
            self._recv_transport = None
        if self._send_transport is not None:
            self._send_transport.close()
            self._send_transport = None
        self._started = False

    # -------------------------------------------------------------- dataplane
    def _handle_datagram(self, data: bytes, source: str) -> None:
        stats = self._stats
        stats.packets_in += 1
        stats.bytes_in += len(data)
        stats.last_source = source
        stats.last_payload_bytes = len(data)
        self._per_source.setdefault(source, 0)
        self._per_source[source] += 1

        if self._allowed_sources and source not in self._allowed_sources:
            stats.source_not_allowed += 1
            return

        table = self._table
        ports = table.ports_for(source)
        if not ports:
            stats.no_subscribers += 1
            return
        transport = self._send_transport
        if transport is None:  # pragma: no cover - only before start
            return

        guard = self._loop_guard
        now = 0.0
        if guard is not None:
            try:
                now = asyncio.get_running_loop().time()
            except RuntimeError:  # pragma: no cover - sync unit tests
                now = time.monotonic()
            if guard.is_echo(data, now):
                stats.relay_echo_ignored += 1
                return

        settings = self._settings
        forwarded = 0
        for port in ports:
            try:
                transport.sendto(data, (settings.multicast_group, port))
                forwarded += 1
                key = str(port)
                self._per_port[key] = self._per_port.get(key, 0) + 1
            except (OSError, RuntimeError) as exc:  # pragma: no cover
                stats.send_errors += 1
                self.last_error = f"send to {settings.multicast_group}:{port} failed: {exc}"
                log.warning(self.last_error)
        stats.packets_forwarded += 1
        stats.copies_forwarded += forwarded
        if guard is not None and forwarded:
            guard.mark(data, now)
