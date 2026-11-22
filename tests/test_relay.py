"""Unit tests for the self-echo guard and packet forwarding decisions."""

from __future__ import annotations

from app.config import Settings
from app.relay import EchoGuard, RelayEngine
from app.table import SubscriptionTable


def make_engine(allowed=()) -> RelayEngine:
    settings = Settings(
        classroom_port_min=16000,
        classroom_port_max=16999,
        allowed_sources=allowed,
    )
    engine = RelayEngine(settings)
    engine.set_table(
        SubscriptionTable.build(
            {16001: "127.0.0.11", 16002: "127.0.0.12"},
            port_min=16000,
            port_max=16999,
        )
    )
    return engine


class RecordingTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, data, addr) -> None:
        self.sent.append((data, addr))


def test_forwards_only_to_ports_subscribed_to_the_source():
    engine = make_engine()
    transport = RecordingTransport()
    engine._send_transport = transport  # type: ignore[attr-defined]
    engine._handle_datagram(b"packet-from-11", "127.0.0.11")
    assert transport.sent == [(b"packet-from-11", ("239.255.42.10", 16001))]
    engine._handle_datagram(b"packet-from-12", "127.0.0.12")
    assert transport.sent[-1] == (b"packet-from-12", ("239.255.42.10", 16002))
    assert engine.stats_view()["packets_forwarded"] == 2


def test_unknown_source_is_not_forwarded_anywhere():
    engine = make_engine()
    transport = RecordingTransport()
    engine._send_transport = transport  # type: ignore[attr-defined]
    engine._handle_datagram(b"from-13", "127.0.0.13")
    assert transport.sent == []
    assert engine.stats_view()["no_subscribers"] == 1


def test_one_source_forwarded_to_all_its_classrooms():
    engine = make_engine()
    engine.set_table(
        SubscriptionTable.build(
            {16001: "127.0.0.11", 16002: "127.0.0.11", 16003: "127.0.0.11"},
            port_min=16000,
            port_max=16999,
        )
    )
    transport = RecordingTransport()
    engine._send_transport = transport  # type: ignore[attr-defined]
    engine._handle_datagram(b"pkt", "127.0.0.11")
    assert [addr[1] for _, addr in transport.sent] == [16001, 16002, 16003]


def test_allow_list_rejects_other_sources():
    engine = make_engine(allowed=("127.0.0.11",))
    transport = RecordingTransport()
    engine._send_transport = transport  # type: ignore[attr-defined]
    engine._handle_datagram(b"pkt", "127.0.0.12")
    assert transport.sent == []
    assert engine.stats_view()["source_not_allowed"] == 1


def test_echo_guard_detects_recently_forwarded_payload():
    guard = EchoGuard(window=0.5)
    guard.mark(b"abc", 10.0)
    assert guard.is_echo(b"abc", 10.1) is True
    assert guard.is_echo(b"abc", 10.6) is False
    # A different payload of the same length is not an echo.
    assert guard.is_echo(b"abd", 10.1) is False


def test_echo_guard_bounded_memory():
    guard = EchoGuard(max_entries=10, window=0.5)
    for index in range(50):
        guard.mark(str(index).encode(), 1.0 + index * 0.001)
    assert len(guard._seen) <= 10  # noqa: SLF001 - internal cap under test


def test_echo_guard_enabled_only_when_groups_and_ports_can_collide():
    colliding = Settings(
        uplink_group="239.1.1.1",
        multicast_group="239.1.1.1",
        classroom_port_min=16000,
        classroom_port_max=16999,
        uplink_port=16001,
    )
    assert RelayEngine(colliding)._loop_guard is not None  # noqa: SLF001

    distinct_group = Settings(
        uplink_group="239.1.1.1",
        multicast_group="239.1.1.2",
        classroom_port_min=16000,
        classroom_port_max=16999,
        uplink_port=16001,
    )
    assert RelayEngine(distinct_group)._loop_guard is None  # noqa: SLF001

    distinct_port = Settings(
        uplink_group="239.1.1.1",
        multicast_group="239.1.1.1",
        classroom_port_min=16000,
        classroom_port_max=16999,
        uplink_port=5004,
    )
    assert RelayEngine(distinct_port)._loop_guard is None  # noqa: SLF001
