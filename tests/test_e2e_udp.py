"""End-to-end UDP tests with real sockets and real server processes.

Starts the up-link simulator and the relay as separate uvicorn processes,
subscribes classroom multicast receivers, and checks that:

* a classroom port only receives packets from the source it is subscribed to;
* the switch is seamless: old-source packets continue until new-source
  packets start arriving on the same classroom port;
* the lost-receipt fault is resolved by reading the actually applied filter.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
# The relay takes over emission of the *original* group: classrooms keep
# listening on the same group they used before, only the port mapping changes.
UPLINK_GROUP = "239.255.42.10"
CLASSROOM_GROUP = UPLINK_GROUP
UPLINK_PORT = 15104


def free_ports(count: int) -> list[int]:
    """Reserve ``count`` distinct ephemeral ports at the same time.

    Calling ``free_port()`` twice can return the same number because the first
    socket is already closed; keeping the probes open until all ports are
    chosen avoids the relay/simulator collision.
    """

    sockets = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM) for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


@dataclass
class ClassroomReceiver:
    port: int
    packets: list[tuple[float, dict]] = field(default_factory=list)
    _sock: socket.socket | None = None
    _thread: threading.Thread | None = None
    _stop: threading.Event = field(default_factory=threading.Event)

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.port))
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton(CLASSROOM_GROUP) + socket.inet_aton("127.0.0.1"),
        )
        sock.settimeout(0.05)
        self._sock = sock

        def run() -> None:
            assert sock is not None
            while not self._stop.is_set():
                try:
                    data, _ = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break
                try:
                    payload = json.loads(data)
                except ValueError:
                    payload = {"raw": data.decode(errors="replace")}
                self.packets.append((time.monotonic(), payload))

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            self._sock.close()
        if self._thread is not None:
            self._thread.join(timeout=1)

    # ------------------------------------------------------------- helpers
    def sources(self) -> list[str]:
        return [packet["src"] for _, packet in self.packets if "src" in packet]

    def count(self, source: str | None = None) -> int:
        if source is None:
            return len(self.packets)
        return sum(1 for _, packet in self.packets if packet.get("src") == source)

    def last_time(self, source: str) -> float | None:
        times = [
            at for at, packet in self.packets if packet.get("src") == source
        ]
        return times[-1] if times else None

    def first_time(self, source: str) -> float | None:
        for at, packet in self.packets:
            if packet.get("src") == source:
                return at
        return None


def wait_http(
    url: str,
    timeout: float = 15.0,
    *,
    process: subprocess.Popen | None = None,
    log_path: Path | None = None,
) -> dict:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=0.5)
            if response.status_code == 200:
                return response.json()
        except httpx.HTTPError as exc:  # not up yet
            last_error = exc
        if process is not None and process.poll() is not None:
            log_tail = ""
            if log_path is not None and log_path.exists():
                log_tail = log_path.read_text(errors="replace")[-3000:]
            raise RuntimeError(
                f"{url} service exited with code {process.returncode}; "
                f"log follows:\n{log_tail}"
            )
        time.sleep(0.1)
    raise RuntimeError(f"{url} did not become ready: {last_error}")


@contextmanager
def run_service(module: str, port: int, env: dict[str, str], log_path: Path):
    merged = os.environ.copy()
    merged.update(env)
    merged["PYTHONPATH"] = str(ROOT)
    log_file = log_path.open("wb")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            module,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=str(ROOT),
        env=merged,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    try:
        yield process
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        log_file.close()


def test_udp_forwarding_and_seamless_switch(tmp_path):
    sim_port, relay_port = free_ports(2)
    classroom_ports = [16041, 16042]
    receivers = [ClassroomReceiver(port) for port in classroom_ports]
    for receiver in receivers:
        receiver.start()
    try:
        sim_env = {
            "SIM_GROUP": UPLINK_GROUP,
            "SIM_PORT": str(UPLINK_PORT),
            "SIM_INTERFACE": "127.0.0.1",
            "SIM_SOURCES": "127.0.0.11,127.0.0.12,127.0.0.13",
            "SIM_INTERVAL": "0.05",
            "SIM_INITIAL_MODE": "allow",
            "SIM_INITIAL_SOURCES": "127.0.0.11",
        }
        with run_service(
            "simulator.main:app", sim_port, sim_env, tmp_path / "sim.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "sim.log",
            )
            relay_env = {
                "RELAY_UPLINK_GROUP": UPLINK_GROUP,
                "RELAY_UPLINK_PORT": str(UPLINK_PORT),
                "RELAY_UPLINK_INTERFACE": "127.0.0.1",
                "RELAY_MULTICAST_GROUP": CLASSROOM_GROUP,
                "RELAY_MULTICAST_INTERFACE": "127.0.0.1",
                "RELAY_CLASSROOM_PORT_MIN": "16040",
                "RELAY_CLASSROOM_PORT_MAX": "16049",
                "RELAY_UPLINK_URL": f"http://127.0.0.1:{sim_port}",
                "RELAY_UPLINK_REQUIRED": "true",
                "RELAY_INITIAL_TABLE": (
                    "16041=127.0.0.11;16042=127.0.0.11"
                ),
            }
            with run_service(
                "app.main:app", relay_port, relay_env, tmp_path / "relay.log"
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "relay.log",
                )
                # Let the old source flow and be forwarded.
                time.sleep(1.2)
                before_old = receivers[0].count("127.0.0.11")
                assert before_old > 3, receivers[0].packets
                # Nothing from the new source may reach classrooms yet.
                assert receivers[0].count("127.0.0.12") == 0
                assert receivers[1].count("127.0.0.12") == 0

                response = httpx.post(
                    f"{base}/migrations",
                    json={
                        "table": {
                            "16041": "127.0.0.12",
                            "16042": "127.0.0.12",
                        },
                        "note": "e2e seamless switch",
                    },
                    timeout=10,
                )
                assert response.status_code == 200, response.text
                body = response.json()
                assert body["state"] == "completed", body

                time.sleep(1.2)
                # Both classroom ports now receive only the new source.
                for receiver in receivers:
                    assert receiver.count("127.0.0.12") > 3, receiver.packets
                    # A stray packet from the old source may have been in
                    # flight during the swap, but the stream must stop.
                    old_times = [
                        at
                        for at, packet in receiver.packets
                        if packet.get("src") == "127.0.0.11"
                    ]
                    if old_times:
                        last_old = old_times[-1]
                        first_new = receiver.first_time("127.0.0.12")
                        assert first_new is not None
                        # Old stream continues until new stream begins: the
                        # hand-over gap stays within a few emission intervals.
                        assert first_new - last_old < 0.35, (
                            receiver.port,
                            first_new - last_old,
                        )

                # Up-link was narrowed to the new source only.
                sim_filter = httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/filter", timeout=5
                ).json()
                assert sim_filter["mode"] == "allow"
                assert sim_filter["sources"] == ["127.0.0.12"]

                # And the relay's own stats confirm per-source forwarding.
                stats = httpx.get(f"{base}/stats", timeout=5).json()
                assert stats["packets_in"] > 0
                assert stats["per_source"].get("127.0.0.12", 0) > 0
    finally:
        for receiver in receivers:
            receiver.stop()


def test_lost_receipt_still_switches_and_forwards(tmp_path):
    sim_port, relay_port = free_ports(2)
    receiver = ClassroomReceiver(16043)
    receiver.start()
    try:
        sim_env = {
            "SIM_GROUP": UPLINK_GROUP,
            "SIM_PORT": "15105",
            "SIM_INTERFACE": "127.0.0.1",
            "SIM_SOURCES": "127.0.0.11,127.0.0.12",
            "SIM_INTERVAL": "0.05",
            "SIM_INITIAL_MODE": "allow",
            "SIM_INITIAL_SOURCES": "127.0.0.11",
        }
        with run_service(
            "simulator.main:app", sim_port, sim_env, tmp_path / "sim2.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "sim2.log",
            )
            relay_env = {
                "RELAY_UPLINK_GROUP": UPLINK_GROUP,
                "RELAY_UPLINK_PORT": "15105",
                "RELAY_UPLINK_INTERFACE": "127.0.0.1",
                "RELAY_MULTICAST_GROUP": CLASSROOM_GROUP,
                "RELAY_MULTICAST_INTERFACE": "127.0.0.1",
                "RELAY_CLASSROOM_PORT_MIN": "16040",
                "RELAY_CLASSROOM_PORT_MAX": "16049",
                "RELAY_UPLINK_URL": f"http://127.0.0.1:{sim_port}",
                "RELAY_UPLINK_REQUIRED": "true",
                "RELAY_INITIAL_TABLE": "16043=127.0.0.11",
            }
            with run_service(
                "app.main:app", relay_port, relay_env, tmp_path / "relay2.log"
            ):
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(f"{base}/healthz")
                time.sleep(0.8)

                # Both the prepare and the narrow change will apply but lose
                # their receipts.
                faults = httpx.post(
                    f"http://127.0.0.1:{sim_port}/sim/faults",
                    json={"drop_receipts": 2},
                    timeout=5,
                ).json()
                assert faults["drop_receipts"] == 2

                response = httpx.post(
                    f"{base}/migrations",
                    json={"table": {"16043": "127.0.0.12"}},
                    timeout=15,
                )
                assert response.status_code == 200, response.text
                body = response.json()
                assert body["state"] == "completed", body
                attempts = {
                    step["name"]: step.get("uplink_attempt")
                    for step in body["steps"]
                }
                assert attempts["prepare"]["ack"] == "lost"
                assert attempts["narrow"]["ack"] == "lost"
                assert attempts["prepare"]["verified_by_actual"] is True
                assert attempts["narrow"]["satisfied"] is True

                time.sleep(1.0)
                assert receiver.count("127.0.0.12") > 3, receiver.packets
                sim_filter = httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/filter", timeout=5
                ).json()
                assert sim_filter["sources"] == ["127.0.0.12"]
    finally:
        receiver.stop()


def test_refused_narrow_blocks_switches_until_retry(tmp_path):
    sim_port, relay_port = free_ports(2)
    receiver = ClassroomReceiver(16044)
    receiver.start()
    try:
        sim_env = {
            "SIM_GROUP": UPLINK_GROUP,
            "SIM_PORT": "15106",
            "SIM_INTERFACE": "127.0.0.1",
            "SIM_SOURCES": "127.0.0.11,127.0.0.12",
            "SIM_INTERVAL": "0.05",
            "SIM_INITIAL_MODE": "allow",
            "SIM_INITIAL_SOURCES": "127.0.0.11",
        }
        with run_service(
            "simulator.main:app", sim_port, sim_env, tmp_path / "sim3.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "sim3.log",
            )
            relay_env = {
                "RELAY_UPLINK_GROUP": UPLINK_GROUP,
                "RELAY_UPLINK_PORT": "15106",
                "RELAY_UPLINK_INTERFACE": "127.0.0.1",
                "RELAY_MULTICAST_GROUP": CLASSROOM_GROUP,
                "RELAY_MULTICAST_INTERFACE": "127.0.0.1",
                "RELAY_CLASSROOM_PORT_MIN": "16040",
                "RELAY_CLASSROOM_PORT_MAX": "16049",
                "RELAY_UPLINK_URL": f"http://127.0.0.1:{sim_port}",
                "RELAY_UPLINK_REQUIRED": "true",
                "RELAY_INITIAL_TABLE": "16044=127.0.0.11",
            }
            with run_service(
                "app.main:app", relay_port, relay_env, tmp_path / "relay3.log"
            ):
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(f"{base}/healthz")
                time.sleep(0.8)

                # Allow prepare ({11,12}), refuse only the narrow ({12}).
                httpx.post(
                    f"http://127.0.0.1:{sim_port}/sim/faults",
                    json={
                        "refuse_changes": 1,
                        "refuse_only_sources": ["127.0.0.12"],
                    },
                    timeout=5,
                )
                first = httpx.post(
                    f"{base}/migrations",
                    json={"table": {"16044": "127.0.0.12"}},
                    timeout=10,
                ).json()
                assert first["state"] == "narrow_failed", first
                assert first["blocked"] is True
                assert first["uplink_filter"] == {
                    "mode": "allow",
                    "sources": ["127.0.0.11", "127.0.0.12"],
                }
                state = httpx.get(f"{base}/state", timeout=5).json()
                # Classrooms already follow the new table during the pending
                # narrow, and the up-link still admits both sources.
                assert state["table"] == {"16044": "127.0.0.12"}
                assert state["gate"]["blocked"] is True
                assert state["gate"]["pending_migration_id"] == first["id"]

                time.sleep(1.0)
                assert receiver.count("127.0.0.12") > 3, receiver.packets

                # New switch-overs are paused until narrowing succeeds.
                blocked = httpx.post(
                    f"{base}/migrations",
                    json={"table": {"16044": "127.0.0.11"}},
                    timeout=10,
                ).json()
                assert blocked["state"] == "blocked"
                assert blocked["reason"] == "blocked_by_pending_narrow"
                assert httpx.get(f"{base}/state", timeout=5).json()["table"] == {
                    "16044": "127.0.0.12"
                }

                # Retry the narrow; the simulator now accepts it.
                retry = httpx.post(
                    f"{base}/migrations/{first['id']}/retry-narrow", timeout=10
                )
                assert retry.status_code == 200, retry.text
                retry_body = retry.json()
                assert retry_body["state"] == "completed"
                assert retry_body["gate"]["blocked"] is False
                time.sleep(0.8)
                sim_filter = httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/filter", timeout=5
                ).json()
                assert sim_filter["sources"] == ["127.0.0.12"]

                # Gate is open again: a fresh switch proceeds normally.
                second = httpx.post(
                    f"{base}/migrations",
                    json={"table": {"16044": "127.0.0.11"}},
                    timeout=10,
                ).json()
                assert second["state"] == "completed", second
                time.sleep(1.0)
                assert receiver.count("127.0.0.11") > 3
    finally:
        receiver.stop()
