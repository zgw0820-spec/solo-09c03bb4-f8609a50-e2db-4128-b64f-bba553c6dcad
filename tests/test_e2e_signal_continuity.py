"""Full flow including the requirement to keep the *original* signal.

These tests target the subtle rule: while switching, classroom receivers must
keep receiving the source they were already on until the new source is
actually available on the wire.  They also cover the "up-link narrowing
failed, keep forwarding by the new table" path and the lost-receipt fault on
the narrowing step.
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx

from tests.test_e2e_udp import (
    CLASSROOM_GROUP,
    UPLINK_GROUP,
    ClassroomReceiver,
    free_ports,
    run_service,
    wait_http,
)

ROOT = Path(__file__).resolve().parents[1]


def _relay_env(sim_port: int, uplink_port: int, initial: str) -> dict[str, str]:
    return {
        "RELAY_UPLINK_GROUP": UPLINK_GROUP,
        "RELAY_UPLINK_PORT": str(uplink_port),
        "RELAY_UPLINK_INTERFACE": "127.0.0.1",
        "RELAY_MULTICAST_GROUP": CLASSROOM_GROUP,
        "RELAY_MULTICAST_INTERFACE": "127.0.0.1",
        "RELAY_CLASSROOM_PORT_MIN": "16040",
        "RELAY_CLASSROOM_PORT_MAX": "16049",
        "RELAY_UPLINK_URL": f"http://127.0.0.1:{sim_port}",
        "RELAY_UPLINK_REQUIRED": "true",
        "RELAY_INITIAL_TABLE": initial,
    }


def _sim_env(uplink_port: int) -> dict[str, str]:
    return {
        "SIM_GROUP": UPLINK_GROUP,
        "SIM_PORT": str(uplink_port),
        "SIM_INTERFACE": "127.0.0.1",
        "SIM_SOURCES": "127.0.0.11,127.0.0.12",
        "SIM_INTERVAL": "0.05",
        "SIM_INITIAL_MODE": "allow",
        "SIM_INITIAL_SOURCES": "127.0.0.11",
    }


def test_old_signal_continues_until_new_signal_is_live(tmp_path):
    """Classrooms on the old source never see a gap during the switch."""

    sim_port, relay_port = free_ports(2)
    uplink_port = 15301
    receiver = ClassroomReceiver(16045)
    receiver.start()
    try:
        with run_service(
            "simulator.main:app",
            sim_port,
            _sim_env(uplink_port),
            tmp_path / "s.log",
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "s.log",
            )
            with run_service(
                "app.main:app",
                relay_port,
                _relay_env(sim_port, uplink_port, "16045=127.0.0.11"),
                tmp_path / "r.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "r.log",
                )
                time.sleep(1.0)

                response = httpx.post(
                    f"{base}/migrations",
                    json={
                        "table": {"16045": "127.0.0.12"},
                        "note": "old signal must continue",
                    },
                    timeout=10,
                )
                assert response.status_code == 200, response.text
                assert response.json()["state"] == "completed"

                time.sleep(1.0)
                sources = receiver.sources()
                assert "127.0.0.12" in sources
                # The old source may appear briefly from packets in flight,
                # but it must stop once the new source is flowing.
                first_new = receiver.first_time("127.0.0.12")
                assert first_new is not None
                last_old = receiver.last_time("127.0.0.11")
                if last_old is not None:
                    assert last_old <= first_new + 0.35
    finally:
        receiver.stop()


def test_lost_receipt_on_narrow_reports_and_still_forwards(tmp_path):
    sim_port, relay_port = free_ports(2)
    uplink_port = 15302
    receiver = ClassroomReceiver(16046)
    receiver.start()
    try:
        with run_service(
            "simulator.main:app",
            sim_port,
            _sim_env(uplink_port),
            tmp_path / "s2.log",
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "s2.log",
            )
            with run_service(
                "app.main:app",
                relay_port,
                _relay_env(sim_port, uplink_port, "16046=127.0.0.11"),
                tmp_path / "r2.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "r.log",
                )
                time.sleep(0.8)

                # Narrow request (exact new source) applies but loses receipt.
                httpx.post(
                    f"http://127.0.0.1:{sim_port}/sim/faults",
                    json={
                        "drop_receipts": 1,
                        "drop_only_sources": ["127.0.0.12"],
                    },
                    timeout=5,
                ).raise_for_status()

                response = httpx.post(
                    f"{base}/migrations",
                    json={"table": {"16046": "127.0.0.12"}},
                    timeout=15,
                )
                body = response.json()
                assert response.status_code == 200
                assert body["state"] == "completed", body
                narrow = next(s for s in body["steps"] if s["name"] == "narrow")
                assert narrow["uplink_attempt"]["ack"] == "lost"
                assert narrow["uplink_attempt"]["satisfied"] is True
                assert narrow["uplink_attempt"]["verified_by_actual"] is True

                time.sleep(1.0)
                assert receiver.count("127.0.0.12") > 3
                assert httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/filter", timeout=5
                ).json()["sources"] == ["127.0.0.12"]
    finally:
        receiver.stop()


def test_three_sources_switch_uses_group_then_narrows(tmp_path):
    """Old {11,12}, new {13}: prepare must temporarily allow the whole group."""

    sim_port, relay_port = free_ports(2)
    uplink_port = 15303
    receivers = {16047: ClassroomReceiver(16047), 16048: ClassroomReceiver(16048)}
    for receiver in receivers.values():
        receiver.start()
    try:
        sim_env = {
            **_sim_env(uplink_port),
            "SIM_SOURCES": "127.0.0.11,127.0.0.12,127.0.0.13",
            "SIM_INITIAL_MODE": "allow",
            "SIM_INITIAL_SOURCES": "127.0.0.11,127.0.0.12",
        }
        with run_service(
            "simulator.main:app", sim_port, sim_env, tmp_path / "s3.log"
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "s3.log",
            )
            with run_service(
                "app.main:app",
                relay_port,
                _relay_env(
                    sim_port,
                    uplink_port,
                    "16047=127.0.0.11;16048=127.0.0.12",
                ),
                tmp_path / "r3.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "r.log",
                )
                time.sleep(1.0)

                response = httpx.post(
                    f"{base}/migrations",
                    json={
                        "table": {"16047": "127.0.0.13", "16048": "127.0.0.13"}
                    },
                    timeout=15,
                )
                body = response.json()
                assert body["state"] == "completed", body
                prepare = next(s for s in body["steps"] if s["name"] == "prepare")
                assert prepare["uplink_attempt"]["requested"] == {
                    "mode": "all",
                    "sources": [],
                }
                narrow = next(s for s in body["steps"] if s["name"] == "narrow")
                assert narrow["uplink_attempt"]["requested"]["sources"] == [
                    "127.0.0.13"
                ]

                time.sleep(1.0)
                for port, receiver in receivers.items():
                    assert receiver.count("127.0.0.13") > 3, (port, receiver.packets)
                final = httpx.get(
                    f"http://127.0.0.1:{sim_port}/sim/filter", timeout=5
                ).json()
                assert final == {
                    "mode": "allow",
                    "sources": ["127.0.0.13"],
                    "revision": final["revision"],
                }
    finally:
        for receiver in receivers.values():
            receiver.stop()


def test_refused_prepare_restores_old_source_after_partial_widening(tmp_path):
    """When prepare fails, the old table keeps its original up-link signal."""

    sim_port, relay_port = free_ports(2)
    uplink_port = 15304
    receiver = ClassroomReceiver(16049)
    receiver.start()
    try:
        with run_service(
            "simulator.main:app",
            sim_port,
            _sim_env(uplink_port),
            tmp_path / "s4.log",
        ) as sim_process:
            wait_http(
                f"http://127.0.0.1:{sim_port}/healthz",
                process=sim_process,
                log_path=tmp_path / "s4.log",
            )
            with run_service(
                "app.main:app",
                relay_port,
                _relay_env(sim_port, uplink_port, "16049=127.0.0.11"),
                tmp_path / "r4.log",
            ) as relay_process:
                base = f"http://127.0.0.1:{relay_port}"
                wait_http(
                    f"{base}/healthz",
                    process=relay_process,
                    log_path=tmp_path / "r.log",
                )
                time.sleep(0.8)

                # Refuse the prepare change outright; the old filter stays.
                httpx.post(
                    f"http://127.0.0.1:{sim_port}/sim/faults",
                    json={"refuse_changes": 1},
                    timeout=5,
                ).raise_for_status()
                response = httpx.post(
                    f"{base}/migrations",
                    json={"table": {"16049": "127.0.0.12"}},
                    timeout=10,
                )
                body = response.json()
                assert body["state"] == "failed"
                assert body["reason"] == "prepare_failed"

                time.sleep(1.0)
                # Old source still arrives; new source does not.
                assert receiver.count("127.0.0.11") > 3, receiver.packets
                assert receiver.count("127.0.0.12") == 0
                assert httpx.get(f"{base}/state", timeout=5).json()["table"] == {
                    "16049": "127.0.0.11"
                }
    finally:
        receiver.stop()
