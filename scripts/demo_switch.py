#!/usr/bin/env python3
"""Drive the Swagger demo: switch, prepare failure, narrow failure + retry.

Usage (relay running on http://127.0.0.1:8000, simulator on :9100):

    python3 -m scripts.demo_switch

The same calls can be made by hand in Swagger UI at ``/docs``; this script
just makes the sequence reproducible and prints the interesting fields.
"""

from __future__ import annotations

import json
import sys

import httpx

RELAY = "http://127.0.0.1:8000"
UPLINK = "http://127.0.0.1:9100"

OLD_TABLE = {"16001": "127.0.0.11", "16002": "127.0.0.11", "16003": "127.0.0.11"}
NEW_TABLE = {"16001": "127.0.0.12", "16002": "127.0.0.12", "16003": "127.0.0.12"}


def show(title: str, payload: dict, keys: list[str]) -> None:
    print(f"\n=== {title} ===")
    for key in keys:
        print(f"  {key}: {json.dumps(payload.get(key), ensure_ascii=False)}")


def steps(payload: dict) -> None:
    print("  steps:")
    for step in payload.get("steps", []):
        attempt = step.get("uplink_attempt") or {}
        suffix = ""
        if attempt:
            suffix = (
                f" [ack={attempt.get('ack')}, "
                f"satisfied={attempt.get('satisfied')}, "
                f"actual={json.dumps(attempt.get('actual'), ensure_ascii=False)}]"
            )
        print(f"    - {step['name']}: {step['status']}{suffix}")


def main() -> int:
    client = httpx.Client(timeout=20)
    try:
        client.get(f"{RELAY}/healthz").raise_for_status()
    except httpx.HTTPError as exc:
        print(f"relay not reachable at {RELAY}: {exc}")
        return 1

    print("reset demo state")
    reset = client.post(
        f"{RELAY}/demo/reset",
        json={"table": OLD_TABLE, "uplink_mode": "allow", "uplink_sources": ["127.0.0.11"]},
    )
    reset.raise_for_status()
    show("reset", reset.json(), ["status", "table", "uplink_filter"])

    # ------------------------------------------------------------------ 1
    print("\n--- 1) normal switch: prepare -> switch -> narrow ---")
    response = client.post(
        f"{RELAY}/migrations",
        json={"table": NEW_TABLE, "note": "demo: normal switch", "requested_by": "demo"},
    )
    response.raise_for_status()
    body = response.json()
    show("switch", body, ["state", "reason", "message", "uplink_filter", "gate"])
    steps(body)
    print("simulator filter:", client.get(f"{UPLINK}/sim/filter").json())

    # ------------------------------------------------------------------ 2
    print("\n--- 2) prepare failure keeps the old table ---")
    client.post(
        f"{RELAY}/demo/reset",
        json={"table": OLD_TABLE, "uplink_mode": "allow", "uplink_sources": ["127.0.0.11"]},
    ).raise_for_status()
    client.post(f"{UPLINK}/sim/faults", json={"refuse_changes": 1}).raise_for_status()
    response = client.post(
        f"{RELAY}/migrations",
        json={"table": NEW_TABLE, "note": "demo: refused prepare"},
    )
    body = response.json()
    show("prepare failure", body, ["state", "reason", "message"])
    steps(body)
    state = client.get(f"{RELAY}/state").json()
    print("  live table unchanged:", state["table"])
    print("  relay still forwarding? see classroom watcher output")

    # ------------------------------------------------------------------ 3
    print("\n--- 3) narrow failure: new table active, switches paused ---")
    client.post(
        f"{RELAY}/demo/reset",
        json={"table": OLD_TABLE, "uplink_mode": "allow", "uplink_sources": ["127.0.0.11"]},
    ).raise_for_status()
    client.post(
        f"{UPLINK}/sim/faults",
        json={"refuse_changes": 1, "refuse_only_sources": ["127.0.0.12"]},
    ).raise_for_status()
    response = client.post(
        f"{RELAY}/migrations",
        json={"table": NEW_TABLE, "note": "demo: refused narrow"},
    )
    body = response.json()
    show("narrow failure", body, ["id", "state", "reason", "message", "uplink_filter", "gate"])
    steps(body)

    blocked = client.post(
        f"{RELAY}/migrations", json={"table": OLD_TABLE, "note": "should be blocked"}
    ).json()
    show("switch while paused", blocked, ["state", "reason", "message"])

    print("\n--- 4) retry the narrow ---")
    retry = client.post(f"{RELAY}/migrations/{body['id']}/retry-narrow")
    retry.raise_for_status()
    retry_body = retry.json()
    show("retry", retry_body, ["state", "reason", "message", "uplink_filter", "gate"])
    steps(retry_body)
    print("simulator filter:", client.get(f"{UPLINK}/sim/filter").json())

    # ------------------------------------------------------------------ 5
    print("\n--- 5) lost receipt: change applied, no acknowledgment ---")
    client.post(
        f"{RELAY}/demo/reset",
        json={"table": OLD_TABLE, "uplink_mode": "allow", "uplink_sources": ["127.0.0.11"]},
    ).raise_for_status()
    client.post(f"{UPLINK}/sim/faults", json={"drop_receipts": 2}).raise_for_status()
    response = client.post(
        f"{RELAY}/migrations",
        json={"table": NEW_TABLE, "note": "demo: lost receipts"},
    )
    body = response.json()
    show("lost receipt", body, ["state", "reason", "message", "uplink_filter"])
    steps(body)
    print("  acknowledged changes were dropped, but the actual filter decided:")
    print("  simulator filter:", client.get(f"{UPLINK}/sim/filter").json())

    print("\nfinal state:")
    print(json.dumps(client.get(f"{RELAY}/state").json(), ensure_ascii=False, indent=2)[:2500])
    return 0


if __name__ == "__main__":
    sys.exit(main())
