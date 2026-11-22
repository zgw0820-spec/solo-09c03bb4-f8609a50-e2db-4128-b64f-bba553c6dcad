"""Classroom-side multicast watcher used by the Compose demo.

Joins the classroom group on the given ports and prints a rolling summary of
which source each classroom port is receiving, so the switch can be observed
without extra tooling.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import sys
import time
from collections import defaultdict


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def main() -> int:
    group = _env("CLASSROOM_GROUP", "239.255.42.20")
    interface = _env("CLASSROOM_INTERFACE", "127.0.0.1")
    ports = [
        int(item)
        for item in _env("CLASSROOM_PORTS", "16001,16002,16003").split(",")
        if item.strip()
    ]
    interval = float(_env("CLASSROOM_SUMMARY_INTERVAL", "2"))

    sockets: dict[int, socket.socket] = {}
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", port))
        mreq = struct.pack(
            "4s4s", socket.inet_aton(group), socket.inet_aton(interface)
        )
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.setblocking(False)
        sockets[port] = sock

    print(
        f"classroom watcher joined {group} on ports {ports} via {interface}",
        flush=True,
    )

    last_seen: dict[int, tuple[str, int]] = {}
    window_sources: dict[int, set[str]] = defaultdict(set)
    windows: dict[int, int] = defaultdict(int)
    elapsed = 0.0
    started = time.monotonic()
    last_report = started

    while True:
        got_any = False
        for port, sock in sockets.items():
            while True:
                try:
                    data, addr = sock.recvfrom(65535)
                except BlockingIOError:
                    break
                except OSError:
                    break
                got_any = True
                source = addr[0]
                try:
                    payload = json.loads(data)
                    source = payload.get("src", source)
                    seq = payload.get("seq")
                except ValueError:
                    seq = None
                last_seen[port] = (source, seq)
                window_sources[port].add(source)
                windows[port] += 1
        if not got_any:
            time.sleep(0.02)

        now = time.monotonic()
        if now - last_report >= interval:
            parts = []
            for port in ports:
                source, seq = last_seen.get(port, ("-", None))
                sources = sorted(window_sources[port])
                parts.append(
                    f"{port}: src={source} seq={seq} last_{interval:g}s="
                    f"{windows[port]} srcs={','.join(sources) or '-'}"
                )
                windows[port] = 0
                window_sources[port] = set()
            elapsed = round(now - started, 1)
            print(f"[t+{elapsed:>6}s] " + " | ".join(parts), flush=True)
            last_report = now


if __name__ == "__main__":
    sys.exit(main())
