"""Integration tests: a fake simulator (no network) + real uvicorn servers.

The unit-level tests call the relay through ASGI with the simulator mocked at
the HTTP layer.  The end-to-end tests in ``test_e2e_udp.py`` exercise real UDP.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from app.config import Settings
from app.main import create_app
from app.table import SubscriptionTable


class FakeSimulator:
    """Implements the subset of the simulator API the relay uses."""

    def __init__(self) -> None:
        self.mode = "all"
        self.sources: list[str] = []
        self.refusals = 0
        self.drops = 0
        self.calls: list[dict[str, Any]] = []

    # -- httpx MockTransport handler
    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/sim/filter":
            return httpx.Response(
                200,
                json={
                    "mode": self.mode,
                    "sources": list(self.sources),
                    "revision": len(self.calls),
                },
            )
        if request.method == "POST" and request.url.path == "/sim/filter":
            body = httpx.Response(200, content=request.content).json()
            self.calls.append(body)
            if self.refusals > 0:
                self.refusals -= 1
                return httpx.Response(
                    409, json={"detail": "injected refusal"}
                )
            self.mode = body["mode"]
            self.sources = sorted(set(body.get("sources", [])))
            if self.drops > 0:
                self.drops -= 1
                # Simulate a broken response: applied but no receipt.
                raise httpx.ReadError("peer closed connection", request=request)
            return httpx.Response(
                200, json={"mode": self.mode, "sources": self.sources}
            )
        if request.method == "GET" and request.url.path == "/healthz":
            return httpx.Response(200, json={"status": "ok"})
        if request.method == "POST" and request.url.path == "/sim/faults":
            body = httpx.Response(200, content=request.content).json()
            if body.get("reset"):
                self.refusals = 0
                self.drops = 0
            self.refusals = body.get("refuse_changes", self.refusals)
            self.drops = body.get("drop_receipts", self.drops)
            return httpx.Response(
                200,
                json={
                    "refuse_changes": self.refusals,
                    "drop_receipts": self.drops,
                },
            )
        return httpx.Response(404, json={"detail": "unknown"})

    def state(self) -> tuple[str, list[str]]:
        return self.mode, list(self.sources)


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        uplink_group="239.255.42.10",
        uplink_port=15004,
        uplink_interface="127.0.0.1",
        multicast_group="239.255.42.20",
        multicast_interface="127.0.0.1",
        classroom_port_min=16000,
        classroom_port_max=16999,
        uplink_url="http://simulator",
        uplink_timeout=1.0,
        allowed_sources=(),
    )


@pytest.fixture
async def api(settings):
    app = create_app(settings)
    # Build the state without starting real sockets: swap in a fake client.
    # The lifespan starts real UDP sockets, which is fine in this container,
    # but the up-link HTTP client is redirected to the mock transport.
    async with app.router.lifespan_context(app):
        state = app.state.demo
        fake = FakeSimulator()
        await state.uplink.aclose()
        state.uplink._client = httpx.AsyncClient(  # noqa: SLF001
            base_url="http://simulator",
            transport=httpx.MockTransport(fake.handler),
            timeout=1.0,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://relay"
        ) as client:
            client.fake = fake  # type: ignore[attr-defined]
            client.relay_state = state  # type: ignore[attr-defined]
            yield client


def make_table(mapping):
    return SubscriptionTable.build(
        mapping, port_min=16000, port_max=16999
    )


async def reset(client, mapping, mode="all", sources=None):
    response = await client.post(
        "/demo/reset",
        json={
            "table": {str(k): v for k, v in mapping.items()},
            "uplink_mode": mode,
            "uplink_sources": sources or [],
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_prepare_switch_narrow_success(api):
    await reset(api, {16001: "127.0.0.11"})
    # Up-link currently only allows 127.0.0.11
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]
    api.fake.calls.clear()

    response = await api.post(
        "/migrations",
        json={"table": {str(16001): "127.0.0.12", "16002": "127.0.0.12"}},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["state"] == "completed", body
    steps = {step["name"]: step for step in body["steps"]}
    assert steps["prepare"]["status"] == "ok"
    assert steps["switch"]["status"] == "ok"
    assert steps["narrow"]["status"] == "ok"
    # Prepare admitted both sources, narrow admitted only the new source.
    assert api.fake.calls[0] == {
        "mode": "allow",
        "sources": ["127.0.0.11", "127.0.0.12"],
    }
    assert api.fake.calls[-1] == {
        "mode": "allow",
        "sources": ["127.0.0.12"],
    }
    assert api.fake.state() == ("allow", ["127.0.0.12"])
    state = (await api.get("/state")).json()
    assert state["table"] == {"16001": "127.0.0.12", "16002": "127.0.0.12"}
    assert state["gate"]["blocked"] is False


async def test_prepare_failure_keeps_old_table(api):
    await reset(api, {16001: "127.0.0.11"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]
    api.fake.refusals = 1  # prepare itself is refused

    response = await api.post(
        "/migrations", json={"table": {"16001": "127.0.0.12"}}
    )
    body = response.json()
    assert body["state"] == "failed"
    assert body["reason"] == "prepare_failed"
    state = (await api.get("/state")).json()
    assert state["table"] == {"16001": "127.0.0.11"}
    assert state["table_sources"] == ["127.0.0.11"]
    assert state["gate"]["blocked"] is False


async def test_narrow_failure_keeps_new_table_and_reports_filter(api):
    await reset(api, {16001: "127.0.0.11"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]
    # Allow prepare, refuse narrow.
    api.fake.calls.clear()
    api.fake.refusals = 0

    # Configure a refusal that hits only the narrow call.
    original_handler = api.fake.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if (
            request.method == "POST"
            and request.url.path == "/sim/filter"
            and api.fake.mode == "allow"
            and sorted(api.fake.sources) == ["127.0.0.11", "127.0.0.12"]
        ):
            # This is the narrow request: refuse it.
            return httpx.Response(
                409, json={"detail": "injected refusal on narrow"}
            )
        return original_handler(request)

    state = api.relay_state
    await state.uplink.aclose()
    state.uplink._client = httpx.AsyncClient(  # noqa: SLF001
        base_url="http://simulator",
        transport=httpx.MockTransport(handler),
        timeout=1.0,
    )

    response = await api.post(
        "/migrations",
        json={"table": {"16001": "127.0.0.12", "16002": "127.0.0.12"}},
    )
    body = response.json()
    assert body["state"] == "narrow_failed", body
    assert body["blocked"] is True
    assert body["uplink_filter"] == {
        "mode": "allow",
        "sources": ["127.0.0.11", "127.0.0.12"],
    }
    state_view = (await api.get("/state")).json()
    assert state_view["table"] == {"16001": "127.0.0.12", "16002": "127.0.0.12"}
    assert state_view["gate"]["blocked"] is True
    assert state_view["gate"]["reason"] == "pending_uplink_narrow"

    # A new switch is paused while narrowing is pending.
    blocked = await api.post(
        "/migrations", json={"table": {"16001": "127.0.0.13"}}
    )
    assert blocked.json()["state"] == "blocked"

    # Retry now succeeds once the simulator accepts changes again.
    state.uplink._client = httpx.AsyncClient(  # noqa: SLF001
        base_url="http://simulator",
        transport=httpx.MockTransport(api.fake.handler),
        timeout=1.0,
    )
    retry = await api.post(f"/migrations/{body['id']}/retry-narrow")
    assert retry.status_code == 200, retry.text
    retry_body = retry.json()
    assert retry_body["state"] == "completed"
    assert retry_body["gate"]["blocked"] is False
    assert api.fake.state() == ("allow", ["127.0.0.12"])


async def test_dropped_receipt_judged_by_actual_filter(api):
    """A switch whose acknowledgment is lost must still succeed."""

    await reset(api, {16001: "127.0.0.11"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]
    api.fake.drops = 2  # prepare and narrow both "lose" their receipts

    response = await api.post(
        "/migrations", json={"table": {"16001": "127.0.0.12"}}
    )
    body = response.json()
    assert body["state"] == "completed", body
    prepare = next(s for s in body["steps"] if s["name"] == "prepare")
    narrow = next(s for s in body["steps"] if s["name"] == "narrow")
    assert prepare["status"] == "ok"
    assert narrow["status"] == "ok"
    assert prepare["uplink_attempt"]["ack"] == "lost"
    assert prepare["uplink_attempt"]["verified_by_actual"] is True
    assert narrow["uplink_attempt"]["ack"] == "lost"
    assert api.fake.state() == ("allow", ["127.0.0.12"])


async def test_dropped_receipt_on_refused_change_is_not_mistaken(api):
    """Lost receipt + unchanged actual filter means the change did not happen."""

    await reset(api, {16001: "127.0.0.11"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/sim/filter":
            return httpx.Response(
                200, json={"mode": api.fake.mode, "sources": api.fake.sources}
            )
        if request.method == "POST" and request.url.path == "/sim/filter":
            raise httpx.ReadError("reset by peer", request=request)
        return httpx.Response(200, json={"status": "ok"})

    state = api.relay_state
    await state.uplink.aclose()
    state.uplink._client = httpx.AsyncClient(  # noqa: SLF001
        base_url="http://simulator",
        transport=httpx.MockTransport(handler),
        timeout=1.0,
    )

    response = await api.post(
        "/migrations", json={"table": {"16001": "127.0.0.12"}}
    )
    body = response.json()
    assert body["state"] == "failed"
    assert body["reason"] == "prepare_failed"
    prepare = next(s for s in body["steps"] if s["name"] == "prepare")
    assert prepare["uplink_attempt"]["ack"] == "lost"
    assert prepare["uplink_attempt"]["satisfied"] is False
    state_view = (await api.get("/state")).json()
    assert state_view["table"] == {"16001": "127.0.0.11"}


async def test_switch_under_two_sources_prepare_uses_exact_pair(api):
    await reset(api, {16001: "127.0.0.11", 16002: "127.0.0.12"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]
    api.fake.calls.clear()

    response = await api.post(
        "/migrations",
        json={"table": {"16001": "127.0.0.12", "16002": "127.0.0.12"}},
    )
    assert response.json()["state"] == "completed"
    assert api.fake.calls[0] == {
        "mode": "allow",
        "sources": ["127.0.0.11", "127.0.0.12"],
    }


async def test_three_distinct_sources_across_versions_prepare_allows_group(api):
    """Old table {11,12}; new table {13} needs 3 sources -> whole group."""

    await reset(api, {16001: "127.0.0.11", 16002: "127.0.0.12"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11", "127.0.0.12"]
    api.fake.calls.clear()

    response = await api.post(
        "/migrations",
        json={"table": {"16001": "127.0.0.13", "16002": "127.0.0.13"}},
    )
    body = response.json()
    assert body["state"] == "completed", body
    assert api.fake.calls[0] == {"mode": "all", "sources": []}
    assert api.fake.calls[-1] == {"mode": "allow", "sources": ["127.0.0.13"]}


async def test_retry_without_pending_returns_409(api):
    await reset(api, {16001: "127.0.0.11"})
    response = await api.post("/migrations/deadbeef/retry-narrow")
    assert response.status_code == 409


async def test_rejects_three_sources_in_new_table(api):
    await reset(api, {16001: "127.0.0.11"})
    response = await api.post(
        "/migrations",
        json={
            "table": {
                "16001": "127.0.0.11",
                "16002": "127.0.0.12",
                "16003": "127.0.0.13",
            }
        },
    )
    assert response.status_code == 422


async def test_rejects_out_of_range_port(api):
    await reset(api, {16001: "127.0.0.11"})
    response = await api.post("/migrations", json={"table": {"80": "127.0.0.11"}})
    assert response.status_code == 422


async def test_failed_prepare_is_retryable_by_new_switch(api):
    await reset(api, {16001: "127.0.0.11"})
    api.fake.mode, api.fake.sources = "allow", ["127.0.0.11"]
    api.fake.refusals = 1
    first = (await api.post(
        "/migrations", json={"table": {"16001": "127.0.0.12"}}
    )).json()
    assert first["state"] == "failed"
    second = (await api.post(
        "/migrations", json={"table": {"16001": "127.0.0.12"}}
    )).json()
    assert second["state"] == "completed"
