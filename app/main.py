"""FastAPI control plane for the classroom multicast relay."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from .config import Settings, load_settings, parse_initial_table
from .migrations import MigrationManager
from .relay import RelayEngine
from .table import SubscriptionTable, validate_source
from .uplink import UplinkClient

log = logging.getLogger(__name__)


class MigrationRequest(BaseModel):
    table: dict[str, str] = Field(
        ...,
        description="新的整批教室订阅表（端口 -> 源 IP），每版最多两个源",
    )
    note: str | None = Field(
        default=None, description="值班员备注，便于审计"
    )
    requested_by: str | None = Field(
        default=None, description="操作人标识（可选）"
    )

    @field_validator("table")
    @classmethod
    def _check_table(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("table must not be empty")
        for port, source in value.items():
            int(port)
            validate_source(source)
        return value


class FaultRequest(BaseModel):
    refuse_changes: int = Field(
        default=0,
        ge=0,
        description="接下来多少次改表请求直接拒绝（409，不改实际过滤）",
    )
    drop_receipts: int = Field(
        default=0,
        ge=0,
        description="接下来多少次改表请求先实际生效、再断开连接不返回回执",
    )
    refuse_only_sources: list[str] | None = Field(
        default=None,
        description=(
            "仅当请求的精确放行源集合与这里一致时才拒绝（例如只拒绝收窄）"
        ),
    )
    drop_only_sources: list[str] | None = Field(
        default=None,
        description="仅当请求的精确放行源集合与这里一致时才丢回执",
    )
    reset: bool = Field(
        default=False, description="先清零所有故障计数和累计统计"
    )


class FilterRequest(BaseModel):
    mode: str = Field(..., pattern="^(all|allow)$")
    sources: list[str] = Field(default_factory=list)

    @field_validator("sources")
    @classmethod
    def _check_sources(cls, value: list[str]) -> list[str]:
        if len(value) > 2:
            raise ValueError("simulator accepts at most two explicit sources")
        return [validate_source(item) for item in value]


class SimulatorSeedRequest(BaseModel):
    mode: str = Field(default="all", pattern="^(all|allow)$")
    sources: list[str] = Field(default_factory=list)


class DemoResetRequest(BaseModel):
    table: dict[str, str] = Field(
        default_factory=lambda: {
            "16001": "127.0.0.11",
            "16002": "127.0.0.11",
            "16003": "127.0.0.11",
        }
    )
    uplink_mode: str = Field(default="all", pattern="^(all|allow)$")
    uplink_sources: list[str] = Field(default_factory=list)


class DemoState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.engine = RelayEngine(settings)
        self.uplink = UplinkClient(settings)
        self.migrations = MigrationManager(self.engine, self.uplink, settings)


def _build_table(state: DemoState, raw: dict[str, str]) -> SubscriptionTable:
    settings = state.settings
    mapping: dict[int, str] = {}
    for port_text, source in raw.items():
        try:
            port = int(port_text)
        except ValueError:
            raise HTTPException(
                status_code=422, detail=f"invalid classroom port {port_text!r}"
            ) from None
        mapping[port] = source
    try:
        return SubscriptionTable.build(
            mapping,
            port_min=settings.classroom_port_min,
            port_max=settings.classroom_port_max,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    state = DemoState(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        await state.uplink.start()
        if state.uplink.managed:
            try:
                await state.uplink.health()
            except (httpx.HTTPError, RuntimeError) as exc:
                if settings.uplink_required:
                    raise RuntimeError(
                        f"up-link simulator unreachable at {settings.uplink_url}: {exc}"
                    ) from exc
                log.warning(
                    "up-link simulator unreachable at %s (%s); "
                    "filter changes will be unverified",
                    settings.uplink_url,
                    exc,
                )
        initial = {}
        if settings.initial_table:
            try:
                initial = parse_initial_table(settings.initial_table)
            except ValueError as exc:
                raise RuntimeError(f"invalid RELAY_INITIAL_TABLE: {exc}") from exc
        if initial:
            try:
                table = SubscriptionTable.build(
                    initial,
                    port_min=settings.classroom_port_min,
                    port_max=settings.classroom_port_max,
                )
            except ValueError as exc:
                raise RuntimeError(f"invalid initial table: {exc}") from exc
            state.engine.set_table(table)
            state.migrations.record_event(
                "initial_table_loaded",
                f"{len(table)} classrooms, sources {', '.join(sorted(table.sources()))}",
            )
        await state.engine.start()
        try:
            yield
        finally:
            await state.engine.stop()
            await state.uplink.aclose()

    app = FastAPI(
        title="教室 IPTV 换订转发器",
        version="1.0.0",
        description=(
            "按源 IP 将上联 UDP 组播包转发到订阅该源的教室端口。\n\n"
            "换订流程：准备（同时放行旧表和新表的源）→ 统一切换教室 → "
            "上联收窄到新表。准备失败时保持旧表；收窄失败时按新表转发、"
            "报告上联实际放行范围、暂停新的换订直到重试成功。"
        ),
        lifespan=lifespan,
    )
    app.state.demo = state

    # ------------------------------------------------------------- read APIs
    @app.get("/healthz", summary="健康检查")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok" if state.engine.started else "starting",
            "relay_started": state.engine.started,
            "uplink_managed": state.uplink.managed,
            "uplink_url": state.uplink.url or None,
        }

    @app.get("/state", summary="当前订阅表、换订门禁、事件")
    async def get_state() -> dict[str, Any]:
        snapshot = state.migrations.snapshot()
        snapshot["relay_stats"] = state.engine.stats_view()
        snapshot["settings"] = {
            "uplink_group": settings.uplink_group,
            "uplink_port": settings.uplink_port,
            "multicast_group": settings.multicast_group,
            "classroom_port_min": settings.classroom_port_min,
            "classroom_port_max": settings.classroom_port_max,
        }
        return snapshot

    @app.get("/stats", summary="转发统计")
    async def get_stats() -> dict[str, Any]:
        view = state.engine.stats_view()
        view["revision"] = state.migrations.revision
        view["phase"] = state.migrations.phase
        return view

    @app.post("/stats/reset", summary="清零转发统计")
    async def reset_stats() -> dict[str, Any]:
        state.engine.reset_stats()
        state.migrations.record_event("stats_reset", "relay counters cleared")
        return {"status": "reset"}

    @app.get("/migrations", summary="换订历史")
    async def list_migrations(
        limit: int = Query(default=20, ge=1, le=50)
    ) -> dict[str, Any]:
        return {"migrations": state.migrations.history(limit)}

    @app.get("/migrations/{migration_id}", summary="单次换订详情")
    async def get_migration(migration_id: str) -> dict[str, Any]:
        record = state.migrations.get(migration_id)
        if record is None:
            raise HTTPException(status_code=404, detail="migration not found")
        return record

    # ---------------------------------------------------------- switch API
    @app.post("/migrations", summary="整批换订教室订阅表")
    async def create_migration(payload: MigrationRequest) -> dict[str, Any]:
        new_table = _build_table(state, payload.table)
        migration = await state.migrations.switch(
            new_table, note=payload.note, requested_by=payload.requested_by
        )
        view = migration.as_view()
        view["current_table"] = state.engine.table.as_dict()
        view["gate"] = state.migrations.snapshot()["gate"]
        return view

    @app.post(
        "/migrations/{migration_id}/retry-narrow",
        summary="重试失败的上联收窄",
    )
    async def retry_narrow(migration_id: str) -> dict[str, Any]:
        migration, outcome = await state.migrations.retry_narrow(migration_id)
        if outcome == "no_pending_narrow":
            raise HTTPException(
                status_code=409,
                detail="没有等待收窄的换订；上联门禁未关闭",
            )
        if outcome == "not_found":
            raise HTTPException(status_code=404, detail="migration not found")
        if outcome == "not_pending":
            raise HTTPException(
                status_code=409,
                detail="该换订不是当前等待收窄的换订",
            )
        assert migration is not None
        view = migration.as_view()
        view["retry_outcome"] = outcome
        view["gate"] = state.migrations.snapshot()["gate"]
        return view

    # -------------------------------------------------------------- demo APIs
    @app.post(
        "/demo/reset",
        summary="演示：重置教室表与上联模拟器状态",
        tags=["demo"],
    )
    async def demo_reset(payload: DemoResetRequest | None = None) -> dict[str, Any]:
        payload = payload or DemoResetRequest()
        table = _build_table(state, payload.table)
        state.migrations.reset_to(table, reason="demo")
        if state.uplink.managed:
            # Clear injected faults first so the reset filter change is never
            # refused or stripped of its receipt.
            _, faults = await _sim_request(
                state, "POST", "/sim/faults", {"reset": True},
                allowed_status={200},
            )
            _, filter_view = await _sim_request(
                state, "POST", "/sim/filter",
                {"mode": payload.uplink_mode,
                 "sources": payload.uplink_sources if payload.uplink_mode == "allow" else []},
                allowed_status={200, 409},
            )
            return {
                "status": "reset",
                "table": table.as_dict(),
                "revision": state.migrations.revision,
                "uplink_filter": filter_view,
                "simulator_faults": faults,
            }
        return {
            "status": "reset",
            "table": table.as_dict(),
            "revision": state.migrations.revision,
            "uplink_filter": None,
            "simulator_faults": None,
        }

    @app.get(
        "/demo/uplink/filter",
        summary="演示：查看模拟器实际过滤",
        tags=["demo"],
    )
    async def demo_get_filter() -> Any:
        _, body = await _sim_request(state, "GET", "/sim/filter", None, {200})
        return body

    @app.post(
        "/demo/uplink/filter",
        summary="演示：直接设置模拟器过滤（绕过换订流程）",
        tags=["demo"],
    )
    async def demo_set_filter(payload: FilterRequest) -> Any:
        _, body = await _sim_request(
            state,
            "POST",
            "/sim/filter",
            {"mode": payload.mode, "sources": payload.sources},
            {200, 409},
        )
        return body

    @app.post(
        "/demo/uplink/faults",
        summary="演示：注入拒绝改表 / 改成后丢回执故障",
        tags=["demo"],
    )
    async def demo_set_faults(payload: FaultRequest) -> Any:
        _, body = await _sim_request(
            state,
            "POST",
            "/sim/faults",
            payload.model_dump(),
            {200},
        )
        return body

    @app.get(
        "/demo/uplink/health",
        summary="演示：模拟器健康状态",
        tags=["demo"],
    )
    async def demo_sim_health() -> Any:
        _, body = await _sim_request(state, "GET", "/healthz", None, {200})
        return body

    return app


async def _sim_request(
    state: DemoState,
    method: str,
    path: str,
    body: dict | None,
    allowed_status: set[int],
) -> tuple[int, Any]:
    if not state.uplink.managed:
        raise HTTPException(
            status_code=503, detail="up-link simulator is not configured"
        )
    try:
        return await state.uplink.request(
            method, path, json_body=body, allowed_status=allowed_status
        )
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                f"up-link simulator returned HTTP {exc.response.status_code}: "
                f"{exc.response.text[:200]}"
            ),
        ) from exc
    except (httpx.HTTPError, RuntimeError) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"up-link simulator unreachable: {exc}",
        ) from exc


app = create_app()
