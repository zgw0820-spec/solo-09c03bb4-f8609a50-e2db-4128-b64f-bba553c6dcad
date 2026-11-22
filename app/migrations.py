"""Switch-over protocol for replacing a classroom subscription table.

The protocol is deliberately staged so that classrooms never lose signal:

1. **prepare** – widen the up-link filter until it admits both the old and the
   new table's sources.  Nothing visible to classrooms changes yet.  If this
   fails, the old table keeps running untouched ("准备失败就照旧发包").
2. **switch** – atomically swap the classroom table.  Because both source sets
   are already admitted, the new subscriptions work from the first packet.
3. **narrow** – shrink the up-link filter to exactly the new table's sources.
   If this fails, the new table stays active, the actual up-link range is
   reported, retrying is allowed, and further switch-overs are paused until a
   retry succeeds.

A lost acknowledgment is never trusted on its own: every filter change is
verified against the simulator's actually applied filter.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .relay import RelayEngine
from .table import SubscriptionTable
from .uplink import FilterAttempt, FilterState, UplinkClient

log = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class StepRecord:
    name: str
    status: str
    detail: str
    started_at: str
    finished_at: str
    attempt: FilterAttempt | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def as_view(self) -> dict[str, Any]:
        view: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }
        if self.attempt is not None:
            view["uplink_attempt"] = self.attempt.as_view()
        if self.extra:
            view.update(self.extra)
        return view


@dataclass
class Migration:
    id: str
    requested_at: str
    note: str | None
    requested_by: str | None
    table_before: dict[str, str]
    table_after: dict[str, str]
    sources_before: list[str]
    sources_after: list[str]
    revision_before: int
    revision_after: int
    state: str = "running"
    reason: str = "running"
    message: str = ""
    finished_at: str | None = None
    uplink_filter: FilterState | None = None
    steps: list[StepRecord] = field(default_factory=list)
    blocked: bool = False

    def as_view(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state,
            "reason": self.reason,
            "message": self.message,
            "blocked": self.blocked,
            "requested_at": self.requested_at,
            "finished_at": self.finished_at,
            "note": self.note,
            "requested_by": self.requested_by,
            "table_before": self.table_before,
            "table_after": self.table_after,
            "sources_before": self.sources_before,
            "sources_after": self.sources_after,
            "revision_before": self.revision_before,
            "revision_after": self.revision_after,
            "uplink_filter": (
                self.uplink_filter.as_view() if self.uplink_filter else None
            ),
            "uplink_filter_description": (
                self.uplink_filter.describe() if self.uplink_filter else None
            ),
            "steps": [step.as_view() for step in self.steps],
        }


class MigrationManager:
    """Serialises switch-overs and owns the pending-narrow gate."""

    def __init__(
        self, engine: RelayEngine, uplink: UplinkClient, settings: Settings
    ) -> None:
        self._engine = engine
        self._uplink = uplink
        self._settings = settings
        self._lock = asyncio.Lock()
        self._revision = 0
        self._history: deque[Migration] = deque(maxlen=50)
        self._pending_narrow: Migration | None = None
        self._events: deque[dict[str, Any]] = deque(
            maxlen=max(10, settings.event_log_size)
        )
        self._last_migration: Migration | None = None
        self._phase = "idle"

    # ------------------------------------------------------------- read-only
    @property
    def revision(self) -> int:
        return self._revision

    @property
    def pending_narrow(self) -> Migration | None:
        return self._pending_narrow

    @property
    def phase(self) -> str:
        return self._phase

    def reset_to(
        self, table: SubscriptionTable, *, reason: str = "reset"
    ) -> None:
        """Force the live table (demo reset only; never used by switching)."""

        self._engine.set_table(table)
        self._revision += 1
        self._pending_narrow = None
        self._phase = "idle"
        self.record_event(
            "table_reset",
            f"revision {self._revision}: {len(table)} classrooms, sources "
            f"{', '.join(sorted(table.sources()))} ({reason})",
        )

    def snapshot(self) -> dict[str, Any]:
        table = self._engine.table
        return {
            "revision": self._revision,
            "phase": self._phase,
            "relay_started": self._engine.started,
            "table": table.as_dict(),
            "ports_by_source": table.ports_by_source(),
            "table_sources": sorted(table.sources()),
            "gate": {
                "blocked": self._pending_narrow is not None,
                "reason": (
                    "pending_uplink_narrow" if self._pending_narrow else None
                ),
                "pending_migration_id": (
                    self._pending_narrow.id if self._pending_narrow else None
                ),
                "pending_since": (
                    self._pending_narrow.finished_at
                    if self._pending_narrow
                    else None
                ),
            },
            "last_migration_id": (
                self._last_migration.id if self._last_migration else None
            ),
            "events": list(self._events),
        }

    def history(self, limit: int = 20) -> list[dict[str, Any]]:
        return [item.as_view() for item in list(self._history)[-limit:]]

    def get(self, migration_id: str) -> dict[str, Any] | None:
        for item in reversed(self._history):
            if item.id == migration_id:
                return item.as_view()
        return None

    def record_event(self, event: str, detail: str = "", **extra: Any) -> None:
        entry = {"at": _now(), "event": event, "detail": detail}
        entry.update(extra)
        self._events.append(entry)

    # ---------------------------------------------------------------- switch
    async def switch(
        self,
        new_table: SubscriptionTable,
        *,
        note: str | None = None,
        requested_by: str | None = None,
    ) -> Migration:
        """Run prepare -> switch -> narrow under the serialisation lock."""

        async with self._lock:
            return await self._switch_locked(
                new_table, note=note, requested_by=requested_by
            )

    async def _switch_locked(
        self,
        new_table: SubscriptionTable,
        *,
        note: str | None,
        requested_by: str | None,
    ) -> Migration:
        old_table = self._engine.table
        migration = Migration(
            id=uuid.uuid4().hex[:12],
            requested_at=_now(),
            note=note,
            requested_by=requested_by,
            table_before=old_table.as_dict(),
            table_after=new_table.as_dict(),
            sources_before=sorted(old_table.sources()),
            sources_after=sorted(new_table.sources()),
            revision_before=self._revision,
            revision_after=self._revision,
        )
        self._last_migration = migration
        self._history.append(migration)
        self.record_event(
            "switch_requested",
            f"revision {self._revision} -> new table with sources "
            f"{', '.join(migration.sources_after)}",
            migration_id=migration.id,
        )

        # Pause switch-overs while a previous narrowing has not succeeded.
        if self._pending_narrow is not None:
            self._finish(
                migration,
                state="blocked",
                reason="blocked_by_pending_narrow",
                message=(
                    "上一次换订的上联收窄尚未成功，已暂停换订。请先重试收窄："
                    f"POST /migrations/{self._pending_narrow.id}/retry-narrow"
                ),
                blocked=True,
            )
            migration.uplink_filter = self._pending_narrow.uplink_filter
            self.record_event(
                "switch_blocked",
                f"pending narrow {self._pending_narrow.id}",
                migration_id=migration.id,
            )
            return migration

        if new_table == old_table:
            self._add_step(
                migration,
                "prepare",
                "skipped",
                "新表与当前表完全相同，无需换订",
            )
            self._add_step(
                migration, "switch", "skipped", "订阅表未变化"
            )
            self._add_step(
                migration, "narrow", "skipped", "上联放行范围未变化"
            )
            migration.revision_after = self._revision
            migration.uplink_filter = await self._uplink.fetch_actual()
            self._finish(
                migration,
                state="noop",
                reason="noop",
                message="新表与当前表相同，未做任何改动",
            )
            return migration

        # ---------------------------------------------------------- 1 prepare
        self._phase = "preparing"
        required = sorted(old_table.sources() | new_table.sources())
        prepare_ok = await self._prepare(migration, required)
        if not prepare_ok:
            # The old table must keep receiving its original signal.  If the
            # failed prepare left the up-link no longer admitting the old
            # sources, try to restore them before giving up.
            await self._restore_sources(migration, old_table, step="prepare_restore")
            self._phase = "idle"
            migration.uplink_filter = await self._uplink.fetch_actual()
            self._finish(
                migration,
                state="failed",
                reason="prepare_failed",
                message=(
                    "上联准备失败：旧表与新表的源未能同时放行，"
                    "教室订阅表未改动，仍按旧表转发。"
                    f"上联当前实际放行范围：{self._describe_filter(migration.uplink_filter)}。"
                ),
            )
            return migration

        # Re-check immediately before the swap: the switch is only allowed
        # when the actually applied filter covers both source sets.
        actual = await self._uplink.fetch_actual()
        if actual is not None and not actual.covers(required):
            migration.uplink_filter = actual
            self._add_step(
                migration,
                "prepare_recheck",
                "failed",
                "切换前复核发现上联放行范围不再覆盖旧表与新表的源",
                extra={"actual": actual.as_view()},
            )
            await self._restore_sources(
                migration, old_table, step="prepare_recheck_restore"
            )
            self._phase = "idle"
            migration.uplink_filter = await self._uplink.fetch_actual()
            self._finish(
                migration,
                state="failed",
                reason="prepare_failed",
                message=(
                    "切换前复核失败：上联实际放行范围不覆盖新旧源，"
                    "教室订阅表未改动，仍按旧表转发。"
                    f"上联当前实际放行范围：{self._describe_filter(migration.uplink_filter)}。"
                ),
            )
            return migration
        migration.uplink_filter = actual

        # ---------------------------------------------------------- 2 switch
        self._phase = "switching"
        started = _now()
        self._engine.set_table(new_table)
        self._revision += 1
        migration.revision_after = self._revision
        self._add_step(
            migration,
            "switch",
            "ok",
            f"已统一切换教室订阅表：{len(old_table)} 个教室 -> "
            f"{len(new_table)} 个教室，revision {self._revision}",
            started=started,
            extra={
                "classrooms_before": len(old_table),
                "classrooms_after": len(new_table),
                "revision": self._revision,
            },
        )
        self.record_event(
            "classrooms_switched",
            f"revision {self._revision}, sources "
            f"{', '.join(migration.sources_after)}",
            migration_id=migration.id,
        )

        # ---------------------------------------------------------- 3 narrow
        self._phase = "narrowing"
        narrow_ok = await self._narrow(migration, new_table)
        self._phase = "idle"
        migration.uplink_filter = await self._uplink.fetch_actual()

        if narrow_ok and (
            migration.uplink_filter is None
            or migration.uplink_filter.exactly(new_table.sources())
        ):
            self._finish(
                migration,
                state="completed",
                reason="completed",
                message=(
                    "换订完成，上联已收窄到新表使用的源："
                    f"{', '.join(migration.sources_after)}"
                    if self._uplink.managed
                    else "换订完成（上联未托管，未执行收窄）"
                ),
            )
            return migration

        if narrow_ok and migration.uplink_filter is not None:
            # The narrow call reported success but the actual state does not
            # match.  Treat it as unverified rather than trusting the receipt.
            self._add_step(
                migration,
                "narrow_verify",
                "failed",
                "收窄回执成功但复核上联实际放行范围与目标不一致",
                extra={"actual": migration.uplink_filter.as_view()},
            )

        # Narrowing failed: the new table stays active, report the actual
        # up-link range, keep the gate closed until a retry succeeds.
        self._pending_narrow = migration
        self._finish(
            migration,
            state="narrow_failed",
            reason=(
                "narrow_failed"
                if not narrow_ok
                else "narrow_unverified"
            ),
            message=(
                "教室已按新表转发，但上联收窄失败。上联当前实际放行范围："
                f"{self._describe_filter(migration.uplink_filter)}。"
                f"可重试收窄：POST /migrations/{migration.id}/retry-narrow；"
                "成功前暂停新的换订。"
            ),
            blocked=True,
        )
        self.record_event(
            "narrow_failed",
            f"actual filter: {self._describe_filter(migration.uplink_filter)}",
            migration_id=migration.id,
        )
        return migration

    # ----------------------------------------------------------- retry narrow
    async def retry_narrow(
        self, migration_id: str | None = None
    ) -> tuple[Migration | None, str]:
        async with self._lock:
            pending = self._pending_narrow
            if pending is None:
                return None, "no_pending_narrow"
            if migration_id is not None and migration_id != pending.id:
                target = next(
                    (
                        item
                        for item in self._history
                        if item.id == migration_id
                    ),
                    None,
                )
                if target is None:
                    return None, "not_found"
                return target, "not_pending"
            if pending.state == "completed":
                return pending, "already_completed"

            self._phase = "narrowing"
            self.record_event(
                "narrow_retry_requested", "", migration_id=pending.id
            )
            table = self._engine.table
            ok = await self._narrow(pending, table, retry=True)
            self._phase = "idle"
            pending.uplink_filter = await self._uplink.fetch_actual()

            if ok and (
                pending.uplink_filter is None
                or pending.uplink_filter.exactly(sorted(table.sources()))
            ):
                self._pending_narrow = None
                pending.blocked = False
                pending.finished_at = _now()
                pending.state = "completed"
                pending.reason = "completed_after_retry"
                pending.message = (
                    "重试收窄成功，上联已收窄到新表使用的源："
                    f"{', '.join(sorted(table.sources()))}；换订门禁已恢复"
                )
                self.record_event(
                    "narrow_retry_succeeded",
                    ", ".join(sorted(table.sources())),
                    migration_id=pending.id,
                )
            else:
                pending.message = (
                    "重试收窄仍失败。上联当前实际放行范围："
                    f"{self._describe_filter(pending.uplink_filter)}。"
                    "继续按新表转发；换订仍暂停。"
                )
                pending.blocked = True
                pending.state = "narrow_failed"
                pending.reason = (
                    "narrow_retry_failed" if not ok else "narrow_retry_unverified"
                )
                pending.finished_at = _now()
                self.record_event(
                    "narrow_retry_failed",
                    self._describe_filter(pending.uplink_filter),
                    migration_id=pending.id,
                )
            return pending, "retried"

    # --------------------------------------------------------------- helpers
    async def _restore_sources(
        self,
        migration: Migration,
        table: SubscriptionTable,
        *,
        step: str,
    ) -> None:
        """Best-effort restore of the up-link admission the active table needs."""

        wanted = sorted(table.sources())
        if not self._uplink.managed or not wanted:
            return
        actual = await self._uplink.fetch_actual()
        if actual is None or actual.covers(wanted):
            return
        attempt = await self._uplink.apply_filter(
            label=step,
            mode="allow",
            sources=wanted,
            predicate=lambda state: state.covers(wanted),
        )
        self._add_step(
            migration,
            step,
            "ok" if attempt.satisfied else "failed",
            (
                "准备失败后已尝试恢复上联对旧表源的放行："
                f"{attempt.detail}"
                if attempt.satisfied
                else "准备失败后无法恢复上联对旧表源的放行："
                f"{attempt.detail}；旧表教室可能收不到信号"
            ),
            attempt=attempt,
            extra={"active_table_sources": wanted},
        )

    async def _prepare(self, migration: Migration, required: list[str]) -> bool:
        started = _now()
        if not self._uplink.managed:
            self._add_step(
                migration,
                "prepare",
                "skipped",
                "未配置上联模拟器，跳过上联放行范围调整",
                started=started,
            )
            return True

        actual = await self._uplink.fetch_actual()
        if actual is not None and actual.covers(required):
            self._add_step(
                migration,
                "prepare",
                "ok",
                "上联实际放行范围已覆盖旧表与新表的源，无需调整",
                started=started,
                attempt=FilterAttempt(
                    label="prepare",
                    requested_mode=actual.mode,
                    requested_sources=actual.sources,
                    ack="already_satisfied",
                    http_status=None,
                    detail=f"actual filter: {actual.describe()}",
                    actual=actual,
                    satisfied=True,
                    verified_by_actual=True,
                ),
            )
            return True

        # The simulator can allow either the whole group or at most two
        # explicit sources.  When the union needs more than two, allow the
        # whole group for the duration of the switch.
        if len(required) <= 2:
            mode, sources = "allow", required
            plan = f"精确放行 {', '.join(required)}"
        else:
            mode, sources = "all", []
            plan = (
                "旧表与新表合计 "
                f"{len(required)} 个源，超过模拟器精确放行上限 2，"
                "准备阶段整组放行"
            )

        attempt = await self._uplink.apply_filter(
            label="prepare",
            mode=mode,
            sources=sources,
            predicate=lambda state: state.covers(required),
        )
        self._add_step(
            migration,
            "prepare",
            "ok" if attempt.satisfied else "failed",
            f"{plan}；结果：{attempt.detail}",
            started=started,
            attempt=attempt,
            extra={"required_sources": required},
        )
        return attempt.satisfied

    async def _narrow(
        self,
        migration: Migration,
        table: SubscriptionTable,
        *,
        retry: bool = False,
    ) -> bool:
        started = _now()
        wanted = sorted(table.sources())
        step_name = "narrow_retry" if retry else "narrow"

        if not self._uplink.managed:
            self._add_step(
                migration,
                step_name,
                "skipped",
                "未配置上联模拟器，跳过上联收窄",
                started=started,
            )
            return True
        if not wanted:
            self._add_step(
                migration,
                step_name,
                "failed",
                "新表没有任何源，无法收窄上联",
                started=started,
            )
            return False

        actual = await self._uplink.fetch_actual()
        if actual is not None and actual.exactly(wanted):
            self._add_step(
                migration,
                step_name,
                "ok",
                f"上联实际放行范围已等于新表使用的源：{', '.join(wanted)}",
                started=started,
                attempt=FilterAttempt(
                    label=step_name,
                    requested_mode="allow",
                    requested_sources=tuple(wanted),
                    ack="already_satisfied",
                    http_status=None,
                    detail=f"actual filter: {actual.describe()}",
                    actual=actual,
                    satisfied=True,
                    verified_by_actual=True,
                ),
            )
            return True

        attempt = await self._uplink.apply_filter(
            label=step_name,
            mode="allow",
            sources=wanted,
            predicate=lambda state: state.exactly(wanted),
        )
        self._add_step(
            migration,
            step_name,
            "ok" if attempt.satisfied else "failed",
            (
                f"收窄到 {', '.join(wanted)}；结果：{attempt.detail}"
                if attempt.satisfied
                else f"收窄到 {', '.join(wanted)} 失败：{attempt.detail}"
            ),
            started=started,
            attempt=attempt,
            extra={"wanted_sources": wanted},
        )
        if attempt.satisfied:
            return True

        # Narrowing must never leave a new-table source unadmitted.  If the
        # actual state stopped covering the new sources (e.g. a partial
        # change), widen back to the new sources so classrooms keep signal.
        current = attempt.actual
        if current is not None and not current.covers(wanted):
            self.record_event(
                "narrow_safety_rewiden",
                f"actual {current.describe()} does not cover {', '.join(wanted)}",
                migration_id=migration.id,
            )
            protect = await self._uplink.apply_filter(
                label=f"{step_name}_protect",
                mode="allow",
                sources=wanted,
                predicate=lambda state: state.covers(wanted),
            )
            self._add_step(
                migration,
                f"{step_name}_protect",
                "ok" if protect.satisfied else "failed",
                (
                    "收窄后的实际放行范围未覆盖新表全部源，已尝试恢复放行："
                    f"{protect.detail}"
                ),
                started=_now(),
                attempt=protect,
            )
            if protect.actual is not None:
                migration.uplink_filter = protect.actual
        return False

    def _add_step(
        self,
        migration: Migration,
        name: str,
        status: str,
        detail: str,
        *,
        started: str | None = None,
        attempt: FilterAttempt | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        migration.steps.append(
            StepRecord(
                name=name,
                status=status,
                detail=detail,
                started_at=started or _now(),
                finished_at=_now(),
                attempt=attempt,
                extra=extra or {},
            )
        )

    @staticmethod
    def _describe_filter(state: FilterState | None) -> str:
        if state is None:
            return "未知（无法读取上联实际过滤）"
        if state.mode == "all":
            return "整个组播组（mode=all）"
        return f"{', '.join(state.sources)}（mode=allow）"

    def _finish(
        self,
        migration: Migration,
        *,
        state: str,
        reason: str,
        message: str,
        blocked: bool = False,
    ) -> None:
        migration.state = state
        migration.reason = reason
        migration.message = message
        migration.blocked = blocked
        migration.finished_at = _now()
        self.record_event(
            f"migration_{state}",
            message,
            migration_id=migration.id,
        )
        log.info(
            "migration %s %s: %s", migration.id, state, message
        )
