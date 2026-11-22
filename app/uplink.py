"""Client for the local up-link simulator's filter API.

The important property is that a missing or refused acknowledgment is *not*
trusted.  After every attempt the relay re-reads the simulator's actually
applied filter and decides from that.  This is what lets a "change applied
but receipt lost" fault succeed, and what makes "refused to change" fail
without guessing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Iterable

import httpx

from .config import Settings

log = logging.getLogger(__name__)

ACK = "ack"
REFUSED = "refused"
LOST = "lost"
ERROR = "error"
SKIPPED = "skipped"


@dataclass(frozen=True)
class FilterState:
    """An actually applied simulator filter."""

    mode: str  # "all" or "allow"
    sources: tuple[str, ...] = ()

    def covers(self, required: Iterable[str]) -> bool:
        required = set(required)
        if self.mode == "all":
            return True
        return required.issubset(set(self.sources))

    def exactly(self, wanted: Iterable[str]) -> bool:
        wanted = set(wanted)
        return self.mode == "allow" and set(self.sources) == wanted

    def as_view(self) -> dict:
        return {
            "mode": self.mode,
            "sources": list(self.sources) if self.mode == "allow" else [],
        }

    def describe(self) -> str:
        if self.mode == "all":
            return "整个组播组"
        return ", ".join(self.sources) if self.sources else "无源"


@dataclass
class FilterAttempt:
    """Result of one filter-change attempt, with post-hoc verification."""

    label: str
    requested_mode: str
    requested_sources: tuple[str, ...]
    ack: str
    http_status: int | None
    detail: str
    actual: FilterState | None
    satisfied: bool
    verified_by_actual: bool

    def as_view(self) -> dict:
        return {
            "label": self.label,
            "requested": {
                "mode": self.requested_mode,
                "sources": list(self.requested_sources),
            },
            "ack": self.ack,
            "http_status": self.http_status,
            "satisfied": self.satisfied,
            "verified_by_actual": self.verified_by_actual,
            "actual": self.actual.as_view() if self.actual else None,
            "detail": self.detail,
        }


class UplinkClient:
    """Async client around the up-link simulator filter endpoints."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._base = settings.uplink_url.rstrip("/")
        self._client: httpx.AsyncClient | None = None

    @property
    def managed(self) -> bool:
        return bool(self._base)

    @property
    def url(self) -> str:
        return self._base

    async def start(self) -> None:
        if self._client is None and self.managed:
            self._client = httpx.AsyncClient(
                base_url=self._base,
                timeout=self._settings.uplink_timeout,
            )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def health(self) -> dict:
        """Probe the simulator; raises httpx.HTTPError when unreachable."""

        if not self.managed or self._client is None:
            raise RuntimeError("up-link simulator is not configured")
        response = await self._client.get("/healthz")
        response.raise_for_status()
        return response.json()

    async def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        allowed_status: set[int] | None = None,
    ) -> tuple[int, object]:
        """Raw simulator request used by the demo endpoints."""

        if not self.managed or self._client is None:
            raise RuntimeError("up-link simulator is not configured")
        response = await self._client.request(method, path, json=json_body)
        if allowed_status is not None and response.status_code not in allowed_status:
            raise httpx.HTTPStatusError(
                f"simulator returned HTTP {response.status_code}",
                request=response.request,
                response=response,
            )
        if response.headers.get("content-type", "").startswith(
            "application/json"
        ):
            try:
                return response.status_code, response.json()
            except ValueError:
                return response.status_code, {"raw": response.text}
        return response.status_code, {"raw": response.text}

    async def fetch_actual(self) -> FilterState | None:
        """Read back the filter the simulator is actually applying."""

        if not self.managed or self._client is None:
            return None
        try:
            response = await self._client.get("/sim/filter")
            response.raise_for_status()
            payload = response.json()
            mode = str(payload["mode"])
            sources = tuple(str(item) for item in payload.get("sources", []))
            if mode not in {"all", "allow"}:
                log.warning("unexpected up-link filter mode %r", mode)
                return None
            return FilterState(mode=mode, sources=tuple(sorted(sources)))
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            log.warning("cannot read actual up-link filter: %s", exc)
            return None

    async def apply_filter(
        self,
        *,
        label: str,
        mode: str,
        sources: Iterable[str],
        predicate: Callable[[FilterState], bool],
    ) -> FilterAttempt:
        """Ask the simulator to apply a filter and verify the actual result.

        ``predicate`` decides whether the actually applied filter satisfies
        the operation.  It is evaluated only on the state read back from the
        simulator, never on the acknowledgment alone (unless the state cannot
        be read at all and the simulator explicitly acknowledged success).
        """

        wanted = tuple(sorted(set(sources)))
        if not self.managed:
            return FilterAttempt(
                label=label,
                requested_mode=mode,
                requested_sources=wanted,
                ack=SKIPPED,
                http_status=None,
                detail="up-link simulator is not configured; filter left unmanaged",
                actual=None,
                satisfied=True,
                verified_by_actual=False,
            )

        ack = ERROR
        http_status: int | None = None
        detail = ""
        body = {"mode": mode, "sources": list(wanted) if mode == "allow" else []}

        assert self._client is not None
        try:
            response = await self._client.post("/sim/filter", json=body)
            http_status = response.status_code
            if 200 <= response.status_code < 300:
                ack = ACK
                detail = "simulator acknowledged the filter change"
            elif response.status_code == 409:
                ack = REFUSED
                detail = _error_detail(response) or "simulator refused the filter change"
            else:
                ack = ERROR
                detail = (
                    _error_detail(response)
                    or f"simulator returned HTTP {response.status_code}"
                )
        except httpx.TimeoutException:
            ack = LOST
            detail = (
                "no acknowledgment within "
                f"{self._settings.uplink_timeout:g}s; judging by actual filtering"
            )
        except httpx.TransportError as exc:
            # Any transport-level failure means no usable receipt reached us.
            # It says nothing about whether the simulator applied the change,
            # so the actual filter is the only ground truth.
            ack = LOST
            detail = (
                "receipt not received "
                f"({exc.__class__.__name__}: {str(exc)[:120] or 'no detail'}); "
                "judging by actual filtering"
            )
        except httpx.HTTPError as exc:
            ack = ERROR
            detail = f"filter request failed ({exc.__class__.__name__})"

        actual = await self.fetch_actual()
        if actual is not None:
            satisfied = bool(predicate(actual))
            verified = True
            if ack != ACK and satisfied:
                detail = (
                    f"{detail}; actual filter verified as satisfying the change "
                    f"({actual.describe()})"
                )
            elif ack == ACK and not satisfied:
                detail = (
                    "simulator acknowledged the change but the actual filter "
                    f"({actual.describe()}) does not match the request"
                )
        else:
            # No ground truth available.  Only a clean acknowledgment can be
            # trusted, and we mark it as unverified.
            satisfied = ack == ACK
            verified = False
            detail = f"{detail}; actual filter could not be read back"

        return FilterAttempt(
            label=label,
            requested_mode=mode,
            requested_sources=wanted,
            ack=ack,
            http_status=http_status,
            detail=detail,
            actual=actual,
            satisfied=satisfied,
            verified_by_actual=verified,
        )


def _error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text[:200]
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("error")
        if isinstance(detail, str):
            return detail
        return str(payload)[:200]
    return str(payload)[:200]
