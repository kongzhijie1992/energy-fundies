from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from fastapi import Depends, FastAPI, HTTPException, Query

from .config import ConfigError, SnowflakeSettings, get_settings
from .db import fetch_all, snowflake_connection
from .models import CongestionRow, FlowRow, NetImportRow
from .queries import (
    build_congestion_query,
    build_flows_query,
    build_net_import_query,
)

app = FastAPI(title="Euro Interconnector API", version="0.1.0")


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _resolve_window(
    start: datetime | None,
    end: datetime | None,
    settings: SnowflakeSettings,
) -> tuple[datetime, datetime]:
    resolved_end = end or datetime.now(timezone.utc)
    resolved_start = start or (resolved_end - timedelta(days=settings.default_lookback_days))
    resolved_start = _coerce_utc(resolved_start)
    resolved_end = _coerce_utc(resolved_end)
    if resolved_end <= resolved_start:
        raise HTTPException(status_code=400, detail="end must be after start")
    return resolved_start, resolved_end


def _cap_limit(limit: int, settings: SnowflakeSettings) -> int:
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    return min(limit, settings.query_limit)


def _get_safe_settings() -> SnowflakeSettings:
    try:
        return get_settings()
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _fetch_rows(query: str, params: dict, settings: SnowflakeSettings) -> list[dict]:
    try:
        with snowflake_connection(settings) as conn:
            return fetch_all(conn, query, params)
    except Exception as exc:  # pragma: no cover - surface Snowflake errors
        raise HTTPException(status_code=500, detail="Snowflake query failed") from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/flows", response_model=list[FlowRow])
def read_flows(
    start: datetime | None = Query(None, description="Inclusive UTC start timestamp"),
    end: datetime | None = Query(None, description="Exclusive UTC end timestamp"),
    border_id: str | None = None,
    from_zone: str | None = None,
    to_zone: str | None = None,
    metric: str | None = None,
    limit: int = Query(5000, ge=1, le=100000),
    settings: SnowflakeSettings = Depends(_get_safe_settings),
) -> Iterable[FlowRow]:
    window_start, window_end = _resolve_window(start, end, settings)
    limit = _cap_limit(limit, settings)
    query, params = build_flows_query(
        settings.table_flows,
        window_start,
        window_end,
        border_id,
        from_zone,
        to_zone,
        metric,
        limit,
    )
    return _fetch_rows(query, params, settings)


@app.get("/net-import", response_model=list[NetImportRow])
def read_net_import(
    start: datetime | None = Query(None, description="Inclusive UTC start timestamp"),
    end: datetime | None = Query(None, description="Exclusive UTC end timestamp"),
    zone: str | None = None,
    limit: int = Query(5000, ge=1, le=100000),
    settings: SnowflakeSettings = Depends(_get_safe_settings),
) -> Iterable[NetImportRow]:
    window_start, window_end = _resolve_window(start, end, settings)
    limit = _cap_limit(limit, settings)
    query, params = build_net_import_query(
        settings.table_net_import,
        window_start,
        window_end,
        zone,
        limit,
    )
    return _fetch_rows(query, params, settings)


@app.get("/congestion", response_model=list[CongestionRow])
def read_congestion(
    start: datetime | None = Query(None, description="Inclusive UTC start timestamp"),
    end: datetime | None = Query(None, description="Exclusive UTC end timestamp"),
    border_id: str | None = None,
    limit: int = Query(5000, ge=1, le=100000),
    settings: SnowflakeSettings = Depends(_get_safe_settings),
) -> Iterable[CongestionRow]:
    window_start, window_end = _resolve_window(start, end, settings)
    limit = _cap_limit(limit, settings)
    query, params = build_congestion_query(
        settings.table_congestion,
        window_start,
        window_end,
        border_id,
        limit,
    )
    return _fetch_rows(query, params, settings)
