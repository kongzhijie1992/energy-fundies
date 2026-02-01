from __future__ import annotations

import re
from datetime import datetime
from typing import Any


_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_.$]+$")


def validate_table_name(name: str) -> str:
    if not name or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            "Invalid table name. Use letters, numbers, underscores, dots, or $."
        )
    return name


def build_flows_query(
    table: str,
    start: datetime,
    end: datetime,
    border_id: str | None,
    from_zone: str | None,
    to_zone: str | None,
    metric: str | None,
    limit: int,
) -> tuple[str, dict[str, Any]]:
    table = validate_table_name(table)
    conditions = ["timestamp_utc >= %(start)s", "timestamp_utc < %(end)s"]
    params: dict[str, Any] = {"start": start, "end": end, "limit": limit}
    if border_id:
        conditions.append("border_id = %(border_id)s")
        params["border_id"] = border_id
    if from_zone:
        conditions.append("from_zone = %(from_zone)s")
        params["from_zone"] = from_zone
    if to_zone:
        conditions.append("to_zone = %(to_zone)s")
        params["to_zone"] = to_zone
    if metric:
        conditions.append("metric = %(metric)s")
        params["metric"] = metric

    where = " AND ".join(conditions)
    query = (
        "SELECT timestamp_utc, border_id, from_zone, to_zone, metric, mw, source, "
        "last_updated_utc "
        f"FROM {table} "
        f"WHERE {where} "
        "ORDER BY timestamp_utc "
        "LIMIT %(limit)s"
    )
    return query, params


def build_net_import_query(
    table: str,
    start: datetime,
    end: datetime,
    zone: str | None,
    limit: int,
) -> tuple[str, dict[str, Any]]:
    table = validate_table_name(table)
    conditions = ["timestamp_utc >= %(start)s", "timestamp_utc < %(end)s"]
    params: dict[str, Any] = {"start": start, "end": end, "limit": limit}
    if zone:
        conditions.append("zone = %(zone)s")
        params["zone"] = zone
    where = " AND ".join(conditions)
    query = (
        "SELECT timestamp_utc, zone, net_import_mw "
        f"FROM {table} "
        f"WHERE {where} "
        "ORDER BY timestamp_utc "
        "LIMIT %(limit)s"
    )
    return query, params


def build_congestion_query(
    table: str,
    start: datetime,
    end: datetime,
    border_id: str | None,
    limit: int,
) -> tuple[str, dict[str, Any]]:
    table = validate_table_name(table)
    conditions = ["timestamp_utc >= %(start)s", "timestamp_utc < %(end)s"]
    params: dict[str, Any] = {"start": start, "end": end, "limit": limit}
    if border_id:
        conditions.append("border_id = %(border_id)s")
        params["border_id"] = border_id
    where = " AND ".join(conditions)
    query = (
        "SELECT timestamp_utc, border_id, pseudo_capacity_mw, congestion_util, "
        "congestion_flag "
        f"FROM {table} "
        f"WHERE {where} "
        "ORDER BY timestamp_utc "
        "LIMIT %(limit)s"
    )
    return query, params
