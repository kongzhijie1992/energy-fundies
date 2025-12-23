from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd


def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def ensure_utc(ts: pd.Timestamp) -> pd.Timestamp:
    if ts.tz is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def parse_datetime_utc(value: str | datetime | date | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


@dataclass(frozen=True)
class DateTimeRange:
    start_utc: pd.Timestamp
    end_utc: pd.Timestamp  # exclusive


def _is_date_only(value: str) -> bool:
    value = value.strip()
    if "T" in value:
        return False
    if " " in value:
        return False
    return len(value) == 10 and value[4] == "-" and value[7] == "-"


def parse_cli_range(start: str, end: str) -> DateTimeRange:
    start_ts = parse_datetime_utc(start)
    end_ts = parse_datetime_utc(end)
    if _is_date_only(start):
        start_ts = start_ts.normalize()
    if _is_date_only(end):
        end_ts = end_ts.normalize() + pd.Timedelta(days=1)
    if end_ts <= start_ts:
        raise ValueError(f"Invalid range: end ({end_ts}) must be after start ({start_ts}).")
    return DateTimeRange(start_utc=start_ts, end_utc=end_ts)


def floor_to_hour_utc(ts: pd.Timestamp) -> pd.Timestamp:
    return ensure_utc(ts).floor("h")


def hourly_index_utc(start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.DatetimeIndex:
    start_utc = ensure_utc(start_utc)
    end_utc = ensure_utc(end_utc)
    return pd.date_range(start_utc, end_utc, freq="h", inclusive="left", tz="UTC")


def utc_index_for_local_day(local_day: date, tz: str) -> pd.DatetimeIndex:
    tzinfo = ZoneInfo(tz)
    local_start = datetime(local_day.year, local_day.month, local_day.day, tzinfo=tzinfo)
    local_end = local_start + timedelta(days=1)
    local_hours = pd.date_range(local_start, local_end, freq="h", inclusive="left")
    return local_hours.tz_convert("UTC")


def iter_month_ranges(start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> Iterator[DateTimeRange]:
    start_utc = ensure_utc(start_utc)
    end_utc = ensure_utc(end_utc)

    cursor = start_utc
    while cursor < end_utc:
        month_start = cursor.normalize().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (month_start + pd.offsets.MonthBegin(1)).tz_convert("UTC")
        chunk_start = cursor
        chunk_end = min(end_utc, next_month)
        yield DateTimeRange(start_utc=chunk_start, end_utc=chunk_end)
        cursor = chunk_end

