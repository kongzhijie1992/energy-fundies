from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd


def ensure_utc(ts: pd.Timestamp) -> pd.Timestamp:
    if ts.tz is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def parse_datetime_utc(value: str | datetime | date | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ensure_utc(ts)


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


def iter_month_ranges(start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> Iterator[tuple[pd.Timestamp, pd.Timestamp]]:
    start_utc = ensure_utc(start_utc)
    end_utc = ensure_utc(end_utc)

    cursor = start_utc
    while cursor < end_utc:
        month_start = cursor.normalize().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (month_start + pd.offsets.MonthBegin(1)).tz_convert("UTC")
        chunk_start = cursor
        chunk_end = min(end_utc, next_month)
        yield chunk_start, chunk_end
        cursor = chunk_end
