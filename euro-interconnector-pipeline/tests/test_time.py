from __future__ import annotations

from datetime import date

import pandas as pd

from eicflows.utils_time import ensure_utc, utc_index_for_local_day


def test_ensure_utc_localizes_naive() -> None:
    ts = pd.Timestamp("2024-01-01 00:00:00")
    out = ensure_utc(ts)
    assert out.tz is not None
    assert str(out.tz) == "UTC"


def test_dst_day_hour_counts_europe_berlin() -> None:
    tz = "Europe/Berlin"

    normal = utc_index_for_local_day(date(2024, 2, 1), tz)
    spring = utc_index_for_local_day(date(2024, 3, 31), tz)  # DST start (23h day)
    fall = utc_index_for_local_day(date(2024, 10, 27), tz)  # DST end (25h day)

    assert len(normal) == 24
    assert len(spring) == 23
    assert len(fall) == 25

