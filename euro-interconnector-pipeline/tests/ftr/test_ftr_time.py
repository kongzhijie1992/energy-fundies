from datetime import date

from fundie.ftr.core.time import utc_index_for_local_day


def test_dst_start_brussels_has_23_hours() -> None:
    idx = utc_index_for_local_day(date(2024, 3, 31), "Europe/Brussels")
    assert len(idx) == 23
