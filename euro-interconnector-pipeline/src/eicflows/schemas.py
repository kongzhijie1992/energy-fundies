from __future__ import annotations

from typing import Final

CLEAN_FLOW_COLUMNS: Final[list[str]] = [
    "timestamp_utc",
    "border_id",
    "from_zone",
    "to_zone",
    "metric",
    "mw",
    "source",
    "last_updated_utc",
]

NET_IMPORT_COLUMNS: Final[list[str]] = ["timestamp_utc", "zone", "net_import_mw"]

CONGESTION_COLUMNS: Final[list[str]] = [
    "timestamp_utc",
    "border_id",
    "metric",
    "mw",
    "pseudo_capacity_mw",
    "congestion_util",
    "congestion_flag",
]

