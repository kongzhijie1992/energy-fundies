from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class FlowRow(BaseModel):
    timestamp_utc: datetime
    border_id: str
    from_zone: str
    to_zone: str
    metric: str
    mw: float
    source: str
    last_updated_utc: datetime | None = None


class NetImportRow(BaseModel):
    timestamp_utc: datetime
    zone: str
    net_import_mw: float = Field(..., description="Sum inbound minus outbound MW")


class CongestionRow(BaseModel):
    timestamp_utc: datetime
    border_id: str
    pseudo_capacity_mw: float
    congestion_util: float
    congestion_flag: bool
