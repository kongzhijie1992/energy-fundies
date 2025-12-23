from __future__ import annotations

from enum import StrEnum
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class ZoneConfig(BaseModel):
    domain: str
    timezone: str


class Metric(StrEnum):
    physical_flow = "physical_flow"
    scheduled_exchange = "scheduled_exchange"


class DirectionConvention(StrEnum):
    positive_from_to = "positive_from_to"


class BorderConfig(BaseModel):
    border_id: str
    from_zone: str
    to_zone: str
    metric: Metric = Metric.physical_flow
    direction_convention: DirectionConvention = DirectionConvention.positive_from_to

    @field_validator("border_id")
    @classmethod
    def _id_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("border_id must be non-empty.")
        return v


class AppConfig(BaseModel):
    zones: dict[str, ZoneConfig] = Field(default_factory=dict)
    borders: list[BorderConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_refs(self) -> AppConfig:
        missing: list[str] = []
        for b in self.borders:
            if b.from_zone not in self.zones:
                missing.append(f"{b.border_id}.from_zone={b.from_zone}")
            if b.to_zone not in self.zones:
                missing.append(f"{b.border_id}.to_zone={b.to_zone}")
            if b.from_zone == b.to_zone:
                missing.append(f"{b.border_id} has same from/to zone {b.from_zone}")
        if missing:
            raise ValueError("Invalid borders config: " + "; ".join(missing))
        return self


def _read_yaml(path: Path) -> object:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_config(config_dir: Path) -> AppConfig:
    zones_path = config_dir / "zones.yml"
    borders_path = config_dir / "borders.yml"
    if not zones_path.exists():
        raise FileNotFoundError(f"Missing zones config: {zones_path}")
    if not borders_path.exists():
        raise FileNotFoundError(f"Missing borders config: {borders_path}")

    zones_raw = _read_yaml(zones_path)
    borders_raw = _read_yaml(borders_path)
    if not isinstance(zones_raw, dict):
        raise ValueError(f"{zones_path} must be a mapping of zone -> config.")
    if not isinstance(borders_raw, list):
        raise ValueError(f"{borders_path} must be a list of border configs.")

    zones = {k: ZoneConfig.model_validate(v) for k, v in zones_raw.items()}
    borders = [BorderConfig.model_validate(x) for x in borders_raw]
    return AppConfig(zones=zones, borders=borders)
