from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator


MissingHourPolicy = Literal["drop", "ffill"]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_cache_dir() -> Path:
    return _project_root() / ".fundie_cache" / "ftr"


class FTRSettings(BaseModel):
    cache_dir: Path = Field(default_factory=_default_cache_dir)
    tz_in: str = "UTC"
    n_scenarios: int = 500
    block_length_days: int = 7
    seed: int = 123
    missing_hour_policy: MissingHourPolicy = "drop"

    @field_validator("n_scenarios", "block_length_days")
    @classmethod
    def _positive_int(cls, v: int) -> int:
        if v < 1:
            raise ValueError("must be >= 1")
        return v

    @field_validator("seed")
    @classmethod
    def _seed_nonnegative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("seed must be >= 0")
        return v
