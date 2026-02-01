from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import pandas as pd

from .time import parse_datetime_utc


class ContractType(StrEnum):
    obligation = "obligation"
    option = "option"


@dataclass(frozen=True)
class ContractSpec:
    source: str
    sink: str
    start_utc: pd.Timestamp
    end_utc: pd.Timestamp
    mw: float
    contract_type: ContractType = ContractType.obligation
    contract_id: str | None = None

    def __post_init__(self) -> None:
        if self.contract_type != ContractType.obligation:
            raise ValueError("Only obligation-style FTRs are supported in this model.")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ContractSpec":
        start = parse_datetime_utc(data["start_utc"])
        end = parse_datetime_utc(data["end_utc"])
        contract_type = ContractType(data.get("contract_type", ContractType.obligation))
        return cls(
            source=str(data["source"]),
            sink=str(data["sink"]),
            start_utc=start,
            end_utc=end,
            mw=float(data["mw"]),
            contract_type=contract_type,
            contract_id=data.get("contract_id"),
        )


@dataclass(frozen=True)
class ValuationResult:
    contract_id: str | None
    price: float
    currency: str
    mean_payoff: float
    stdev_payoff: float
    p5_payoff: float
    p95_payoff: float
    n_scenarios: int
    model: str
    data_version: str
    metadata: dict[str, Any]
