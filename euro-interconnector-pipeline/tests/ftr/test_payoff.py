import pandas as pd
import pytest

from fundie.ftr.config.settings import FTRSettings
from fundie.ftr.core.types import ContractSpec
from fundie.ftr.pricing.engine import price_contract


def _prices_df(start: pd.Timestamp, spreads: list[float]) -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(spreads), freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp_utc": list(idx) * 2,
            "node": ["A"] * len(idx) + ["B"] * len(idx),
            "price": [0.0] * len(idx) + spreads,
        }
    )


def _curve_df(start: pd.Timestamp, spreads: list[float]) -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(spreads), freq="h", tz="UTC")
    return pd.DataFrame({"timestamp_utc": idx, "spread": spreads})


def test_obligation_payoff_allows_negative() -> None:
    start = pd.Timestamp("2024-01-01T00:00:00Z")
    prices_df = _prices_df(start, [0.0, 0.0, 0.0])
    curve_df = _curve_df(start, [-5.0, -5.0, -5.0])
    settings = FTRSettings(n_scenarios=1, block_length_days=1, seed=0)

    obligation = ContractSpec.from_dict(
        {
            "source": "A",
            "sink": "B",
            "start_utc": start,
            "end_utc": start + pd.Timedelta(hours=3),
            "mw": 1.0,
            "contract_type": "obligation",
        }
    )
    obligation_result = price_contract(obligation, prices_df, curve_df, settings=settings)
    assert obligation_result.price < 0


def test_option_contract_rejected() -> None:
    start = pd.Timestamp("2024-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="Only obligation-style FTRs are supported"):
        ContractSpec.from_dict(
            {
                "source": "A",
                "sink": "B",
                "start_utc": start,
                "end_utc": start + pd.Timedelta(hours=3),
                "mw": 1.0,
                "contract_type": "option",
            }
        )
