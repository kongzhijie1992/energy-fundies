import pandas as pd

from fundie.ftr.config.settings import FTRSettings
from fundie.ftr.core.time import hourly_index_utc
from fundie.ftr.core.types import ContractSpec
from fundie.ftr.features.spreads import compute_spread_series
from fundie.ftr.models.hs import bootstrap_scenarios
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


def test_pricing_hs_matches_bootstrap_expectation() -> None:
    start = pd.Timestamp("2024-02-01T00:00:00Z")
    prices_df = _prices_df(start, [1.0, 2.0, 3.0, 4.0])

    spec = ContractSpec.from_dict(
        {
            "source": "A",
            "sink": "B",
            "start_utc": start,
            "end_utc": start + pd.Timedelta(hours=2),
            "mw": 2.0,
            "contract_type": "obligation",
        }
    )
    settings = FTRSettings(n_scenarios=3, block_length_days=1, seed=7)

    result = price_contract(spec, prices_df, None, settings=settings)

    spread_series = compute_spread_series(
        prices_df,
        source="A",
        sink="B",
        tz_in="UTC",
        missing_policy=settings.missing_hour_policy,
    )
    contract_hours = hourly_index_utc(spec.start_utc, spec.end_utc)
    curve_series = pd.Series(spread_series.mean(), index=contract_hours, dtype="float")

    residuals = spread_series - spread_series.mean()
    scenarios = bootstrap_scenarios(residuals, n_hours=len(contract_hours), settings=settings)

    expected_payoffs = []
    base = curve_series.to_numpy()
    for scenario in scenarios:
        expected_payoffs.append(sum(base + scenario))

    expected_price = pd.Series(expected_payoffs).mean() * spec.mw
    assert result.price == expected_price
