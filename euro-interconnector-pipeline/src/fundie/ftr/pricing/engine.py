from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

import pandas as pd

from ..version import __version__
from ..config.settings import FTRSettings
from ..core.time import hourly_index_utc
from ..core.types import ContractSpec, ContractType, ValuationResult
from ..data.cache import compute_data_version
from ..features.spreads import compute_spread_series, prepare_curve
from ..models.hs import bootstrap_scenarios

PriceProvider = Callable[[ContractSpec], pd.DataFrame]
CurveProvider = Callable[[ContractSpec], pd.DataFrame | None]


def _ensure_settings(settings: FTRSettings | None) -> FTRSettings:
    return settings or FTRSettings()


def _payoff(spread: float, contract_type: ContractType) -> float:
    if contract_type == ContractType.option:
        return max(spread, 0.0)
    return spread


def _price_hs(
    *,
    spread_history: pd.Series,
    curve_series: pd.Series,
    contract_type: ContractType,
    settings: FTRSettings,
) -> list[float]:
    residuals = spread_history - spread_history.mean()
    scenarios = bootstrap_scenarios(residuals, n_hours=len(curve_series), settings=settings)

    base = curve_series.to_numpy()
    scenario_payoffs: list[float] = []
    for scenario in scenarios:
        payoff = 0.0
        for base_value, residual in zip(base, scenario, strict=True):
            payoff += _payoff(base_value + residual, contract_type)
        scenario_payoffs.append(payoff)
    return scenario_payoffs


def price_contract(
    contract_spec: ContractSpec,
    prices_df: pd.DataFrame,
    curve: pd.DataFrame | None,
    *,
    model: str = "hs",
    settings: FTRSettings | None = None,
) -> ValuationResult:
    """Price a single FTR contract using historical simulation."""
    settings = _ensure_settings(settings)
    spread_series = compute_spread_series(
        prices_df,
        source=contract_spec.source,
        sink=contract_spec.sink,
        tz_in=settings.tz_in,
        missing_policy=settings.missing_hour_policy,
    )

    contract_hours = hourly_index_utc(contract_spec.start_utc, contract_spec.end_utc)
    if curve is not None:
        curve_series = prepare_curve(curve, contract_hours=contract_hours, tz_in=settings.tz_in)
    else:
        curve_series = pd.Series(spread_series.mean(), index=contract_hours, dtype="float")

    if model != "hs":
        raise ValueError(f"Unsupported model: {model}")

    scenario_payoffs = _price_hs(
        spread_history=spread_series,
        curve_series=curve_series,
        contract_type=contract_spec.contract_type,
        settings=settings,
    )

    payoffs = pd.Series(scenario_payoffs, dtype="float") * contract_spec.mw
    data_version = compute_data_version(
        file_paths=None,
        settings=settings,
        code_version=__version__,
        dataframes=[prices_df] + ([curve] if curve is not None else []),
    )

    return ValuationResult(
        contract_id=contract_spec.contract_id,
        price=float(payoffs.mean()),
        currency="EUR",
        mean_payoff=float(payoffs.mean()),
        stdev_payoff=float(payoffs.std(ddof=0)),
        p5_payoff=float(payoffs.quantile(0.05)),
        p95_payoff=float(payoffs.quantile(0.95)),
        n_scenarios=len(payoffs),
        model=model,
        data_version=data_version,
        metadata={
            "hours": len(contract_hours),
            "spread_mean": float(spread_series.mean()),
            "curve_mean": float(curve_series.mean()),
        },
    )


def _normalize_specs(specs: Iterable[ContractSpec] | pd.DataFrame) -> list[ContractSpec]:
    if isinstance(specs, pd.DataFrame):
        return [ContractSpec.from_dict(row) for row in specs.to_dict("records")]
    return [spec if isinstance(spec, ContractSpec) else ContractSpec.from_dict(spec) for spec in specs]


def price_batch(
    specs: Iterable[ContractSpec] | pd.DataFrame,
    data_provider: pd.DataFrame | PriceProvider,
    curve: pd.DataFrame | CurveProvider | None,
    *,
    model: str = "hs",
    settings: FTRSettings | None = None,
) -> pd.DataFrame:
    """Price a batch of contracts, returning a result DataFrame."""
    settings = _ensure_settings(settings)
    normalized = _normalize_specs(specs)

    rows: list[dict[str, Any]] = []
    for spec in normalized:
        prices_df = data_provider(spec) if callable(data_provider) else data_provider
        curve_df = curve(spec) if callable(curve) else curve
        result = price_contract(
            spec,
            prices_df,
            curve_df,
            model=model,
            settings=settings,
        )
        rows.append(
            {
                "contract_id": spec.contract_id,
                "source": spec.source,
                "sink": spec.sink,
                "start_utc": spec.start_utc,
                "end_utc": spec.end_utc,
                "mw": spec.mw,
                "contract_type": spec.contract_type.value,
                "price": result.price,
                "mean_payoff": result.mean_payoff,
                "stdev_payoff": result.stdev_payoff,
                "p5_payoff": result.p5_payoff,
                "p95_payoff": result.p95_payoff,
                "n_scenarios": result.n_scenarios,
                "model": result.model,
                "data_version": result.data_version,
            }
        )
    return pd.DataFrame(rows)
