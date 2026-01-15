from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..core.types import ValuationResult


def valuation_to_frame(result: ValuationResult) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "contract_id": result.contract_id,
                "price": result.price,
                "currency": result.currency,
                "mean_payoff": result.mean_payoff,
                "stdev_payoff": result.stdev_payoff,
                "p5_payoff": result.p5_payoff,
                "p95_payoff": result.p95_payoff,
                "n_scenarios": result.n_scenarios,
                "model": result.model,
                "data_version": result.data_version,
            }
        ]
    )


def write_report(frame: pd.DataFrame, path: Path) -> Path:
    if path.suffix.lower() == ".parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)
    return path
