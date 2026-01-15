from __future__ import annotations

import random
from collections.abc import Iterable

import pandas as pd

from ..config.settings import FTRSettings


def _block_length_hours(series_len: int, settings: FTRSettings) -> int:
    block_len = settings.block_length_days * 24
    if block_len < 1:
        block_len = 1
    return min(block_len, max(series_len, 1))


def _take_block(values: list[float], start: int, block_len: int) -> list[float]:
    end = start + block_len
    if end <= len(values):
        return values[start:end]
    tail = values[start:]
    head = values[: end - len(values)]
    return tail + head


def bootstrap_scenarios(
    residual_series: pd.Series,
    *,
    n_hours: int,
    settings: FTRSettings,
) -> list[list[float]]:
    """Block bootstrap residuals into hourly scenarios."""
    if n_hours < 1:
        raise ValueError("n_hours must be >= 1")
    if residual_series.empty:
        raise ValueError("residual_series must be non-empty")

    values = residual_series.dropna().tolist()
    if not values:
        raise ValueError("residual_series must contain finite values")

    block_len = _block_length_hours(len(values), settings)
    rng = random.Random(settings.seed)

    scenarios: list[list[float]] = []
    for _ in range(settings.n_scenarios):
        draws: list[float] = []
        while len(draws) < n_hours:
            start = rng.randrange(0, len(values))
            draws.extend(_take_block(values, start, block_len))
        scenarios.append(draws[:n_hours])
    return scenarios
