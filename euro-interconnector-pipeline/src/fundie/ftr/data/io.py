from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_prices(path: Path) -> pd.DataFrame:
    """Read nodal prices with columns: timestamp_utc, node, price."""
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    return df


def read_curve(path: Path) -> pd.DataFrame:
    """Read forward spread curve with columns: timestamp_utc, spread."""
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    return df
