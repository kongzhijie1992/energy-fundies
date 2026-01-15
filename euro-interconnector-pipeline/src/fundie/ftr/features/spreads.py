from __future__ import annotations

from typing import Iterable

import pandas as pd

from ..config.settings import MissingHourPolicy


_TS_COL_CANDIDATES = ("timestamp_utc", "timestamp")


def _pick_timestamp_column(df: pd.DataFrame) -> str:
    for col in _TS_COL_CANDIDATES:
        if col in df.columns:
            return col
    raise ValueError("prices_df must include a timestamp_utc or timestamp column")


def _coerce_timestamp(series: pd.Series, tz_in: str) -> pd.Series:
    ts = pd.to_datetime(series, utc=False)
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(tz_in)
    return ts.dt.tz_convert("UTC")


def _prepare_node_prices(
    prices_df: pd.DataFrame,
    node: str,
    tz_in: str,
    missing_policy: MissingHourPolicy,
) -> pd.Series:
    ts_col = _pick_timestamp_column(prices_df)
    subset = prices_df.loc[prices_df["node"] == node, [ts_col, "price"]].copy()
    if subset.empty:
        raise ValueError(f"No prices found for node={node}.")

    subset["timestamp_utc"] = _coerce_timestamp(subset[ts_col], tz_in)
    subset = subset.groupby("timestamp_utc", as_index=False)["price"].mean()
    subset = subset.sort_values("timestamp_utc")
    series = subset.set_index("timestamp_utc")["price"].sort_index()

    if missing_policy == "ffill":
        full_index = pd.date_range(series.index.min(), series.index.max(), freq="h", tz="UTC")
        series = series.reindex(full_index).ffill()
    return series


def compute_spread_series(
    prices_df: pd.DataFrame,
    *,
    source: str,
    sink: str,
    tz_in: str,
    missing_policy: MissingHourPolicy,
) -> pd.Series:
    """Compute hourly price spread (sink - source) as a UTC-indexed series."""
    source_series = _prepare_node_prices(prices_df, source, tz_in, missing_policy)
    sink_series = _prepare_node_prices(prices_df, sink, tz_in, missing_policy)
    joined = pd.concat({"source": source_series, "sink": sink_series}, axis=1, join="inner")
    return joined["sink"] - joined["source"]


def prepare_curve(
    curve_df: pd.DataFrame,
    *,
    contract_hours: Iterable[pd.Timestamp],
    tz_in: str,
) -> pd.Series:
    ts_col = _pick_timestamp_column(curve_df)
    value_col = "spread" if "spread" in curve_df.columns else "expected_spread"
    if value_col not in curve_df.columns:
        raise ValueError("curve must include spread or expected_spread column")

    curve = curve_df[[ts_col, value_col]].copy()
    curve["timestamp_utc"] = _coerce_timestamp(curve[ts_col], tz_in)
    curve = curve.groupby("timestamp_utc", as_index=True)[value_col].mean().sort_index()
    contract_index = pd.DatetimeIndex(contract_hours, tz="UTC")
    curve = curve.reindex(contract_index)
    if curve.isna().any():
        curve = curve.ffill().bfill()
    return curve
