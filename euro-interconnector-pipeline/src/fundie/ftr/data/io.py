from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from .synthetic import (
    generate_zone_prices,
    generate_multi_zone_prices,
    get_available_zones as _get_available_zones,
    generate_default_correlation_matrix,
)

log = logging.getLogger(__name__)


def read_prices(path: Path) -> pd.DataFrame:
    """Read nodal prices with columns: timestamp_utc, node, price."""
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path, parse_dates=["timestamp_utc"])
    
    # Ensure timestamp is timezone-aware UTC
    if "timestamp_utc" in df.columns and not pd.api.types.is_datetime64tz_dtype(df["timestamp_utc"]):
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    
    return df


def read_curve(path: Path) -> pd.DataFrame:
    """Read forward spread curve with columns: timestamp_utc, spread."""
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path, parse_dates=["timestamp_utc"])
    
    # Ensure timestamp is timezone-aware UTC
    if "timestamp_utc" in df.columns and not pd.api.types.is_datetime64tz_dtype(df["timestamp_utc"]):
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    
    return df


def read_synthetic_prices(
    zones: list[str],
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    base_prices: dict[str, float] | None = None,
    volatility: float = 0.15,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic prices for specified zones and date range.
    
    Args:
        zones: List of zone codes
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        base_prices: Optional dict of base prices per zone
        volatility: Base volatility level (default: 0.15)
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with columns: timestamp_utc, node, price
    """
    log.info(f"Generating synthetic prices for {len(zones)} zones from {start_date} to {end_date}")
    
    if len(zones) == 1:
        base_price = (base_prices or {}).get(zones[0])
        return generate_zone_prices(
            zone=zones[0],
            start_date=start_date,
            end_date=end_date,
            base_price=base_price,
            volatility=volatility,
            seed=seed,
        )
    else:
        corr_matrix = generate_default_correlation_matrix(zones)
        return generate_multi_zone_prices(
            zones=zones,
            start_date=start_date,
            end_date=end_date,
            correlation_matrix=corr_matrix,
            base_prices=base_prices,
            volatility=volatility,
            seed=seed,
        )


def get_available_zones() -> list[str]:
    """Get list of available zones from configuration.
    
    Returns:
        List of zone codes sorted alphabetically
    """
    return _get_available_zones()


def validate_price_data(prices: pd.DataFrame) -> tuple[bool, list[str]]:
    """Validate price data for completeness and quality.
    
    Args:
        prices: DataFrame with timestamp_utc, node, price
    
    Returns:
        Tuple of (is_valid, list of warning messages)
    """
    warnings = []
    
    # Check required columns
    required_cols = {"timestamp_utc", "node", "price"}
    missing_cols = required_cols - set(prices.columns)
    if missing_cols:
        warnings.append(f"Missing required columns: {missing_cols}")
        return False, warnings
    
    # Check for negative prices
    if (prices["price"] < 0).any():
        n_negative = (prices["price"] < 0).sum()
        warnings.append(f"Found {n_negative} negative prices")
    
    # Check for missing values
    if prices["price"].isna().any():
        n_missing = prices["price"].isna().sum()
        warnings.append(f"Found {n_missing} missing price values")
    
    # Check for extreme outliers (> 500 EUR/MWh)
    if (prices["price"] > 500).any():
        n_extreme = (prices["price"] > 500).sum()
        warnings.append(f"Found {n_extreme} prices > 500 EUR/MWh (potential outliers)")
    
    # Check for time gaps (for each node)
    for node in prices["node"].unique():
        node_data = prices[prices["node"] == node].sort_values("timestamp_utc")
        time_diffs = node_data["timestamp_utc"].diff()
        expected_diff = pd.Timedelta(hours=1)
        
        gaps = time_diffs[time_diffs > expected_diff]
        if len(gaps) > 0:
            warnings.append(f"Node {node}: Found {len(gaps)} time gaps > 1 hour")
    
    is_valid = len(warnings) == 0
    return is_valid, warnings


def save_prices(prices: pd.DataFrame, path: Path, format: str = "auto") -> None:
    """Save price data to file.
    
    Args:
        prices: DataFrame with price data
        path: Output file path
        format: Output format ('csv', 'parquet', or 'auto' to infer from path)
    """
    if format == "auto":
        format = "parquet" if path.suffix.lower() == ".parquet" else "csv"
    
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if format == "parquet":
        prices.to_parquet(path, index=False)
    else:
        prices.to_csv(path, index=False)
    
    log.info(f"Saved {len(prices)} price records to {path}")

