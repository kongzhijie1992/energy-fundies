"""Synthetic price data generation for FTR pricing when historical data is unavailable.

This module generates realistic hourly electricity price data for European zones with:
- Zone-specific base prices
- Seasonal patterns (winter premium, summer discount)
- Hourly patterns (peak/off-peak)
- Volatility clustering (GARCH-like)
- Price spikes during congestion events
- Inter-zonal correlation structure
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd
import yaml
from pathlib import Path

from ..core.time import parse_datetime_utc


# Default base prices for major European zones (EUR/MWh)
DEFAULT_BASE_PRICES = {
    "FR": 50.0,
    "DE_LU": 45.0,
    "GB": 60.0,
    "ES": 48.0,
    "IT": 55.0,
    "NL": 47.0,
    "BE": 49.0,
    "CH": 52.0,
    "AT": 46.0,
    "PL": 42.0,
    "CZ": 43.0,
    "NO_1": 35.0,
    "NO_2": 35.0,
    "SE_1": 32.0,
    "SE_2": 33.0,
    "SE_3": 34.0,
    "SE_4": 36.0,
    "DK_1": 44.0,
    "DK_2": 45.0,
}


def _get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).resolve().parents[4]


def _load_zones() -> dict[str, dict]:
    """Load zone configuration from zones.yml."""
    zones_path = _get_project_root() / "config" / "zones.yml"
    if zones_path.exists():
        with open(zones_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def _seasonal_factor(timestamp: pd.Timestamp) -> float:
    """Calculate seasonal price factor based on month.
    
    Winter (Dec-Feb): +20%
    Summer (Jun-Aug): -15%
    Shoulder: baseline (1.0)
    """
    month = timestamp.month
    if month in [12, 1, 2]:  # Winter
        return 1.20
    elif month in [6, 7, 8]:  # Summer
        return 0.85
    elif month in [3, 4, 5]:  # Spring
        return 1.05
    else:  # Fall (9, 10, 11)
        return 1.10


def _hourly_factor(hour: int) -> float:
    """Calculate hourly price factor.
    
    Peak hours (8am-8pm): +30%
    Off-peak: -20%
    """
    if 8 <= hour < 20:
        return 1.30
    else:
        return 0.80


def _weekend_factor(timestamp: pd.Timestamp) -> float:
    """Calculate weekend discount factor.
    
    Weekend: -10%
    Weekday: baseline
    """
    if timestamp.dayofweek >= 5:  # Saturday=5, Sunday=6
        return 0.90
    return 1.0


def _generate_garch_volatility(
    n_hours: int,
    base_vol: float = 0.15,
    alpha: float = 0.1,
    beta: float = 0.85,
    seed: int | None = None,
) -> np.ndarray:
    """Generate GARCH(1,1)-like volatility clustering.
    
    Args:
        n_hours: Number of hours to generate
        base_vol: Base volatility level
        alpha: ARCH parameter (shock persistence)
        beta: GARCH parameter (volatility persistence)
        seed: Random seed for reproducibility
    
    Returns:
        Array of volatility values
    """
    rng = np.random.default_rng(seed)
    
    volatility = np.zeros(n_hours)
    volatility[0] = base_vol
    
    shocks = rng.standard_normal(n_hours)
    
    for t in range(1, n_hours):
        # GARCH(1,1): σ²_t = ω + α*ε²_{t-1} + β*σ²_{t-1}
        omega = base_vol**2 * (1 - alpha - beta)
        volatility[t] = np.sqrt(
            omega + alpha * (volatility[t-1] * shocks[t-1])**2 + beta * volatility[t-1]**2
        )
    
    return volatility


def generate_zone_prices(
    zone: str,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    base_price: float | None = None,
    volatility: float = 0.15,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate synthetic hourly prices for a single zone.
    
    Args:
        zone: Zone code (e.g., 'FR', 'DE_LU')
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        base_price: Base price in EUR/MWh (if None, uses default for zone)
        volatility: Base volatility level (default: 0.15 = 15%)
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with columns: timestamp_utc, node, price
    """
    if isinstance(start_date, str):
        start_date = parse_datetime_utc(start_date)
    if isinstance(end_date, str):
        end_date = parse_datetime_utc(end_date)
    
    # Generate hourly timestamps
    timestamps = pd.date_range(start_date, end_date, freq="h", tz="UTC")
    n_hours = len(timestamps)
    
    # Get base price
    if base_price is None:
        base_price = DEFAULT_BASE_PRICES.get(zone, 50.0)
    
    # Initialize RNG
    rng = np.random.default_rng(seed)
    
    # Generate GARCH volatility
    vol_series = _generate_garch_volatility(n_hours, base_vol=volatility, seed=seed)
    
    # Generate base prices with patterns
    prices = np.zeros(n_hours)
    for i, ts in enumerate(timestamps):
        # Apply seasonal, hourly, and weekend factors
        seasonal = _seasonal_factor(ts)
        hourly = _hourly_factor(ts.hour)
        weekend = _weekend_factor(ts)
        
        # Base price with patterns
        pattern_price = base_price * seasonal * hourly * weekend
        
        # Add volatility
        shock = rng.standard_normal()
        prices[i] = pattern_price * (1 + vol_series[i] * shock)
    
    # Add occasional price spikes (5% of hours)
    spike_mask = rng.random(n_hours) < 0.05
    spike_multipliers = 1 + rng.uniform(0.5, 2.0, n_hours)
    prices[spike_mask] *= spike_multipliers[spike_mask]
    
    # Ensure no negative prices
    prices = np.maximum(prices, 0.01)
    
    return pd.DataFrame({
        "timestamp_utc": timestamps,
        "node": zone,
        "price": prices,
    })


def generate_multi_zone_prices(
    zones: list[str],
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    correlation_matrix: pd.DataFrame | None = None,
    base_prices: dict[str, float] | None = None,
    volatility: float = 0.15,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate correlated synthetic prices for multiple zones.
    
    Args:
        zones: List of zone codes
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        correlation_matrix: Optional correlation matrix (zones × zones)
        base_prices: Optional dict of base prices per zone
        volatility: Base volatility level
        seed: Random seed
    
    Returns:
        DataFrame with columns: timestamp_utc, node, price
    """
    if isinstance(start_date, str):
        start_date = parse_datetime_utc(start_date)
    if isinstance(end_date, str):
        end_date = parse_datetime_utc(end_date)
    
    timestamps = pd.date_range(start_date, end_date, freq="h", tz="UTC")
    n_hours = len(timestamps)
    n_zones = len(zones)
    
    # Initialize RNG
    rng = np.random.default_rng(seed)
    
    # Create correlation matrix if not provided
    if correlation_matrix is None:
        # Default: high correlation (0.7) between all zones
        corr = np.full((n_zones, n_zones), 0.7)
        np.fill_diagonal(corr, 1.0)
    else:
        corr = correlation_matrix.loc[zones, zones].values
    
    # Generate correlated shocks using Cholesky decomposition
    L = np.linalg.cholesky(corr)
    independent_shocks = rng.standard_normal((n_hours, n_zones))
    correlated_shocks = independent_shocks @ L.T
    
    # Generate prices for each zone
    all_prices = []
    for i, zone in enumerate(zones):
        base_price = (base_prices or {}).get(zone) or DEFAULT_BASE_PRICES.get(zone, 50.0)
        
        # Generate GARCH volatility
        vol_series = _generate_garch_volatility(n_hours, base_vol=volatility, seed=seed + i)
        
        prices = np.zeros(n_hours)
        for t, ts in enumerate(timestamps):
            # Apply patterns
            seasonal = _seasonal_factor(ts)
            hourly = _hourly_factor(ts.hour)
            weekend = _weekend_factor(ts)
            
            pattern_price = base_price * seasonal * hourly * weekend
            
            # Add correlated volatility
            prices[t] = pattern_price * (1 + vol_series[t] * correlated_shocks[t, i])
        
        # Add spikes
        spike_mask = rng.random(n_hours) < 0.05
        spike_multipliers = 1 + rng.uniform(0.5, 2.0, n_hours)
        prices[spike_mask] *= spike_multipliers[spike_mask]
        
        # Ensure no negative prices
        prices = np.maximum(prices, 0.01)
        
        zone_df = pd.DataFrame({
            "timestamp_utc": timestamps,
            "node": zone,
            "price": prices,
        })
        all_prices.append(zone_df)
    
    return pd.concat(all_prices, ignore_index=True)


def calibrate_to_flows(
    prices: pd.DataFrame,
    flows: pd.DataFrame,
    congestion_threshold: float = 0.9,
    spike_multiplier: float = 1.5,
) -> pd.DataFrame:
    """Calibrate synthetic prices to create spreads during congestion events.
    
    When flows are high (near capacity), increase price spreads between zones.
    
    Args:
        prices: DataFrame with timestamp_utc, node, price
        flows: DataFrame with timestamp_utc, border_id, from_zone, to_zone, mw
        congestion_threshold: Utilization threshold to trigger spread increase
        spike_multiplier: Multiplier for price spread during congestion
    
    Returns:
        Calibrated prices DataFrame
    """
    # This is a placeholder for future enhancement
    # Would require congestion proxy data from the pipeline
    # For now, just return prices as-is
    return prices.copy()


def get_available_zones() -> list[str]:
    """Get list of available zones from configuration.
    
    Returns:
        List of zone codes
    """
    zones_config = _load_zones()
    return sorted(zones_config.keys())


def generate_default_correlation_matrix(zones: list[str]) -> pd.DataFrame:
    """Generate a default correlation matrix for zones.
    
    Assumes higher correlation between geographically close zones.
    
    Args:
        zones: List of zone codes
    
    Returns:
        Correlation matrix DataFrame
    """
    n = len(zones)
    
    # Default: moderate correlation (0.6) between all zones
    corr = np.full((n, n), 0.6)
    np.fill_diagonal(corr, 1.0)
    
    # Increase correlation for known close pairs
    close_pairs = [
        ("FR", "DE_LU"),
        ("FR", "BE"),
        ("DE_LU", "NL"),
        ("DE_LU", "AT"),
        ("NO_1", "NO_2"),
        ("NO_2", "NO_3"),
        ("SE_1", "SE_2"),
        ("SE_2", "SE_3"),
        ("SE_3", "SE_4"),
        ("DK_1", "DK_2"),
    ]
    
    for zone1, zone2 in close_pairs:
        if zone1 in zones and zone2 in zones:
            i, j = zones.index(zone1), zones.index(zone2)
            corr[i, j] = corr[j, i] = 0.85
    
    return pd.DataFrame(corr, index=zones, columns=zones)
