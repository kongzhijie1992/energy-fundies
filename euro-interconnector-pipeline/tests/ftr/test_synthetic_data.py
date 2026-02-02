"""Tests for synthetic price data generation."""

from datetime import datetime

import pandas as pd
import pytest

from fundie.ftr.data.synthetic import (
    generate_zone_prices,
    generate_multi_zone_prices,
    get_available_zones,
    generate_default_correlation_matrix,
    _seasonal_factor,
    _hourly_factor,
    _weekend_factor,
)


def test_generate_zone_prices_basic():
    """Test basic price generation for a single zone."""
    prices = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        base_price=50.0,
        volatility=0.15,
        seed=42,
    )
    
    # Check shape
    assert len(prices) == 7 * 24 + 1  # 7 days + 1 hour (inclusive)
    assert list(prices.columns) == ["timestamp_utc", "node", "price"]
    
    # Check zone
    assert (prices["node"] == "FR").all()
    
    # Check no negative prices
    assert (prices["price"] >= 0).all()
    
    # Check reasonable price range (should be around 50 +/- some volatility)
    assert prices["price"].min() > 0
    assert prices["price"].max() < 200  # Reasonable upper bound


def test_generate_zone_prices_reproducibility():
    """Test that same seed produces same results."""
    prices1 = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        seed=42,
    )
    
    prices2 = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        seed=42,
    )
    
    pd.testing.assert_frame_equal(prices1, prices2)


def test_generate_zone_prices_different_seeds():
    """Test that different seeds produce different results."""
    prices1 = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        seed=42,
    )
    
    prices2 = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        seed=123,
    )
    
    # Prices should be different
    assert not prices1["price"].equals(prices2["price"])


def test_seasonal_patterns():
    """Test that seasonal factors are applied correctly."""
    # Winter (January)
    winter_ts = pd.Timestamp("2024-01-15", tz="UTC")
    winter_factor = _seasonal_factor(winter_ts)
    assert winter_factor == 1.20  # +20% in winter
    
    # Summer (July)
    summer_ts = pd.Timestamp("2024-07-15", tz="UTC")
    summer_factor = _seasonal_factor(summer_ts)
    assert summer_factor == 0.85  # -15% in summer
    
    # Spring (April)
    spring_ts = pd.Timestamp("2024-04-15", tz="UTC")
    spring_factor = _seasonal_factor(spring_ts)
    assert spring_factor == 1.05


def test_hourly_patterns():
    """Test that hourly factors are applied correctly."""
    # Peak hour (2pm)
    peak_factor = _hourly_factor(14)
    assert peak_factor == 1.30  # +30% during peak
    
    # Off-peak hour (2am)
    offpeak_factor = _hourly_factor(2)
    assert offpeak_factor == 0.80  # -20% during off-peak


def test_weekend_patterns():
    """Test that weekend factors are applied correctly."""
    # Weekday (Monday)
    weekday_ts = pd.Timestamp("2024-01-01", tz="UTC")  # Monday
    weekday_factor = _weekend_factor(weekday_ts)
    assert weekday_factor == 1.0
    
    # Weekend (Saturday)
    weekend_ts = pd.Timestamp("2024-01-06", tz="UTC")  # Saturday
    weekend_factor = _weekend_factor(weekend_ts)
    assert weekend_factor == 0.90  # -10% on weekend


def test_generate_multi_zone_prices():
    """Test multi-zone price generation with correlation."""
    zones = ["FR", "DE_LU", "GB"]
    
    prices = generate_multi_zone_prices(
        zones=zones,
        start_date="2024-01-01",
        end_date="2024-01-07",
        seed=42,
    )
    
    # Check shape
    expected_rows = (7 * 24 + 1) * 3  # 7 days + 1 hour, 3 zones
    assert len(prices) == expected_rows
    
    # Check all zones present
    assert set(prices["node"].unique()) == set(zones)
    
    # Check each zone has same number of timestamps
    for zone in zones:
        zone_data = prices[prices["node"] == zone]
        assert len(zone_data) == 7 * 24 + 1


def test_multi_zone_correlation():
    """Test that multi-zone prices are correlated."""
    zones = ["FR", "DE_LU"]
    
    prices = generate_multi_zone_prices(
        zones=zones,
        start_date="2024-01-01",
        end_date="2024-01-31",
        seed=42,
    )
    
    # Pivot to wide format
    prices_wide = prices.pivot(index="timestamp_utc", columns="node", values="price")
    
    # Calculate correlation
    corr = prices_wide.corr().loc["FR", "DE_LU"]
    
    # Should have positive correlation (default is 0.7)
    assert corr > 0.5  # Allow some variation due to randomness


def test_get_available_zones():
    """Test getting available zones from config."""
    zones = get_available_zones()
    
    # Should return a list
    assert isinstance(zones, list)
    
    # Should have some common zones
    assert "FR" in zones
    assert "DE_LU" in zones or "DE_AT_LU" in zones


def test_generate_default_correlation_matrix():
    """Test correlation matrix generation."""
    zones = ["FR", "DE_LU", "GB"]
    corr_matrix = generate_default_correlation_matrix(zones)
    
    # Check shape
    assert corr_matrix.shape == (3, 3)
    
    # Check diagonal is 1.0
    assert (corr_matrix.values.diagonal() == 1.0).all()
    
    # Check symmetry
    assert (corr_matrix.values == corr_matrix.values.T).all()
    
    # Check all correlations are between 0 and 1
    assert (corr_matrix.values >= 0).all()
    assert (corr_matrix.values <= 1).all()


def test_price_volatility_parameter():
    """Test that volatility parameter affects price variation."""
    # Low volatility
    prices_low = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-31",
        volatility=0.05,
        seed=42,
    )
    
    # High volatility
    prices_high = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-31",
        volatility=0.30,
        seed=42,
    )
    
    # High volatility should have higher standard deviation
    std_low = prices_low["price"].std()
    std_high = prices_high["price"].std()
    
    assert std_high > std_low


def test_timestamp_timezone():
    """Test that timestamps are timezone-aware UTC."""
    prices = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-02",
        seed=42,
    )
    
    # Check timezone
    assert prices["timestamp_utc"].dt.tz is not None
    assert str(prices["timestamp_utc"].dt.tz) == "UTC"


def test_base_price_parameter():
    """Test that base_price parameter affects price levels."""
    prices_low = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        base_price=30.0,
        volatility=0.10,
        seed=42,
    )
    
    prices_high = generate_zone_prices(
        zone="FR",
        start_date="2024-01-01",
        end_date="2024-01-07",
        base_price=70.0,
        volatility=0.10,
        seed=42,
    )
    
    # Higher base price should have higher mean
    assert prices_high["price"].mean() > prices_low["price"].mean()
