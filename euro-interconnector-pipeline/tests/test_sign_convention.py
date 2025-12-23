from __future__ import annotations

import pandas as pd

from eicflows.transform import standardize_direction


def test_positive_from_to_enforced_by_flip_when_reversed() -> None:
    df = pd.DataFrame(
        {
            "timestamp_utc": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "mw": [100.0],
        }
    )
    flipped = standardize_direction(
        df,
        extracted_from_zone="B",
        extracted_to_zone="A",
        desired_from_zone="A",
        desired_to_zone="B",
    )
    assert float(flipped["mw"].iloc[0]) == -100.0


def test_standardize_direction_raises_on_mismatch() -> None:
    df = pd.DataFrame(
        {
            "timestamp_utc": [pd.Timestamp("2024-01-01T00:00:00Z")],
            "mw": [100.0],
        }
    )
    try:
        standardize_direction(
            df,
            extracted_from_zone="X",
            extracted_to_zone="Y",
            desired_from_zone="A",
            desired_to_zone="B",
        )
    except ValueError:
        return
    raise AssertionError("Expected ValueError")

