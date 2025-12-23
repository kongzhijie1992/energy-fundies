from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import BorderConfig, Metric
from .schemas import CLEAN_FLOW_COLUMNS
from .utils_time import DateTimeRange, ensure_utc, hourly_index_utc, now_utc


@dataclass(frozen=True)
class CleanFlowsAndQc:
    clean: pd.DataFrame
    qc: pd.DataFrame


def standardize_direction(
    df: pd.DataFrame,
    *,
    extracted_from_zone: str,
    extracted_to_zone: str,
    desired_from_zone: str,
    desired_to_zone: str,
) -> pd.DataFrame:
    if extracted_from_zone == desired_from_zone and extracted_to_zone == desired_to_zone:
        return df
    if extracted_from_zone == desired_to_zone and extracted_to_zone == desired_from_zone:
        flipped = df.copy()
        flipped["mw"] = -flipped["mw"]
        return flipped
    raise ValueError(
        "Cannot standardize direction: extracted direction "
        f"{extracted_from_zone}->{extracted_to_zone} does not match desired "
        f"{desired_from_zone}->{desired_to_zone}."
    )


def clean_border_series(
    *,
    border: BorderConfig,
    metric: Metric,
    extracted_from_zone: str,
    extracted_to_zone: str,
    series: pd.Series,
    range_utc: DateTimeRange,
    source: str = "ENTSOE",
) -> CleanFlowsAndQc:
    if not isinstance(series.index, pd.DatetimeIndex):
        raise ValueError("Expected a DatetimeIndex for series.")
    idx = series.index
    idx = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")

    vals = pd.to_numeric(series, errors="coerce").astype("float64")
    s = pd.Series(vals.to_numpy(), index=idx).sort_index()
    s = s.loc[(s.index >= range_utc.start_utc) & (s.index < range_utc.end_utc)]

    expected = hourly_index_utc(range_utc.start_utc, range_utc.end_utc)

    duplicates = int(s.index.duplicated().sum())
    s_agg = s.groupby(level=0).mean()
    aligned = s_agg.reindex(expected)

    df = pd.DataFrame({"timestamp_utc": aligned.index, "mw": aligned.to_numpy()})
    df["border_id"] = border.border_id
    df["from_zone"] = border.from_zone
    df["to_zone"] = border.to_zone
    df["metric"] = metric.value
    df["source"] = source
    df["last_updated_utc"] = now_utc()

    df = standardize_direction(
        df,
        extracted_from_zone=extracted_from_zone,
        extracted_to_zone=extracted_to_zone,
        desired_from_zone=border.from_zone,
        desired_to_zone=border.to_zone,
    )
    df = df[CLEAN_FLOW_COLUMNS]

    qc = pd.DataFrame(
        [
            {
                "border_id": border.border_id,
                "metric": metric.value,
                "start_utc": ensure_utc(range_utc.start_utc),
                "end_utc": ensure_utc(range_utc.end_utc),
                "expected_hours": int(len(expected)),
                "available_points": int(s.shape[0]),
                "duplicate_timestamps": duplicates,
                "missing_hours": int(aligned.isna().sum()),
                "nan_hours": int(pd.isna(aligned).sum()),
            }
        ]
    )
    return CleanFlowsAndQc(clean=df, qc=qc)


def write_clean_partitioned(df: pd.DataFrame, *, clean_dir: Path) -> list[Path]:
    if df.empty:
        return []
    clean_dir.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["year"] = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.month

    written: list[Path] = []
    for (year, month, border_id, metric), g in df.groupby(
        ["year", "month", "border_id", "metric"], sort=True
    ):
        part_dir = clean_dir / f"year={year:04d}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        out_path = part_dir / f"{border_id}_{metric}.parquet"
        g.drop(columns=["year", "month"]).to_parquet(out_path, index=False)
        written.append(out_path)
    return written


def _iter_year_months(range_utc: DateTimeRange) -> list[tuple[int, int]]:
    start = ensure_utc(range_utc.start_utc).normalize().replace(day=1)
    end_inclusive = ensure_utc(range_utc.end_utc) - pd.Timedelta(nanoseconds=1)
    end = end_inclusive.normalize().replace(day=1)
    months: list[tuple[int, int]] = []
    cursor = start
    while cursor <= end:
        months.append((int(cursor.year), int(cursor.month)))
        cursor = (cursor + pd.offsets.MonthBegin(1)).tz_convert("UTC")
    return months


def read_clean_range(clean_dir: Path, range_utc: DateTimeRange) -> pd.DataFrame:
    clean_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for year, month in _iter_year_months(range_utc):
        part_dir = clean_dir / f"year={year:04d}" / f"month={month:02d}"
        if part_dir.exists():
            paths.extend(sorted(part_dir.glob("*.parquet")))
    if not paths:
        return pd.DataFrame(columns=CLEAN_FLOW_COLUMNS)
    frames: list[pd.DataFrame] = []
    for p in paths:
        try:
            frames.append(pd.read_parquet(p))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=CLEAN_FLOW_COLUMNS)
    df = pd.concat(frames, ignore_index=True)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    mask = (df["timestamp_utc"] >= range_utc.start_utc) & (df["timestamp_utc"] < range_utc.end_utc)
    return df.loc[mask].sort_values(["border_id", "timestamp_utc"]).reset_index(drop=True)
