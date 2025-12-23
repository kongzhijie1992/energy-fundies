from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import BorderConfig, Metric, ZoneConfig
from .entsoe_client import EntsoeClient
from .utils_time import DateTimeRange, ensure_utc, iter_month_ranges, now_utc


@dataclass(frozen=True)
class ExtractResult:
    border_id: str
    metric: Metric
    from_zone: str
    to_zone: str
    series: pd.Series  # UTC index, hourly (best-effort)


def _series_to_frame(series: pd.Series) -> pd.DataFrame:
    if not isinstance(series.index, pd.DatetimeIndex):
        raise ValueError("Expected a DatetimeIndex from ENTSO-E response.")
    idx = series.index
    idx = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
    mw = pd.to_numeric(series, errors="coerce").astype("float64")
    df = pd.DataFrame({"timestamp_utc": idx, "mw": mw.to_numpy()})
    return df


def _raw_path(raw_dir: Path, metric: Metric, border_id: str, month: str) -> Path:
    return raw_dir / metric.value / border_id / f"{month}.parquet"


def extract_border(
    *,
    client: EntsoeClient,
    border: BorderConfig,
    zones: dict[str, ZoneConfig],
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    raw_dir: Path,
) -> ExtractResult:
    start_utc = ensure_utc(start_utc)
    end_utc = ensure_utc(end_utc)
    raw_dir.mkdir(parents=True, exist_ok=True)

    from_domain = zones[border.from_zone].domain
    to_domain = zones[border.to_zone].domain

    monthly_series: list[pd.Series] = []
    extracted_at = now_utc()

    for chunk in iter_month_ranges(start_utc, end_utc):
        month = chunk.start_utc.strftime("%Y-%m")
        out_path = _raw_path(raw_dir, border.metric, border.border_id, month)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if border.metric == Metric.physical_flow:
            s = client.query_crossborder_physical_flows(
                from_domain=from_domain,
                to_domain=to_domain,
                start_utc=chunk.start_utc,
                end_utc=chunk.end_utc,
            )
        elif border.metric == Metric.scheduled_exchange:
            s = client.query_scheduled_exchanges(
                from_domain=from_domain,
                to_domain=to_domain,
                start_utc=chunk.start_utc,
                end_utc=chunk.end_utc,
            )
        else:
            raise ValueError(f"Unsupported metric: {border.metric}")

        df = _series_to_frame(s)
        df["extracted_at_utc"] = extracted_at
        df.to_parquet(out_path, index=False)
        monthly_series.append(s)

    combined = pd.concat(monthly_series) if monthly_series else pd.Series(dtype="float64")
    if isinstance(combined.index, pd.DatetimeIndex):
        combined = combined.sort_index()
        if combined.index.tz is None:
            combined.index = combined.index.tz_localize("UTC")
        else:
            combined.index = combined.index.tz_convert("UTC")

    combined = combined.loc[(combined.index >= start_utc) & (combined.index < end_utc)]
    return ExtractResult(
        border_id=border.border_id,
        metric=border.metric,
        from_zone=border.from_zone,
        to_zone=border.to_zone,
        series=combined,
    )


def extract_physical_flows(
    *,
    client: EntsoeClient,
    border: BorderConfig,
    zones: dict[str, ZoneConfig],
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    raw_dir: Path,
) -> ExtractResult:
    if border.metric != Metric.physical_flow:
        raise ValueError(f"Border {border.border_id} metric={border.metric} is not physical_flow.")
    return extract_border(
        client=client,
        border=border,
        zones=zones,
        start_utc=start_utc,
        end_utc=end_utc,
        raw_dir=raw_dir,
    )


def extract_all(
    *,
    client: EntsoeClient,
    borders: list[BorderConfig],
    zones: dict[str, ZoneConfig],
    range_utc: DateTimeRange,
    raw_dir: Path,
) -> list[ExtractResult]:
    results: list[ExtractResult] = []
    for border in borders:
        results.append(
            extract_border(
                client=client,
                border=border,
                zones=zones,
                start_utc=range_utc.start_utc,
                end_utc=range_utc.end_utc,
                raw_dir=raw_dir,
            )
        )
    return results
