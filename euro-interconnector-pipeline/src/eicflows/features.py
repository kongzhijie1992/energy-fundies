from __future__ import annotations

from pathlib import Path

import pandas as pd

from .schemas import CONGESTION_COLUMNS, NET_IMPORT_COLUMNS


def compute_net_import(flows: pd.DataFrame) -> pd.DataFrame:
    if flows.empty:
        return pd.DataFrame(columns=NET_IMPORT_COLUMNS)

    df = flows[["timestamp_utc", "from_zone", "to_zone", "mw"]].copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    inbound = df.rename(columns={"to_zone": "zone"})[["timestamp_utc", "zone", "mw"]]
    outbound = df.rename(columns={"from_zone": "zone"})[["timestamp_utc", "zone", "mw"]]
    outbound["mw"] = -outbound["mw"]
    stacked = pd.concat([inbound, outbound], ignore_index=True)

    out = (
        stacked.groupby(["timestamp_utc", "zone"], sort=True)["mw"]
        .sum(min_count=1)
        .rename("net_import_mw")
        .reset_index()
        .sort_values(["zone", "timestamp_utc"])
        .reset_index(drop=True)
    )
    return out[NET_IMPORT_COLUMNS]


def compute_congestion_proxy(flows: pd.DataFrame) -> pd.DataFrame:
    if flows.empty:
        return pd.DataFrame(columns=CONGESTION_COLUMNS)

    df = flows[["timestamp_utc", "border_id", "metric", "mw"]].copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values(["border_id", "timestamp_utc"]).reset_index(drop=True)
    df["abs_mw"] = df["mw"].abs()

    pseudo_caps: list[pd.DataFrame] = []
    for border_id, g in df.groupby("border_id", sort=False):
        s = g.set_index("timestamp_utc")["abs_mw"]
        cap = s.rolling("30D", min_periods=24 * 7).quantile(0.95)
        pseudo_caps.append(
            cap.rename("pseudo_capacity_mw")
            .reset_index()
            .assign(border_id=border_id)[["timestamp_utc", "border_id", "pseudo_capacity_mw"]]
        )

    caps = pd.concat(pseudo_caps, ignore_index=True) if pseudo_caps else pd.DataFrame()
    out = df.merge(caps, on=["timestamp_utc", "border_id"], how="left")
    out["pseudo_capacity_mw"] = pd.to_numeric(out["pseudo_capacity_mw"], errors="coerce").astype(
        "float64"
    )
    out["congestion_util"] = (out["abs_mw"] / out["pseudo_capacity_mw"]).clip(0.0, 2.0)
    out["congestion_flag"] = out["congestion_util"] >= 0.9
    out = out.drop(columns=["abs_mw"])
    return out[CONGESTION_COLUMNS]


def write_outputs(
    *,
    net_import: pd.DataFrame,
    congestion: pd.DataFrame,
    outputs_dir: Path,
) -> None:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    net_import.to_parquet(outputs_dir / "net_import.parquet", index=False)
    net_import.to_csv(outputs_dir / "net_import.csv", index=False)
    congestion.to_parquet(outputs_dir / "congestion_proxy.parquet", index=False)
    congestion.to_csv(outputs_dir / "congestion_proxy.csv", index=False)
