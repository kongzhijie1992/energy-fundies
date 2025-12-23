from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import streamlit as st

from eicflows.config import load_config
from eicflows.features import compute_congestion_proxy, compute_net_import
from eicflows.transform import read_clean_range
from eicflows.utils_time import DateTimeRange, ensure_utc, hourly_index_utc


@dataclass(frozen=True)
class Paths:
    project_root: Path
    data_dir: Path
    clean_dir: Path
    outputs_dir: Path


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _paths(project_root: Path, data_dir: Path | None = None) -> Paths:
    data_dir = data_dir or (project_root / "data")
    return Paths(
        project_root=project_root,
        data_dir=data_dir,
        clean_dir=data_dir / "clean" / "flows",
        outputs_dir=data_dir / "outputs",
    )


def _to_range_utc(start: pd.Timestamp, end: pd.Timestamp) -> DateTimeRange:
    start_utc = ensure_utc(pd.Timestamp(start))
    end_utc = ensure_utc(pd.Timestamp(end))
    if end_utc <= start_utc:
        raise ValueError("End must be after start.")
    return DateTimeRange(start_utc=start_utc, end_utc=end_utc)


@st.cache_data(show_spinner=False)
def _load_clean_flows(clean_dir: str, start_utc: str, end_utc: str) -> pd.DataFrame:
    rng = DateTimeRange(start_utc=pd.Timestamp(start_utc), end_utc=pd.Timestamp(end_utc))
    return read_clean_range(Path(clean_dir), rng)


@st.cache_data(show_spinner=False)
def _load_outputs(outputs_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    out_dir = Path(outputs_dir)
    net_path = out_dir / "net_import.parquet"
    cong_path = out_dir / "congestion_proxy.parquet"

    net = pd.read_parquet(net_path) if net_path.exists() else pd.DataFrame()
    cong = pd.read_parquet(cong_path) if cong_path.exists() else pd.DataFrame()

    if not net.empty and "timestamp_utc" in net.columns:
        net["timestamp_utc"] = pd.to_datetime(net["timestamp_utc"], utc=True)
    if not cong.empty and "timestamp_utc" in cong.columns:
        cong["timestamp_utc"] = pd.to_datetime(cong["timestamp_utc"], utc=True)
    return net, cong


def _filter_list(values: Iterable[str]) -> list[str]:
    out = sorted({str(v) for v in values if v is not None and str(v) != ""})
    return out


def _qc_summary(flows: pd.DataFrame, rng: DateTimeRange) -> pd.DataFrame:
    if flows.empty:
        return pd.DataFrame()
    expected = hourly_index_utc(rng.start_utc, rng.end_utc)
    expected_hours = int(len(expected))
    rows: list[dict[str, object]] = []
    for (border_id, metric), g in flows.groupby(["border_id", "metric"], sort=True):
        g = g.sort_values("timestamp_utc")
        dupes = int(g.duplicated(subset=["timestamp_utc"]).sum())
        nan_hours = int(g["mw"].isna().sum())
        observed_hours = int(g["timestamp_utc"].nunique())
        rows.append(
            {
                "border_id": border_id,
                "metric": metric,
                "expected_hours": expected_hours,
                "observed_hours": observed_hours,
                "missing_hours": max(expected_hours - observed_hours, 0),
                "extra_hours": max(observed_hours - expected_hours, 0),
                "duplicate_timestamps": dupes,
                "nan_mw_hours": nan_hours,
            }
        )
    return pd.DataFrame(rows).sort_values(["missing_hours", "nan_mw_hours"], ascending=False)


def main() -> None:
    st.set_page_config(page_title="EIC Flows Dashboard", layout="wide")
    st.title("European Interconnector Flows (ENTSO-E)")

    project_root_default = _default_project_root()

    with st.sidebar:
        st.header("Settings")
        project_root = Path(
            st.text_input(
                "Project root",
                value=str(project_root_default),
                help="Folder containing config/ and data/",
            )
        )
        data_dir = Path(
            st.text_input(
                "Data dir",
                value=str(project_root / "data"),
                help="Folder containing clean/ and outputs/",
            )
        )
        config_dir = project_root / "config"

        try:
            cfg = load_config(config_dir)
        except Exception as exc:
            st.error(f"Failed to load config from {config_dir}: {exc}")
            st.stop()

        st.divider()
        st.subheader("Time range (UTC)")
        end_default = pd.Timestamp.now(tz="UTC").floor("h")
        start_default = end_default - pd.Timedelta(days=7)

        start_dt = st.date_input("Start date", value=start_default.date())
        end_dt = st.date_input(
            "End date (inclusive)", value=(end_default - pd.Timedelta(days=1)).date()
        )
        start_utc = pd.Timestamp(start_dt).tz_localize("UTC")
        end_utc_excl = (pd.Timestamp(end_dt).tz_localize("UTC") + pd.Timedelta(days=1))
        rng = _to_range_utc(start_utc, end_utc_excl)

        st.divider()
        st.subheader("Filters")
        metric = st.selectbox("Metric", ["physical_flow", "scheduled_exchange"], index=0)
        configured_borders = [b.border_id for b in cfg.borders if b.metric.value == metric]
        selected_borders = st.multiselect(
            "Borders",
            options=configured_borders,
            default=configured_borders[: min(4, len(configured_borders))],
        )
        selected_zones = st.multiselect(
            "Zones", options=sorted(cfg.zones.keys()), default=["DE_LU", "FR"]
        )

        st.divider()
        st.caption("If outputs are missing, run `eicflows features --start ... --end ...`.")

    paths = _paths(project_root=project_root, data_dir=data_dir)

    flows = _load_clean_flows(
        str(paths.clean_dir),
        rng.start_utc.isoformat(),
        rng.end_utc.isoformat(),
    )
    if flows.empty:
        st.warning(f"No clean flows found under {paths.clean_dir}. Run `eicflows backfill` first.")
        st.stop()

    flows["timestamp_utc"] = pd.to_datetime(flows["timestamp_utc"], utc=True)
    flows = flows.loc[flows["metric"] == metric]
    if selected_borders:
        flows = flows.loc[flows["border_id"].isin(selected_borders)]

    net_out, cong_out = _load_outputs(str(paths.outputs_dir))
    net = net_out.copy()
    cong = cong_out.copy()

    if net.empty:
        net = compute_net_import(flows)
    else:
        net = net.loc[
            (net["timestamp_utc"] >= rng.start_utc) & (net["timestamp_utc"] < rng.end_utc)
        ]
        if selected_zones:
            net = net.loc[net["zone"].isin(selected_zones)]

    if cong.empty:
        cong = compute_congestion_proxy(flows)
    else:
        cong = cong.loc[
            (cong["timestamp_utc"] >= rng.start_utc) & (cong["timestamp_utc"] < rng.end_utc)
        ]
        if selected_borders:
            cong = cong.loc[cong["border_id"].isin(selected_borders)]
        cong = cong.loc[cong["metric"] == metric]

    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1:
        st.metric("Hours in range", f"{len(hourly_index_utc(rng.start_utc, rng.end_utc)):,}")
    with kpi2:
        st.metric("Flow rows", f"{len(flows):,}")
    with kpi3:
        flag_rate = float(cong["congestion_flag"].mean()) if not cong.empty else 0.0
        st.metric("Congestion flag rate", f"{100.0 * flag_rate:.1f}%")

    tab_flows, tab_net, tab_cong, tab_qc = st.tabs(
        ["Border flows", "Net import", "Congestion proxy", "QC"]
    )

    with tab_flows:
        st.subheader("Clean border flows (MW)")
        plot_df = flows[["timestamp_utc", "border_id", "mw"]].dropna().copy()
        if plot_df.empty:
            st.info("No non-NaN flow points in the current selection.")
        else:
            pivot = plot_df.pivot_table(
                index="timestamp_utc", columns="border_id", values="mw", aggfunc="mean"
            ).sort_index()
            st.line_chart(pivot)
        st.dataframe(
            flows.sort_values(["timestamp_utc", "border_id"]),
            use_container_width=True,
            height=320,
        )

    with tab_net:
        st.subheader("Zone net import (MW)")
        if net.empty:
            st.info("Net import table is empty for this range/filters.")
        else:
            pivot = net.pivot_table(
                index="timestamp_utc", columns="zone", values="net_import_mw", aggfunc="mean"
            ).sort_index()
            st.line_chart(pivot)
            st.dataframe(
                net.sort_values(["zone", "timestamp_utc"]),
                use_container_width=True,
                height=320,
            )

    with tab_cong:
        st.subheader("Congestion proxy")
        if cong.empty:
            st.info(
                "Congestion proxy is empty for this range/filters "
                "(needs enough history for rolling window)."
            )
        else:
            plot = cong[["timestamp_utc", "border_id", "congestion_util"]].dropna()
            pivot = plot.pivot_table(
                index="timestamp_utc", columns="border_id", values="congestion_util", aggfunc="mean"
            ).sort_index()
            st.line_chart(pivot)
            top = cong.sort_values(["congestion_util"], ascending=False).head(200)
            st.dataframe(top, use_container_width=True, height=320)

    with tab_qc:
        st.subheader("Missing hours / duplicates / NaNs")
        qc = _qc_summary(flows, rng)
        if qc.empty:
            st.info("No QC data.")
        else:
            st.dataframe(qc, use_container_width=True, height=320)


if __name__ == "__main__":
    main()
