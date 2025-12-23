from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import typer
from dotenv import load_dotenv
from rich.logging import RichHandler

from .config import Metric, load_config
from .entsoe_client import EntsoeClient, EntsoeError, get_entsoe_api_key
from .extract import extract_all
from .features import compute_congestion_proxy, compute_net_import, write_outputs
from .load import ensure_data_dirs
from .transform import clean_border_series, read_clean_range, write_clean_partitioned
from .utils_time import (
    DateTimeRange,
    floor_to_hour_utc,
    hourly_index_utc,
    now_utc,
    parse_cli_range,
)

app = typer.Typer(add_completion=False, help="ENTSO-E interconnector flows pipeline")


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_time=True, show_level=True)],
    )


def _project_root() -> Path:
    env_root = os.getenv("EICFLOWS_PROJECT_ROOT")
    if env_root:
        return Path(env_root)
    return Path(__file__).resolve().parents[2]


def _default_config_dir() -> Path:
    return _project_root() / "config"


def _default_data_dir() -> Path:
    return _project_root() / "data"


def _load_env() -> None:
    load_dotenv(override=False)


def _parse_range(start: str, end: str) -> DateTimeRange:
    try:
        return parse_cli_range(start, end)
    except Exception as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.callback()
def _init(
    verbose: bool = typer.Option(False, "--verbose", help="Verbose logging"),
) -> None:
    _setup_logging(verbose)
    _load_env()


@app.command()
def backfill(
    start: str = typer.Option(..., "--start", help="Start (UTC) date or datetime"),
    end: str = typer.Option(..., "--end", help="End (UTC) date or datetime (date is inclusive)"),
    metric: Metric = typer.Option(Metric.physical_flow, "--metric"),
    config_dir: Path = typer.Option(_default_config_dir(), "--config-dir", exists=True),
    data_dir: Path = typer.Option(_default_data_dir(), "--data-dir"),
) -> None:
    log = logging.getLogger("eicflows")
    range_utc = _parse_range(start, end)
    cfg = load_config(config_dir)

    borders = [b for b in cfg.borders if b.metric == metric]
    if not borders:
        raise typer.BadParameter(f"No borders configured for metric={metric}.")

    try:
        api_key = get_entsoe_api_key()
    except EntsoeError as exc:
        log.error(str(exc))
        raise typer.Exit(code=1) from exc

    dirs = ensure_data_dirs(data_dir)
    client = EntsoeClient(api_key=api_key)

    log.info(
        "Backfill %s: %s -> %s (%d borders)",
        metric,
        range_utc.start_utc.isoformat(),
        range_utc.end_utc.isoformat(),
        len(borders),
    )
    results = extract_all(
        client=client,
        borders=borders,
        zones=cfg.zones,
        range_utc=range_utc,
        raw_dir=dirs["raw"],
    )

    qc_frames: list[pd.DataFrame] = []
    clean_frames: list[pd.DataFrame] = []
    for border, res in zip(borders, results, strict=True):
        cleaned = clean_border_series(
            border=border,
            metric=res.metric,
            extracted_from_zone=res.from_zone,
            extracted_to_zone=res.to_zone,
            series=res.series,
            range_utc=range_utc,
        )
        clean_frames.append(cleaned.clean)
        qc_frames.append(cleaned.qc)

    clean_df = pd.concat(clean_frames, ignore_index=True) if clean_frames else pd.DataFrame()
    written = write_clean_partitioned(clean_df, clean_dir=dirs["clean"])
    qc = pd.concat(qc_frames, ignore_index=True) if qc_frames else pd.DataFrame()

    log.info("Wrote %d clean parquet files under %s", len(written), dirs["clean"])
    if not qc.empty:
        log.info("QC summary:\n%s", qc.to_string(index=False))


@app.command()
def daily(
    days: int = typer.Option(7, "--days", min=1, help="Pull last N days (UTC)"),
    metric: Metric = typer.Option(Metric.physical_flow, "--metric"),
    config_dir: Path = typer.Option(_default_config_dir(), "--config-dir", exists=True),
    data_dir: Path = typer.Option(_default_data_dir(), "--data-dir"),
) -> None:
    end_utc = floor_to_hour_utc(now_utc())
    start_utc = end_utc - pd.Timedelta(days=days)
    backfill(
        start=start_utc.isoformat(),
        end=end_utc.isoformat(),
        metric=metric,
        config_dir=config_dir,
        data_dir=data_dir,
    )


@app.command()
def features(
    start: str = typer.Option(..., "--start"),
    end: str = typer.Option(..., "--end"),
    config_dir: Path = typer.Option(_default_config_dir(), "--config-dir", exists=True),
    data_dir: Path = typer.Option(_default_data_dir(), "--data-dir"),
) -> None:
    log = logging.getLogger("eicflows")
    range_utc = _parse_range(start, end)
    _ = load_config(config_dir)
    dirs = ensure_data_dirs(data_dir)

    flows = read_clean_range(dirs["clean"], range_utc)
    if flows.empty:
        log.error("No clean flows found under %s for requested range.", dirs["clean"])
        raise typer.Exit(code=1)

    net_import = compute_net_import(flows)
    congestion = compute_congestion_proxy(flows)
    write_outputs(net_import=net_import, congestion=congestion, outputs_dir=dirs["outputs"])
    log.info("Wrote outputs under %s", dirs["outputs"])


@app.command()
def qc(
    start: str = typer.Option(..., "--start"),
    end: str = typer.Option(..., "--end"),
    data_dir: Path = typer.Option(_default_data_dir(), "--data-dir"),
) -> None:
    log = logging.getLogger("eicflows")
    range_utc = _parse_range(start, end)
    dirs = ensure_data_dirs(data_dir)

    flows = read_clean_range(dirs["clean"], range_utc)
    if flows.empty:
        log.error("No clean flows found under %s for requested range.", dirs["clean"])
        raise typer.Exit(code=1)

    flows["timestamp_utc"] = pd.to_datetime(flows["timestamp_utc"], utc=True)
    expected = hourly_index_utc(range_utc.start_utc, range_utc.end_utc)
    expected_hours = int(len(expected))

    rows: list[dict[str, object]] = []
    for (border_id, metric), g in flows.groupby(["border_id", "metric"], sort=True):
        g = g.sort_values("timestamp_utc")
        dupes = int(g.duplicated(subset=["timestamp_utc"]).sum())
        nan_hours = int(g["mw"].isna().sum())
        observed_hours = int(g["timestamp_utc"].nunique())
        missing_hours = max(expected_hours - observed_hours, 0)
        extra_hours = max(observed_hours - expected_hours, 0)
        rows.append(
            {
                "border_id": border_id,
                "metric": metric,
                "expected_hours": expected_hours,
                "observed_hours": observed_hours,
                "missing_hours": missing_hours,
                "extra_hours": extra_hours,
                "duplicate_timestamps": dupes,
                "nan_mw_hours": nan_hours,
            }
        )

    report = pd.DataFrame(rows).sort_values(["missing_hours", "nan_mw_hours"], ascending=False)
    log.info("QC range: %s -> %s", range_utc.start_utc.isoformat(), range_utc.end_utc.isoformat())
    log.info("QC per border:\n%s", report.to_string(index=False))


def main() -> None:
    app()
