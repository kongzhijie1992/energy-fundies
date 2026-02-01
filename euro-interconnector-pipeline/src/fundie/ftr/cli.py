from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pandas as pd
import typer

from .config.settings import FTRSettings, MissingHourPolicy
from .core.types import ContractSpec
from .data.cache import compute_data_version, ensure_cache_dir, write_cache_manifest
from .data.io import read_curve, read_prices
from .pricing.engine import price_batch, price_contract
from .version import __version__
from .reporting.tables import valuation_to_frame, write_report

app = typer.Typer(add_completion=False, help="FTR pricing commands")


def _settings_from_options(
    *,
    cache_dir: Path | None,
    tz_in: str,
    n_scenarios: int,
    block_length_days: int,
    seed: int,
    missing_hour_policy: MissingHourPolicy,
) -> FTRSettings:
    settings = FTRSettings(
        tz_in=tz_in,
        n_scenarios=n_scenarios,
        block_length_days=block_length_days,
        seed=seed,
        missing_hour_policy=missing_hour_policy,
    )
    if cache_dir is not None:
        settings = settings.model_copy(update={"cache_dir": cache_dir})
    return settings


@app.command()
def ingest(
    prices: Path = typer.Option(..., "--prices", exists=True, readable=True),
    curve: Path | None = typer.Option(None, "--curve", exists=True, readable=True),
    cache_dir: Path | None = typer.Option(None, "--cache-dir"),
    tz_in: str = typer.Option("UTC", "--tz-in"),
    n_scenarios: int = typer.Option(500, "--n-scenarios"),
    block_length_days: int = typer.Option(7, "--block-length-days"),
    seed: int = typer.Option(123, "--seed"),
    missing_hour_policy: MissingHourPolicy = typer.Option("drop", "--missing-hour-policy"),
) -> None:
    """Cache input price data and record a data version manifest."""
    log = logging.getLogger("fundie.ftr")
    settings = _settings_from_options(
        cache_dir=cache_dir,
        tz_in=tz_in,
        n_scenarios=n_scenarios,
        block_length_days=block_length_days,
        seed=seed,
        missing_hour_policy=missing_hour_policy,
    )
    cache_root = ensure_cache_dir(settings)
    file_paths = [prices] + ([curve] if curve is not None else [])
    data_version = compute_data_version(
        file_paths=file_paths,
        settings=settings,
        code_version=__version__,
    )

    dest_dir = cache_root / data_version
    dest_dir.mkdir(parents=True, exist_ok=True)
    for path in file_paths:
        shutil.copy2(path, dest_dir / path.name)
    write_cache_manifest(
        cache_dir=dest_dir,
        data_version=data_version,
        settings=settings,
        file_paths=file_paths,
    )
    log.info("Cached inputs under %s", dest_dir)
    log.info("data_version=%s", data_version)


@app.command()
def price(
    prices: Path = typer.Option(..., "--prices", exists=True, readable=True),
    curve: Path | None = typer.Option(None, "--curve", exists=True, readable=True),
    source: str = typer.Option(..., "--source"),
    sink: str = typer.Option(..., "--sink"),
    start: str = typer.Option(..., "--start"),
    end: str = typer.Option(..., "--end"),
    mw: float = typer.Option(..., "--mw"),
    contract_type: str = typer.Option(
        "obligation",
        "--contract-type",
        help="Obligation only; EU FTRs do not include optionality.",
    ),
    output: Path | None = typer.Option(None, "--output"),
    model: str = typer.Option("hs", "--model"),
    cache_dir: Path | None = typer.Option(None, "--cache-dir"),
    tz_in: str = typer.Option("UTC", "--tz-in"),
    n_scenarios: int = typer.Option(500, "--n-scenarios"),
    block_length_days: int = typer.Option(7, "--block-length-days"),
    seed: int = typer.Option(123, "--seed"),
    missing_hour_policy: MissingHourPolicy = typer.Option("drop", "--missing-hour-policy"),
) -> None:
    """Price a single FTR contract."""
    log = logging.getLogger("fundie.ftr")
    settings = _settings_from_options(
        cache_dir=cache_dir,
        tz_in=tz_in,
        n_scenarios=n_scenarios,
        block_length_days=block_length_days,
        seed=seed,
        missing_hour_policy=missing_hour_policy,
    )
    prices_df = read_prices(prices)
    curve_df = read_curve(curve) if curve is not None else None
    normalized_contract_type = contract_type.strip().lower()
    if normalized_contract_type != "obligation":
        raise typer.BadParameter(
            "Only obligation-style FTRs are supported (optional-style FTRs are not available).",
            param_hint="--contract-type",
        )
    spec = ContractSpec.from_dict(
        {
            "source": source,
            "sink": sink,
            "start_utc": start,
            "end_utc": end,
            "mw": mw,
            "contract_type": normalized_contract_type,
        }
    )
    result = price_contract(spec, prices_df, curve_df, model=model, settings=settings)
    frame = valuation_to_frame(result)

    if output is not None:
        write_report(frame, output)
        log.info("Wrote report to %s", output)
    else:
        log.info("\n%s", frame.to_string(index=False))


@app.command()
def batch(
    specs: Path = typer.Option(..., "--specs", exists=True, readable=True),
    prices: Path = typer.Option(..., "--prices", exists=True, readable=True),
    curve: Path | None = typer.Option(None, "--curve", exists=True, readable=True),
    output: Path | None = typer.Option(None, "--output"),
    model: str = typer.Option("hs", "--model"),
    cache_dir: Path | None = typer.Option(None, "--cache-dir"),
    tz_in: str = typer.Option("UTC", "--tz-in"),
    n_scenarios: int = typer.Option(500, "--n-scenarios"),
    block_length_days: int = typer.Option(7, "--block-length-days"),
    seed: int = typer.Option(123, "--seed"),
    missing_hour_policy: MissingHourPolicy = typer.Option("drop", "--missing-hour-policy"),
) -> None:
    """Price multiple contracts from a CSV/Parquet specs file."""
    log = logging.getLogger("fundie.ftr")
    settings = _settings_from_options(
        cache_dir=cache_dir,
        tz_in=tz_in,
        n_scenarios=n_scenarios,
        block_length_days=block_length_days,
        seed=seed,
        missing_hour_policy=missing_hour_policy,
    )

    specs_df = pd.read_parquet(specs) if specs.suffix.lower() == ".parquet" else pd.read_csv(specs)
    prices_df = read_prices(prices)
    curve_df = read_curve(curve) if curve is not None else None

    results = price_batch(specs_df, prices_df, curve_df, model=model, settings=settings)
    if output is not None:
        write_report(results, output)
        log.info("Wrote batch report to %s", output)
    else:
        log.info("\n%s", results.to_string(index=False))
