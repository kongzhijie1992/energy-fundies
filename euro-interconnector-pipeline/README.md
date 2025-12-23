# euro-interconnector-pipeline

Config-driven ingestion of hourly European cross-border interconnector flows from ENTSO-E Transparency, with canonical UTC timestamps and DST-safe handling, plus a few simple derived tables.

## Setup

Requirements: Python 3.11+

```bash
cd euro-interconnector-pipeline
poetry install
cp .env.example .env
```

Set your API key in your shell or `.env`:

```bash
export ENTSOE_API_KEY="..."
```

The CLI also accepts `ENTSOE_API_TOKEN` as an alias, but the canonical variable name in this project is `ENTSOE_API_KEY`.

## Config

- `config/zones.yml`: zone name -> ENTSO-E domain EIC code and local timezone
- `config/borders.yml`: list of borders to ingest, with an enforced sign convention

Example `config/zones.yml` and `config/borders.yml` are included for a small demo set of zones and borders.

## CLI

All timestamps are stored and processed in UTC (`timestamp_utc`).

Backfill a range (end date is inclusive if you pass a date without time):

```bash
poetry run eicflows backfill --start 2024-01-01 --end 2024-01-31 --metric physical_flow
```

Daily ingestion for the last N days (UTC):

```bash
poetry run eicflows daily --days 2
```

Compute features from the clean fact table:

```bash
poetry run eicflows features --start 2024-01-01 --end 2024-01-31
```

QC report (missing hours, NaNs, duplicates):

```bash
poetry run eicflows qc --start 2024-01-01 --end 2024-01-31
```

## Outputs

### 1) Clean border-flow fact table (Parquet)

- Written to `data/clean/flows/year=YYYY/month=MM/*.parquet`
- Columns:
  - `timestamp_utc` (timezone-aware UTC, hourly)
  - `border_id`, `from_zone`, `to_zone`
  - `metric` (`physical_flow`; `scheduled_exchange` planned)
  - `mw` (float)
  - `source` (`ENTSOE`)
  - `last_updated_utc` (ingestion time)

Sign convention: `mw > 0` means flow from `from_zone` → `to_zone` (enforced by configuration and transformations).

### 2) Zone-level net import (Parquet/CSV)

- `data/outputs/net_import.parquet`
- `data/outputs/net_import.csv`
- Definition: `net_import_mw = sum(inbound) - sum(outbound)`

### 3) Congestion proxy (Parquet/CSV)

Proxy A (no capacity data): per border, a rolling 30-day 95th percentile of `abs(flow)` defines a pseudo-capacity.

- `pseudo_capacity_mw`
- `congestion_util = abs(flow) / pseudo_capacity` (clipped 0..2)
- `congestion_flag = congestion_util >= 0.9`

Written to:

- `data/outputs/congestion_proxy.parquet`
- `data/outputs/congestion_proxy.csv`

## DST / timezone notes

- ENTSO-E data can be published in different time bases depending on endpoint and client behavior; this project normalizes to canonical UTC at the earliest practical step.
- DST is handled with timezone-aware timestamps; QC flags duplicates rather than dropping them blindly.

## Makefile quickstart

```bash
make install
make test
make backfill START=2024-01-01 END=2024-01-31
make daily DAYS=7
make features START=2024-01-01 END=2024-01-31
```

## Streamlit dashboard

After you have ingested data (and ideally computed features), run:

```bash
make dashboard
```

Or:

```bash
poetry run streamlit run dashboard/app.py
```

The dashboard reads from `data/clean/flows/` and `data/outputs/` and can compute net import / congestion proxy on-the-fly if outputs are missing.
