#!/usr/bin/env bash
set -euo pipefail

START="${START:-2024-01-01}"
END="${END:-2024-01-31}"
METRIC="${METRIC:-physical_flow}"

poetry run eicflows backfill --start "$START" --end "$END" --metric "$METRIC"

