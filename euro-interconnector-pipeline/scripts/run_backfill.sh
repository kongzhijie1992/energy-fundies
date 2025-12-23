#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

START="${START:-2024-01-01}"
END="${END:-2024-01-31}"
METRIC="${METRIC:-physical_flow}"

poetry run eicflows backfill --start "$START" --end "$END" --metric "$METRIC"
