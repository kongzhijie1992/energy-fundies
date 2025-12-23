#!/usr/bin/env bash
set -euo pipefail

DAYS="${DAYS:-7}"
poetry run eicflows daily --days "$DAYS"

