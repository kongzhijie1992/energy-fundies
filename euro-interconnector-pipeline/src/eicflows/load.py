from __future__ import annotations

from pathlib import Path

import pandas as pd

from .schemas import CLEAN_FLOW_COLUMNS


def ensure_data_dirs(base_dir: Path) -> dict[str, Path]:
    raw_dir = base_dir / "raw"
    clean_dir = base_dir / "clean" / "flows"
    outputs_dir = base_dir / "outputs"
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return {"raw": raw_dir, "clean": clean_dir, "outputs": outputs_dir}


def empty_clean_df() -> pd.DataFrame:
    return pd.DataFrame(columns=CLEAN_FLOW_COLUMNS)

