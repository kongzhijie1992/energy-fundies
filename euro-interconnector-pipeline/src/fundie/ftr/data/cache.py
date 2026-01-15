from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from ..config.settings import FTRSettings


def ensure_cache_dir(settings: FTRSettings) -> Path:
    cache_dir = settings.cache_dir
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_dataframe(df: pd.DataFrame) -> str:
    if df.empty:
        return hashlib.sha256(b"empty").hexdigest()
    hashed = pd.util.hash_pandas_object(df, index=True).values.tobytes()
    return hashlib.sha256(hashed).hexdigest()


def compute_data_version(
    *,
    file_paths: Iterable[Path] | None,
    settings: FTRSettings,
    code_version: str,
    dataframes: Iterable[pd.DataFrame] | None = None,
) -> str:
    digest = hashlib.sha256()
    digest.update(code_version.encode("utf-8"))
    digest.update(json.dumps(settings.model_dump(mode="json"), sort_keys=True).encode("utf-8"))

    if file_paths:
        for path in file_paths:
            digest.update(path.as_posix().encode("utf-8"))
            digest.update(_hash_file(path).encode("utf-8"))

    if dataframes:
        for df in dataframes:
            digest.update(_hash_dataframe(df).encode("utf-8"))

    return digest.hexdigest()


def write_cache_manifest(
    *,
    cache_dir: Path,
    data_version: str,
    settings: FTRSettings,
    file_paths: Iterable[Path] | None,
) -> Path:
    manifest = {
        "data_version": data_version,
        "settings": settings.model_dump(mode="json"),
        "files": [path.as_posix() for path in file_paths] if file_paths else [],
    }
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = cache_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path
