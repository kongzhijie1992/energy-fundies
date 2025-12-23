from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd
from entsoe import EntsoePandasClient

from .utils_time import ensure_utc


class EntsoeError(RuntimeError):
    pass


def get_entsoe_api_key() -> str:
    key = os.getenv("ENTSOE_API_KEY") or os.getenv("ENTSOE_API_TOKEN")
    if not key:
        raise EntsoeError(
            "Missing ENTSO-E API key. Set ENTSOE_API_KEY (preferred) or ENTSOE_API_TOKEN."
        )
    return key


@dataclass(frozen=True)
class EntsoeClient:
    api_key: str
    max_attempts: int = 4
    backoff_base_seconds: float = 1.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "_client", EntsoePandasClient(api_key=self.api_key))

    @property
    def client(self) -> EntsoePandasClient:
        return self._client

    def _retry(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        last_exc: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # entsoe-py raises various exceptions
                last_exc = exc
                if attempt >= self.max_attempts:
                    break
                sleep_s = self.backoff_base_seconds * (2 ** (attempt - 1))
                time.sleep(sleep_s)
        raise EntsoeError(
            f"ENTSO-E request failed after {self.max_attempts} attempts."
        ) from last_exc

    def query_crossborder_physical_flows(
        self, *, from_domain: str, to_domain: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp
    ) -> pd.Series:
        start_utc = ensure_utc(pd.Timestamp(start_utc))
        end_utc = ensure_utc(pd.Timestamp(end_utc))
        return self._retry(
            self.client.query_crossborder_flows,
            from_domain,
            to_domain,
            start=start_utc,
            end=end_utc,
        )

    def query_scheduled_exchanges(
        self, *, from_domain: str, to_domain: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp
    ) -> pd.Series:
        start_utc = ensure_utc(pd.Timestamp(start_utc))
        end_utc = ensure_utc(pd.Timestamp(end_utc))
        return self._retry(
            self.client.query_scheduled_exchanges,
            from_domain,
            to_domain,
            start=start_utc,
            end=end_utc,
        )
