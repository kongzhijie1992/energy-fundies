from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

class ConfigError(RuntimeError):
    pass


def _get_env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Invalid int for {name}: {raw}") from exc


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str | None
    table_flows: str
    table_net_import: str
    table_congestion: str
    query_limit: int
    default_lookback_days: int

    @classmethod
    def from_env(cls) -> "SnowflakeSettings":
        return cls(
            account=_get_env("SNOWFLAKE_ACCOUNT", required=True),
            user=_get_env("SNOWFLAKE_USER", required=True),
            password=_get_env("SNOWFLAKE_PASSWORD", required=True),
            warehouse=_get_env("SNOWFLAKE_WAREHOUSE", required=True),
            database=_get_env("SNOWFLAKE_DATABASE", required=True),
            schema=_get_env("SNOWFLAKE_SCHEMA", required=True),
            role=_get_env("SNOWFLAKE_ROLE"),
            table_flows=_get_env("SNOWFLAKE_TABLE_FLOWS", "CLEAN_FLOWS"),
            table_net_import=_get_env("SNOWFLAKE_TABLE_NET_IMPORT", "NET_IMPORT"),
            table_congestion=_get_env("SNOWFLAKE_TABLE_CONGESTION", "CONGESTION_PROXY"),
            query_limit=_get_int("API_QUERY_LIMIT", 50000),
            default_lookback_days=_get_int("API_DEFAULT_LOOKBACK_DAYS", 7),
        )

    def conn_kwargs(self) -> dict[str, str]:
        kwargs = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.role:
            kwargs["role"] = self.role
        return kwargs


@lru_cache(maxsize=1)
def get_settings() -> SnowflakeSettings:
    project_root = Path(__file__).resolve().parents[2]
    path = project_root.parent / ".env"
    if path.exists():
        load_dotenv(path, override=False)
    return SnowflakeSettings.from_env()
