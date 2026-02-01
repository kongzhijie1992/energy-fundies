from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable

import snowflake.connector
from snowflake.connector import DictCursor

from .config import SnowflakeSettings


def normalize_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append({str(key).lower(): value for key, value in row.items()})
    return normalized


@contextmanager
def snowflake_connection(settings: SnowflakeSettings):
    conn = snowflake.connector.connect(**settings.conn_kwargs())
    try:
        yield conn
    finally:
        conn.close()


def fetch_all(conn, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(query, params)
        return normalize_rows(cursor.fetchall())
