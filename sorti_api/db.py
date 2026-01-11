"""
db.py - DB layer per Sorti Dashboard

- In locale: SQLite (sorti.db)
- Su Render: Postgres se DATABASE_URL è presente

Compatibilità:
- Il tuo main.py usa placeholder "?" (stile SQLite)
- Postgres usa "%s"
  => qui traduciamo automaticamente "?" -> "%s" quando siamo su Postgres.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Optional, Sequence

# =========================
# Config
# =========================

SQLITE_PATH = Path(__file__).resolve().parent.parent / "sorti.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

DB_BACKEND = "postgres" if DATABASE_URL else "sqlite"


def _translate_sql_for_postgres(sql: str) -> str:
    s = sql.replace("?", "%s")

    # Fix mirato per stats_daily (SQLite datetime('now'...) -> Postgres NOW() - interval)
    if "datetime('now'" in sql and "FROM events" in sql and "substr(ts" in sql:
        s = """
            SELECT
              to_char(ts::timestamptz, 'YYYY-MM-DD') AS day,
              COALESCE(SUM(weight_g), 0) AS weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS co2_saved_g
            FROM events
            WHERE ts::timestamptz >= NOW() - (%s * INTERVAL '1 day')
            GROUP BY to_char(ts::timestamptz, 'YYYY-MM-DD')
            ORDER BY day ASC
        """.strip()

    return s


class _DBConn:
    def __init__(self, backend: str, raw_conn: Any):
        self.backend = backend
        self.raw_conn = raw_conn
        self._closed = False

    def __enter__(self) -> "_DBConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self.raw_conn.commit()
            else:
                self.raw_conn.rollback()
        finally:
            self.close()

    def close(self) -> None:
        if not self._closed:
            try:
                self.raw_conn.close()
            finally:
                self._closed = True

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        if params is None:
            params = []

        if self.backend == "sqlite":
            return self.raw_conn.execute(sql, params)

        # Postgres (psycopg v3)
        sql_pg = _translate_sql_for_postgres(sql)
        cur = self.raw_conn.cursor()
        cur.execute(sql_pg, params)
        return cur


def get_conn() -> _DBConn:
    if DB_BACKEND == "sqlite":
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return _DBConn("sqlite", conn)

    # Postgres via psycopg v3
    import psycopg
    from psycopg.rows import dict_row

    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return _DBConn("postgres", conn)


def init_db() -> None:
    with get_conn() as conn:
        if DB_BACKEND == "sqlite":
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bins (
                    bin_id TEXT PRIMARY KEY,
                    capacity_g REAL NOT NULL DEFAULT 10000,
                    current_weight_g REAL NOT NULL DEFAULT 0,
                    last_seen TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    bin_id TEXT NOT NULL,
                    material TEXT NOT NULL,
                    weight_g REAL NOT NULL,
                    co2_saved_g REAL NOT NULL,
                    FOREIGN KEY(bin_id) REFERENCES bins(bin_id)
                )
            """)
        else:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bins (
                    bin_id TEXT PRIMARY KEY,
                    capacity_g DOUBLE PRECISION NOT NULL DEFAULT 10000,
                    current_weight_g DOUBLE PRECISION NOT NULL DEFAULT 0,
                    last_seen TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id BIGSERIAL PRIMARY KEY,
                    ts TEXT NOT NULL,
                    bin_id TEXT NOT NULL REFERENCES bins(bin_id),
                    material TEXT NOT NULL,
                    weight_g DOUBLE PRECISION NOT NULL,
                    co2_saved_g DOUBLE PRECISION NOT NULL
                )
            """)

