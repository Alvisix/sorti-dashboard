from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Optional, Sequence

SQLITE_PATH = Path(__file__).resolve().parent.parent / "sorti.db"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)


def _qmarks_to_psycopg(sql: str) -> str:
    return sql.replace("?", "%s")


def _rewrite_daily_sql(sql: str) -> str:
    if "substr(ts, 1, 10) AS day" in sql and "datetime('now'" in sql:
        return """
            SELECT
              to_char(ts::timestamptz, 'YYYY-MM-DD') AS day,
              COALESCE(SUM(weight_g), 0) AS weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS co2_saved_g
            FROM events
            WHERE ts::timestamptz >= NOW() - (%s * INTERVAL '1 day')
            GROUP BY to_char(ts::timestamptz, 'YYYY-MM-DD')
            ORDER BY day ASC
        """.strip()
    return sql


class DBConn:
    def __init__(self, backend: str, raw: Any):
        self.backend = backend
        self.raw = raw
        self._closed = False

    def __enter__(self) -> "DBConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self.raw.commit()
            else:
                self.raw.rollback()
        finally:
            self.close()

    def close(self) -> None:
        if not self._closed:
            try:
                self.raw.close()
            finally:
                self._closed = True

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        if params is None:
            params = []

        if self.backend == "sqlite":
            return self.raw.execute(sql, params)

        sql_pg = _rewrite_daily_sql(_qmarks_to_psycopg(sql))
        cur = self.raw.cursor()
        cur.execute(sql_pg, params)
        return cur


def get_conn() -> DBConn:
    if not USE_POSTGRES:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return DBConn("sqlite", conn)

    import psycopg
    from psycopg.rows import dict_row
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return DBConn("postgres", conn)


def _sqlite_add_column_if_missing(conn: DBConn, table: str, col: str, coltype: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {r["name"] for r in rows}
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")


def _pg_add_column_if_missing(conn: DBConn, table: str, col: str, coltype: str) -> None:
    # IF NOT EXISTS è supportato nelle versioni moderne di Postgres
    conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {coltype}")


def init_db() -> None:
    with get_conn() as conn:
        if not USE_POSTGRES:
            # SQLite: crea base
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

            # Migrazioni Step 12: bins.ingest_key
            _sqlite_add_column_if_missing(conn, "bins", "ingest_key", "TEXT")

            # Migrazioni Step 10: eventi AI-ready
            _sqlite_add_column_if_missing(conn, "events", "source", "TEXT")
            _sqlite_add_column_if_missing(conn, "events", "model_version", "TEXT")
            _sqlite_add_column_if_missing(conn, "events", "confidence", "REAL")
            _sqlite_add_column_if_missing(conn, "events", "topk_json", "TEXT")
            _sqlite_add_column_if_missing(conn, "events", "image_ref", "TEXT")

        else:
            # Postgres: crea base
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

            # Migrazioni Step 12: bins.ingest_key
            _pg_add_column_if_missing(conn, "bins", "ingest_key", "TEXT")

            # Migrazioni Step 10: eventi AI-ready
            _pg_add_column_if_missing(conn, "events", "source", "TEXT")
            _pg_add_column_if_missing(conn, "events", "model_version", "TEXT")
            _pg_add_column_if_missing(conn, "events", "confidence", "DOUBLE PRECISION")
            _pg_add_column_if_missing(conn, "events", "topk_json", "TEXT")
            _pg_add_column_if_missing(conn, "events", "image_ref", "TEXT")





