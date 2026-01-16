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

USE_POSTGRES = bool(DATABASE_URL)


def _qmarks_to_psycopg(sql: str) -> str:
    """
    Converte placeholder SQLite (?) in placeholder Postgres (%s)
    """
    return sql.replace("?", "%s")


def _rewrite_daily_sql(sql: str) -> str:
    """
    Riscrive SOLO la query di stats_daily (che su SQLite usa datetime('now'...))
    in sintassi Postgres.
    """
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
    """
    Wrapper per avere:
    - with get_conn() as conn:
        conn.execute(...)
    sia su SQLite che su Postgres.
    """

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

        # Postgres (psycopg v3)
        sql_pg = _rewrite_daily_sql(_qmarks_to_psycopg(sql))
        cur = self.raw.cursor()
        cur.execute(sql_pg, params)
        return cur


def get_conn() -> DBConn:
    """
    Se DATABASE_URL è presente -> Postgres (Render)
    Altrimenti -> SQLite (locale)
    """
    if not USE_POSTGRES:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return DBConn("sqlite", conn)

    import psycopg
    from psycopg.rows import dict_row

    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return DBConn("postgres", conn)


def _sqlite_has_column(conn: DBConn, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for r in rows:
        # sqlite Row: r["name"]
        if (r["name"] if isinstance(r, sqlite3.Row) else r[1]) == col:
            return True
    return False


def init_db() -> None:
    """
    Crea tabelle se non esistono e applica migrazioni leggere (event_id).
    """
    with get_conn() as conn:
        if conn.backend == "sqlite":
            # SQLite
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
                    event_id TEXT,
                    FOREIGN KEY(bin_id) REFERENCES bins(bin_id)
                )
            """)

            # Migrazione: se events esisteva senza event_id, aggiungo colonna
            if not _sqlite_has_column(conn, "events", "event_id"):
                conn.execute("ALTER TABLE events ADD COLUMN event_id TEXT")

            # Unique index (idempotenza) - parziale: solo se event_id non NULL
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id
                ON events(event_id)
                WHERE event_id IS NOT NULL
            """)

        else:
            # Postgres
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
                    co2_saved_g DOUBLE PRECISION NOT NULL,
                    event_id TEXT
                )
            """)

            # Migrazione: aggiungi colonna se manca
            conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS event_id TEXT")

            # Unique index parziale su Postgres (idempotenza)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id
                ON events(event_id)
                WHERE event_id IS NOT NULL
            """)



