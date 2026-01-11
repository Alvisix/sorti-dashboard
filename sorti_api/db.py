"""
db.py - gestione DB per Sorti Dashboard

Obiettivo:
- In locale: usare SQLite (file sorti.db)
- Su Render (o in cloud): usare Postgres se esiste la variabile d’ambiente DATABASE_URL

Nota importante:
Il tuo main.py usa query in stile SQLite con placeholder "?".
Postgres invece usa "%s".
Per NON toccare tutto il main, qui sotto traduciamo automaticamente "?" -> "%s"
quando siamo su Postgres.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Optional, Sequence, Union

# =========================
# Config
# =========================

# Percorso del DB SQLite (solo per uso locale)
# repo_root/sorti.db
SQLITE_PATH = Path(__file__).resolve().parent.parent / "sorti.db"

# Se su Render hai un database Postgres collegato, Render ti fornisce DATABASE_URL.
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Per debug: capiamo che backend stiamo usando
DB_BACKEND = "postgres" if DATABASE_URL else "sqlite"


# =========================
# Helper: traduzione query SQLite -> Postgres
# =========================

def _translate_sql_for_postgres(sql: str) -> str:
    """
    Traduce una query scritta per SQLite (placeholder '?')
    in una query adatta a Postgres (placeholder '%s').

    Inoltre, gestiamo un caso comune nel tuo progetto: stats_daily,
    che in SQLite usa datetime('now', ...). In Postgres non esiste.
    Quindi facciamo una riscrittura mirata se troviamo quel pattern.
    """
    s = sql

    # 1) Placeholder: SQLite usa "?" / Postgres usa "%s"
    # ATTENZIONE: assumiamo che "?" sia usato SOLO come placeholder (nel tuo codice è così).
    s = s.replace("?", "%s")

    # 2) Fix mirato per endpoint /api/stats/daily (pattern tipico del tuo main)
    # SQLite:
    #   substr(ts, 1, 10) AS day
    #   WHERE ts >= datetime('now', '-' || ? || ' days')
    #
    # Postgres:
    #   to_char(ts::timestamptz, 'YYYY-MM-DD') AS day
    #   WHERE ts::timestamptz >= NOW() - (%s * INTERVAL '1 day')
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


# =========================
# Wrapper conn: stessa interfaccia per SQLite e Postgres
# =========================

class _DBConn:
    """
    Wrapper che espone .execute() come in sqlite3, anche quando sotto c’è Postgres.
    Così il tuo main.py può restare identico.
    """

    def __init__(self, backend: str, raw_conn: Any):
        self.backend = backend
        self.raw_conn = raw_conn
        self._closed = False

    def __enter__(self) -> "_DBConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Se tutto ok -> commit, altrimenti rollback
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
        """
        Esegue una query e ritorna un "cursor" con .fetchone()/.fetchall()
        - SQLite: sqlite cursor
        - Postgres: cursor con dict rows
        """
        if params is None:
            params = []

        if self.backend == "sqlite":
            return self.raw_conn.execute(sql, params)

        # Postgres
        sql_pg = _translate_sql_for_postgres(sql)

        # cursor con righe come dict (così row["capacity_g"] continua a funzionare)
        cur = self.raw_conn.cursor()
        cur.execute(sql_pg, params)
        return cur


# =========================
# Connessione
# =========================

def get_conn() -> _DBConn:
    """
    Ritorna una connessione “compatibile” con il tuo main:
    - con .execute()
    - usabile in "with get_conn() as conn:"
    """
    if DB_BACKEND == "sqlite":
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row  # permette row["colonna"]
        return _DBConn("sqlite", conn)

    # Postgres
    # Import qui dentro così in locale non serve installare psycopg2 se non vuoi
    import psycopg2
    import psycopg2.extras

    # Render spesso fornisce postgres://... e psycopg2 lo accetta
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    # commit/rollback li gestiamo nel __exit__
    return _DBConn("postgres", conn)


# =========================
# Init DB (tabelle)
# =========================

def init_db() -> None:
    """
    Crea le tabelle se non esistono.
    Usiamo SQL compatibile:
    - SQLite: tipi standard
    - Postgres: usiamo tipi equivalenti
    """
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
                    co2_saved_g DOUBLE PRECISION NOT NULL
                )
            """)
