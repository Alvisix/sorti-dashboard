import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "sorti.db"

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with get_conn() as conn:
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