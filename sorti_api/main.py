from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import json
import os
import csv
import io
from pathlib import Path

from .db import init_db, get_conn

# =========================
# Percorsi del progetto
# =========================

# Cartella dove si trova questo file (sorti_api/)
APP_DIR = Path(__file__).resolve().parent

# File dei fattori CO2 (sorti_api/co2_factors.json)
FACTORS_PATH = APP_DIR / "co2_factors.json"

# Cartella della dashboard statica (sorti_api/static/)
STATIC_DIR = APP_DIR / "static"

# File HTML principale della dashboard
INDEX_HTML = STATIC_DIR / "index.html"


def load_factors() -> dict:
    """
    Carica i fattori CO2 da co2_factors.json.
    Ritorna un dict tipo: {"plastica": 2.5, "carta": 1.3, ...}
    """
    if not FACTORS_PATH.exists():
        raise RuntimeError(f"Manca il file fattori CO2: {FACTORS_PATH}")
    return json.loads(FACTORS_PATH.read_text(encoding="utf-8"))


# =========================
# Modelli dati (Pydantic)
# =========================

class EventIn(BaseModel):
    bin_id: str = Field(..., examples=["SORTI_001"])
    material: str = Field(..., examples=["plastica"])
    weight_g: float = Field(..., gt=0, examples=[18])


class BinConfigIn(BaseModel):
    capacity_g: float = Field(..., gt=0, examples=[120000])


# =========================
# APP FastAPI
# =========================

app = FastAPI(title="Sorti SmartBin Tracker")

# =========================
# Sicurezza: API KEY
# =========================
API_KEY = os.getenv("SORTI_API_KEY", "SORTI-DEMO-KEY-BINARO-2026-01")


def require_api_key(x_api_key: str | None) -> None:
    """Blocca la richiesta se la chiave non è corretta."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# =========================
# Static files (dashboard)
# =========================

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# =========================
# Startup: inizializza DB
# =========================

@app.on_event("startup")
def _startup():
    init_db()


# =========================
# Pagina principale
# =========================

@app.get("/", response_class=HTMLResponse)
def home():
    if INDEX_HTML.exists():
        html = INDEX_HTML.read_text(encoding="utf-8")
        return HTMLResponse(html)

    return HTMLResponse(
        "<h2>Sorti server attivo ✅</h2>"
        "<p>Non trovo <code>sorti_api/static/index.html</code>. "
        "Crea la dashboard oppure usa <a href='/docs'>/docs</a> per testare le API.</p>"
    )


# =========================
# API: configura cestino (PROTETTO)
# =========================

@app.post("/api/bins/{bin_id}/config")
def set_bin_config(
    bin_id: str,
    cfg: BinConfigIn,
    x_api_key: str | None = Header(default=None),
):
    require_api_key(x_api_key)

    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO bins(bin_id, capacity_g, current_weight_g, last_seen)
            VALUES(?, ?, 0, ?)
            ON CONFLICT(bin_id) DO UPDATE SET
              capacity_g=excluded.capacity_g,
              last_seen=excluded.last_seen
        """, (bin_id, float(cfg.capacity_g), now))

    return {"ok": True, "bin_id": bin_id, "capacity_g": cfg.capacity_g}


# =========================
# API: aggiungi evento (PROTETTO)
# =========================

@app.post("/api/event")
def add_event(
    ev: EventIn,
    x_api_key: str | None = Header(default=None),
):
    require_api_key(x_api_key)

    factors = load_factors()
    material = ev.material.strip().lower()

    if material not in factors:
        raise HTTPException(status_code=400, detail=f"Materiale sconosciuto: {material}")

    factor = float(factors[material])
    co2_saved_g = float(ev.weight_g) * factor
    ts = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO bins(bin_id, capacity_g, current_weight_g, last_seen)
            VALUES(?, 10000, 0, ?)
            ON CONFLICT(bin_id) DO UPDATE SET last_seen=excluded.last_seen
        """, (ev.bin_id, ts))

        conn.execute("""
            INSERT INTO events(ts, bin_id, material, weight_g, co2_saved_g)
            VALUES(?, ?, ?, ?, ?)
        """, (ts, ev.bin_id, material, float(ev.weight_g), co2_saved_g))

        conn.execute("""
            UPDATE bins
            SET current_weight_g = current_weight_g + ?, last_seen=?
            WHERE bin_id=?
        """, (float(ev.weight_g), ts, ev.bin_id))

        row = conn.execute(
            "SELECT capacity_g, current_weight_g FROM bins WHERE bin_id=?",
            (ev.bin_id,)
        ).fetchone()

    capacity = float(row["capacity_g"])
    current = float(row["current_weight_g"])
    fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)

    return {
        "ok": True,
        "ts": ts,
        "bin_id": ev.bin_id,
        "material": material,
        "weight_g": ev.weight_g,
        "factor_gco2_per_g": factor,
        "co2_saved_g": co2_saved_g,
        "bin": {
            "capacity_g": capacity,
            "current_weight_g": current,
            "fill_percent": fill_percent
        }
    }


# =========================
# API: lista bins (LIBERO)
# =========================

@app.get("/api/bins")
def list_bins():
    with get_conn() as conn:
        bins = conn.execute("""
            SELECT bin_id, capacity_g, current_weight_g, last_seen
            FROM bins
            ORDER BY bin_id
        """).fetchall()

    out = []
    for b in bins:
        capacity = float(b["capacity_g"])
        current = float(b["current_weight_g"])
        fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)

        out.append({
            "bin_id": b["bin_id"],
            "capacity_g": capacity,
            "current_weight_g": current,
            "fill_percent": fill_percent,
            "last_seen": b["last_seen"]
        })

    return out


# =========================
# API: statistiche totali (LIBERO)
# =========================

@app.get("/api/stats/total")
def stats_total():
    with get_conn() as conn:
        row = conn.execute("""
            SELECT
              COALESCE(SUM(weight_g), 0) AS total_weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS total_co2_saved_g
            FROM events
        """).fetchone()

    return {
        "total_weight_g": float(row["total_weight_g"]),
        "total_co2_saved_g": float(row["total_co2_saved_g"])
    }


# =========================
# Upgrade 3: report per materiale (LIBERO)
# =========================

@app.get("/api/stats/by_material")
def stats_by_material():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
              material,
              COALESCE(SUM(weight_g), 0) AS weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS co2_saved_g
            FROM events
            GROUP BY material
            ORDER BY weight_g DESC
        """).fetchall()

    return [
        {
            "material": r["material"],
            "weight_g": float(r["weight_g"]),
            "co2_saved_g": float(r["co2_saved_g"]),
        }
        for r in rows
    ]


# =========================
# Upgrade 3: report giornaliero (LIBERO)
# =========================

@app.get("/api/stats/daily")
def stats_daily(days: int = 30):
    days = max(1, min(int(days), 365))

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
              substr(ts, 1, 10) AS day,
              COALESCE(SUM(weight_g), 0) AS weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS co2_saved_g
            FROM events
            WHERE ts >= datetime('now', '-' || ? || ' days')
            GROUP BY day
            ORDER BY day ASC
        """, (days,)).fetchall()

    return [
        {
            "day": r["day"],
            "weight_g": float(r["weight_g"]),
            "co2_saved_g": float(r["co2_saved_g"]),
        }
        for r in rows
    ]


# =========================
# Export CSV (PROTETTO) - compatibile Excel IT
#   - separatore: ;
#   - BOM UTF-8: per Excel
# =========================

@app.get("/api/export/events.csv")
def export_events_csv(x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT ts, bin_id, material, weight_g, co2_saved_g
            FROM events
            ORDER BY ts ASC
        """).fetchall()

    def gen():
        buf = io.StringIO()

        # BOM UTF-8 (Excel)
        yield "\ufeff"

        w = csv.writer(buf, delimiter=";")

        w.writerow(["ts", "bin_id", "material", "weight_g", "co2_saved_g"])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        for r in rows:
            w.writerow([
                r["ts"],
                r["bin_id"],
                r["material"],
                float(r["weight_g"]),
                float(r["co2_saved_g"]),
            ])
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return StreamingResponse(
        gen(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sorti_events.csv"},
    )


@app.get("/api/export/daily.csv")
def export_daily_csv(days: int = 30, x_api_key: str | None = Header(default=None)):
    require_api_key(x_api_key)
    days = max(1, min(int(days), 365))

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT
              substr(ts, 1, 10) AS day,
              COALESCE(SUM(weight_g), 0) AS total_weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS total_co2_saved_g
            FROM events
            WHERE ts >= datetime('now', '-' || ? || ' days')
            GROUP BY day
            ORDER BY day ASC
        """, (days,)).fetchall()

    def gen():
        buf = io.StringIO()

        # BOM UTF-8 (Excel)
        yield "\ufeff"

        w = csv.writer(buf, delimiter=";")

        w.writerow(["day", "total_weight_g", "total_co2_saved_g"])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        for r in rows:
            w.writerow([
                r["day"],
                float(r["total_weight_g"]),
                float(r["total_co2_saved_g"]),
            ])
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return StreamingResponse(
        gen(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=sorti_daily_{days}d.csv"},
    )


# =========================
# API: svuota bin (PROTETTO)
# =========================

@app.post("/api/bins/{bin_id}/empty")
def empty_bin(
    bin_id: str,
    x_api_key: str | None = Header(default=None),
):
    require_api_key(x_api_key)

    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        exists = conn.execute(
            "SELECT bin_id FROM bins WHERE bin_id=?",
            (bin_id,)
        ).fetchone()

        if not exists:
            raise HTTPException(status_code=404, detail="Bin non trovato")

        conn.execute(
            "UPDATE bins SET current_weight_g=0, last_seen=? WHERE bin_id=?",
            (now, bin_id)
        )

    return {"ok": True, "bin_id": bin_id, "emptied_at": now}
