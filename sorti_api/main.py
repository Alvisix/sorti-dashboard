from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from datetime import datetime, timezone, timedelta
import json
import os
import csv
import io
import uuid
from pathlib import Path

from .db import init_db, get_conn

# =========================
# Percorsi del progetto
# =========================

APP_DIR = Path(__file__).resolve().parent
FACTORS_PATH = APP_DIR / "co2_factors.json"
STATIC_DIR = APP_DIR / "static"
INDEX_HTML = STATIC_DIR / "index.html"


def load_factors() -> dict:
    if not FACTORS_PATH.exists():
        raise RuntimeError(f"Manca il file fattori CO2: {FACTORS_PATH}")
    return json.loads(FACTORS_PATH.read_text(encoding="utf-8"))


# =========================
# Material aliases
# =========================
MATERIAL_ALIASES = {
    "plastic": "plastica",
    "plastico": "plastica",
    "pet": "plastica",
    "bottiglia_plastica": "plastica",
    "bottle_plastic": "plastica",
    "paper": "carta",
    "cartone": "carta",
    "cardboard": "carta",
    "glass": "vetro",
    "bottiglia_vetro": "vetro",
    "metal": "metallo",
    "alu": "metallo",
    "alluminio": "metallo",
    "acciaio": "metallo",
    "lattina": "metallo",
    "can": "metallo",
    "organic": "organico",
    "umido": "organico",
}


def normalize_material(s: str) -> str:
    m = (s or "").strip().lower()
    m = m.replace("-", "_").replace(" ", "_")
    return MATERIAL_ALIASES.get(m, m)


# =========================
# Modelli dati (Pydantic)
# =========================

class EventIn(BaseModel):
    bin_id: str = Field(..., examples=["SORTI_001"])
    material: str = Field(..., examples=["plastica"])
    weight_g: float = Field(..., gt=0, examples=[18])
    event_id: str | None = Field(default=None, examples=["2f9c0a7e-2d3c-4c21-9d2c-45cf6c2b2c3e"])


class BinConfigIn(BaseModel):
    capacity_g: float = Field(..., gt=0, examples=[120000])


# =========================
# APP FastAPI
# =========================

app = FastAPI(title="Sorti SmartBin Tracker")

# =========================
# Sicurezza: 2 chiavi separate
# =========================
ADMIN_KEY = os.getenv("SORTI_ADMIN_KEY", "SORTI-DEMO-KEY-BINARO-2026-01")
INGEST_KEY = os.getenv("SORTI_INGEST_KEY", "SORTI-DEMO-KEY-BINARO-2026-01")


def require_admin_key(x_api_key: str | None) -> None:
    if x_api_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized (admin)")


def require_ingest_key(x_ingest_key: str | None) -> None:
    if x_ingest_key != INGEST_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized (ingest)")


def is_admin_key_valid(x_api_key: str | None) -> bool:
    return x_api_key == ADMIN_KEY


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
        return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))

    return HTMLResponse(
        "<h2>Sorti server attivo ✅</h2>"
        "<p>Non trovo <code>sorti_api/static/index.html</code>. "
        "Crea la dashboard oppure usa <a href='/docs'>/docs</a>.</p>"
    )


# =========================
# Healthcheck
# =========================

@app.get("/health")
def health():
    with get_conn() as conn:
        conn.execute("SELECT 1").fetchone()
    return {"ok": True}


# =========================
# Helper: daily aggregation DB-agnostico
# =========================

def compute_daily(days: int) -> list[dict]:
    days = max(1, min(int(days), 365))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_iso = cutoff.isoformat()

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT ts, weight_g, co2_saved_g
            FROM events
            WHERE ts >= ?
            ORDER BY ts ASC
        """, (cutoff_iso,)).fetchall()

    agg: dict[str, dict] = {}
    for r in rows:
        ts_val = r["ts"]
        if isinstance(ts_val, datetime):
            day = ts_val.date().isoformat()
        else:
            day = str(ts_val)[:10]

        w = float(r["weight_g"] or 0)
        c = float(r["co2_saved_g"] or 0)

        if day not in agg:
            agg[day] = {"day": day, "weight_g": 0.0, "co2_saved_g": 0.0}

        agg[day]["weight_g"] += w
        agg[day]["co2_saved_g"] += c

    return [agg[k] for k in sorted(agg.keys())]


# =========================
# API: configura cestino (ADMIN)
# =========================

@app.post("/api/bins/{bin_id}/config")
def set_bin_config(
    bin_id: str,
    cfg: BinConfigIn,
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)
    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO bins(bin_id, capacity_g, current_weight_g, last_seen)
            VALUES(?, ?, 0, ?)
            ON CONFLICT(bin_id) DO UPDATE SET
              capacity_g=excluded.capacity_g,
              last_seen=excluded.last_seen
        """, (bin_id, float(cfg.capacity_g), now))

    return {"ok": True, "bin_id": bin_id, "capacity_g": float(cfg.capacity_g)}


# =========================
# API: aggiungi evento (INGEST)
# =========================

@app.post("/api/event")
def add_event(
    ev: EventIn,
    x_ingest_key: str | None = Header(default=None),
):
    require_ingest_key(x_ingest_key)

    w_in = float(ev.weight_g)
    if not (0 < w_in <= 5000):
        raise HTTPException(status_code=400, detail="weight_g fuori range (1..5000)")

    factors = load_factors()
    material = normalize_material(ev.material)

    if material not in factors:
        raise HTTPException(status_code=400, detail=f"Materiale sconosciuto: {material}")

    factor = float(factors[material])
    co2_saved_g = w_in * factor
    ts = datetime.now(timezone.utc).isoformat()

    event_id = (ev.event_id or "").strip() or str(uuid.uuid4())

    duplicate = False

    with get_conn() as conn:
        conn.execute("""
            INSERT INTO bins(bin_id, capacity_g, current_weight_g, last_seen)
            VALUES(?, 10000, 0, ?)
            ON CONFLICT(bin_id) DO UPDATE SET last_seen=excluded.last_seen
        """, (ev.bin_id, ts))

        try:
            conn.execute("""
                INSERT INTO events(ts, bin_id, material, weight_g, co2_saved_g, event_id)
                VALUES(?, ?, ?, ?, ?, ?)
            """, (ts, ev.bin_id, material, w_in, co2_saved_g, event_id))

            conn.execute("""
                UPDATE bins
                SET current_weight_g = current_weight_g + ?, last_seen=?
                WHERE bin_id=?
            """, (w_in, ts, ev.bin_id))

        except Exception as e:
            msg = str(e).lower()
            if "unique" in msg and "event" in msg and "event_id" in msg:
                duplicate = True
                conn.execute("UPDATE bins SET last_seen=? WHERE bin_id=?", (ts, ev.bin_id))
            else:
                raise

        row = conn.execute(
            "SELECT capacity_g, current_weight_g FROM bins WHERE bin_id=?",
            (ev.bin_id,)
        ).fetchone()

    capacity = float(row["capacity_g"])
    current = float(row["current_weight_g"])
    fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)

    return {
        "ok": True,
        "duplicate": duplicate,
        "event_id": event_id,
        "ts": ts,
        "bin_id": ev.bin_id,
        "material": material,
        "weight_g": w_in,
        "factor_gco2_per_g": factor,
        "co2_saved_g": co2_saved_g,
        "bin": {"capacity_g": capacity, "current_weight_g": current, "fill_percent": fill_percent}
    }


# =========================
# API: lista bins (LIBERO)
# =========================

@app.get("/api/bins")
def list_bins():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT bin_id, capacity_g, current_weight_g, last_seen
            FROM bins
            ORDER BY bin_id
        """).fetchall()

    out = []
    for r in rows:
        capacity = float(r["capacity_g"])
        current = float(r["current_weight_g"])
        fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)

        out.append({
            "bin_id": r["bin_id"],
            "capacity_g": capacity,
            "current_weight_g": current,
            "fill_percent": fill_percent,
            "last_seen": r["last_seen"]
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
# API: report per materiale (LIBERO)
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
        {"material": r["material"], "weight_g": float(r["weight_g"]), "co2_saved_g": float(r["co2_saved_g"])}
        for r in rows
    ]


# =========================
# API: report giornaliero (LIBERO)
# =========================

@app.get("/api/stats/daily")
def stats_daily(days: int = 30):
    return compute_daily(days)


# =========================
# API: ultimi eventi (ADMIN)
# =========================

@app.get("/api/events/recent")
def recent_events(
    limit: int = 20,
    bin_id: str | None = None,
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)
    limit = max(1, min(int(limit), 200))

    with get_conn() as conn:
        if bin_id:
            rows = conn.execute("""
                SELECT ts, bin_id, material, weight_g, co2_saved_g, event_id
                FROM events
                WHERE bin_id = ?
                ORDER BY ts DESC
                LIMIT ?
            """, (bin_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT ts, bin_id, material, weight_g, co2_saved_g, event_id
                FROM events
                ORDER BY ts DESC
                LIMIT ?
            """, (limit,)).fetchall()

    return [
        {
            "ts": r["ts"],
            "bin_id": r["bin_id"],
            "material": r["material"],
            "weight_g": float(r["weight_g"]),
            "co2_saved_g": float(r["co2_saved_g"]),
            "event_id": r["event_id"],
        }
        for r in rows
    ]


# =========================
# ✅ Step 6: endpoint unico dashboard
# =========================

@app.get("/api/dashboard")
def dashboard(
    days: int = 30,
    events_limit: int = 20,
    x_api_key: str | None = Header(default=None),
):
    days = max(1, min(int(days), 365))
    events_limit = max(1, min(int(events_limit), 200))

    is_admin = is_admin_key_valid(x_api_key)

    # daily
    daily = compute_daily(days)

    # totals + by_material + bins + recent_events in 1 conn
    with get_conn() as conn:
        # totals
        trow = conn.execute("""
            SELECT
              COALESCE(SUM(weight_g), 0) AS total_weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS total_co2_saved_g
            FROM events
        """).fetchone()

        totals = {
            "total_weight_g": float(trow["total_weight_g"]),
            "total_co2_saved_g": float(trow["total_co2_saved_g"]),
        }

        # by material
        mats = conn.execute("""
            SELECT
              material,
              COALESCE(SUM(weight_g), 0) AS weight_g,
              COALESCE(SUM(co2_saved_g), 0) AS co2_saved_g
            FROM events
            GROUP BY material
            ORDER BY weight_g DESC
        """).fetchall()

        by_material = [
            {"material": r["material"], "weight_g": float(r["weight_g"]), "co2_saved_g": float(r["co2_saved_g"])}
            for r in mats
        ]

        # bins
        bvals = conn.execute("""
            SELECT bin_id, capacity_g, current_weight_g, last_seen
            FROM bins
            ORDER BY bin_id
        """).fetchall()

        bins = []
        for r in bvals:
            capacity = float(r["capacity_g"])
            current = float(r["current_weight_g"])
            fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)
            bins.append({
                "bin_id": r["bin_id"],
                "capacity_g": capacity,
                "current_weight_g": current,
                "fill_percent": fill_percent,
                "last_seen": r["last_seen"],
            })

        # recent events only if admin
        recent = []
        if is_admin:
            rows = conn.execute("""
                SELECT ts, bin_id, material, weight_g, co2_saved_g, event_id
                FROM events
                ORDER BY ts DESC
                LIMIT ?
            """, (events_limit,)).fetchall()

            recent = [
                {
                    "ts": r["ts"],
                    "bin_id": r["bin_id"],
                    "material": r["material"],
                    "weight_g": float(r["weight_g"]),
                    "co2_saved_g": float(r["co2_saved_g"]),
                    "event_id": r["event_id"],
                }
                for r in rows
            ]

    return {
        "server_time": datetime.now(timezone.utc).isoformat(),
        "days": days,
        "events_limit": events_limit,
        "is_admin": is_admin,
        "totals": totals,
        "daily": daily,
        "by_material": by_material,
        "bins": bins,
        "recent_events": recent,
    }


# =========================
# Export CSV eventi (ADMIN)
# =========================

@app.get("/api/export/events.csv")
def export_events_csv(
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT ts, bin_id, material, weight_g, co2_saved_g, event_id
            FROM events
            ORDER BY ts ASC
        """).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts", "bin_id", "material", "weight_g", "co2_saved_g", "event_id"])
    for r in rows:
        w.writerow([r["ts"], r["bin_id"], r["material"], r["weight_g"], r["co2_saved_g"], r["event_id"]])

    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=sorti_events.csv"},
    )


# =========================
# Export CSV giornaliero (ADMIN)
# =========================

@app.get("/api/export/daily.csv")
def export_daily_csv(
    days: int = 30,
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)
    rows = compute_daily(days)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["day", "weight_g", "co2_saved_g"])
    for r in rows:
        w.writerow([r["day"], r["weight_g"], r["co2_saved_g"]])

    d = max(1, min(int(days), 365))
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=sorti_daily_{d}d.csv"},
    )


# =========================
# Svuota bin (ADMIN)
# =========================

@app.post("/api/bins/{bin_id}/empty")
def empty_bin(
    bin_id: str,
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)

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






