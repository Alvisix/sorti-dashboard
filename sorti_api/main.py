from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from datetime import datetime, timezone, timedelta
import json
import os
import csv
import io
import time
import secrets
import asyncio
from pathlib import Path
from typing import Any, Optional

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
# Modelli dati (Pydantic)
# =========================

class EventIn(BaseModel):
    bin_id: str = Field(..., examples=["SORTI_001"])
    material: str = Field(..., examples=["plastica"])
    weight_g: float = Field(..., gt=0, examples=[18])

    # Step 10: AI-ready (opzionali)
    source: Optional[str] = Field(default=None, examples=["raspberry", "simulator"])
    model_version: Optional[str] = Field(default=None, examples=["mobilenet_v2_1.3"])
    confidence: Optional[float] = Field(default=None, ge=0, le=1, examples=[0.87])
    topk: Optional[list[dict[str, Any]]] = Field(
        default=None,
        examples=[[{"label": "plastica", "p": 0.87}, {"label": "metallo", "p": 0.09}]]
    )
    image_ref: Optional[str] = Field(default=None, examples=["frame_2026-01-18T10:22:11Z.jpg"])


class BinConfigIn(BaseModel):
    capacity_g: float = Field(..., gt=0, examples=[120000])


class BinKeyOut(BaseModel):
    bin_id: str
    ingest_key: str


# =========================
# APP FastAPI
# =========================
app = FastAPI(title="Sorti SmartBin Tracker")

# =========================
# Sicurezza: 2 chiavi separate
# =========================
ADMIN_KEY = os.getenv("SORTI_ADMIN_KEY", "SORTI-DEMO-KEY-BINARO-2026-01")
GLOBAL_INGEST_KEY = os.getenv("SORTI_INGEST_KEY", "SORTI-DEMO-KEY-BINARO-2026-01")
RATE_EVENTS_PER_MIN = int(os.getenv("SORTI_RATE_EVENTS_PER_MIN", "60"))


def require_admin_key(x_api_key: str | None) -> None:
    if x_api_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized (admin)")


def is_admin(x_api_key: str | None) -> bool:
    return (x_api_key == ADMIN_KEY)


# =========================
# Rate limiter semplice (in-memory)
# =========================
_rl: dict[str, list[float]] = {}

def rate_limit_or_429(key: str) -> None:
    now = time.time()
    window = 60.0
    limit = max(1, RATE_EVENTS_PER_MIN)

    arr = _rl.get(key, [])
    arr = [t for t in arr if (now - t) <= window]

    if len(arr) >= limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded (ingest)")

    arr.append(now)
    _rl[key] = arr


# =========================
# SSE: broker aggiornamenti
# =========================
_sse_clients: set[asyncio.Queue] = set()

def sse_publish(event: str = "update", data: dict | str | None = None) -> None:
    if data is None:
        payload = ""
    elif isinstance(data, str):
        payload = data
    else:
        payload = json.dumps(data, ensure_ascii=False)

    dead = []
    for q in list(_sse_clients):
        try:
            q.put_nowait((event, payload))
        except Exception:
            dead.append(q)
    for q in dead:
        _sse_clients.discard(q)


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
# ✅ SSE stream realtime (PUBBLICO)
# =========================
@app.get("/api/stream")
async def stream():
    async def event_generator():
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        _sse_clients.add(q)

        yield "event: hello\ndata: ok\n\n"

        try:
            while True:
                try:
                    event, payload = await asyncio.wait_for(q.get(), timeout=15.0)
                    if payload == "":
                        yield f"event: {event}\ndata: \n\n"
                    else:
                        yield f"event: {event}\ndata: {payload}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _sse_clients.discard(q)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# =========================
# Helper: daily aggregation
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
# Step 12: per-bin ingest key
# =========================
def resolve_bin_ingest_key(bin_id: str) -> str:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT ingest_key FROM bins WHERE bin_id=?",
            (bin_id,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Bin non trovato (configura prima il bin)")

    k = row.get("ingest_key") if isinstance(row, dict) else row["ingest_key"]
    if k and str(k).strip():
        return str(k).strip()
    return GLOBAL_INGEST_KEY


def require_ingest_for_bin(bin_id: str, x_ingest_key: str | None) -> None:
    expected = resolve_bin_ingest_key(bin_id)
    if x_ingest_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized (ingest/bin)")
    rate_limit_or_429(expected)


# =========================
# API: configura bin (ADMIN)
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
            INSERT INTO bins(bin_id, capacity_g, current_weight_g, last_seen, ingest_key)
            VALUES(?, ?, 0, ?, NULL)
            ON CONFLICT(bin_id) DO UPDATE SET
              capacity_g=excluded.capacity_g,
              last_seen=excluded.last_seen
        """, (bin_id, float(cfg.capacity_g), now))

    sse_publish("update", {"type": "config", "bin_id": bin_id, "ts": now})
    return {"ok": True, "bin_id": bin_id, "capacity_g": float(cfg.capacity_g)}


# =========================
# Step 12: ruota ingest key bin (ADMIN)
# =========================
@app.post("/api/bins/{bin_id}/rotate_ingest_key", response_model=BinKeyOut)
def rotate_ingest_key(
    bin_id: str,
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)

    new_key = "SORTI-BIN-" + secrets.token_urlsafe(24)
    now = datetime.now(timezone.utc).isoformat()

    with get_conn() as conn:
        exists = conn.execute(
            "SELECT bin_id FROM bins WHERE bin_id=?",
            (bin_id,)
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Bin non trovato (crealo prima con config)")

        conn.execute(
            "UPDATE bins SET ingest_key=?, last_seen=? WHERE bin_id=?",
            (new_key, now, bin_id)
        )

    sse_publish("update", {"type": "rotate_key", "bin_id": bin_id, "ts": now})
    return {"bin_id": bin_id, "ingest_key": new_key}


# =========================
# API: aggiungi evento (INGEST)
# =========================
@app.post("/api/event")
def add_event(
    ev: EventIn,
    x_ingest_key: str | None = Header(default=None),
):
    require_ingest_for_bin(ev.bin_id, x_ingest_key)

    factors = load_factors()
    material = ev.material.strip().lower()
    if material not in factors:
        raise HTTPException(status_code=400, detail=f"Materiale sconosciuto: {material}")

    factor = float(factors[material])
    co2_saved_g = float(ev.weight_g) * factor
    ts = datetime.now(timezone.utc).isoformat()

    topk_json = None
    if ev.topk is not None:
        try:
            topk_json = json.dumps(ev.topk, ensure_ascii=False)
        except Exception:
            topk_json = None

    event_id: int | None = None

    with get_conn() as conn:
        conn.execute("UPDATE bins SET last_seen=? WHERE bin_id=?", (ts, ev.bin_id))

        if conn.backend == "postgres":
            cur = conn.execute("""
                INSERT INTO events(
                  ts, bin_id, material, weight_g, co2_saved_g,
                  source, model_version, confidence, topk_json, image_ref
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """, (
                ts, ev.bin_id, material, float(ev.weight_g), co2_saved_g,
                ev.source, ev.model_version, ev.confidence, topk_json, ev.image_ref
            ))
            row_id = cur.fetchone()
            if row_id:
                # psycopg dict_row => {"id": ...} oppure tuple
                try:
                    event_id = int(row_id["id"])  # type: ignore[index]
                except Exception:
                    event_id = int(row_id[0])
        else:
            cur = conn.execute("""
                INSERT INTO events(
                  ts, bin_id, material, weight_g, co2_saved_g,
                  source, model_version, confidence, topk_json, image_ref
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ts, ev.bin_id, material, float(ev.weight_g), co2_saved_g,
                ev.source, ev.model_version, ev.confidence, topk_json, ev.image_ref
            ))
            try:
                event_id = int(cur.lastrowid)  # sqlite cursor
            except Exception:
                event_id = None

        conn.execute("""
            UPDATE bins
            SET current_weight_g = current_weight_g + ?, last_seen=?
            WHERE bin_id=?
        """, (float(ev.weight_g), ts, ev.bin_id))

        row = conn.execute(
            "SELECT capacity_g, current_weight_g, last_seen FROM bins WHERE bin_id=?",
            (ev.bin_id,)
        ).fetchone()

    capacity = float(row["capacity_g"])
    current = float(row["current_weight_g"])
    fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)

    # 🔥 SSE payload completo: evento + stato bin aggiornato
    sse_publish("update", {
        "type": "event",
        "ts": ts,
        "bin_id": ev.bin_id,
        "event": {
            "id": event_id,
            "ts": ts,
            "bin_id": ev.bin_id,
            "material": material,
            "weight_g": float(ev.weight_g),
            "co2_saved_g": co2_saved_g
        },
        "bin": {
            "bin_id": ev.bin_id,
            "capacity_g": capacity,
            "current_weight_g": current,
            "fill_percent": fill_percent,
            "last_seen": row["last_seen"]
        }
    })

    return {
        "ok": True,
        "id": event_id,
        "ts": ts,
        "bin_id": ev.bin_id,
        "material": material,
        "weight_g": float(ev.weight_g),
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
        {
            "material": r["material"],
            "weight_g": float(r["weight_g"]),
            "co2_saved_g": float(r["co2_saved_g"]),
        }
        for r in rows
    ]


# =========================
# API: report giornaliero (LIBERO)
# =========================
@app.get("/api/stats/daily")
def stats_daily(days: int = 30):
    return compute_daily(days)


# =========================
# ✅ recent events (ADMIN)
# =========================
@app.get("/api/events/recent")
def recent_events(
    limit: int = 20,
    x_api_key: str | None = Header(default=None),
):
    require_admin_key(x_api_key)
    limit = max(1, min(int(limit), 200))

    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, ts, bin_id, material, weight_g, co2_saved_g,
                   source, model_version, confidence, image_ref
            FROM events
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

    out = []
    for r in rows:
        out.append({
            "id": r["id"],
            "ts": r["ts"],
            "bin_id": r["bin_id"],
            "material": r["material"],
            "weight_g": float(r["weight_g"]),
            "co2_saved_g": float(r["co2_saved_g"]),
            "source": r.get("source") if isinstance(r, dict) else r["source"],
            "model_version": r.get("model_version") if isinstance(r, dict) else r["model_version"],
            "confidence": (float(r["confidence"]) if r.get("confidence") is not None else None) if isinstance(r, dict)
                          else (float(r["confidence"]) if r["confidence"] is not None else None),
            "image_ref": r.get("image_ref") if isinstance(r, dict) else r["image_ref"],
        })
    return out


# =========================
# ✅ dashboard aggregate (USATO DALLA UI)
# =========================
@app.get("/api/dashboard")
def dashboard(
    days: int = 30,
    x_api_key: str | None = Header(default=None),
):
    bins = list_bins()
    totals = stats_total()
    daily = stats_daily(days=days)
    mats = stats_by_material()

    admin = is_admin(x_api_key)
    recent = []
    if admin:
        recent = recent_events(limit=20, x_api_key=x_api_key)

    return {
        "ok": True,
        "days": max(1, min(int(days), 365)),
        "is_admin": admin,
        "totals": totals,
        "bins": bins,
        "daily": daily,
        "by_material": mats,
        "recent_events": recent,
        "ts": datetime.now(timezone.utc).isoformat()
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
            SELECT ts, bin_id, material, weight_g, co2_saved_g
            FROM events
            ORDER BY ts ASC
        """).fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts", "bin_id", "material", "weight_g", "co2_saved_g"])
    for r in rows:
        w.writerow([r["ts"], r["bin_id"], r["material"], r["weight_g"], r["co2_saved_g"]])

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
    w.writerow(["day", "total_weight_g", "total_co2_saved_g"])
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

        row = conn.execute(
            "SELECT capacity_g, current_weight_g, last_seen FROM bins WHERE bin_id=?",
            (bin_id,)
        ).fetchone()

    capacity = float(row["capacity_g"])
    current = float(row["current_weight_g"])
    fill_percent = 0.0 if capacity <= 0 else min(100.0, (current / capacity) * 100.0)

    sse_publish("update", {
        "type": "empty",
        "bin_id": bin_id,
        "ts": now,
        "bin": {
            "bin_id": bin_id,
            "capacity_g": capacity,
            "current_weight_g": current,
            "fill_percent": fill_percent,
            "last_seen": row["last_seen"]
        }
    })

    return {"ok": True, "bin_id": bin_id, "emptied_at": now}










