(() => {
  const API_BASE = (window.SORTI_API_BASE || "").replace(/\/+$/, "");
  const apiUrl = (path) => API_BASE ? `${API_BASE}${path}` : path;

    const $ = (id) => document.getElementById(id);

  const LS_DEV = "SORTI_DEV_MODE";
  const isDevMode = () => (localStorage.getItem(LS_DEV) === "1");

  let refreshing = false;
  let lastBinsSig = "";
  let lastEventsSig = "";

  let es = null;
  let pollTimer = null;
  let lastUpdateTs = 0;

  let refreshTimer = null;
  let pendingRefresh = false;

  let lastIsAdmin = false;

  const LS_THRESH = "sorti_thresholds_v1";
  let thresholds = { warn: 70, critical: 85 };

  const LS_ADMIN = "sorti_admin_key";
  const LS_INGEST = "sorti_ingest_key";
  const adminKey = () => localStorage.getItem(LS_ADMIN) || "";
  const ingestKey = () => localStorage.getItem(LS_INGEST) || "";

  let drawerOpen = false;
  let activeBinId = null;
  let ddLineChart = null;
  let ddPieChart = null;

  function safeText(id, txt) {
    const el = $(id);
    if (el) el.textContent = txt;
  }

  function safeHTML(id, html) {
    const el = $(id);
    if (el) el.innerHTML = html;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function loadThresholds() {
    try {
      const raw = localStorage.getItem(LS_THRESH);
      if (!raw) return;
      const obj = JSON.parse(raw);
      const w = Number(obj?.warn);
      const c = Number(obj?.critical);
      if (isFinite(w) && isFinite(c) && w > 0 && c > 0 && w < c && c <= 100) {
        thresholds = { warn: w, critical: c };
      }
    } catch { }
  }

  function saveThresholds() {
    localStorage.setItem(LS_THRESH, JSON.stringify(thresholds));
  }

  function updateThresholdLabels() {
    safeText("warnLabel", `${thresholds.warn}%`);
    safeText("critLabel", `${thresholds.critical}%`);
  }

  function setConnState(state) {
    const dot = $("connDot");
    if (!dot) return;
    dot.classList.remove("warn", "bad");
    if (state === "polling") dot.classList.add("warn");
    if (state === "offline") dot.classList.add("bad");
  }

  function setLive(text, state) {
    safeText("liveText", text);
    if (state) setConnState(state);
  }

  function showError(msg) {
    const err = $("errBox");
    if (err) {
      err.style.display = "block";
      err.textContent = msg;
    }
    setLive("Offline", "offline");
  }

  function clearError() {
    const err = $("errBox");
    if (err) {
      err.style.display = "none";
      err.textContent = "";
    }
  }

  function formatWeight(g) {
    const n = Number(g || 0);
    if (!isFinite(n)) return "—";
    if (n >= 1000) {
      const kg = n / 1000;
      const txt = (kg < 100 ? kg.toFixed(1) : Math.round(kg).toString());
      return txt + " kg";
    }
    return Math.round(n) + " g";
  }

  const formatCO2 = formatWeight;

  function numOrNull(v) {
    const n = Number(v);
    return isFinite(n) ? n : null;
  }

  function clampPct(n) {
    return Math.max(0, Math.min(100, Number(n || 0)));
  }

  function isoToDate(s) {
    if (!s) return null;
    const d = new Date(s);
    if (isNaN(d.getTime())) return null;
    return d;
  }

  function isoToNice(s) {
    const d = isoToDate(s);
    if (!d) return (s || "—");
    return d.toLocaleString();
  }

  function timeAgo(iso) {
    const d = isoToDate(iso);
    if (!d) return "—";
    const now = new Date();
    let diff = Math.floor((now - d) / 1000);
    if (!isFinite(diff)) return "—";
    if (diff < 0) diff = 0;

    if (diff < 10) return "adesso";
    if (diff < 60) return `${diff}s fa`;
    const m = Math.floor(diff / 60);
    if (m < 60) return `${m} min fa`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h} h fa`;
    const days = Math.floor(h / 24);
    if (days === 1) return "ieri";
    if (days < 7) return `${days} gg fa`;
    const w = Math.floor(days / 7);
    if (w < 5) return `${w} sett fa`;
    const mo = Math.floor(days / 30);
    if (mo < 12) return `${mo} mesi fa`;
    const y = Math.floor(days / 365);
    return `${y} anni fa`;
  }

  function shortId(s) {
    if (!s) return "—";
    const str = String(s);
    if (str.length <= 10) return str;
    return str.slice(0, 8) + "…" + str.slice(-6);
  }

  function getCompartments(obj) {
    const arr = obj?.compartments;
    return Array.isArray(arr) ? arr : [];
  }

  function getSensorFillPercent(obj) {
    if (!obj || typeof obj !== "object") return null;
    const keys = ["sensor_fill_percent", "fill_percent_sensor", "ultrasonic_fill_percent"];
    for (const k of keys) {
      const n = numOrNull(obj?.[k]);
      if (n !== null) return clampPct(n);
    }
    return null;
  }

  function getSensorState(obj) {
    const keys = ["sensor_level_state", "level_state", "ultrasonic_state"];
    for (const k of keys) {
      const v = obj?.[k];
      if (typeof v === "string" && v.trim()) return v.trim().toUpperCase();
    }
    return null;
  }

  function getSensorDistanceCm(obj) {
    const keys = ["sensor_distance_cm", "distance_cm", "ultrasonic_distance_cm"];
    for (const k of keys) {
      const n = numOrNull(obj?.[k]);
      if (n !== null) return n;
    }
    return null;
  }

  function getFillSource(obj) {
    return getCompartments(obj).length > 0 ? "sensor" : "sensor";
  }

  function getFillPercent(obj) {
    const comps = getCompartments(obj);

    if (comps.length > 0) {
      let num = 0;
      let den = 0;
      for (const c of comps) {
        const fillRaw = getSensorFillPercent(c);
        const fill = fillRaw !== null ? clampPct(fillRaw) : 0;
        const cap = numOrNull(c?.capacity_percent);
        if (cap !== null && cap > 0) {
          num += fill * cap;
          den += cap;
        }
      }
      if (den > 0) return clampPct(num / den);
      return 0;
    }

    const sensor = getSensorFillPercent(obj);
    if (sensor !== null) return clampPct(sensor);

    return 0;
  }

  function formatDistanceCm(v) {
    const n = numOrNull(v);
    return n === null ? "—" : `${n.toFixed(1)} cm`;
  }

  function countCompartmentStates(obj) {
    const comps = getCompartments(obj);
    let full = 0, med = 0, empty = 0, fail = 0;
    for (const c of comps) {
      const st = getSensorState(c);
      if (st === "FULL") full++;
      else if (st === "MEDIUM") med++;
      else if (st === "EMPTY") empty++;
      else fail++;
    }
    return { full, med, empty, fail, total: comps.length };
  }

  async function fetchJSON(url, opts = {}) {
    const res = await fetch(url, opts);
    const text = await res.text();
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}\n${text}`);
    try { return JSON.parse(text); }
    catch { throw new Error(`Risposta non-JSON da ${url}\n${text}`); }
  }

  async function downloadFile(url, filename, headers) {
    const res = await fetch(url, { headers });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(`${res.status} ${res.statusText}\n${t}`);
    }
    const blob = await res.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  }

  async function setCapacityKg(binId, currentCapacityG) {
    try {
      if (!adminKey()) return alert("Manca Admin key (X-API-Key).");

      const curKg = (Number(currentCapacityG || 0) / 1000);
      const proposed = (isFinite(curKg) && curKg > 0) ? curKg.toFixed(curKg < 100 ? 1 : 0) : "";
      const input = prompt(`Imposta capacità per ${binId} (in kg):`, proposed);
      if (input === null) return;

      const kg = Number(String(input).replace(",", ".").trim());
      if (!isFinite(kg) || kg <= 0) return alert("Valore non valido. Inserisci un numero > 0 (kg).");
      if (kg > 10000) return alert("Valore troppo alto. Controlla (kg).");

      const capacity_g = Math.round(kg * 1000);

      await fetchJSON(apiUrl(`/api/bins/${encodeURIComponent(binId)}/config`), {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-Key": adminKey() },
        body: JSON.stringify({ capacity_g })
      });

      await refresh();
      if (drawerOpen && activeBinId === binId) await openBinDrawer(binId, { soft: true });
    } catch (e) {
      alert("Errore modifica capacità:\n\n" + (e?.message || String(e)));
    }
  }
  window.setCapacityKg = setCapacityKg;

  async function emptyBin(binId) {
    try {
      if (!adminKey()) return alert("Manca Admin key (X-API-Key).");
      const ok = confirm(`Confermi svuotamento ${binId}?\n(Azzera solo il peso bin, non cancella eventi)`);
      if (!ok) return;

      await fetchJSON(apiUrl(`/api/bins/${encodeURIComponent(binId)}/empty`), {
        method: "POST",
        headers: { "X-API-Key": adminKey() }
      });

      await refresh();
      if (drawerOpen && activeBinId === binId) await openBinDrawer(binId, { soft: true });
    } catch (e) {
      alert("Errore svuotamento:\n\n" + (e?.message || String(e)));
    }
  }
  window.emptyBin = emptyBin;

  function barColor(fill) {
    const f = Number(fill || 0);
    if (f >= thresholds.critical) return "linear-gradient(90deg, rgba(255,93,93,.95), rgba(255,140,73,.95))";
    if (f >= thresholds.warn) return "linear-gradient(90deg, rgba(247,201,72,.95), rgba(255,140,73,.85))";
    return "linear-gradient(90deg, rgba(67,206,162,.95), rgba(24,90,157,.90))";
  }

  function fillBadge(fill) {
    const f = Number(fill || 0);
    if (f >= thresholds.critical) return `<span class="badge bBAD">CRITICO</span>`;
    if (f >= thresholds.warn) return `<span class="badge bWARN">ATTENZIONE</span>`;
    return `<span class="badge bOK">OK</span>`;
  }

  function priorityOf(fill) {
    const f = Number(fill || 0);
    if (f >= thresholds.critical) return 2;
    if (f >= thresholds.warn) return 1;
    return 0;
  }

  function sensorMetaHTML(obj) {
    const source = getFillSource(obj);
    const comps = getCompartments(obj);
    const s = countCompartmentStates(obj);

    const parts = [];
    parts.push(source === "sensor" ? "Fonte: sensori" : "Fonte: stima");
    if (comps.length > 0) {
      parts.push(`Scomparti: ${comps.length}`);
      parts.push(`FULL ${s.full} • MED ${s.med} • EMP ${s.empty}`);
    }
    return `<div class="sensorMeta">${parts.join(" • ")}</div>`;
  }

  function compactCompartmentsHTML(obj) {
    const comps = getCompartments(obj)
      .slice()
      .sort((a, b) => Number(a?.compartment_id || 0) - Number(b?.compartment_id || 0));

    if (!comps.length) {
      return `
        <div style="margin-top:8px; color:rgba(255,255,255,.58); font-size:12px;">
          Nessun ultimo livello sensore disponibile.
        </div>
      `;
    }

    return `
      <div style="margin-top:10px; display:grid; gap:8px;">
        ${comps.map(c => {
          const fill = getSensorFillPercent(c);
          const state = getSensorState(c) || "—";
          const dist = getSensorDistanceCm(c);
          const label = c?.label || `Scomparto ${c?.compartment_id ?? "—"}`;
          const pct = fill !== null ? `${Math.round(fill)}%` : "—";
          const barWidth = Math.max(0, Math.min(100, fill ?? 0));

          let stateClass = "bOK";
          if (state === "FULL") stateClass = "bBAD";
          else if (state === "MEDIUM") stateClass = "bWARN";

          return `
            <div style="border:1px solid rgba(255,255,255,.08); border-radius:14px; padding:8px 10px; background:rgba(255,255,255,.03);">
              <div style="display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap:wrap;">
                <div style="font-size:12.5px; font-weight:900;">${escapeHtml(label)}</div>
                <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                  <span class="badge ${stateClass}">${escapeHtml(state)}</span>
                  <span class="fillPill">${escapeHtml(pct)}</span>
                </div>
              </div>
              <div style="margin-top:6px; color:rgba(255,255,255,.62); font-size:12px;">
                Distanza: <b>${escapeHtml(formatDistanceCm(dist))}</b>
              </div>
              <div style="margin-top:8px; height:10px; width:100%; border-radius:999px; border:1px solid rgba(255,255,255,.10); background:rgba(255,255,255,.05); overflow:hidden;">
                <div style="height:100%; width:${barWidth}%; background:${barColor(fill ?? 0)};"></div>
              </div>
            </div>
          `;
        }).join("")}
      </div>
    `;
  }

  function renderQuickAlerts(bins) {
    const warn = thresholds.warn;
    const crit = thresholds.critical;

    const criticalBins = bins.filter(b => getFillPercent(b) >= crit);
    const warnBins = bins.filter(b => getFillPercent(b) >= warn && getFillPercent(b) < crit);

    const summary = $("alertSummary");
    if (summary) {
      if (criticalBins.length > 0) {
        summary.className = "chip bad";
        summary.textContent = `${criticalBins.length} critici`;
      } else if (warnBins.length > 0) {
        summary.className = "chip warn";
        summary.textContent = `${warnBins.length} warning`;
      } else {
        summary.className = "chip ok";
        summary.textContent = "0";
      }
    }

    const list = $("alertsList");
    const totalAlerts = criticalBins.length + warnBins.length;
    if (!list) return;

    if (totalAlerts === 0) {
      list.style.display = "none";
      list.textContent = "";
      return;
    }

    const items = [
      ...criticalBins.map(b => ({ ...b, _p: "crit" })),
      ...warnBins.map(b => ({ ...b, _p: "warn" })),
    ].sort((a, b) => getFillPercent(b) - getFillPercent(a));

    const lines = items.slice(0, 8).map(b => {
      const f = Math.round(getFillPercent(b));
      const tag = b._p === "crit" ? "CRITICO" : "WARNING";
      return `• ${tag} — ${b.bin_id} (${f}%)`;
    }).join("\n");

    list.style.display = "block";
    list.textContent = `Da attenzionare:\n${lines}`;
  }

  function touchBinRow(binId, isoTs) {
    if (!binId) return;

    const tr = document.getElementById(`binrow-${binId}`);
    if (tr) {
      const tdLast = tr.querySelector('[data-col="last"]');
      if (tdLast) {
        tdLast.title = isoToNice(isoTs);
        tdLast.textContent = timeAgo(isoTs);
      }
    }

    const card = document.getElementById(`bincard-${binId}`);
    if (card) {
      const el = card.querySelector('[data-col="last"]');
      if (el) {
        el.title = isoToNice(isoTs);
        el.textContent = timeAgo(isoTs);
      }
    }

    if (drawerOpen && activeBinId === binId) {
      safeText("ddLast", timeAgo(isoTs));
      const ddLast = $("ddLast");
      if (ddLast) ddLast.title = isoToNice(isoTs);
    }
  }

  function prependRecentEvent(payload) {
    if (!payload) return;
    if (!adminKey()) return;
    if (!lastIsAdmin) return;

    const eventsBody = $("eventsBody");
    const eventsEmpty = $("eventsEmpty");
    if (!eventsBody || !eventsEmpty) return;

    eventsEmpty.style.display = "none";
    eventsEmpty.textContent = "";

    const ts = payload.ts || new Date().toISOString();
    const whenHuman = timeAgo(ts);
    const whenTitle = isoToNice(ts);
    const binId = payload.bin_id || "—";
    const material = payload.material || "—";
    const w = payload.weight_g;
    const co2 = payload.co2_saved_g;
    const eid = payload.event_id ?? "";

    if (eid) {
      const existing = eventsBody.querySelector(`tr[data-eid="${CSS.escape(String(eid))}"]`);
      if (existing) return;
    }

    const rowHtml = `
      <tr data-eid="${String(eid)}">
        <td class="muted" title="${whenTitle}">${whenHuman}</td>
        <td><b>${escapeHtml(binId)}</b></td>
        <td>${escapeHtml(material)}</td>
        <td>${formatWeight(w)}</td>
        <td>${formatCO2(co2)}</td>
        <td class="mono" style="text-align:right" title="${escapeHtml(String(eid))}">${shortId(eid)}</td>
      </tr>
    `;

    eventsBody.insertAdjacentHTML("afterbegin", rowHtml);

    const rows = eventsBody.querySelectorAll("tr");
    for (let i = 20; i < rows.length; i++) rows[i].remove();

    lastEventsSig = "LIVE";

    if (drawerOpen && activeBinId === binId) {
      const ddBody = $("ddEventsBody");
      const ddEmpty = $("ddEventsEmpty");
      if (!ddBody || !ddEmpty) return;

      ddEmpty.style.display = "none";
      ddEmpty.textContent = "";

      const row = `
        <tr data-eid="${String(eid)}">
          <td class="muted" title="${whenTitle}">${whenHuman}</td>
          <td>${escapeHtml(material)}</td>
          <td>${formatWeight(w)}</td>
          <td>${formatCO2(co2)}</td>
          <td class="mono" style="text-align:right" title="${escapeHtml(String(eid))}">${shortId(eid)}</td>
        </tr>
      `;
      ddBody.insertAdjacentHTML("afterbegin", row);
      const rows2 = ddBody.querySelectorAll("tr");
      for (let i = 20; i < rows2.length; i++) rows2[i].remove();
    }
  }

  function scheduleRefresh(ms = 800) {
    if (refreshTimer) clearTimeout(refreshTimer);
    refreshTimer = setTimeout(() => {
      refreshTimer = null;
      refresh();
    }, ms);
  }

  function syncResponsiveBinsView() {
    const mobile = window.matchMedia("(max-width: 820px)").matches;
    const binsTableWrap = $("binsBody")?.closest(".tableWrap");
    const binsCards = $("binsCards");

    if (binsTableWrap) binsTableWrap.style.display = mobile ? "none" : "block";
    if (binsCards) binsCards.style.display = mobile ? "grid" : "none";
  }

  Chart.defaults.color = "rgba(255,255,255,.82)";
  Chart.defaults.borderColor = "rgba(255,255,255,.08)";
  Chart.defaults.font.family = "Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif";

  const donutCenterText = {
    id: "donutCenterText",
    afterDraw(chart) {
      const { ctx, chartArea } = chart;
      if (!chartArea) return;
      if (chart.config?.type !== "doughnut") return;

      const ds = chart.data?.datasets?.[0];
      if (!ds || !Array.isArray(ds.data)) return;

      const totalG = ds.data.reduce((s, v) => s + (Number(v) || 0), 0);
      const totalTxt = formatWeight(totalG);

      const x = (chartArea.left + chartArea.right) / 2;
      const y = (chartArea.top + chartArea.bottom) / 2;

      ctx.save();
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";

      ctx.font = "700 12px Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif";
      ctx.fillStyle = "rgba(255,255,255,.70)";
      ctx.fillText("Totale", x, y - 10);

      ctx.font = "900 16px Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif";
      ctx.fillStyle = "rgba(255,255,255,.92)";
      ctx.fillText(totalTxt, x, y + 10);

      ctx.restore();
    }
  };
  Chart.register(donutCenterText);

  let lineChart = null;
  let doughnutChart = null;

  function initCharts() {
    lineChart = new Chart($("co2Line").getContext("2d"), {
      type: "line",
      data: {
        labels: [], datasets: [{
          label: "CO₂",
          data: [],
          borderWidth: 2,
          tension: .25,
          pointRadius: 2,
          pointHoverRadius: 5,
          fill: true
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (ctx) => ` ${formatCO2(ctx.parsed.y)}` } }
        },
        scales: {
          x: { grid: { display: false } },
          y: {
            grid: { color: "rgba(255,255,255,.06)" },
            ticks: { callback: (v) => formatCO2(v) }
          }
        }
      }
    });

    doughnutChart = new Chart($("matPie").getContext("2d"), {
      type: "doughnut",
      data: {
        labels: [], datasets: [{
          data: [],
          backgroundColor: [
            "rgba(67,206,162,.90)",
            "rgba(24,90,157,.90)",
            "rgba(247,201,72,.90)",
            "rgba(255,93,93,.90)",
            "rgba(164,121,255,.90)",
            "rgba(255,140,73,.90)",
            "rgba(96,215,255,.90)",
            "rgba(160,255,160,.90)"
          ],
          borderColor: "rgba(255,255,255,.14)",
          borderWidth: 1.2
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        cutout: "64%",
        plugins: {
          legend: { position: "top", labels: { boxWidth: 12 } },
          tooltip: { callbacks: { label: (ctx) => ` ${ctx.label}: ${formatWeight(ctx.parsed)}` } },
          donutCenterText: {}
        }
      }
    });

    ddLineChart = new Chart($("ddLine").getContext("2d"), {
      type: "line",
      data: {
        labels: [], datasets: [{
          label: "CO₂ bin",
          data: [],
          borderWidth: 2,
          tension: .25,
          pointRadius: 2,
          pointHoverRadius: 5,
          fill: true
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false } },
          y: { grid: { color: "rgba(255,255,255,.06)" }, ticks: { callback: (v) => formatCO2(v) } }
        }
      }
    });

    ddPieChart = new Chart($("ddPie").getContext("2d"), {
      type: "doughnut",
      data: {
        labels: [], datasets: [{
          data: [],
          backgroundColor: [
            "rgba(67,206,162,.90)",
            "rgba(24,90,157,.90)",
            "rgba(247,201,72,.90)",
            "rgba(255,93,93,.90)",
            "rgba(164,121,255,.90)",
            "rgba(255,140,73,.90)",
            "rgba(96,215,255,.90)",
            "rgba(160,255,160,.90)"
          ],
          borderColor: "rgba(255,255,255,.14)",
          borderWidth: 1.2
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        cutout: "64%",
        plugins: { legend: { position: "top", labels: { boxWidth: 12 } }, donutCenterText: {} }
      }
    });
  }

  async function fetchDashboard(days) {
    const d = Math.max(1, Math.min(365, Number(days || 30)));
    const url = apiUrl(`/api/dashboard?days=${encodeURIComponent(d)}&events_limit=20`);
    const headers = {};
    if (adminKey()) headers["X-API-Key"] = adminKey();
    return await fetchJSON(url, { headers });
  }

  async function fetchBinDetail(binId, days, eventsLimit = 20) {
    const d = Math.max(1, Math.min(365, Number(days || 30)));
    const url = apiUrl(`/api/bins/${encodeURIComponent(binId)}?days=${encodeURIComponent(d)}&events_limit=${encodeURIComponent(eventsLimit)}`);
    const headers = {};
    if (adminKey()) headers["X-API-Key"] = adminKey();
    return await fetchJSON(url, { headers });
  }

  function startPollingFallback() {
    if (pollTimer) return;
    setLive("Polling", "polling");
    pollTimer = setInterval(() => refresh(), 10000);
  }

  function startSSE() {
    try {
      if (es) es.close();
      if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }

      es = new EventSource(apiUrl("/api/stream"));

      es.addEventListener("hello", () => {
        setLive("Realtime", "realtime");
        refresh();
      });

      es.addEventListener("update", (ev) => {
        const now = Date.now();
        if (now - lastUpdateTs < 120) return;
        lastUpdateTs = now;

        setLive("Realtime", "realtime");

        let payload = null;
        try { payload = ev?.data ? JSON.parse(ev.data) : null; } catch { payload = null; }

        const binId = payload?.bin_id;
        const ts = payload?.ts || new Date().toISOString();
        if (binId) touchBinRow(binId, ts);

        if (payload?.type === "event") {
          prependRecentEvent(payload);
        }

        if (drawerOpen && activeBinId && activeBinId === binId) {
          scheduleBinRefresh(900);
        }

        const t = String(payload?.type || "update");
        const delay =
          (t === "config" || t === "empty" || t === "rotate_key") ? 350 :
          (t === "event") ? 650 :
          800;

        scheduleRefresh(delay);
      });

      es.onerror = () => {
        try { es.close(); } catch { }
        es = null;
        startPollingFallback();
      };
    } catch {
      startPollingFallback();
    }
  }

  function openDrawer() {
    drawerOpen = true;
    $("drawer").classList.add("open");
    $("drawerOverlay").classList.add("open");
    $("drawer").setAttribute("aria-hidden", "false");
    $("drawerOverlay").setAttribute("aria-hidden", "false");
    document.body.style.overflow = "hidden";
  }

  function closeDrawer() {
    drawerOpen = false;
    activeBinId = null;
    $("drawer").classList.remove("open");
    $("drawerOverlay").classList.remove("open");
    $("drawer").setAttribute("aria-hidden", "true");
    $("drawerOverlay").setAttribute("aria-hidden", "true");
    document.body.style.overflow = "";
  }

  let binRefreshTimer = null;
  function scheduleBinRefresh(ms = 900) {
    if (!drawerOpen || !activeBinId) return;
    if (binRefreshTimer) clearTimeout(binRefreshTimer);
    binRefreshTimer = setTimeout(async () => {
      binRefreshTimer = null;
      await openBinDrawer(activeBinId, { soft: true });
    }, ms);
  }

  function ddSetLoading(binId) {
    safeText("ddTitle", `Bin ${binId}`);
    safeText("ddSub", "Caricamento…");
    safeText("ddFill", "—");
    safeText("ddWeight", "—");
    safeText("ddCap", "—");
    safeText("ddLast", "—");
    safeText("ddRangeLabel", `${$("rangeSel").value} giorni`);

    const body = $("ddCompartmentsBody");
    if (body) body.innerHTML = "";

    const empty = $("ddCompartmentsEmpty");
    if (empty) {
      empty.style.display = "none";
      empty.textContent = "";
    }

    const ddEventsEmpty = $("ddEventsEmpty");
    if (ddEventsEmpty) {
      ddEventsEmpty.style.display = "none";
      ddEventsEmpty.textContent = "";
    }

    const ddEventsBody = $("ddEventsBody");
    if (ddEventsBody) ddEventsBody.innerHTML = "";

    const ddLineEmpty = $("ddLineEmpty");
    if (ddLineEmpty) ddLineEmpty.style.display = "none";

    const ddPieEmpty = $("ddPieEmpty");
    if (ddPieEmpty) ddPieEmpty.style.display = "none";
  }

  function renderCompartments(bin) {
    const comps = getCompartments(bin);
    const body = $("ddCompartmentsBody");
    const empty = $("ddCompartmentsEmpty");
    if (!body || !empty) return;

    if (!comps.length) {
      body.innerHTML = "";
      empty.style.display = "block";
      empty.textContent = "Nessun dato compartimenti disponibile per questo bin.";
      return;
    }

    empty.style.display = "none";
    empty.textContent = "";

    body.innerHTML = comps.map(c => {
      const fill = getSensorFillPercent(c);
      const state = getSensorState(c);
      const dist = getSensorDistanceCm(c);
      const cap = numOrNull(c?.capacity_percent);
      const f = fill !== null ? fill : 0;
      const barWidth = Math.max(0, Math.min(100, f));
      const badge = fill !== null ? fillBadge(f) : `<span class="badge bWARN">NO DATA</span>`;

      return `
        <div class="compCard">
          <div class="compCardTop">
            <div>
              <div class="compTitle">${escapeHtml(c.label || `Scomparto ${c.compartment_id}`)}</div>
              <div class="compMeta">ID: ${escapeHtml(c.compartment_id ?? "—")} • Quota capacità: ${cap !== null ? `${cap.toFixed(0)}%` : "—"}</div>
            </div>
            <div class="fillPill">${fill !== null ? `${Math.round(fill)}%` : "—"}</div>
          </div>

          <div class="compRow">
            ${badge}
            <span class="compTiny">Stato: <b>${escapeHtml(state || "—")}</b></span>
            <span class="compTiny">Distanza: <b>${escapeHtml(formatDistanceCm(dist))}</b></span>
          </div>

          <div class="compBar">
            <div style="width:${barWidth}%; background:${barColor(f)}"></div>
          </div>
        </div>
      `;
    }).join("");
  }

  async function openBinDrawer(binId, { soft = false } = {}) {
    try {
      activeBinId = binId;
      const days = Number($("rangeSel").value || 30);

      if (!soft) {
        ddSetLoading(binId);
        openDrawer();
      }

      $("ddBtnCapacity").onclick = () => setCapacityKg(binId, window.__lastCapGByBin?.[binId] || 0);
      $("ddBtnEmpty").onclick = () => emptyBin(binId);

      const data = await fetchBinDetail(binId, days, 20);
      const b = data?.bin || {};

      const fill = getFillPercent(b);
      const wG = Number(b.current_weight_g || 0);
      const capG = Number(b.capacity_g || 0);
      const lastSeen = b.last_seen || null;
      const fillSource = getFillSource(b);
      const comps = getCompartments(b);

      safeText("ddTitle", `Bin ${binId}`);
      safeText("ddFill", `${Math.round(fill)}%`);
      safeHTML("ddFillHint", `${fillBadge(fill)} <span class="muted small">${fillSource === "sensor" ? "totale da scomparti" : "totale stimato"}</span>`);
      safeText("ddWeight", formatWeight(wG));

      const capKg = capG / 1000;
      safeText("ddCap", isFinite(capKg) ? (capKg < 100 ? capKg.toFixed(1) : Math.round(capKg).toString()) + " kg" : "—");

      safeText("ddLast", timeAgo(lastSeen));
      const ddLast = $("ddLast");
      if (ddLast) ddLast.title = isoToNice(lastSeen);

      safeText(
        "ddSub",
        `Ultimo update: ${timeAgo(lastSeen)} • ${fill >= thresholds.critical ? "CRITICO" : fill >= thresholds.warn ? "WARNING" : "OK"} • ${fillSource === "sensor" ? `${comps.length} scomparti` : "stima peso/capacità"}`
      );

      safeText("ddRangeLabel", `${days} giorni`);

      renderCompartments(b);

      const daily = data?.daily || [];
      if ($("ddLineEmpty")) $("ddLineEmpty").style.display = daily.some(r => Number(r.co2_saved_g || 0) > 0) ? "none" : "block";
      ddLineChart.data.labels = daily.map(r => r.day);
      ddLineChart.data.datasets[0].data = daily.map(r => Number(r.co2_saved_g || 0));
      ddLineChart.update();

      const mats = data?.by_material || [];
      if ($("ddPieEmpty")) $("ddPieEmpty").style.display = mats.some(r => Number(r.weight_g || 0) > 0) ? "none" : "block";
      ddPieChart.data.labels = mats.map(r => r.material);
      ddPieChart.data.datasets[0].data = mats.map(r => Number(r.weight_g || 0));
      ddPieChart.update();

      const isAdmin = !!data?.is_admin;
      const events = data?.recent_events || [];

      if (!adminKey()) {
        $("ddEventsEmpty").style.display = "block";
        $("ddEventsEmpty").textContent = "🔒 Inserisci e salva la Admin key per vedere gli eventi del bin.";
        $("ddEventsBody").innerHTML = "";
      } else if (!isAdmin) {
        $("ddEventsEmpty").style.display = "block";
        $("ddEventsEmpty").textContent = "❌ Admin key non valida (401).";
        $("ddEventsBody").innerHTML = "";
      } else if (!events || events.length === 0) {
        $("ddEventsEmpty").style.display = "block";
        $("ddEventsEmpty").textContent = "Nessun evento recente per questo bin.";
        $("ddEventsBody").innerHTML = "";
      } else {
        $("ddEventsEmpty").style.display = "none";
        $("ddEventsEmpty").textContent = "";
        $("ddEventsBody").innerHTML = events.map(e => {
          const whenHuman = timeAgo(e.ts);
          const whenTitle = isoToNice(e.ts);
          const eid = e.id ?? "";
          return `
            <tr data-eid="${String(eid)}">
              <td class="muted" title="${whenTitle}">${whenHuman}</td>
              <td>${escapeHtml(e.material)}</td>
              <td>${formatWeight(e.weight_g)}</td>
              <td>${formatCO2(e.co2_saved_g)}</td>
              <td class="mono" style="text-align:right" title="${escapeHtml(String(eid))}">${shortId(eid)}</td>
            </tr>
          `;
        }).join("");
      }

    } catch (e) {
      safeText("ddSub", "Errore caricamento dettagli.");
      if ($("ddCompartmentsEmpty")) {
        $("ddCompartmentsEmpty").style.display = "block";
        $("ddCompartmentsEmpty").textContent = (e?.message || String(e));
      }
      if ($("ddEventsEmpty")) {
        $("ddEventsEmpty").style.display = "block";
        $("ddEventsEmpty").textContent = (e?.message || String(e));
      }
    }
  }

  function renderRecentEvents(dash) {
    const eventsBody = $("eventsBody");
    const eventsEmpty = $("eventsEmpty");
    if (!eventsBody || !eventsEmpty) return;

    const isAdmin = !!dash?.is_admin;
    const events = dash?.recent_events || [];

    if (!adminKey()) {
      const msg = "🔒 Inserisci e salva la Admin key per vedere gli ultimi eventi.";
      if (lastEventsSig !== "NOADMIN") {
        lastEventsSig = "NOADMIN";
        eventsBody.innerHTML = "";
        eventsEmpty.style.display = "block";
        eventsEmpty.textContent = msg;
      }
      return;
    }

    if (!isAdmin) {
      const msg = "❌ Admin key non valida (401).";
      if (lastEventsSig !== "BADADMIN") {
        lastEventsSig = "BADADMIN";
        eventsBody.innerHTML = "";
        eventsEmpty.style.display = "block";
        eventsEmpty.textContent = msg;
      }
      return;
    }

    const sig = (events || []).map(e =>
      `${e.ts || ""}|${e.bin_id || ""}|${e.material || ""}|${Number(e.weight_g || 0)}|${Number(e.co2_saved_g || 0)}|${e.id || ""}`
    ).join(";;");

    if (sig === lastEventsSig) return;
    lastEventsSig = sig;

    if (!events || events.length === 0) {
      eventsBody.innerHTML = "";
      eventsEmpty.style.display = "block";
      eventsEmpty.textContent = "Nessun evento registrato (ancora).";
      return;
    }

    eventsEmpty.style.display = "none";
    eventsEmpty.textContent = "";

    eventsBody.innerHTML = events.map(e => {
      const whenHuman = timeAgo(e.ts);
      const whenTitle = isoToNice(e.ts);
      const eid = e.id ?? "";
      return `
        <tr data-eid="${String(eid)}">
          <td class="muted" title="${whenTitle}">${whenHuman}</td>
          <td><b>${escapeHtml(e.bin_id)}</b></td>
          <td>${escapeHtml(e.material)}</td>
          <td>${formatWeight(e.weight_g)}</td>
          <td>${formatCO2(e.co2_saved_g)}</td>
          <td class="mono" style="text-align:right" title="${escapeHtml(String(eid))}">${shortId(eid)}</td>
        </tr>
      `;
    }).join("");
  }

  window.__lastCapGByBin = {};

  function renderBinsDesktopAndMobile(bins, onlyAlerts) {
    const binsSig = (onlyAlerts ? "ONLY|" : "ALL|") + bins.map(b =>
      `${b.bin_id}|${Number(b.capacity_g || 0)}|${Number(b.current_weight_g || 0)}|${b.last_seen || ""}|${getFillPercent(b)}|${JSON.stringify(getCompartments(b))}`
    ).join(";;");

    if (binsSig !== lastBinsSig) {
      lastBinsSig = binsSig;

      const rowsHTML = bins.map(b => {
        const fill = getFillPercent(b);
        const wTxt = formatWeight(b.current_weight_g);
        const cTxt = formatWeight(b.capacity_g);
        const sensorMeta = sensorMetaHTML(b);
        const compPreview = compactCompartmentsHTML(b);
        const barWidth = Math.max(0, Math.min(100, fill));
        const lastHuman = timeAgo(b.last_seen);
        const lastTitle = isoToNice(b.last_seen);

        const capKg = Number(b.capacity_g || 0) / 1000;
        const capKgTxt = (isFinite(capKg) ? (capKg < 100 ? capKg.toFixed(1) : Math.round(capKg).toString()) : "—");

        const p = priorityOf(fill);
        const priClass = (p === 2) ? "priCritical" : (p === 1) ? "priWarn" : "";

        window.__lastCapGByBin[b.bin_id] = Number(b.capacity_g || 0);

        return `
          <tr id="binrow-${escapeHtml(b.bin_id)}" class="${priClass}" style="cursor:pointer"
              title="Apri dettagli"
              onclick="window.__openBin('${String(b.bin_id).replaceAll("'", "\\'")}')">
            <td>${escapeHtml(b.bin_id)}</td>
            <td data-col="weight">${wTxt}</td>
            <td data-col="cap" title="${escapeHtml(cTxt)}">${capKgTxt} kg</td>
            <td data-col="fill">
              <div class="fillWrap">
                <span class="fillPill">${Math.round(fill)}%</span>
                ${fillBadge(fill)}
                <span class="bar">
                  <div style="width:${barWidth}%; background:${barColor(fill)}"></div>
                </span>
              </div>
              ${sensorMeta}
              ${compPreview}
            </td>
            <td data-col="last" class="muted" title="${escapeHtml(lastTitle)}">${lastHuman}</td>
            <td style="text-align:right" onclick="event.stopPropagation()">
              <button class="btnGhost btnMini" onclick="setCapacityKg('${String(b.bin_id).replaceAll("'", "\\'")}', ${Number(b.capacity_g || 0)})">Capacità</button>
              <button class="btnDanger btnMini" style="margin-left:8px" onclick="emptyBin('${String(b.bin_id).replaceAll("'", "\\'")}')">Svuota</button>
            </td>
          </tr>
        `;
      }).join("");

      safeHTML("binsBody", rowsHTML || `<tr><td colspan="6" class="muted">Nessun bin da mostrare (filtro attivo).</td></tr>`);

      const cards = bins.map(b => {
        const fill = getFillPercent(b);
        const sensorMeta = sensorMetaHTML(b);
        const compPreview = compactCompartmentsHTML(b);
        const barWidth = Math.max(0, Math.min(100, fill));
        const lastHuman = timeAgo(b.last_seen);
        const lastTitle = isoToNice(b.last_seen);

        const wTxt = formatWeight(b.current_weight_g);
        const capG = Number(b.capacity_g || 0);
        const capKg = capG / 1000;
        const capTxt = isFinite(capKg) ? (capKg < 100 ? capKg.toFixed(1) : Math.round(capKg).toString()) + " kg" : "—";

        const p = priorityOf(fill);
        const pri = p === 2 ? "CRITICO" : p === 1 ? "WARNING" : "OK";

        window.__lastCapGByBin[b.bin_id] = capG;

        return `
          <div id="bincard-${escapeHtml(b.bin_id)}" class="binCard" onclick="window.__openBin('${String(b.bin_id).replaceAll("'", "\\'")}')">
            <div class="binCardTop">
              <div>
                <div class="binCardId">${escapeHtml(b.bin_id)}</div>
                <div class="binCardMeta" title="${escapeHtml(lastTitle)}">
                  Ultimo update: <span data-col="last">${lastHuman}</span> • ${pri}
                </div>
              </div>
              <div class="fillPill">${Math.round(fill)}%</div>
            </div>

            <div class="binCardFillRow">
              <div class="binCardFillLeft">
                ${fillBadge(fill)}
                <span class="bar">
                  <div style="width:${barWidth}%; background:${barColor(fill)}"></div>
                </span>
              </div>
              <div class="binCardMeta">
                ${wTxt} / ${capTxt}
              </div>
            </div>

            ${sensorMeta}
            ${compPreview}

            <div class="sep" style="margin:10px 0"></div>

            <div class="binCardActions" onclick="event.stopPropagation()">
              <button class="btnGhost btnMini" onclick="setCapacityKg('${String(b.bin_id).replaceAll("'", "\\'")}', ${capG})">Capacità</button>
              <button class="btnDanger btnMini" onclick="emptyBin('${String(b.bin_id).replaceAll("'", "\\'")}')">Svuota</button>
            </div>
          </div>
        `;
      }).join("");

      safeHTML("binsCards", cards);
    }

    syncResponsiveBinsView();
  }

  async function refresh() {
    if (refreshing) {
      pendingRefresh = true;
      return;
    }
    refreshing = true;

    try {
      clearError();
      const days = Number($("rangeSel").value || 30);
      const dash = await fetchDashboard(days);

      lastIsAdmin = !!dash?.is_admin;

      safeText("kpiTotalWeight", formatWeight(dash?.totals?.total_weight_g));
      safeText("kpiTotalCO2", formatCO2(dash?.totals?.total_co2_saved_g));

      let binsAll = (dash?.bins || []).slice();
      safeText("binsCount", binsAll.length.toString());

      binsAll.sort((a, b) => {
        const pa = priorityOf(getFillPercent(a));
        const pb = priorityOf(getFillPercent(b));
        if (pa !== pb) return pb - pa;
        return getFillPercent(b) - getFillPercent(a);
      });

      renderQuickAlerts(binsAll);

      const onlyAlerts = !!$("onlyAlerts").checked;
      const bins = onlyAlerts ? binsAll.filter(b => priorityOf(getFillPercent(b)) > 0) : binsAll;

      renderBinsDesktopAndMobile(bins, onlyAlerts);

      const daily = dash?.daily || [];
      const rangeW = daily.reduce((s, r) => s + Number(r.weight_g || 0), 0);
      const rangeC = daily.reduce((s, r) => s + Number(r.co2_saved_g || 0), 0);

      safeText("kpiRangeWeight", formatWeight(rangeW));
      safeText("kpiRangeCO2", formatCO2(rangeC));

      $("lineEmpty").style.display = daily.some(r => Number(r.co2_saved_g || 0) > 0) ? "none" : "block";
      lineChart.data.labels = daily.map(r => r.day);
      lineChart.data.datasets[0].data = daily.map(r => Number(r.co2_saved_g || 0));
      lineChart.update();

      const mats = dash?.by_material || [];
      $("pieEmpty").style.display = mats.some(r => Number(r.weight_g || 0) > 0) ? "none" : "block";
      doughnutChart.data.labels = mats.map(r => r.material);
      doughnutChart.data.datasets[0].data = mats.map(r => Number(r.weight_g || 0));
      doughnutChart.update();

      renderRecentEvents(dash);

      safeText("lastUpdated", new Date().toLocaleString());

      if (drawerOpen && activeBinId) {
        safeText("ddRangeLabel", `${days} giorni`);
      }

    } catch (e) {
      showError(e?.message || String(e));
    } finally {
      refreshing = false;
      if (pendingRefresh) {
        pendingRefresh = false;
        scheduleRefresh(350);
      }
    }
  }

  function wireUI() {
    const devWrap = $("devSimWrap");
    if (devWrap) {
      devWrap.style.display = isDevMode() ? "block" : "none";
    }

    $("btnSaveAdmin").onclick = () => {
      const v = $("adminKeyInput").value.trim();
      if (!v) return alert("Incolla la ADMIN key.");
      localStorage.setItem(LS_ADMIN, v);
      $("adminKeyInput").value = "";
      alert("Admin key salvata ✅");
      refresh();
      if (drawerOpen && activeBinId) openBinDrawer(activeBinId, { soft: true });
    };

    $("btnSaveIngest").onclick = () => {
      const v = $("ingestKeyInput").value.trim();
      if (!v) return alert("Incolla la INGEST key.");
      localStorage.setItem(LS_INGEST, v);
      $("ingestKeyInput").value = "";
      alert("Ingest key salvata ✅");
    };

    $("btnEditThresholds").onclick = () => {
      const w = prompt("Soglia WARNING (%):", String(thresholds.warn));
      if (w === null) return;
      const c = prompt("Soglia CRITICAL (%):", String(thresholds.critical));
      if (c === null) return;

      const wn = Number(String(w).replace(",", ".").trim());
      const cn = Number(String(c).replace(",", ".").trim());

      if (!isFinite(wn) || !isFinite(cn) || wn <= 0 || cn <= 0 || wn >= cn || cn > 100) {
        return alert("Valori non validi. Regola: 0 < warn < critical ≤ 100");
      }
      thresholds = { warn: wn, critical: cn };
      saveThresholds();
      updateThresholdLabels();
      refresh();
      if (drawerOpen && activeBinId) openBinDrawer(activeBinId, { soft: true });
    };

    const btnSim = $("btnSimEvent");
    if (btnSim) {
      btnSim.onclick = async () => {
        if (!isDevMode()) return alert("Simulatore disabilitato (non sei in DEV mode).");
        try {
          if (!ingestKey()) return alert("Manca Ingest key (X-Ingest-Key).");
          const bin = $("simBin").value.trim();
          const mat = $("simMat").value.trim();
          const w = Number($("simW").value || 0);
          if (!bin || !mat || !w) return alert("Compila bin_id, materiale e grammi.");

          const resp = await fetchJSON(apiUrl("/api/event"), {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Ingest-Key": ingestKey() },
            body: JSON.stringify({ bin_id: bin, material: mat, weight_g: w, source: "simulator" })
          });

          $("simW").value = "";
          const r = $("simResult");
          if (r) {
            r.style.display = "block";
            r.textContent =
              `✅ Evento registrato\n` +
              `bin: ${resp.bin_id}\n` +
              `materiale: ${resp.material}\n` +
              `peso: ${formatWeight(resp.weight_g)}\n` +
              `CO₂ risparmiata: ${formatCO2(resp.co2_saved_g)}\n` +
              `riempimento totale: ${Math.round(getFillPercent(resp.bin || {}))}%`;
          }

          await refresh();
        } catch (e) {
          if ($("simResult")) $("simResult").style.display = "none";
          alert("Errore invio evento:\n\n" + (e?.message || String(e)));
        }
      };
    }

    $("btnExportEvents").onclick = async () => {
      try {
        if (!adminKey()) return alert("Manca Admin key.");
        await downloadFile(apiUrl("/api/export/events.csv"), "sorti_events.csv", { "X-API-Key": adminKey() });
      } catch (e) {
        alert("Errore export eventi:\n\n" + (e?.message || String(e)));
      }
    };

    $("btnExportDaily").onclick = async () => {
      try {
        if (!adminKey()) return alert("Manca Admin key.");
        const d = Math.max(1, Math.min(365, Number($("exportDays").value || 30)));
        await downloadFile(apiUrl(`/api/export/daily.csv?days=${d}`), `sorti_daily_${d}d.csv`, { "X-API-Key": adminKey() });
      } catch (e) {
        alert("Errore export daily:\n\n" + (e?.message || String(e)));
      }
    };

    $("rangeSel").onchange = async () => {
      await refresh();
      if (drawerOpen && activeBinId) await openBinDrawer(activeBinId, { soft: true });
    };

    $("onlyAlerts").onchange = () => refresh();

    $("btnCloseDrawer").onclick = () => closeDrawer();
    $("drawerOverlay").onclick = () => closeDrawer();

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && drawerOpen) closeDrawer();
    });

    window.addEventListener("resize", syncResponsiveBinsView);
    window.__openBin = (binId) => openBinDrawer(binId);
  }

  document.addEventListener("DOMContentLoaded", () => {
    loadThresholds();
    updateThresholdLabels();
    initCharts();
    wireUI();
    syncResponsiveBinsView();
    startSSE();
    refresh();
  });
})();
