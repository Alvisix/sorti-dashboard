(() => {
  const $ = (id) => document.getElementById(id);

  let refreshing = false;
  let lastBinsSig = "";
  let lastEventsSig = "";

  let es = null;
  let pollTimer = null;
  let lastUpdateTs = 0;

  // realtime intelligente
  let refreshTimer = null;
  let pendingRefresh = false;

  // admin state ultimo fetch
  let lastIsAdmin = false;

  const LS_THRESH = "sorti_thresholds_v1";
  let thresholds = { warn: 70, critical: 85 };

  const LS_ADMIN = "sorti_admin_key";
  const LS_INGEST = "sorti_ingest_key";
  const adminKey = () => localStorage.getItem(LS_ADMIN) || "";
  const ingestKey = () => localStorage.getItem(LS_INGEST) || "";

  function loadThresholds(){
    try{
      const raw = localStorage.getItem(LS_THRESH);
      if (!raw) return;
      const obj = JSON.parse(raw);
      const w = Number(obj?.warn);
      const c = Number(obj?.critical);
      if (isFinite(w) && isFinite(c) && w > 0 && c > 0 && w < c && c <= 100){
        thresholds = { warn: w, critical: c };
      }
    }catch{}
  }
  function saveThresholds(){ localStorage.setItem(LS_THRESH, JSON.stringify(thresholds)); }
  function updateThresholdLabels(){
    $("warnLabel").textContent = `${thresholds.warn}%`;
    $("critLabel").textContent = `${thresholds.critical}%`;
  }

  function setConnState(state){
    const dot = $("connDot");
    dot.classList.remove("warn","bad");
    if (state === "polling") dot.classList.add("warn");
    if (state === "offline") dot.classList.add("bad");
  }
  function setLive(text, state){
    $("liveText").textContent = text;
    if (state) setConnState(state);
  }

  function showError(msg){
    $("errBox").style.display = "block";
    $("errBox").textContent = msg;
    setLive("Offline", "offline");
  }
  function clearError(){
    $("errBox").style.display = "none";
    $("errBox").textContent = "";
  }

  function formatWeight(g){
    const n = Number(g || 0);
    if (!isFinite(n)) return "—";
    if (n >= 1000){
      const kg = n/1000;
      const txt = (kg < 100 ? kg.toFixed(1) : Math.round(kg).toString());
      return txt + " kg";
    }
    return Math.round(n) + " g";
  }
  const formatCO2 = formatWeight;

  function isoToDate(s){
    if (!s) return null;
    const d = new Date(s);
    if (isNaN(d.getTime())) return null;
    return d;
  }
  function isoToNice(s){
    const d = isoToDate(s);
    if (!d) return (s || "—");
    return d.toLocaleString();
  }
  function timeAgo(iso){
    const d = isoToDate(iso);
    if (!d) return "—";
    const now = new Date();
    let diff = Math.floor((now - d) / 1000);
    if (!isFinite(diff)) return "—";
    if (diff < 0) diff = 0;

    if (diff < 10) return "adesso";
    if (diff < 60) return `${diff}s fa`;
    const m = Math.floor(diff/60);
    if (m < 60) return `${m} min fa`;
    const h = Math.floor(m/60);
    if (h < 24) return `${h} h fa`;
    const days = Math.floor(h/24);
    if (days === 1) return "ieri";
    if (days < 7) return `${days} gg fa`;
    const w = Math.floor(days/7);
    if (w < 5) return `${w} sett fa`;
    const mo = Math.floor(days/30);
    if (mo < 12) return `${mo} mesi fa`;
    const y = Math.floor(days/365);
    return `${y} anni fa`;
  }
  function shortId(s){
    if (!s) return "—";
    const str = String(s);
    if (str.length <= 10) return str;
    return str.slice(0, 8) + "…" + str.slice(-6);
  }

  async function fetchJSON(url, opts={}){
    const res = await fetch(url, opts);
    const text = await res.text();
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}\n${text}`);
    try { return JSON.parse(text); }
    catch { throw new Error(`Risposta non-JSON da ${url}\n${text}`); }
  }

  async function downloadFile(url, filename, headers){
    const res = await fetch(url, { headers });
    if (!res.ok){
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

  async function setCapacityKg(binId, currentCapacityG){
    try{
      if (!adminKey()) return alert("Manca Admin key (X-API-Key).");

      const curKg = (Number(currentCapacityG||0) / 1000);
      const proposed = (isFinite(curKg) && curKg > 0) ? curKg.toFixed(curKg < 100 ? 1 : 0) : "";
      const input = prompt(`Imposta capacità per ${binId} (in kg):`, proposed);
      if (input === null) return;

      const kg = Number(String(input).replace(",", ".").trim());
      if (!isFinite(kg) || kg <= 0) return alert("Valore non valido. Inserisci un numero > 0 (kg).");
      if (kg > 10000) return alert("Valore troppo alto. Controlla (kg).");

      const capacity_g = Math.round(kg * 1000);

      await fetchJSON(`/api/bins/${encodeURIComponent(binId)}/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-Key": adminKey() },
        body: JSON.stringify({ capacity_g })
      });

      await refresh();
    }catch(e){
      alert("Errore modifica capacità:\n\n" + (e?.message || String(e)));
    }
  }
  window.setCapacityKg = setCapacityKg;

  async function emptyBin(binId){
    try{
      if (!adminKey()) return alert("Manca Admin key (X-API-Key).");
      const ok = confirm(`Confermi svuotamento ${binId}?\n(Azzera solo il peso bin, non cancella eventi)`);
      if (!ok) return;

      await fetchJSON(`/api/bins/${encodeURIComponent(binId)}/empty`, {
        method:"POST",
        headers: { "X-API-Key": adminKey() }
      });
      await refresh();
    }catch(e){
      alert("Errore svuotamento:\n\n" + (e?.message || String(e)));
    }
  }
  window.emptyBin = emptyBin;

  function barColor(fill){
    const f = Number(fill||0);
    if (f >= thresholds.critical) return "linear-gradient(90deg, rgba(255,93,93,.95), rgba(255,140,73,.95))";
    if (f >= thresholds.warn) return "linear-gradient(90deg, rgba(247,201,72,.95), rgba(255,140,73,.85))";
    return "linear-gradient(90deg, rgba(67,206,162,.95), rgba(24,90,157,.90))";
  }
  function fillBadge(fill){
    const f = Number(fill || 0);
    if (f >= thresholds.critical) return `<span class="badge bBAD">CRITICO</span>`;
    if (f >= thresholds.warn) return `<span class="badge bWARN">ATTENZIONE</span>`;
    return `<span class="badge bOK">OK</span>`;
  }
  function priorityOf(fill){
    const f = Number(fill || 0);
    if (f >= thresholds.critical) return 2;
    if (f >= thresholds.warn) return 1;
    return 0;
  }

  function renderQuickAlerts(bins){
    const warn = thresholds.warn;
    const crit = thresholds.critical;

    const criticalBins = bins.filter(b => Number(b.fill_percent||0) >= crit);
    const warnBins = bins.filter(b => Number(b.fill_percent||0) >= warn && Number(b.fill_percent||0) < crit);

    const summary = $("alertSummary");
    if (criticalBins.length > 0){
      summary.className = "chip bad";
      summary.textContent = `${criticalBins.length} critici`;
    } else if (warnBins.length > 0){
      summary.className = "chip warn";
      summary.textContent = `${warnBins.length} warning`;
    } else {
      summary.className = "chip ok";
      summary.textContent = "0";
    }

    const list = $("alertsList");
    const totalAlerts = criticalBins.length + warnBins.length;
    if (totalAlerts === 0){
      list.style.display = "none";
      list.textContent = "";
      return;
    }

    const items = [
      ...criticalBins.map(b => ({...b, _p:"crit"})),
      ...warnBins.map(b => ({...b, _p:"warn"})),
    ].sort((a,b) => Number(b.fill_percent||0) - Number(a.fill_percent||0));

    const lines = items.slice(0, 8).map(b => {
      const f = Math.round(Number(b.fill_percent||0));
      const tag = b._p === "crit" ? "CRITICO" : "WARNING";
      return `• ${tag} — ${b.bin_id} (${f}%)`;
    }).join("\n");

    list.style.display = "block";
    list.textContent = `Da attenzionare:\n${lines}`;
  }

  function touchBinRow(binId, isoTs){
    if (!binId) return;
    const tr = document.getElementById(`binrow-${binId}`);
    if (!tr) return;

    const tdLast = tr.querySelector('[data-col="last"]');
    if (!tdLast) return;

    tdLast.title = isoToNice(isoTs);
    tdLast.textContent = timeAgo(isoTs);
  }

  function prependRecentEvent(payload){
    if (!payload) return;
    if (!adminKey()) return;
    if (!lastIsAdmin) return;

    const eventsBody = $("eventsBody");
    const eventsEmpty = $("eventsEmpty");

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

    if (eid){
      const existing = eventsBody.querySelector(`tr[data-eid="${CSS.escape(String(eid))}"]`);
      if (existing) return;
    }

    const rowHtml = `
      <tr data-eid="${String(eid)}">
        <td class="muted" title="${whenTitle}">${whenHuman}</td>
        <td><b>${binId}</b></td>
        <td>${material}</td>
        <td>${formatWeight(w)}</td>
        <td>${formatCO2(co2)}</td>
        <td class="mono" style="text-align:right" title="${String(eid)}">${shortId(eid)}</td>
      </tr>
    `;

    eventsBody.insertAdjacentHTML("afterbegin", rowHtml);

    const rows = eventsBody.querySelectorAll("tr");
    for (let i = 20; i < rows.length; i++) rows[i].remove();

    lastEventsSig = "LIVE";
  }

  function scheduleRefresh(ms=800){
    if (refreshTimer) clearTimeout(refreshTimer);
    refreshTimer = setTimeout(() => {
      refreshTimer = null;
      refresh();
    }, ms);
  }

  // Chart defaults
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

  async function fetchDashboard(days){
    const d = Math.max(1, Math.min(365, Number(days || 30)));
    const url = `/api/dashboard?days=${encodeURIComponent(d)}&events_limit=20`;
    const headers = {};
    if (adminKey()) headers["X-API-Key"] = adminKey();
    return await fetchJSON(url, { headers });
  }

  function startPollingFallback(){
    if (pollTimer) return;
    setLive("Polling", "polling");
    pollTimer = setInterval(() => refresh(), 10000);
  }

  function startSSE(){
    try{
      if (es) es.close();
      if (pollTimer){ clearInterval(pollTimer); pollTimer = null; }

      es = new EventSource("/api/stream");

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
        try{ payload = ev?.data ? JSON.parse(ev.data) : null; }catch{ payload = null; }

        const binId = payload?.bin_id;
        const ts = payload?.ts || new Date().toISOString();
        if (binId) touchBinRow(binId, ts);

        if (payload?.type === "event"){
          prependRecentEvent(payload);
        }

        const t = String(payload?.type || "update");
        const delay =
          (t === "config" || t === "empty" || t === "rotate_key") ? 350 :
          (t === "event") ? 650 :
          800;

        scheduleRefresh(delay);
      });

      es.onerror = () => {
        try { es.close(); } catch {}
        es = null;
        startPollingFallback();
      };
    } catch {
      startPollingFallback();
    }
  }

  function renderRecentEvents(dash){
    const eventsBody = $("eventsBody");
    const eventsEmpty = $("eventsEmpty");

    const isAdmin = !!dash?.is_admin;
    const events = dash?.recent_events || [];

    if (!adminKey()){
      const msg = "🔒 Inserisci e salva la Admin key per vedere gli ultimi eventi.";
      if (lastEventsSig !== "NOADMIN"){
        lastEventsSig = "NOADMIN";
        eventsBody.innerHTML = "";
        eventsEmpty.style.display = "block";
        eventsEmpty.textContent = msg;
      }
      return;
    }

    if (!isAdmin){
      const msg = "❌ Admin key non valida (401).";
      if (lastEventsSig !== "BADADMIN"){
        lastEventsSig = "BADADMIN";
        eventsBody.innerHTML = "";
        eventsEmpty.style.display = "block";
        eventsEmpty.textContent = msg;
      }
      return;
    }

    const sig = (events || []).map(e =>
      `${e.ts||""}|${e.bin_id||""}|${e.material||""}|${Number(e.weight_g||0)}|${Number(e.co2_saved_g||0)}|${e.id||""}`
    ).join(";;");

    if (sig === lastEventsSig) return;
    lastEventsSig = sig;

    if (!events || events.length === 0){
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
          <td><b>${e.bin_id}</b></td>
          <td>${e.material}</td>
          <td>${formatWeight(e.weight_g)}</td>
          <td>${formatCO2(e.co2_saved_g)}</td>
          <td class="mono" style="text-align:right" title="${String(eid)}">${shortId(eid)}</td>
        </tr>
      `;
    }).join("");
  }

  async function refresh(){
    if (refreshing){
      pendingRefresh = true;
      return;
    }
    refreshing = true;

    try{
      clearError();
      const days = Number($("rangeSel").value || 30);
      const dash = await fetchDashboard(days);

      lastIsAdmin = !!dash?.is_admin;

      $("kpiTotalWeight").textContent = formatWeight(dash?.totals?.total_weight_g);
      $("kpiTotalCO2").textContent = formatCO2(dash?.totals?.total_co2_saved_g);

      let binsAll = (dash?.bins || []).slice();
      $("binsCount").textContent = binsAll.length.toString();

      binsAll.sort((a,b) => {
        const pa = priorityOf(a.fill_percent);
        const pb = priorityOf(b.fill_percent);
        if (pa !== pb) return pb - pa;
        return Number(b.fill_percent||0) - Number(a.fill_percent||0);
      });

      renderQuickAlerts(binsAll);

      const onlyAlerts = !!$("onlyAlerts").checked;
      let bins = onlyAlerts ? binsAll.filter(b => priorityOf(b.fill_percent) > 0) : binsAll;

      const binsSig = (onlyAlerts ? "ONLY|" : "ALL|") + bins.map(b =>
        `${b.bin_id}|${Number(b.capacity_g||0)}|${Number(b.current_weight_g||0)}|${b.last_seen||""}|${Number(b.fill_percent||0)}`
      ).join(";;");

      if (binsSig !== lastBinsSig){
        lastBinsSig = binsSig;

        const rowsHTML = bins.map(b => {
          const fill = Number(b.fill_percent || 0);
          const wTxt = formatWeight(b.current_weight_g);
          const cTxt = formatWeight(b.capacity_g);
          const barWidth = Math.max(0, Math.min(100, fill));
          const lastHuman = timeAgo(b.last_seen);
          const lastTitle = isoToNice(b.last_seen);

          const capKg = Number(b.capacity_g||0) / 1000;
          const capKgTxt = (isFinite(capKg) ? (capKg < 100 ? capKg.toFixed(1) : Math.round(capKg).toString()) : "—");

          const p = priorityOf(fill);
          const priClass = (p === 2) ? "priCritical" : (p === 1) ? "priWarn" : "";

          return `
            <tr id="binrow-${b.bin_id}" class="${priClass}">
              <td>${b.bin_id}</td>
              <td data-col="weight">${wTxt}</td>
              <td data-col="cap" title="${cTxt}">${capKgTxt} kg</td>
              <td data-col="fill">
                <div class="fillWrap">
                  <span class="fillPill">${Math.round(fill)}%</span>
                  ${fillBadge(fill)}
                  <span class="bar">
                    <div style="width:${barWidth}%; background:${barColor(fill)}"></div>
                  </span>
                </div>
              </td>
              <td data-col="last" class="muted" title="${lastTitle}">${lastHuman}</td>
              <td style="text-align:right">
                <button class="btnGhost btnMini" onclick="setCapacityKg('${b.bin_id}', ${Number(b.capacity_g||0)})">Capacità</button>
                <button class="btnDanger btnMini" style="margin-left:8px" onclick="emptyBin('${b.bin_id}')">Svuota</button>
              </td>
            </tr>
          `;
        }).join("");

        $("binsBody").innerHTML = rowsHTML || `<tr><td colspan="6" class="muted">Nessun bin da mostrare (filtro attivo).</td></tr>`;
      }

      const daily = dash?.daily || [];
      const rangeW = daily.reduce((s,r)=>s+Number(r.weight_g||0), 0);
      const rangeC = daily.reduce((s,r)=>s+Number(r.co2_saved_g||0), 0);

      $("kpiRangeWeight").textContent = formatWeight(rangeW);
      $("kpiRangeCO2").textContent = formatCO2(rangeC);

      $("lineEmpty").style.display = daily.some(r => Number(r.co2_saved_g||0) > 0) ? "none" : "block";
      lineChart.data.labels = daily.map(r=>r.day);
      lineChart.data.datasets[0].data = daily.map(r=>Number(r.co2_saved_g||0));
      lineChart.update();

      const mats = dash?.by_material || [];
      $("pieEmpty").style.display = mats.some(r => Number(r.weight_g||0) > 0) ? "none" : "block";
      doughnutChart.data.labels = mats.map(r=>r.material);
      doughnutChart.data.datasets[0].data = mats.map(r=>Number(r.weight_g||0));
      doughnutChart.update();

      renderRecentEvents(dash);

      $("lastUpdated").textContent = new Date().toLocaleString();
    } catch (e){
      showError(e?.message || String(e));
    } finally {
      refreshing = false;
      if (pendingRefresh){
        pendingRefresh = false;
        scheduleRefresh(350);
      }
    }
  }

  function initCharts(){
    const co2Ctx = $("co2Line").getContext("2d");
    const pieCtx = $("matPie").getContext("2d");

    lineChart = new Chart(co2Ctx, {
      type:"line",
      data:{labels:[], datasets:[{
        label:"CO₂",
        data:[],
        borderWidth:2,
        tension:.25,
        pointRadius:2,
        pointHoverRadius:5,
        fill:true
      }]},
      options:{
        responsive:true, maintainAspectRatio:false,
        plugins:{
          legend:{display:false},
          tooltip:{ callbacks:{ label:(ctx)=> ` ${formatCO2(ctx.parsed.y)}` } }
        },
        scales:{
          x:{ grid:{display:false} },
          y:{
            grid:{ color:"rgba(255,255,255,.06)" },
            ticks:{ callback:(v)=>formatCO2(v) }
          }
        }
      }
    });

    doughnutChart = new Chart(pieCtx, {
      type:"doughnut",
      data:{labels:[], datasets:[{
        data:[],
        backgroundColor:[
          "rgba(67,206,162,.90)",
          "rgba(24,90,157,.90)",
          "rgba(247,201,72,.90)",
          "rgba(255,93,93,.90)",
          "rgba(164,121,255,.90)",
          "rgba(255,140,73,.90)",
          "rgba(96,215,255,.90)",
          "rgba(160,255,160,.90)"
        ],
        borderColor:"rgba(255,255,255,.14)",
        borderWidth:1.2
      }]},
      options:{
        responsive:true, maintainAspectRatio:false,
        cutout:"64%",
        plugins:{
          legend:{ position:"top", labels:{ boxWidth:12 } },
          tooltip:{ callbacks:{ label:(ctx)=> ` ${ctx.label}: ${formatWeight(ctx.parsed)}` } },
          donutCenterText: {}
        }
      }
    });
  }

  function wireUI(){
    $("btnSaveAdmin").onclick = () => {
      const v = $("adminKeyInput").value.trim();
      if (!v) return alert("Incolla la ADMIN key.");
      localStorage.setItem(LS_ADMIN, v);
      $("adminKeyInput").value = "";
      alert("Admin key salvata ✅");
      refresh();
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

      if (!isFinite(wn) || !isFinite(cn) || wn <= 0 || cn <= 0 || wn >= cn || cn > 100){
        return alert("Valori non validi. Regola: 0 < warn < critical ≤ 100");
      }
      thresholds = { warn: wn, critical: cn };
      saveThresholds();
      updateThresholdLabels();
      refresh();
    };

    $("btnSimEvent").onclick = async () => {
      try{
        if (!ingestKey()) return alert("Manca Ingest key (X-Ingest-Key).");
        const bin = $("simBin").value.trim();
        const mat = $("simMat").value.trim();
        const w = Number($("simW").value || 0);
        if (!bin || !mat || !w) return alert("Compila bin_id, materiale e grammi.");

        const resp = await fetchJSON("/api/event", {
          method:"POST",
          headers:{ "Content-Type":"application/json", "X-Ingest-Key": ingestKey() },
          body: JSON.stringify({ bin_id: bin, material: mat, weight_g: w })
        });

        $("simW").value = "";
        const r = $("simResult");
        r.style.display = "block";
        r.textContent =
          `✅ Evento registrato\n` +
          `bin: ${resp.bin_id}\n` +
          `materiale: ${resp.material}\n` +
          `peso: ${formatWeight(resp.weight_g)}\n` +
          `CO₂ risparmiata: ${formatCO2(resp.co2_saved_g)}\n` +
          `riempimento: ${Math.round(resp.bin.fill_percent)}%`;

        await refresh();
      }catch(e){
        $("simResult").style.display = "none";
        alert("Errore invio evento:\n\n" + (e?.message || String(e)));
      }
    };

    $("btnExportEvents").onclick = async () => {
      try{
        if (!adminKey()) return alert("Manca Admin key.");
        await downloadFile("/api/export/events.csv", "sorti_events.csv", { "X-API-Key": adminKey() });
      }catch(e){
        alert("Errore export eventi:\n\n" + (e?.message || String(e)));
      }
    };

    $("btnExportDaily").onclick = async () => {
      try{
        if (!adminKey()) return alert("Manca Admin key.");
        const d = Math.max(1, Math.min(365, Number($("exportDays").value || 30)));
        await downloadFile(`/api/export/daily.csv?days=${d}`, `sorti_daily_${d}d.csv`, { "X-API-Key": adminKey() });
      }catch(e){
        alert("Errore export daily:\n\n" + (e?.message || String(e)));
      }
    };

    $("rangeSel").onchange = () => refresh();
    $("onlyAlerts").onchange = () => refresh();
  }

  document.addEventListener("DOMContentLoaded", () => {
    loadThresholds();
    updateThresholdLabels();
    initCharts();
    wireUI();
    startSSE();
    refresh();
  });

})();
