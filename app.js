// app.js
// UI for Weekly ETF Dashboard
// - Collapsible alerts (saves space)
// - Robust rendering even when fields are missing
// - Uses data/weekly_etfs.json primarily, falls back to data/items.json

const DATA_PRIMARY = "data/weekly_etfs.json";
const DATA_FALLBACK = "data/items.json";
const ALERTS_PATH = "data/alerts.json";

function el(tag, attrs = {}, children = []) {
  const n = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === "class") n.className = v;
    else if (k === "html") n.innerHTML = v;
    else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v);
  });
  children.forEach(c => n.appendChild(typeof c === "string" ? document.createTextNode(c) : c));
  return n;
}

async function fetchJson(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`${url} => ${r.status}`);
  return await r.json();
}

async function loadData() {
  try {
    return await fetchJson(DATA_PRIMARY);
  } catch (e) {
    console.warn("Primary data load failed, falling back:", e);
    return await fetchJson(DATA_FALLBACK);
  }
}

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtPct(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)}%`;
}

function fmtMoney(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  return `$${n.toFixed(digits)}`;
}

function safeText(x) {
  return (x === null || x === undefined || x === "") ? "—" : String(x);
}

function issuerBucket(issuer) {
  const s = (issuer || "").toLowerCase();
  if (s.includes("yieldmax")) return "YieldMax";
  if (s.includes("granite")) return "GraniteShares";
  if (s.includes("roundhill")) return "Roundhill";
  if (!issuer) return "Other";
  return "Other";
}

function renderSummary(payload, items) {
  const summaryBar = document.getElementById("summaryBar");
  if (!summaryBar) return;

  const counts = {
    total: items.length,
    YieldMax: 0,
    GraniteShares: 0,
    Roundhill: 0,
    Other: 0
  };

  items.forEach(it => {
    const b = issuerBucket(it.issuer);
    counts[b] = (counts[b] || 0) + 1;
  });

  summaryBar.innerHTML = "";
  summaryBar.appendChild(el("div", { class: "pill" }, [`Weekly ETFs tracked: ${counts.total}`]));
  summaryBar.appendChild(el("div", { class: "pill" }, [`YieldMax: ${counts.YieldMax}`]));
  summaryBar.appendChild(el("div", { class: "pill" }, [`GraniteShares: ${counts.GraniteShares}`]));
  summaryBar.appendChild(el("div", { class: "pill" }, [`Roundhill: ${counts.Roundhill}`]));
  summaryBar.appendChild(el("div", { class: "pill" }, [`Other: ${counts.Other}`]));
  summaryBar.appendChild(el("div", { class: "pill" }, [`Last update: ${safeText(payload.generated_at)}`]));
}

async function renderAlerts() {
  const alertsWrap = document.getElementById("alerts");
  if (!alertsWrap) return;

  alertsWrap.innerHTML = "Loading alerts…";

  let data;
  try {
    data = await fetchJson(ALERTS_PATH);
  } catch (e) {
    alertsWrap.innerHTML = "";
    alertsWrap.appendChild(el("div", { class: "muted" }, ["Alerts not available."]));
    return;
  }

  const alerts = Array.isArray(data.alerts) ? data.alerts : [];
  alertsWrap.innerHTML = "";

  // Collapsible block
  const details = el("details", { class: "alerts-details" }, []);
  // Default collapsed; if you want default open, add: details.setAttribute("open","")
  const summary = el("summary", { class: "alerts-summary" }, [
    `Alerts (${alerts.length}) — click to expand`
  ]);

  details.appendChild(summary);

  const list = el("div", { class: "alerts-list" }, []);
  if (alerts.length === 0) {
    list.appendChild(el("div", { class: "muted" }, ["No alerts."]));
  } else {
    alerts.forEach(a => {
      const line = el("div", { class: "alert-line" }, [
        el("strong", {}, [safeText(a.ticker)]),
        ` — ${safeText(a.message)} `,
        el("span", { class: "muted" }, [a.ex_dividend_date ? `(Ex-div: ${a.ex_dividend_date})` : "" ])
      ]);
      list.appendChild(line);
    });
  }

  details.appendChild(list);
  alertsWrap.appendChild(details);
}

function renderTable(items) {
  const tbody = document.querySelector("#etfTable tbody");
  if (!tbody) return;

  tbody.innerHTML = "";

  items.forEach(it => {
    const tr = document.createElement("tr");

    // Match your existing table headers/columns:
    // Ticker | Issuer | Reference | Dist | Div% | Freq | Decl | Ex | Rec | Pay | NAV | Price | Price% | Dist% | etc...
    // We'll render a safe subset that matches what you've shown and leaves blanks as "—".

    const cols = [
      it.ticker,
      it.name || issuerBucket(it.issuer), // if name missing, at least show issuer bucket
      it.issuer,
      it.reference_asset,
      fmtMoney(it.distribution_per_share, 4),
      fmtPct(it.div_pct_per_share, 2),
      it.frequency,
      it.declaration_date,
      it.ex_dividend_date,
      it.record_date,
      it.pay_date,
      "—", // NAV (not provided by WeeklyPayers)
      fmtMoney(it.share_price, 2),
      it.price_chg_ex_1w_pct == null ? "—" : fmtPct(it.price_chg_ex_1w_pct, 2),
      it.price_chg_ex_1m_pct == null ? "—" : fmtPct(it.price_chg_ex_1m_pct, 2),
      it.dist_chg_ex_1w_pct == null ? "—" : fmtPct(it.dist_chg_ex_1w_pct, 2),
      it.dist_chg_ex_1m_pct == null ? "—" : fmtPct(it.dist_chg_ex_1m_pct, 2),
      it.days_since_ex_div == null ? "—" : safeText(it.days_since_ex_div),
      it.dist_stability_score == null ? "—" : safeText(it.dist_stability_score),
      it.annualized_yield_pct == null ? "—" : fmtPct(it.annualized_yield_pct, 2),
    ];

    cols.forEach(v => {
      const td = document.createElement("td");
      td.textContent = safeText(v);
      tr.appendChild
