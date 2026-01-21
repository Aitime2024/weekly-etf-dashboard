const issuerOrder = { "YieldMax": 1, "GraniteShares": 2, "Roundhill": 3, "Other": 9 };

function normalizeIssuer(i) {
  if (!i) return "Other";
  const s = String(i).toLowerCase();
  if (s.includes("yieldmax")) return "YieldMax";
  if (s.includes("granite")) return "GraniteShares";
  if (s.includes("roundhill")) return "Roundhill";
  return "Other";
}

function setHighContrast(on) {
  document.body.classList.toggle("hc", !!on);
  try { localStorage.setItem("hc", on ? "1" : "0"); } catch {}
}

function initHighContrastToggle() {
  const el = document.getElementById("highContrast");
  if (!el) return;

  let saved = "0";
  try { saved = localStorage.getItem("hc") || "0"; } catch {}
  const on = saved === "1";

  el.checked = on;
  setHighContrast(on);

  el.addEventListener("change", () => setHighContrast(el.checked));
}

const fmt2 = (cell) => {
  const v = cell.getValue();
  if (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) return "—";
  return Number(v).toFixed(2);
};

const fmt4 = (cell) => {
  const v = cell.getValue();
  if (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) return "—";
  return Number(v).toFixed(4);
};

const fmtPctAccessible = (cell) => {
  const v = cell.getValue();
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  const n = Number(v);

  let cls = "zero";
  let icon = "●";
  if (n > 0) { cls = "pos"; icon = "▲"; }
  else if (n < 0) { cls = "neg"; icon = "▼"; }

  return `<span class="${cls}">${icon} ${n.toFixed(2)}%</span>`;
};

// Div %/Share formatter (plain percent, not up/down arrows)
const fmtDivPct = (cell) => {
  const v = cell.getValue();
  if (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) return "—";
  return `${Number(v).toFixed(2)}%`;
};

async function loadAlerts() {
  try {
    const r = await fetch("./data/alerts.json", { cache: "no-store" });
    const a = await r.json();
    const box = document.getElementById("alertsBox");

    if (!a.alerts || a.alerts.length === 0) {
      box.textContent = `No alerts (threshold ${a.threshold_drop_pct}%).`;
      return;
    }

    box.innerHTML = a.alerts.slice(0, 20).map(x =>
      `<div class="alertItem"><b>${x.ticker}</b> — ${x.message} (Ex-div: ${x.ex_dividend_date || "—"})</div>`
    ).join("");
  } catch {
    const box = document.getElementById("alertsBox");
    if (box) box.textContent = "No alerts data yet.";
  }
}

function renderSummary(rows, generatedAt) {
  const counts = { YieldMax: 0, GraniteShares: 0, Roundhill: 0, Other: 0 };
  for (const r of rows) {
    const g = r.issuer_group || "Other";
    counts[g] = (counts[g] ?? 0) + 1;
  }
  const total = rows.length;

  const box = document.getElementById("summaryBox");
  if (!box) return;

  box.innerHTML = `
    <span class="summaryPill">Weekly ETFs tracked: ${total}</span>
    <span class="summaryPill">YieldMax: ${counts.YieldMax}</span>
    <span class="summaryPill">GraniteShares: ${counts.GraniteShares}</span>
    <span class="summaryPill">Roundhill: ${counts.Roundhill}</span>
    <span class="summaryPill">Other: ${counts.Other}</span>
    <span class="summaryPill">Last update: ${generatedAt}</span>
  `;
}

async function main() {
  initHighContrastToggle();
  await loadAlerts();

  const res = await fetch("./data/weekly_etfs.json", { cache: "no-store" });
  const payload = await res.json();

  document.getElementById("lastUpdated").textContent = `Last updated: ${payload.generated_at}`;

  const rows = (payload.items || []).map(x => {
    const issuer_group = normalizeIssuer(x.issuer);
    const issuer_sort = issuerOrder[issuer_group] ?? 9;

    // Use proxy price as the share price (since that's what you have in JSON)
    const price = (x.price_proxy !== null && x.price_proxy !== undefined) ? Number(x.price_proxy) : null;
    const dist = (x.distribution_per_share !== null && x.distribution_per_share !== undefined) ? Number(x.distribution_per_share) : null;

    // Div %/Share = dist / price * 100
    let div_pct_per_share = null;
    if (Number.isFinite(price) && price > 0 && Number.isFinite(dist) && dist >= 0) {
      div_pct_per_share = (dist / price) * 100;
    }

    return {
      ...x,
      issuer_group,
      issuer_sort,

      // New fields for the table
      share_price: Number.isFinite(price) ? price : null,
      div_pct_per_share,
    };
  });

  renderSummary(rows, payload.generated_at);

  const table = new Tabulator("#table", {
    data: rows,
    layout: "fitDataStretch",
    height: "calc(100vh - 235px)",
    initialSort: [
      { column: "issuer_sort", dir: "asc" },
      { column: "ticker", dir: "asc" },
    ],
    columns: [
      { title: "Ticker", field: "ticker", width: 90 },
      { title: "ETF Name", field: "name", minWidth: 260 },
      { title: "Issuer", field: "issuer_group", width: 140 },
      { title: "Reference Asset", field: "reference_asset", width: 150 },

      // ✅ NEW: Share Price BEFORE Distribution
      { title: "Share Price", field: "share_price", width: 120, sorter: "number", formatter: fmt2 },

      { title: "Distribution/Share", field: "distribution_per_share", width: 160, sorter: "number", formatter: fmt4 },

      // ✅ NEW: Div %/Share AFTER Distribution
      { title: "Div %/Share", field: "div_pct_per_share", width: 120, sorter: "number", formatter: fmtDivPct },

      { title: "Frequency", field: "frequency", width: 110 },

      { title: "Declaration", field: "declaration_date", width: 120 },
      { title: "Ex-Dividend", field: "ex_dividend_date", width: 120 },
      { title: "Record", field: "record_date", width: 105 },
      { title: "Pay", field: "pay_date", width: 95 },

      { title: "NAV (Official)", field: "nav_official", width: 120, sorter: "number", formatter: fmt2 },
      { title: "Price (Proxy)", field: "price_proxy", width: 110, sorter: "number", formatter: fmt2 },

      { title: "Price % (Ex-1w)", field: "price_chg_ex_1w_pct", width: 140, sorter: "number", formatter: fmtPctAccessible },
      { title: "Price % (Ex-1m)", field: "price_chg_ex_1m_pct", width: 140, sorter: "number", formatter: fmtPctAccessible },

      { title: "Dist % (Ex-1w)", field: "dist_chg_ex_1w_pct", width: 130, sorter: "number", formatter: fmtPctAccessible },
      { title: "Dist % (Ex-1m)", field: "dist_chg_ex_1m_pct", width: 130, sorter: "number", formatter: fmtPctAccessible },

      { title: "NAV % (Ex-1w)", field: "nav_chg_ex_1w_pct", width: 130, sorter: "number", formatter: fmtPctAccessible },
      { title: "NAV % (Ex-1m)", field: "nav_chg_ex_1m_pct", width: 130, sorter: "number", formatter: fmtPctAccessible },

      { title: "Dist Σ 8w", field: "dist_sum_8w", width: 110, sorter: "number", formatter: fmt4 },
      { title: "Dist Slope 8w", field: "dist_slope_8w", width: 120, sorter: "number",
        formatter: (c)=> c.getValue()==null ? "—" : Number(c.getValue()).toFixed(6) },
      { title: "Stability", field: "dist_stability_score", width: 110, sorter: "number",
        formatter: (c)=> c.getValue()==null ? "—" : Number(c.getValue()).toFixed(1) },

      { title: "Days Since Ex", field: "days_since_ex_div", width: 120, sorter: "number" },
      { title: "Notes", field: "notes", minWidth: 200 },
    ],
  });

  const search = document.getElementById("search");
  const issuerFilter = document.getElementById("issuerFilter");

  function applyFilters() {
    const q = (search.value || "").toLowerCase().trim();
    const issuer = issuerFilter.value;

    table.setFilter((data) => {
      const matchesSearch =
        !q ||
        (data.ticker || "").toLowerCase().includes(q) ||
        (data.name || "").toLowerCase().includes(q) ||
        (data.issuer_group || "").toLowerCase().includes(q) ||
        (data.reference_asset || "").toLowerCase().includes(q);

      const matchesIssuer = !issuer || data.issuer_group === issuer;

      return matchesSearch && matchesIssuer;
    });
  }

  search.addEventListener("input", applyFilters);
  issuerFilter.addEventListener("change", applyFilters);
}

main().catch(err => {
  console.error(err);
  alert("Failed to load data. Check console for details.");
});
