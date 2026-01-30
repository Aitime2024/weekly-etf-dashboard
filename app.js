/* global window, document, fetch */

const DATA_URL = "data/weekly_etfs.json";
const ALERTS_URL = "data/alerts.json";

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(Number(x))) return "—";
  return Number(x).toFixed(digits);
}

function fmtPct(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(Number(x))) return "—";
  return `${Number(x).toFixed(digits)}%`;
}

function fmtDate(iso) {
  if (!iso) return "—";
  return iso;
}

function qs(sel) {
  return document.querySelector(sel);
}

function el(tag, attrs = {}, children = []) {
  const node = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  });
  children.forEach((c) => node.appendChild(c));
  return node;
}

async function loadJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`Failed to load ${url}: ${r.status}`);
  return await r.json();
}

function renderHeader(meta) {
  qs("#lastUpdated").textContent = meta.generated_at || "—";
  qs("#countAll").textContent = String(meta.items?.length || 0);

  // issuer counts
  const counts = {};
  (meta.items || []).forEach((it) => {
    const issuer = it.issuer || "Other";
    counts[issuer] = (counts[issuer] || 0) + 1;
  });

  qs("#countYieldMax").textContent = String(counts["YieldMax"] || 0);
  qs("#countGranite").textContent = String(counts["GraniteShares"] || 0);
  qs("#countRoundhill").textContent = String(counts["Roundhill"] || 0);

  // Everything else
  const known = (counts["YieldMax"] || 0) + (counts["GraniteShares"] || 0) + (counts["Roundhill"] || 0);
  qs("#countOther").textContent = String((meta.items?.length || 0) - known);
}

function renderAlerts(alertPayload) {
  const alerts = (alertPayload && alertPayload.alerts) ? alertPayload.alerts : [];
  const host = qs("#alertsHost");
  host.innerHTML = "";

  const summary = el("div", { class: "alerts-summary" }, [
    el("div", { class: "alerts-title", text: `Alerts (${alerts.length})` }),
    el("button", { class: "btn", id: "alertsToggleBtn", text: "Collapse" })
  ]);

  const list = el("div", { class: "alerts-list", id: "alertsList" });

  if (!alerts.length) {
    list.appendChild(el("div", { class: "alert-item", text: "No alerts today." }));
  } else {
    alerts.forEach((a) => {
      list.appendChild(
        el("div", { class: "alert-item" }, [
          el("span", { class: "alert-ticker", text: a.ticker || "—" }),
          el("span", { class: "alert-msg", text: a.message || "" }),
          el("span", { class: "alert-date", text: a.ex_dividend_date ? `Ex: ${a.ex_dividend_date}` : "" })
        ])
      );
    });
  }

  host.appendChild(summary);
  host.appendChild(list);

  // Collapsible behavior
  let collapsed = false;
  const btn = qs("#alertsToggleBtn");
  btn.addEventListener("click", () => {
    collapsed = !collapsed;
    qs("#alertsList").style.display = collapsed ? "none" : "block";
    btn.textContent = collapsed ? "Expand" : "Collapse";
  });
}

function renderTable(items) {
  const tbody = qs("#tbody");
  tbody.innerHTML = "";

  const search = qs("#searchInput");
  const issuerSel = qs("#issuerSelect");

  function applyFilters() {
    const q = (search.value || "").trim().toLowerCase();
    const issuer = issuerSel.value;

    const filtered = items.filter((it) => {
      const matchIssuer = issuer === "ALL" ? true : (it.issuer || "Other") === issuer;
      const matchText =
        !q ||
        (String(it.ticker || "").toLowerCase().includes(q)) ||
        (String(it.name || "").toLowerCase().includes(q)) ||
        (String(it.issuer || "").toLowerCase().includes(q));

      return matchIssuer && matchText;
    });

    draw(filtered);
  }

  function draw(rows) {
    tbody.innerHTML = "";
    rows.forEach((it) => {
      const tr = document.createElement("tr");

      const dist = it.distribution_per_share;
      const px = it.share_price;

      const cells = [
        it.ticker || "—",
        it.name || "—",
        it.issuer || "Other",
        it.frequency || "Weekly",
        fmtNum(dist, 4),
        fmtPct(it.div_pct_per_share, 2),
        fmtDate(it.declaration_date),
        fmtDate(it.ex_dividend_date),
        fmtDate(it.record_date),
        fmtDate(it.pay_date),
        fmtNum(px, 2),
        fmtPct(it.annualized_yield_pct, 2),
        fmtNum(it.payout_per_1000, 2),
        fmtNum(it.monthly_income_per_1000, 2),
        fmtPct(it.dist_chg_ex_1w_pct, 2),
        fmtPct(it.dist_chg_ex_1m_pct, 2),
        (it.days_since_ex_div === null || it.days_since_ex_div === undefined) ? "—" : String(it.days_since_ex_div),
      ];

      cells.forEach((c) => {
        const td = document.createElement("td");
        td.textContent = c;
        tr.appendChild(td);
      });

      tbody.appendChild(tr);
    });

    qs("#shownCount").textContent = String(rows.length);
  }

  // Populate issuer dropdown
  const issuers = Array.from(new Set(items.map((x) => x.issuer || "Other"))).sort();
  issuerSel.innerHTML = "";
  issuerSel.appendChild(el("option", { value: "ALL", text: "All Issuers" }));
  issuers.forEach((iss) => issuerSel.appendChild(el("option", { value: iss, text: iss })));

  search.addEventListener("input", applyFilters);
  issuerSel.addEventListener("change", applyFilters);

  applyFilters();
}

async function main() {
  try {
    const data = await loadJSON(DATA_URL);
    renderHeader(data);
    renderTable(data.items || []);
  } catch (e) {
    qs("#lastUpdated").textContent = "Failed to load data";
    console.error(e);
  }

  try {
    const alerts = await loadJSON(ALERTS_URL);
    renderAlerts(alerts);
  } catch (e) {
    // Don’t break the whole page if alerts are missing
    renderAlerts({ alerts: [] });
    console.warn("alerts.json missing or failed:", e);
  }
}

window.addEventListener("DOMContentLoaded", main);
