/* global document, fetch */

const DATA_URL = "data/weekly_etfs.json";
const ALERTS_URL = "data/alerts.json";

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  return n.toFixed(digits);
}

function fmtPct(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  return `${n.toFixed(digits)}%`;
}

function fmtMoney(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const n = Number(x);
  return `$${n.toFixed(digits)}`;
}

function byText(q) {
  const el = document.querySelector(q);
  return el ? el.textContent : "";
}

function setText(q, text) {
  const el = document.querySelector(q);
  if (el) el.textContent = text;
}

function setHTML(q, html) {
  const el = document.querySelector(q);
  if (el) el.innerHTML = html;
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

let ALL_ITEMS = [];

function issuerCounts(items) {
  const counts = {};
  for (const it of items) {
    const k = it.issuer || "Other";
    counts[k] = (counts[k] || 0) + 1;
  }
  return counts;
}

function renderTopStats(items, generatedAt) {
  const counts = issuerCounts(items);
  setText("#stat-total", String(items.length));
  setText("#stat-updated", generatedAt || "—");

  // common issuers from your UI pills
  setText("#stat-yieldmax", String(counts["YieldMax"] || 0));
  setText("#stat-granite", String(counts["GraniteShares"] || 0));
  setText("#stat-roundhill", String(counts["RoundHill Investments"] || counts["Roundhill"] || 0));

  const known = (counts["YieldMax"] || 0) + (counts["GraniteShares"] || 0) + (counts["RoundHill Investments"] || counts["Roundhill"] || 0);
  setText("#stat-other", String(items.length - known));
}

function renderAlertsCollapsible(alertPayload) {
  const alerts = (alertPayload && alertPayload.alerts) ? alertPayload.alerts : [];
  const count = alerts.length;

  const container = document.querySelector("#alerts");
  if (!container) return;

  const rows = alerts.slice(0, 200).map(a => {
    const t = escapeHtml(a.ticker || "");
    const msg = escapeHtml(a.message || "");
    const ex = escapeHtml(a.ex_dividend_date || "—");
    return `<div class="alert-row"><b>${t}</b> — ${msg} (Ex-div: ${ex})</div>`;
  }).join("");

  container.innerHTML = `
    <details class="alerts-details" ${count ? "" : "open"}>
      <summary class="alerts-summary">
        <span>Alerts</span>
        <span class="alerts-count">${count}</span>
        <span class="alerts-hint">${count ? "click to expand/collapse" : "no alerts"}</span>
      </summary>
      <div class="alerts-body">
        ${count ? rows : `<div class="muted">No alerts triggered.</div>`}
      </div>
    </details>
  `;
}

function buildRow(it) {
  const t = escapeHtml(it.ticker);
  const name = escapeHtml(it.name || "");
  const issuer = escapeHtml(it.issuer || "Other");
  const ref = escapeHtml(it.reference_asset || "");

  const dist = (it.distribution_per_share !== null && it.distribution_per_share !== undefined)
    ? fmtMoney(it.distribution_per_share, 2)
    : "—";

  const divPct = fmtPct(it.div_pct_per_share, 2);

  const freq = escapeHtml(it.frequency || "Weekly");

  const decl = escapeHtml(it.declaration_date || "");
  const ex   = escapeHtml(it.ex_dividend_date || "");
  const rec  = escapeHtml(it.record_date || "");
  const pay  = escapeHtml(it.pay_date || "");

  const nav = (it.nav_official === null || it.nav_official === undefined) ? "—" : fmtMoney(it.nav_official, 2);
  const price = (it.price_proxy === null || it.price_proxy === undefined) ? "—" : fmtMoney(it.price_proxy, 2);

  const price1w = fmtPct(it.price_chg_ex_1w_pct, 2);
  const price1m = fmtPct(it.price_chg_ex_1m_pct, 2);
  const dist1w  = fmtPct(it.dist_chg_ex_1w_pct, 2);
  const dist1m  = fmtPct(it.dist_chg_ex_1m_pct, 2);
  const nav1w   = fmtPct(it.nav_chg_ex_1w_pct, 2);
  const nav1m   = fmtPct(it.nav_chg_ex_1m_pct, 2);

  return `
    <tr>
      <td class="mono">${t}</td>
      <td>${name}</td>
      <td>${issuer}</td>
      <td class="mono">${ref}</td>
      <td class="mono">${dist}</td>
      <td class="mono">${divPct}</td>
      <td>${freq}</td>
      <td class="mono">${decl || "—"}</td>
      <td class="mono">${ex || "—"}</td>
      <td class="mono">${rec || "—"}</td>
      <td class="mono">${pay || "—"}</td>
      <td class="mono">${nav}</td>
      <td class="mono">${price}</td>
      <td class="mono">${price1w}</td>
      <td class="mono">${price1m}</td>
      <td class="mono">${dist1w}</td>
      <td class="mono">${dist1m}</td>
      <td class="mono">${nav1w}</td>
      <td class="mono">${nav1m}</td>
    </tr>
  `;
}

function renderTable(items) {
  const tbody = document.querySelector("#tbody");
  if (!tbody) return;
  tbody.innerHTML = items.map(buildRow).join("");
}

function applyFilters() {
  const q = (document.querySelector("#search")?.value || "").trim().toLowerCase();
  const issuer = (document.querySelector("#issuer")?.value || "ALL");

  let items = ALL_ITEMS;

  if (issuer !== "ALL") {
    items = items.filter(it => (it.issuer || "Other") === issuer);
  }

  if (q) {
    items = items.filter(it => {
      const hay = `${it.ticker || ""} ${it.name || ""} ${it.issuer || ""}`.toLowerCase();
      return hay.includes(q);
    });
  }

  renderTable(items);
}

function populateIssuerFilter(items) {
  const sel = document.querySelector("#issuer");
  if (!sel) return;

  const counts = issuerCounts(items);
  const issuers = Object.keys(counts).sort((a, b) => a.localeCompare(b));

  sel.innerHTML = `<option value="ALL">All Issuers</option>` +
    issuers.map(i => `<option value="${escapeHtml(i)}">${escapeHtml(i)} (${counts[i]})</option>`).join("");
}

async function init() {
  try {
    const [dataRes, alertRes] = await Promise.all([
      fetch(DATA_URL, { cache: "no-store" }),
      fetch(ALERTS_URL, { cache: "no-store" }).catch(() => null),
    ]);

    if (!dataRes.ok) throw new Error(`Failed to load ${DATA_URL}`);

    const payload = await dataRes.json();
    const items = Array.isArray(payload.items) ? payload.items : [];

    ALL_ITEMS = items;

    renderTopStats(items, payload.generated_at || "");
    populateIssuerFilter(items);
    renderTable(items);

    if (alertRes && alertRes.ok) {
      const alerts = await alertRes.json();
      renderAlertsCollapsible(alerts);
    } else {
      renderAlertsCollapsible({ alerts: [] });
    }

    document.querySelector("#search")?.addEventListener("input", applyFilters);
    document.querySelector("#issuer")?.addEventListener("change", applyFilters);
  } catch (err) {
    console.error(err);
    setHTML("#main", `<div class="error">Error loading data. Check that <code>${DATA_URL}</code> exists and is valid JSON.</div>`);
  }
}

document.addEventListener("DOMContentLoaded", init);
