import json
import re
import time
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone, date
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ============================================================
# Outputs (UI expects these)
# ============================================================
OUTFILE_PRIMARY = "data/weekly_etfs.json"
OUTFILE_BACKUP  = "data/items.json"
ALERTS_FILE     = "data/alerts.json"

# Alert rule: distribution drop vs prior ex-div week/month
ALERT_DROP_PCT = -15.0

# Safety net: if parsing fails, do not wipe dataset
MIN_EXPECTED_ITEMS = 25

# ============================================================
# Primary source: WeeklyPayers
# ============================================================
WEEKLYPAYERS_TABLE_URL = "https://weeklypayers.com/"
WEEKLYPAYERS_CAL_URL   = "https://weeklypayers.com/calendar/"

# If you ever need to limit how far we scan calendar for future dates:
CALENDAR_PAGES_TO_SCAN = 4  # tries "next" paging a few times if present

UA = {
    "User-Agent": "weekly-etf-dashboard/3.0 (+github-actions)",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_FETCH_CACHE: Dict[str, str] = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35

MANUAL_TICKERS_FILE = Path("data/manual_tickers.json")


# ============================================================
# Small helpers
# ============================================================
def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def pct_change(a, b) -> Optional[float]:
    try:
        if a is None or b is None:
            return None
        a = float(a)
        b = float(b)
        if b == 0:
            return None
        return (a - b) / b * 100.0
    except Exception:
        return None

def _parse_float(s: str) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    # remove $, commas, percent signs etc
    t = t.replace("$", "").replace(",", "").replace("%", "")
    t = re.sub(r"[^0-9.\-]", "", t)
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None

def _parse_date_to_iso(s: str) -> Optional[str]:
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None

    t = t.replace("Sept.", "Sep.").replace("Sept ", "Sep ")

    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except Exception:
            pass

    m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", t)
    if m:
        return _parse_date_to_iso(m.group(1))

    return None

def read_json_if_exists(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default

def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def fetch_text(url: str) -> str:
    global _LAST_FETCH_AT
    if url in _FETCH_CACHE:
        return _FETCH_CACHE[url]

    now = time.time()
    dt = now - _LAST_FETCH_AT
    if dt < _MIN_FETCH_INTERVAL_SEC:
        time.sleep(_MIN_FETCH_INTERVAL_SEC - dt)

    r = requests.get(url, timeout=30, headers=UA)
    r.raise_for_status()

    text = r.text
    _FETCH_CACHE[url] = text
    _LAST_FETCH_AT = time.time()
    return text

def fetch_soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_text(url), "lxml")

def dedupe(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for it in items:
        key = (it.get("ticker"), it.get("issuer"))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def load_manual_tickers() -> List[Dict]:
    """
    Optional: data/manual_tickers.json supports entries like:
    [
      {"ticker":"XXXX","issuer":"SomeIssuer","name":"...", "reference_asset":"..."}
    ]
    """
    data = read_json_if_exists(MANUAL_TICKERS_FILE, [])
    if not isinstance(data, list):
        return []
    out = []
    for m in data:
        if not isinstance(m, dict):
            continue
        t = str(m.get("ticker", "")).strip().upper()
        if not t:
            continue
        out.append({
            "ticker": t,
            "issuer": m.get("issuer") or "Other",
            "frequency": "Weekly",
            "name": m.get("name"),
            "reference_asset": m.get("reference_asset"),
            "notes": "Manually added"
        })
    return out


# ============================================================
# WeeklyPayers table parsing
# ============================================================
def _find_best_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """
    WeeklyPayers uses a big sortable table. We'll find the table whose headers
    contain 'Ticker' and 'Last Dividend' and 'Current Price' (or similar).
    """
    for tbl in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        blob = " | ".join(headers)
        if "ticker" in blob and ("last dividend" in blob or "dividend" in blob) and ("current price" in blob or "price" in blob):
            return tbl
    return None

def weeklypayers_discover_from_table() -> List[Dict]:
    soup = fetch_soup(WEEKLYPAYERS_TABLE_URL)
    table = _find_best_table(soup)
    if not table:
        return []

    headers = [norm_space(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    headers_l = [h.lower() for h in headers]

    def idx(*needles) -> Optional[int]:
        for i, h in enumerate(headers_l):
            ok = True
            for n in needles:
                if n not in h:
                    ok = False
                    break
            if ok:
                return i
        return None

    i_ticker = idx("ticker")
    i_mgr    = idx("fund manager") or idx("manager") or idx("issuer")
    i_price  = idx("current", "price") or idx("price")
    i_last   = idx("last", "dividend") or idx("last dividend") or idx("dividend")
    # Some table has "Dividend per $" column (optional)
    i_div_per_dollar = idx("dividend", "per")  # loose
    # Some table has "ETF Name" or similar
    i_name = idx("etf name") or idx("name")

    items = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i):
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        ticker = (cell(i_ticker) or "").upper()
        if not ticker or not re.match(r"^[A-Z0-9\.\-]{1,10}$", ticker):
            continue

        mgr = cell(i_mgr) or "Other"

        price = _parse_float(cell(i_price))
        last_div = _parse_float(cell(i_last))

        # Keep as issuer for UI grouping
        issuer = mgr

        items.append({
            "ticker": ticker,
            "issuer": issuer,
            "frequency": "Weekly",
            "name": cell(i_name),
            "reference_asset": None,
            "wp_current_price": price,
            "wp_last_dividend": last_div,
            "wp_dividend_per_dollar": _parse_float(cell(i_div_per_dollar)) if i_div_per_dollar is not None else None,
            "notes": "Sourced from WeeklyPayers table"
        })

    return dedupe(items)


# ============================================================
# WeeklyPayers calendar parsing (Ex/Record vs Payment)
# ============================================================
@dataclass
class CalHit:
    kind: str  # "EX_RECORD" or "PAYMENT"
    iso_date: str

def _calendar_try_extract_week_header_date(s: str) -> Optional[str]:
    """
    Header looks like "Week of January 19, 2026" or "Dividend Calendar January 2026".
    We'll return ISO for the week start if present.
    """
    m = re.search(r"Week of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", s)
    if m:
        return _parse_date_to_iso(m.group(1))
    return None

def _calendar_parse_day_labels(day_header_text: str, fallback_year: Optional[int] = None) -> Optional[date]:
    """
    Day labels often like:
      "Tuesday Jan 20"
    but year isn't present. We'll infer year from page header if possible.
    """
    t = norm_space(day_header_text)
    m = re.search(r"\b([A-Za-z]{3})\s+(\d{1,2})\b", t)
    if not m:
        return None
    mon_abbr = m.group(1)
    dd = int(m.group(2))
    try:
        # Use 2000 then replace year later
        dt = datetime.strptime(f"{mon_abbr} {dd} 2000", "%b %d %Y").date()
        if fallback_year:
            return date(fallback_year, dt.month, dt.day)
        return dt
    except Exception:
        return None

def weeklypayers_calendar_map(tickers: List[str]) -> Dict[str, Dict]:
    """
    Returns map:
      ticker -> { ex_dividend_date, record_date, pay_date }
    WeeklyPayers calendar shows:
      - pink/red chips: Ex/Record
      - green chips: Payment
    We'll set:
      ex_dividend_date = earliest upcoming EX/RECORD
      record_date = same as ex_dividend_date (site groups them)
      pay_date = earliest upcoming PAYMENT
    """
    if not tickers:
        return {}

    want = set(tickers)
    out_hits: Dict[str, List[CalHit]] = defaultdict(list)

    # We'll scan the calendar page and click "next" pages if a link exists in HTML.
    # If there's no link, we still parse the first page.
    next_url = WEEKLYPAYERS_CAL_URL
    scanned = 0

    today = date.today()

    while next_url and scanned < CALENDAR_PAGES_TO_SCAN:
        scanned += 1
        soup = fetch_soup(next_url)

        page_text = soup.get_text("\n", strip=True)
        # Infer year from any "2026" shown in header area
        year = None
        m_year = re.search(r"\b(20\d{2})\b", page_text)
        if m_year:
            try:
                year = int(m_year.group(1))
            except Exception:
                year = None

        # Find the calendar grid tables (there may be one main grid)
        # We'll look for tables that have weekday column headers.
        cal_tables = []
        for tbl in soup.find_all("table"):
            ths = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
            if any("monday" in h for h in ths) and any("friday" in h for h in ths):
                cal_tables.append(tbl)

        # If not found, also accept div blocks-based layout
        if not cal_tables:
            cal_tables = [None]

        def classify_chip(tag) -> Optional[str]:
            """
            Determine if a ticker chip is EX/RECORD or PAYMENT by CSS class or style.
            We keep it robust: look at class names + legend words nearby.
            """
            cls = " ".join(tag.get("class", [])).lower()
            if "payment" in cls or "pay" in cls or "green" in cls:
                return "PAYMENT"
            if "ex" in cls or "record" in cls or "pink" in cls or "red" in cls:
                return "EX_RECORD"

            # fallback by inline style background color
            style = (tag.get("style") or "").lower()
            if "green" in style:
                return "PAYMENT"
            if "pink" in style or "red" in style:
                return "EX_RECORD"

            # fallback by data-legend attributes
            data_kind = (tag.get("data-type") or tag.get("data-kind") or "").lower()
            if "pay" in data_kind:
                return "PAYMENT"
            if "ex" in data_kind or "record" in data_kind:
                return "EX_RECORD"

            return None

        # Parse table-based layout
        if cal_tables and cal_tables[0] is not None:
            for tbl in cal_tables:
                # Column headers have day labels like "Tuesday Jan 20"
                headers = tbl.find_all("th")
                day_dates: List[Optional[date]] = []
                for th in headers:
                    d = _calendar_parse_day_labels(th.get_text(" ", strip=True), fallback_year=year)
                    day_dates.append(d)

                # Row cells contain ticker chips
                rows = tbl.find_all("tr")
                # Skip header row
                for tr in rows[1:]:
                    tds = tr.find_all("td")
                    for col_i, td in enumerate(tds):
                        d = day_dates[col_i] if col_i < len(day_dates) else None
                        if not d or d < today:
                            continue
                        iso = d.isoformat()

                        # Chips may be spans/divs/links
                        for chip in td.find_all(["span", "a", "div"]):
                            txt = norm_space(chip.get_text(" ", strip=True)).upper()
                            if not txt or txt not in want:
                                continue
                            kind = classify_chip(chip)
                            if not kind:
                                # If the cell is colored, infer from parent class
                                parent_cls = " ".join((td.get("class", []) + tr.get("class", []) + tbl.get("class", []))).lower()
                                if "payment" in parent_cls or "green" in parent_cls:
                                    kind = "PAYMENT"
                                elif "ex" in parent_cls or "record" in parent_cls or "pink" in parent_cls or "red" in parent_cls:
                                    kind = "EX_RECORD"
                            if kind:
                                out_hits[txt].append(CalHit(kind=kind, iso_date=iso))

        # Parse div-based layout fallback
        else:
            # Look for weekday blocks by headings + ticker chips
            # We'll scan for patterns like "Tuesday Jan 20" then subsequent chips nearby.
            blocks = soup.find_all(["div", "section"])
            for b in blocks:
                title = b.get_text(" ", strip=True)
                if not title:
                    continue
                d = _calendar_parse_day_labels(title, fallback_year=year)
                if not d or d < today:
                    continue
                iso = d.isoformat()
                for chip in b.find_all(["span", "a", "div"]):
                    txt = norm_space(chip.get_text(" ", strip=True)).upper()
                    if txt not in want:
                        continue
                    kind = classify_chip(chip)
                    if kind:
                        out_hits[txt].append(CalHit(kind=kind, iso_date=iso))

        # Find "next" link (if calendar has a paging control)
        next_link = None
        for a in soup.find_all("a"):
            label = norm_space(a.get_text(" ", strip=True)).lower()
            href = a.get("href") or ""
            if "next" == label or "›" == label or ">" == label:
                if href and "calendar" in href:
                    next_link = href
                    break

        if next_link:
            if next_link.startswith("http"):
                next_url = next_link
            else:
                next_url = "https://weeklypayers.com" + next_link
        else:
            next_url = None

    # Consolidate hits -> map
    result: Dict[str, Dict] = {}
    for t in tickers:
        hits = out_hits.get(t, [])
        if not hits:
            continue

        ex_dates = sorted({h.iso_date for h in hits if h.kind == "EX_RECORD"})
        pay_dates = sorted({h.iso_date for h in hits if h.kind == "PAYMENT"})

        result[t] = {
            "source_url": WEEKLYPAYERS_CAL_URL,
            "ex_dividend_date": ex_dates[0] if ex_dates else None,
            "record_date": ex_dates[0] if ex_dates else None,
            "pay_date": pay_dates[0] if pay_dates else None,
            "declaration_date": None,
        }

    return result


# ============================================================
# History + metrics (kept from your prior approach)
# ============================================================
def write_history_snapshot(payload):
    hist_dir = Path("data/history")
    hist_dir.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = hist_dir / f"{day}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path

def load_history(days=45):
    hist_dir = Path("data/history")
    if not hist_dir.exists():
        return []
    files = sorted(hist_dir.glob("*.json"))[-days:]
    out = []
    for f in files:
        try:
            out.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    return out

def stability_score_from_dist(dists):
    dists = [float(x) for x in dists if x is not None]
    if len(dists) < 4:
        return None
    mean = statistics.mean(dists)
    if mean <= 0:
        return None
    stdev = statistics.pstdev(dists)
    cv = stdev / mean
    cuts = 0
    for a, b in zip(dists[:-1], dists[1:]):
        if b < a:
            cuts += 1
    cut_rate = cuts / (len(dists) - 1)
    score = 100
    score -= 60 * cut_rate
    score -= 80 * cv
    return round(clamp(score, 0, 100), 1)

def trend_slope(values):
    vals = [float(v) for v in values if v is not None]
    if len(vals) < 4:
        return None
    return (vals[-1] - vals[0]) / (len(vals) - 1)

def compute_ex_div_comparisons(current_items):
    history = load_history(45)
    timeline = defaultdict(list)
    for snap in history:
        snap_date = (snap.get("generated_at", "")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency", "")).lower() != "weekly":
                continue
            ticker = it.get("ticker")
            if not ticker:
                continue
            timeline[ticker].append({
                "run_date": snap_date,
                "ex_div": it.get("ex_dividend_date"),
                "price": it.get("price_proxy"),
                "dist": it.get("distribution_per_share"),
                "nav": it.get("nav_official"),
            })

    today = date.today()

    for it in current_items:
        t = it.get("ticker")
        rows = timeline.get(t, [])
        if not rows:
            continue

        rows.sort(key=lambda x: x["run_date"])
        rows = [r for r in rows if r.get("ex_div")]
        if len(rows) < 2:
            continue

        latest = rows[-1]
        try:
            latest_ex = date.fromisoformat(latest["ex_div"])
        except Exception:
            continue

        def find_prior(days_back):
            for r in reversed(rows[:-1]):
                try:
                    ex = date.fromisoformat(r["ex_div"])
                except Exception:
                    continue
                delta = (latest_ex - ex).days
                if abs(delta - days_back) <= 3:
                    return r
            return None

        prev_w = find_prior(7)
        prev_m = find_prior(30)

        it["days_since_ex_div"] = (today - latest_ex).days

        if prev_w:
            it["price_chg_ex_1w_pct"] = pct_change(latest["price"], prev_w["price"])
            it["dist_chg_ex_1w_pct"]  = pct_change(latest["dist"], prev_w["dist"])
            it["nav_chg_ex_1w_pct"]   = pct_change(latest["nav"],  prev_w["nav"])

        if prev_m:
            it["price_chg_ex_1m_pct"] = pct_change(latest["price"], prev_m["price"])
            it["dist_chg_ex_1m_pct"]  = pct_change(latest["dist"],  prev_m["dist"])
            it["nav_chg_ex_1m_pct"]   = pct_change(latest["nav"],   prev_m["nav"])

        by_ex = {}
        for r in rows:
            by_ex[r["ex_div"]] = r
        ex_dates_sorted = sorted(by_ex.keys())
        last_ex_dates = ex_dates_sorted[-8:]
        last_dists = [by_ex[d].get("dist") for d in last_ex_dates]

        it["dist_stability_score"] = stability_score_from_dist(last_dists)

        d8 = [x for x in last_dists if x is not None]
        it["dist_sum_8w"] = round(sum(d8), 4) if len(d8) >= 4 else None
        sl = trend_slope(last_dists)
        it["dist_slope_8w"] = round(sl, 6) if sl is not None else None

def generate_alerts(items):
    alerts = []
    for it in items:
        w = it.get("dist_chg_ex_1w_pct")
        m = it.get("dist_chg_ex_1m_pct")
        if w is not None and w <= ALERT_DROP_PCT:
            alerts.append({
                "ticker": it["ticker"],
                "type": "DIVIDEND_DROP_VS_1W",
                "pct": round(w, 2),
                "ex_dividend_date": it.get("ex_dividend_date"),
                "message": f"{it['ticker']} distribution down {w:.2f}% vs prior ex-div week"
            })
        if m is not None and m <= ALERT_DROP_PCT:
            alerts.append({
                "ticker": it["ticker"],
                "type": "DIVIDEND_DROP_VS_1M",
                "pct": round(m, 2),
                "ex_dividend_date": it.get("ex_dividend_date"),
                "message": f"{it['ticker']} distribution down {m:.2f}% vs prior ex-div month"
            })
    return alerts


# ============================================================
# Build items (WeeklyPayers-only)
# ============================================================
def build_items() -> List[Dict]:
    # 1) Universe from WeeklyPayers + optional manual list
    discovered = weeklypayers_discover_from_table()
    discovered += load_manual_tickers()
    discovered = dedupe(discovered)

    all_tickers = [d["ticker"] for d in discovered if d.get("ticker")]

    print(f"[discovery] WeeklyPayers table tickers={len(discovered)}")

    # 2) Calendar mapping (dates)
    cal_map = weeklypayers_calendar_map(all_tickers)
    print(f"[calendar] tickers with any date info={len(cal_map)}")

    # 3) Build rows
    items: List[Dict] = []
    for d in discovered:
        ticker = d["ticker"]
        issuer = d.get("issuer") or "Other"

        # WeeklyPayers provides "Current Price" and "Last Dividend"
        price = d.get("wp_current_price")
        dist  = d.get("wp_last_dividend")

        # Dates (from calendar)
        cal = cal_map.get(ticker, {})

        row = {
            "ticker": ticker,
            "name": d.get("name"),
            "issuer": issuer,
            "reference_asset": d.get("reference_asset"),
            "frequency": "Weekly",

            # Core distribution/dates
            "distribution_per_share": dist,
            "declaration_date": None,
            "ex_dividend_date": cal.get("ex_dividend_date"),
            "record_date": cal.get("record_date"),
            "pay_date": cal.get("pay_date"),

            # Prices (Yahoo disabled; WeeklyPayers is primary)
            "share_price": None,
            "price_proxy": price,

            # Derived columns
            "div_pct_per_share": None,
            "payout_per_1000": None,
            "annualized_yield_pct": None,        # dist*52/price
            "monthly_income_per_1000": None,

            # existing columns used by UI comparisons
            "nav_official": None,
            "price_chg_ex_1w_pct": None,
            "price_chg_ex_1m_pct": None,
            "dist_chg_ex_1w_pct": None,
            "dist_chg_ex_1m_pct": None,
            "nav_chg_ex_1w_pct": None,
            "nav_chg_ex_1m_pct": None,
            "days_since_ex_div": None,
            "dist_sum_8w": None,
            "dist_slope_8w": None,
            "dist_stability_score": None,

            "notes": d.get("notes") or ""
        }

        # Notes: include sources
        row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + WEEKLYPAYERS_TABLE_URL)
        if cal.get("source_url"):
            row["notes"] += " | " + str(cal["source_url"])

        # Derived calculations (requires price + dist)
        if price is not None and dist is not None and price > 0:
            row["div_pct_per_share"] = (dist / price) * 100.0
            row["payout_per_1000"] = (1000.0 / price) * dist
            row["annualized_yield_pct"] = (dist * 52.0 / price) * 100.0
            row["monthly_income_per_1000"] = (row["payout_per_1000"] * 52.0) / 12.0

        items.append(row)

    items_weekly = [x for x in items if str(x.get("frequency", "")).lower() == "weekly"]

    # Safety net: if we parsed too few items, restore previous snapshot instead of wiping.
    if len(items_weekly) < MIN_EXPECTED_ITEMS:
        prev = read_json_if_exists(Path(OUTFILE_BACKUP), None)
        if isinstance(prev, dict) and isinstance(prev.get("items"), list) and len(prev["items"]) >= MIN_EXPECTED_ITEMS:
            items_weekly = prev["items"]
            print(f"[fallback] restored previous snapshot from {OUTFILE_BACKUP}, count={len(items_weekly)}")
        elif isinstance(prev, list) and len(prev) >= MIN_EXPECTED_ITEMS:
            items_weekly = prev
            print(f"[fallback] restored previous snapshot(list) from {OUTFILE_BACKUP}, count={len(items_weekly)}")
        else:
            print("[fallback] no valid previous snapshot found; keeping small result")

    return items_weekly


# ============================================================
# Main
# ============================================================
def main():
    items = build_items()
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "items": items
    }

    # Write today's snapshot first
    write_history_snapshot(payload)

    # Compute comparisons using history
    compute_ex_div_comparisons(items)
    payload["items"] = items

    Path("data").mkdir(exist_ok=True)

    Path(OUTFILE_PRIMARY).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    Path(OUTFILE_BACKUP).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    alerts = generate_alerts(items)
    Path(ALERTS_FILE).write_text(json.dumps({
        "generated_at": payload["generated_at"],
        "threshold_drop_pct": ALERT_DROP_PCT,
        "alerts": alerts
    }, indent=2), encoding="utf-8")

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_BACKUP} with {len(items)} items; alerts={len(alerts)}")


if __name__ == "__main__":
    main()
