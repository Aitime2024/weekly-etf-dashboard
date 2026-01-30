# scraper.py
# WeeklyPayers-only scraper for Weekly ETF Dashboard
# Outputs:
#   data/weekly_etfs.json (primary)
#   data/items.json       (backup)
#   data/alerts.json
#   data/history/YYYY-MM-DD.json
#
# Notes:
# - Uses https://weeklypayers.com/ as the primary source for prices + last dividend
# - Uses https://weeklypayers.com/calendar/ to populate ex/record + pay dates
# - Annualized yield calculation is: dist * 52 / price
# - Alerts are generated from historical ex-div snapshots (like before), but simplified + robust

import json
import re
import time
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
WEEKLYPAYERS_BASE = "https://weeklypayers.com"
URL_TABLE = f"{WEEKLYPAYERS_BASE}/"
URL_CAL  = f"{WEEKLYPAYERS_BASE}/calendar/"

OUTFILE_PRIMARY = Path("data/weekly_etfs.json")
OUTFILE_BACKUP  = Path("data/items.json")
ALERTS_FILE     = Path("data/alerts.json")
HISTORY_DIR     = Path("data/history")

ALERT_DROP_PCT  = -15.0
MIN_EXPECTED_ITEMS = 25

UA = {
    "User-Agent": "weekly-etf-dashboard/3.0 (+github-actions)",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Requests throttling (be nice)
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35

# =========================
# Helpers
# =========================
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _parse_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    t = t.replace("$", "").replace(",", "")
    t = re.sub(r"[^0-9.\-]", "", t)
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None

def _parse_pct(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    t = t.replace("%", "").strip()
    return _parse_float(t)

def _parse_date_guess(s: Optional[str]) -> Optional[str]:
    """Return ISO date (YYYY-MM-DD) if possible."""
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None

    # common formats on the calendar
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except Exception:
            pass

    # Try embedded "Month dd yyyy"
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2})(?:,)?\s+(\d{4})", t)
    if m:
        mon, dd, yyyy = m.group(1), m.group(2), m.group(3)
        try:
            return datetime.strptime(f"{mon} {dd} {yyyy}", "%b %d %Y").date().isoformat()
        except Exception:
            try:
                return datetime.strptime(f"{mon} {dd} {yyyy}", "%B %d %Y").date().isoformat()
            except Exception:
                return None

    return None

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

def fetch_soup(url: str) -> BeautifulSoup:
    global _LAST_FETCH_AT
    now = time.time()
    dt = now - _LAST_FETCH_AT
    if dt < _MIN_FETCH_INTERVAL_SEC:
        time.sleep(_MIN_FETCH_INTERVAL_SEC - dt)

    r = requests.get(url, headers=UA, timeout=40)
    r.raise_for_status()
    _LAST_FETCH_AT = time.time()
    return BeautifulSoup(r.text, "lxml")

def read_json_if_exists(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default

def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=False), encoding="utf-8")

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def write_history_snapshot(payload: dict) -> Path:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = HISTORY_DIR / f"{day}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path

def load_history(days=60) -> List[dict]:
    if not HISTORY_DIR.exists():
        return []
    files = sorted(HISTORY_DIR.glob("*.json"))[-days:]
    out = []
    for f in files:
        try:
            out.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            pass
    return out

def stability_score_from_dist(dists: List[Optional[float]]) -> Optional[float]:
    vals = [float(x) for x in dists if x is not None]
    if len(vals) < 4:
        return None
    mean = statistics.mean(vals)
    if mean <= 0:
        return None
    stdev = statistics.pstdev(vals)
    cv = stdev / mean
    cuts = sum(1 for a, b in zip(vals[:-1], vals[1:]) if b < a)
    cut_rate = cuts / (len(vals) - 1)
    score = 100
    score -= 60 * cut_rate
    score -= 80 * cv
    return round(clamp(score, 0, 100), 1)

def trend_slope(values: List[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    if len(vals) < 4:
        return None
    return (vals[-1] - vals[0]) / (len(vals) - 1)


# =========================
# 1) Parse main WeeklyPayers table (prices + last dividend)
# =========================
@dataclass
class TableRow:
    ticker: str
    issuer: Optional[str]
    current_price: Optional[float]
    last_dividend: Optional[float]
    ann_yield_pct_site: Optional[float]

def _find_best_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """
    WeeklyPayers uses a visible HTML table (DataTables).
    We'll pick the table that contains headers like "Ticker" and "Current Price".
    """
    candidates = soup.find_all("table")
    best = None
    best_score = 0

    for tbl in candidates:
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        if not headers:
            continue
        blob = " ".join(headers)
        score = 0
        for key in ["ticker", "fund", "manager", "current price", "last dividend", "ann. yield", "ann yield", "dividend per"]:
            if key in blob:
                score += 1
        if score > best_score:
            best_score = score
            best = tbl

    return best

def parse_weeklypayers_table() -> List[TableRow]:
    soup = fetch_soup(URL_TABLE)
    tbl = _find_best_table(soup)
    if not tbl:
        return []

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]

    def idx_of(*needles) -> Optional[int]:
        for i, h in enumerate(headers):
            ok = True
            for n in needles:
                if n not in h:
                    ok = False
                    break
            if ok:
                return i
        return None

    idx_ticker = idx_of("ticker")
    idx_mgr    = idx_of("fund") or idx_of("manager") or idx_of("fund manager")
    idx_price  = idx_of("current", "price") or idx_of("price")
    idx_last   = idx_of("last", "dividend") or idx_of("last dividend")
    idx_ann    = idx_of("ann") or idx_of("ann.", "yield") or idx_of("yield")

    if idx_ticker is None:
        return []

    out: List[TableRow] = []
    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i: Optional[int]) -> Optional[str]:
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        raw_ticker = cell(idx_ticker)
        if not raw_ticker:
            continue

        # Sometimes tickers show a "+" expand icon; extract clean symbol
        m = re.search(r"\b[A-Z]{1,6}\b", raw_ticker.upper())
        if not m:
            continue
        ticker = m.group(0).upper()

        issuer = cell(idx_mgr)
        price  = _parse_float(cell(idx_price))
        last   = _parse_float(cell(idx_last))
        annpct = _parse_pct(cell(idx_ann))

        out.append(TableRow(
            ticker=ticker,
            issuer=issuer,
            current_price=price,
            last_dividend=last,
            ann_yield_pct_site=annpct,
        ))

    # dedupe by ticker
    dedup: Dict[str, TableRow] = {}
    for r in out:
        dedup[r.ticker] = r
    return list(dedup.values())


# =========================
# 2) Parse WeeklyPayers calendar (ex/record + pay dates)
# =========================
# The calendar view shows:
# - Month header: "Dividend Calendar January 2026" OR week view "Week of ..."
# - Cells per weekday with tickers colored:
#     - Ex/Record (pink)
#     - Payment (green)
#
# We do a robust parse:
# - scan ALL visible date labels inside the calendar grid
# - for each day cell, collect ticker tokens inside that cell
# - attempt to classify by CSS class names or by legend-based colors
#   (fallback: if cannot classify, store in "unknown")
#
# Result mapping:
#   dates_map[ticker] = {"ex_dividend_date": iso?, "record_date": iso?, "pay_date": iso?}
#
# WeeklyPayers seems to conflate Ex + Record in same bucket, so we store it as ex_dividend_date
# (and also record_date = ex_dividend_date if you want both)

def _month_year_from_title(text: str) -> Optional[Tuple[int, int]]:
    """
    Extract (year, month) from headers like "Dividend Calendar January 2026"
    or "Week of January 19, 2026"
    """
    t = norm_space(text)
    # Month view
    m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})", t, re.I)
    if m:
        month_name = m.group(1)
        year = int(m.group(2))
        month = datetime.strptime(month_name[:3], "%b").month
        return year, month

    # Week view "Week of January 19, 2026"
    m = re.search(r"Week of\s+([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", t, re.I)
    if m:
        month_name = m.group(1)
        year = int(m.group(3))
        month = datetime.strptime(month_name[:3], "%b").month
        return year, month

    return None

def _extract_calendar_root(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """
    Grab the main calendar area by looking for weekday headers or a big calendar table/grid.
    """
    # Try common containers
    for sel in ["div.calendar", "div#calendar", "div.calendar-container", "main"]:
        node = soup.select_one(sel)
        if node and len(node.get_text(strip=True)) > 20:
            # heuristically accept
            return node

    # fallback: whole doc
    return soup

def _guess_day_cells(root: BeautifulSoup) -> List[BeautifulSoup]:
    """
    WeeklyPayers calendar is not guaranteed to be a <table>.
    We'll attempt:
    - any <td> cells that contain weekday columns
    - any <div> blocks that contain a day number + many tickers
    """
    # Prefer td cells (many calendars use tables)
    tds = root.find_all("td")
    # Filter: contain a day number somewhere
    td_cells = []
    for td in tds:
        txt = norm_space(td.get_text(" ", strip=True))
        if re.search(r"\b\d{1,2}\b", txt) and re.search(r"\b[A-Z]{2,6}\b", txt):
            td_cells.append(td)
    if len(td_cells) >= 10:
        return td_cells

    # fallback: div "cells"
    divs = root.find_all("div")
    div_cells = []
    for d in divs:
        txt = norm_space(d.get_text(" ", strip=True))
        # day number + tickers
        if re.search(r"\b\d{1,2}\b", txt) and len(re.findall(r"\b[A-Z]{2,6}\b", txt)) >= 3:
            div_cells.append(d)
    return div_cells

def _extract_day_number(cell: BeautifulSoup) -> Optional[int]:
    # Find first day number in the cell
    txt = norm_space(cell.get_text(" ", strip=True))
    m = re.search(r"\b(\d{1,2})\b", txt)
    if not m:
        return None
    day = int(m.group(1))
    if 1 <= day <= 31:
        return day
    return None

def _extract_tickers_from_cell(cell: BeautifulSoup) -> List[str]:
    # Extract all ticker-like tokens
    tokens = re.findall(r"\b[A-Z]{2,6}\b", cell.get_text(" ", strip=True).upper())
    # remove weekday names / common words
    blacklist = {"MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY","SATURDAY","SUNDAY"}
    out = []
    for t in tokens:
        if t in blacklist:
            continue
        out.append(t)
    return sorted(set(out))

def _classify_cell_events(cell: BeautifulSoup) -> Dict[str, List[str]]:
    """
    Try to classify tickers in this cell as ex/record vs payment based on classes.
    WeeklyPayers uses colored chips; often implemented as spans with classes.
    We'll scan for elements containing ticker text and use class hints:
      - contains 'payment' or 'pay' => payment
      - contains 'ex' or 'record' => ex_record
      - fallback: unknown
    """
    ex_record: List[str] = []
    payment: List[str] = []
    unknown: List[str] = []

    # Look for small elements (spans/a/div) that contain tickers
    for el in cell.find_all(["span", "a", "div", "p", "li"]):
        txt = norm_space(el.get_text(" ", strip=True)).upper()
        if not txt:
            continue
        tickers = re.findall(r"\b[A-Z]{2,6}\b", txt)
        if not tickers:
            continue
        cls = " ".join(el.get("class") or []).lower()

        bucket = "unknown"
        if "pay" in cls or "payment" in cls or "paid" in cls:
            bucket = "payment"
        if "ex" in cls or "record" in cls:
            bucket = "ex_record"

        # Sometimes color class names:
        # green => payment, pink/red => ex/record
        if bucket == "unknown":
            if any(k in cls for k in ["green", "success"]):
                bucket = "payment"
            elif any(k in cls for k in ["red", "pink", "danger"]):
                bucket = "ex_record"

        for t in tickers:
            if bucket == "payment":
                payment.append(t)
            elif bucket == "ex_record":
                ex_record.append(t)
            else:
                unknown.append(t)

    # If classification failed (no elements), fallback to all tickers unknown
    if not ex_record and not payment:
        unknown = _extract_tickers_from_cell(cell)

    return {
        "ex_record": sorted(set(ex_record)),
        "payment": sorted(set(payment)),
        "unknown": sorted(set(unknown)),
    }

def parse_weeklypayers_calendar(max_months_ahead: int = 2) -> Dict[str, Dict[str, str]]:
    """
    Parse current calendar page and attempt to build ticker -> dates.
    We only have one page URL; it has next/prev in UI but no guaranteed query params.
    So we extract whatever is visible (usually current month).
    """
    soup = fetch_soup(URL_CAL)
    root = _extract_calendar_root(soup)

    # Extract month/year from page header text
    page_text = soup.get_text("\n", strip=True)
    my = None
    # Try h1/h2 first
    for tag in soup.find_all(["h1", "h2", "h3"]):
        my = _month_year_from_title(tag.get_text(" ", strip=True))
        if my:
            break
    if not my:
        my = _month_year_from_title(page_text)
    if not my:
        # fallback: use current UTC month/year
        today = datetime.now(timezone.utc).date()
        my = (today.year, today.month)

    year, month = my

    cells = _guess_day_cells(root)
    if not cells:
        return {}

    # Build a map ticker -> best dates (prefer the soonest upcoming)
    today = datetime.now(timezone.utc).date()

    def cell_date_for_day(day_num: int) -> Optional[date]:
        try:
            return date(year, month, day_num)
        except Exception:
            return None

    # We store the "best" date:
    # prefer upcoming (>=today), otherwise keep latest past date
    def pick_better(existing_iso: Optional[str], candidate: date) -> str:
        cand_iso = candidate.isoformat()
        if not existing_iso:
            return cand_iso
        try:
            existing = date.fromisoformat(existing_iso)
        except Exception:
            return cand_iso

        # Choose date closer to today but not in past if possible
        # Priority: upcoming dates, then closest absolute
        ex_up = existing >= today
        ca_up = candidate >= today
        if ca_up and not ex_up:
            return cand_iso
        if ex_up and not ca_up:
            return existing_iso

        # both upcoming or both past => pick closest to today
        if abs((candidate - today).days) < abs((existing - today).days):
            return cand_iso
        return existing_iso

    out: Dict[str, Dict[str, str]] = {}

    for cell in cells:
        day_num = _extract_day_number(cell)
        if day_num is None:
            continue
        d = cell_date_for_day(day_num)
        if not d:
            continue

        buckets = _classify_cell_events(cell)
        ex_tickers = buckets["ex_record"]
        pay_tickers = buckets["payment"]
        unk_tickers = buckets["unknown"]

        # If we couldn't classify and everything is unknown,
        # we do nothing here (dates would be unreliable).
        # BUT: WeeklyPayers calendar clearly labels Ex/Record vs Payment by color.
        # If class parsing fails, you'll still get unknowns. We keep a gentle fallback:
        # - if there is a legend present, sometimes the tickers are in separate containers
        #   and our classify picks them up. If not, unknown remains.
        #
        # We'll still set unknown -> ex_dividend_date as a last-resort,
        # because "missing dates" is worse than "maybe ex date".
        if unk_tickers and not ex_tickers and not pay_tickers:
            ex_tickers = unk_tickers
            unk_tickers = []

        for t in ex_tickers:
            rec = out.setdefault(t, {})
            rec["ex_dividend_date"] = pick_better(rec.get("ex_dividend_date"), d)
            rec["record_date"] = rec["ex_dividend_date"]  # WeeklyPayers groups them

        for t in pay_tickers:
            rec = out.setdefault(t, {})
            rec["pay_date"] = pick_better(rec.get("pay_date"), d)

    return out


# =========================
# 3) Build items + derived metrics
# =========================
def compute_history_comparisons(items: List[dict]) -> None:
    """
    Adds:
      - days_since_ex_div
      - price_chg_ex_1w_pct, price_chg_ex_1m_pct
      - dist_chg_ex_1w_pct, dist_chg_ex_1m_pct
      - dist_stability_score, dist_sum_8w, dist_slope_8w
    Based on history snapshots that contain ex_dividend_date + price + dist.
    """
    history = load_history(days=60)
    timeline = defaultdict(list)

    for snap in history:
        snap_date = (snap.get("generated_at", "")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency", "")).lower() != "weekly":
                continue
            t = it.get("ticker")
            ex = it.get("ex_dividend_date")
            if not t or not ex:
                continue
            timeline[t].append({
                "run_date": snap_date,
                "ex_div": ex,
                "price": it.get("share_price"),
                "dist": it.get("distribution_per_share"),
            })

    today = datetime.now(timezone.utc).date()

    for it in items:
        t = it.get("ticker")
        rows = timeline.get(t, [])
        if len(rows) < 2:
            continue

        # group by ex_div
        by_ex = {}
        for r in rows:
            by_ex[r["ex_div"]] = r
        ex_dates_sorted = sorted(by_ex.keys())
        if len(ex_dates_sorted) < 2:
            continue

        latest_ex_iso = ex_dates_sorted[-1]
        try:
            latest_ex = date.fromisoformat(latest_ex_iso)
        except Exception:
            continue

        it["days_since_ex_div"] = (today - latest_ex).days

        # find prior approx 7d / 30d
        def find_prior(days_back: int) -> Optional[dict]:
            for ex_iso in reversed(ex_dates_sorted[:-1]):
                try:
                    ex_d = date.fromisoformat(ex_iso)
                except Exception:
                    continue
                delta = (latest_ex - ex_d).days
                if abs(delta - days_back) <= 3:
                    return by_ex[ex_iso]
            return None

        prev_w = find_prior(7)
        prev_m = find_prior(30)

        latest = by_ex[latest_ex_iso]
        if prev_w:
            it["price_chg_ex_1w_pct"] = pct_change(latest.get("price"), prev_w.get("price"))
            it["dist_chg_ex_1w_pct"]  = pct_change(latest.get("dist"),  prev_w.get("dist"))
        if prev_m:
            it["price_chg_ex_1m_pct"] = pct_change(latest.get("price"), prev_m.get("price"))
            it["dist_chg_ex_1m_pct"]  = pct_change(latest.get("dist"),  prev_m.get("dist"))

        # last 8 ex-div events
        last_ex_isos = ex_dates_sorted[-8:]
        last_dists = [by_ex[x].get("dist") for x in last_ex_isos]
        it["dist_stability_score"] = stability_score_from_dist(last_dists)

        d8 = [x for x in last_dists if x is not None]
        it["dist_sum_8w"] = round(sum(d8), 4) if len(d8) >= 4 else None

        sl = trend_slope(last_dists)
        it["dist_slope_8w"] = round(sl, 6) if sl is not None else None


def generate_alerts(items: List[dict]) -> List[dict]:
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
    # stable order
    alerts.sort(key=lambda a: (a.get("type",""), a.get("ticker","")))
    return alerts


def build_items() -> List[dict]:
    # A) main table: price + last dividend + issuer
    rows = parse_weeklypayers_table()

    # Safety fallback restore (do not wipe)
    if len(rows) < MIN_EXPECTED_ITEMS:
        prev = read_json_if_exists(OUTFILE_BACKUP, None)
        if isinstance(prev, dict) and isinstance(prev.get("items"), list) and len(prev["items"]) >= MIN_EXPECTED_ITEMS:
            return prev["items"]
        if isinstance(prev, list) and len(prev) >= MIN_EXPECTED_ITEMS:
            return prev
        # else keep the small result

    # B) calendar dates map
    cal_map = parse_weeklypayers_calendar()

    items: List[dict] = []
    for r in rows:
        price = r.current_price
        dist  = r.last_dividend

        issuer = r.issuer or "Other"

        item = {
            # identity
            "ticker": r.ticker,
            "name": None,                 # WeeklyPayers table doesn't clearly include fund name in screenshots
            "issuer": issuer,
            "reference_asset": None,
            "frequency": "Weekly",

            # core distribution + dates
            "distribution_per_share": dist,
            "declaration_date": None,
            "ex_dividend_date": None,
            "record_date": None,
            "pay_date": None,

            # pricing (WeeklyPayers provides current price)
            "share_price": price,

            # derived
            "div_pct_per_share": None,        # dist/price * 100
            "payout_per_1000": None,          # (1000/price)*dist
            "annualized_yield_pct": None,     # dist*52/price *100
            "monthly_income_per_1000": None,  # annual payout / 12

            # history/comparison fields
            "price_chg_ex_1w_pct": None,
            "price_chg_ex_1m_pct": None,
            "dist_chg_ex_1w_pct": None,
            "dist_chg_ex_1m_pct": None,
            "days_since_ex_div": None,
            "dist_sum_8w": None,
            "dist_slope_8w": None,
            "dist_stability_score": None,

            # metadata
            "source": "weeklypayers.com",
            "notes": URL_TABLE
        }

        # dates from calendar
        d = cal_map.get(r.ticker)
        if d:
            item["ex_dividend_date"] = d.get("ex_dividend_date")
            item["record_date"] = d.get("record_date")
            item["pay_date"] = d.get("pay_date")
            item["notes"] = f"{URL_TABLE} | {URL_CAL}"

        # derived calcs (dist*52/price)
        if price is not None and dist is not None and price > 0:
            item["div_pct_per_share"] = (dist / price) * 100.0
            item["payout_per_1000"] = (1000.0 / price) * dist
            item["annualized_yield_pct"] = (dist * 52.0 / price) * 100.0
            item["monthly_income_per_1000"] = (item["payout_per_1000"] * 52.0) / 12.0

        items.append(item)

    # stable sort
    items.sort(key=lambda x: x.get("ticker", ""))

    return items


def main():
    items = build_items()

    payload = {
        "generated_at": iso_utc_now(),
        "source": {
            "table": URL_TABLE,
            "calendar": URL_CAL
        },
        "items": items
    }

    # Write history snapshot BEFORE comparisons
    write_history_snapshot(payload)

    # Comparisons
    compute_history_comparisons(items)
    payload["items"] = items

    # Write main outputs
    write_json(OUTFILE_PRIMARY, payload)
    write_json(OUTFILE_BACKUP, payload)

    alerts = generate_alerts(items)
    write_json(ALERTS_FILE, {
        "generated_at": payload["generated_at"],
        "threshold_drop_pct": ALERT_DROP_PCT,
        "alerts": alerts
    })

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_BACKUP} with {len(items)} items; alerts={len(alerts)}")


if __name__ == "__main__":
    main()
