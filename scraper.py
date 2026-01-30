import json
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
OUTFILE_PRIMARY = Path("data/weekly_etfs.json")   # UI reads this
OUTFILE_BACKUP  = Path("data/items.json")        # fallback + history comparisons
ALERTS_FILE     = Path("data/alerts.json")
HISTORY_DIR     = Path("data/history")

WEEKLYPAYERS_LIST_URL = "https://weeklypayers.com/"
WEEKLYPAYERS_CAL_URL  = "https://weeklypayers.com/calendar/"

# If the scrape fails, don't wipe your dataset:
MIN_EXPECTED_ITEMS = 25

ALERT_DROP_PCT = -15.0

UA = {
    "User-Agent": "weekly-etf-dashboard/3.0 (+github-actions)",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_FETCH_CACHE: Dict[str, str] = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35


# =========================
# Helpers
# =========================
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
    t = t.replace("$", "").replace(",", "")
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

    # Normalize variants
    t = t.replace("Sept.", "Sep.").replace("Sept ", "Sep ")
    # Common formats
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except Exception:
            pass

    # Try to pull Month dd, yyyy from inside strings
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
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

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


# =========================
# Data model
# =========================
@dataclass
class Item:
    ticker: str
    issuer: str
    name: Optional[str] = None

    frequency: str = "Weekly"

    # Dates
    declaration_date: Optional[str] = None
    ex_dividend_date: Optional[str] = None
    record_date: Optional[str] = None
    pay_date: Optional[str] = None

    # Distribution
    distribution_per_share: Optional[float] = None

    # WeeklyPayers provides current price on the list table
    share_price: Optional[float] = None

    # Derived
    div_pct_per_share: Optional[float] = None
    payout_per_1000: Optional[float] = None
    annualized_yield_pct: Optional[float] = None
    monthly_income_per_1000: Optional[float] = None

    # Historical comparisons
    price_chg_ex_1w_pct: Optional[float] = None
    price_chg_ex_1m_pct: Optional[float] = None
    dist_chg_ex_1w_pct: Optional[float] = None
    dist_chg_ex_1m_pct: Optional[float] = None
    days_since_ex_div: Optional[int] = None

    notes: str = ""

    def to_dict(self) -> Dict:
        d = asdict(self)

        # Normalize blanks
        for k, v in list(d.items()):
            if v == "":
                d[k] = None
        return d


# =========================
# WeeklyPayers parsing
# =========================
def parse_weeklypayers_list() -> Dict[str, Item]:
    """
    Parses https://weeklypayers.com/ table "Weekly Dividend ETFs".
    Returns mapping ticker -> Item with price, last dividend, weekly dividend per share, manager.
    """
    soup = fetch_soup(WEEKLYPAYERS_LIST_URL)

    # Find the main table by title text
    # WeeklyPayers uses a DataTables-like table under "Weekly Dividend ETFs"
    table = None
    for t in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in t.find_all("th")]
        if not headers:
            continue
        header_blob = " | ".join(headers)

        # we expect: ticker, fund manager, current price, last dividend, dividend per $, etc.
        if ("ticker" in header_blob and "fund manager" in header_blob and "current price" in header_blob):
            table = t
            break

    if not table:
        return {}

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]

    def idx_of(needle: str) -> Optional[int]:
        for i, h in enumerate(headers):
            if needle in h:
                return i
        return None

    idx_ticker = idx_of("ticker")
    idx_mgr    = idx_of("fund manager")
    idx_price  = idx_of("current price")
    idx_last   = idx_of("last dividend")

    # WeeklyPayers shows "Dividend per $" in screenshot
    # Sometimes it may be named slightly differently; try a few needles.
    idx_div_per_dollar = None
    for needle in ["dividend per $", "dividend per", "dividend/$", "dividend per dollar"]:
        idx_div_per_dollar = idx_of(needle)
        if idx_div_per_dollar is not None:
            break

    items: Dict[str, Item] = {}

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i):
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        ticker = (cell(idx_ticker) or "").upper()
        if not ticker or not re.match(r"^[A-Z0-9]{2,6}$", ticker):
            continue

        issuer = cell(idx_mgr) or "Other"
        px = _parse_float(cell(idx_price))
        last_div = _parse_float(cell(idx_last))

        # If available, infer weekly dividend from "Dividend per $":  (div_per_$ * price)
        dist = None
        div_per_dollar = _parse_float(cell(idx_div_per_dollar)) if idx_div_per_dollar is not None else None
        if div_per_dollar is not None and px is not None:
            dist = div_per_dollar * px
        elif last_div is not None:
            # fallback: use last dividend as weekly distribution (WeeklyPayers is weekly ETF list)
            dist = last_div

        it = Item(
            ticker=ticker,
            issuer=issuer,
            name=None,
            share_price=px,
            distribution_per_share=dist,
            notes="Source: weeklypayers.com (list)"
        )
        items[ticker] = it

    return items


def parse_weeklypayers_calendar_month() -> Dict[str, Dict[str, Optional[str]]]:
    """
    Parses https://weeklypayers.com/calendar/ for Ex/Record and Payment dates.
    WeeklyPayers calendar shows colored blocks:
      - Ex/Record (pink)
      - Payment (green)
    Returns mapping: ticker -> { ex_dividend_date, record_date, pay_date }
    """
    soup = fetch_soup(WEEKLYPAYERS_CAL_URL)

    # Month + day cells are rendered in HTML; tickers appear in many colored <span>/<div>.
    # We'll parse by scanning each day cell and capturing:
    #   - The day date label for that cell (Month is on page title: "Dividend Calendar January 2026")
    #   - Tickers within the cell, separated by color meaning.
    text = soup.get_text("\n", strip=True)

    # Find month/year from heading like: "Dividend Calendar January 2026"
    m = re.search(r"Dividend Calendar\s+([A-Za-z]+)\s+(\d{4})", text)
    if not m:
        # fallback: try "January 2026" anywhere
        m = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b\s+(\d{4})", text)
    if not m:
        return {}

    month_name = m.group(1)
    year = int(m.group(2))

    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    }
    month = month_map.get(month_name.lower())
    if not month:
        return {}

    # Heuristic: day number appears as a header in each cell. We'll iterate all elements and
    # detect day numbers + ticker blocks by traversing the calendar table.
    # We cannot rely on exact class names, so we detect:
    #   - a day number 1..31
    #   - ticker tokens (A-Z 2-6)
    # and assign them to that day based on DOM cell grouping.

    # Find candidate "day cells"
    # WeeklyPayers uses a calendar grid table; the tickers are grouped inside each day cell.
    day_cells = []
    for el in soup.find_all(["td", "div", "section"]):
        # day cells usually contain many tickers; quickly filter by having multiple tickers text
        raw = el.get_text(" ", strip=True)
        if not raw:
            continue
        # must contain at least one ticker-ish token
        if re.search(r"\b[A-Z]{2,6}\b", raw) and re.search(r"\b(1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31)\b", raw):
            day_cells.append(el)

    # If too many false positives, narrow to <td> first
    tds = soup.find_all("td")
    if tds:
        day_cells = tds

    out: Dict[str, Dict[str, Optional[str]]] = {}

    # Color detection:
    # Payment blocks appear green, Ex/Record appear pink in your screenshots.
    # We'll detect via class/style containing 'green'/'pink' OR known words 'payment'/'ex'
    def classify_block(tag) -> str:
        cls = " ".join(tag.get("class", [])).lower()
        style = (tag.get("style") or "").lower()
        blob = f"{cls} {style}"

        if "green" in blob:
            return "pay"
        if "pink" in blob or "red" in blob:
            return "exrec"
        # fallback: unknown
        return "unknown"

    for cell in day_cells:
        # Find a day number in this cell
        cell_text = cell.get_text(" ", strip=True)
        dm = re.search(r"\b(3[01]|[12]\d|[1-9])\b", cell_text)
        if not dm:
            continue
        day_num = int(dm.group(1))
        try:
            day_date = date(year, month, day_num).isoformat()
        except Exception:
            continue

        # Find ticker blocks inside this cell
        # We'll look at spans/divs and read tickers grouped by class color
        blocks = cell.find_all(["span", "div"])
        if not blocks:
            # fallback: just extract all tickers as unknown
            tokens = re.findall(r"\b[A-Z]{2,6}\b", cell_text)
            for t in tokens:
                rec = out.setdefault(t, {"ex_dividend_date": None, "record_date": None, "pay_date": None})
                # If we don't know, don't overwrite
            continue

        for b in blocks:
            block_text = b.get_text(" ", strip=True)
            if not block_text:
                continue
            tickers = re.findall(r"\b[A-Z]{2,6}\b", block_text)
            if not tickers:
                continue

            kind = classify_block(b)

            for t in tickers:
                rec = out.setdefault(t, {"ex_dividend_date": None, "record_date": None, "pay_date": None})

                if kind == "pay":
                    rec["pay_date"] = day_date
                elif kind == "exrec":
                    # WeeklyPayers groups Ex and Record together, so set both
                    rec["ex_dividend_date"] = day_date
                    rec["record_date"] = day_date

    return out


# =========================
# History comparisons + alerts
# =========================
def write_history_snapshot(payload: Dict) -> Path:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = HISTORY_DIR / f"{day}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path

def load_history(days: int = 45) -> List[Dict]:
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

def compute_ex_div_comparisons(items: List[Dict]) -> None:
    history = load_history(45)

    timeline: Dict[str, List[Dict]] = {}
    for snap in history:
        snap_date = (snap.get("generated_at", "")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency", "")).lower() != "weekly":
                continue
            t = it.get("ticker")
            if not t:
                continue
            timeline.setdefault(t, []).append({
                "run_date": snap_date,
                "ex_div": it.get("ex_dividend_date"),
                "price": it.get("share_price"),
                "dist": it.get("distribution_per_share"),
            })

    today = date.today()

    for it in items:
        t = it.get("ticker")
        rows = timeline.get(t, [])
        rows = [r for r in rows if r.get("ex_div")]
        if len(rows) < 2:
            continue
        rows.sort(key=lambda x: x["ex_div"])

        latest = rows[-1]
        try:
            latest_ex = date.fromisoformat(latest["ex_div"])
        except Exception:
            continue

        def find_prior(days_back: int):
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

        if prev_m:
            it["price_chg_ex_1m_pct"] = pct_change(latest["price"], prev_m["price"])
            it["dist_chg_ex_1m_pct"]  = pct_change(latest["dist"], prev_m["dist"])

def generate_alerts(items: List[Dict]) -> List[Dict]:
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


# =========================
# Build items
# =========================
def build_items() -> List[Dict]:
    base = parse_weeklypayers_list()
    cal = parse_weeklypayers_calendar_month()

    # Merge calendar dates into base items
    for t, it in base.items():
        c = cal.get(t)
        if c:
            it.ex_dividend_date = c.get("ex_dividend_date")
            it.record_date = c.get("record_date")
            it.pay_date = c.get("pay_date")
            it.notes = (it.notes + " | Source: weeklypayers.com (calendar)")

        # Derived calculations (weekly dist*52/price)
        px = it.share_price
        dist = it.distribution_per_share
        if px is not None and dist is not None and px > 0:
            it.div_pct_per_share = (dist / px) * 100.0
            it.payout_per_1000 = (1000.0 / px) * dist
            it.annualized_yield_pct = (dist * 52.0 / px) * 100.0
            it.monthly_income_per_1000 = (it.payout_per_1000 * 52.0) / 12.0

    items = [base[k].to_dict() for k in sorted(base.keys())]

    # Safety net: if scrape failed badly, restore previous snapshot
    if len(items) < MIN_EXPECTED_ITEMS:
        prev = read_json_if_exists(OUTFILE_BACKUP, None)
        if isinstance(prev, dict) and isinstance(prev.get("items"), list) and len(prev["items"]) >= MIN_EXPECTED_ITEMS:
            return prev["items"]
        if isinstance(prev, list) and len(prev) >= MIN_EXPECTED_ITEMS:
            return prev
        # else keep small result

    return items


def main():
    items = build_items()
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "items": items
    }

    # history snapshot first
    write_history_snapshot(payload)

    # comparisons (needs history)
    compute_ex_div_comparisons(items)
    payload["items"] = items

    OUTFILE_PRIMARY.parent.mkdir(parents=True, exist_ok=True)

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
