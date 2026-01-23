import json
import re
import time
import statistics
from datetime import datetime, timezone, date
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
OUTFILE_PRIMARY = "data/weekly_etfs.json"   # UI reads this
OUTFILE_BACKUP  = "data/items.json"        # fallback + history comparisons
ALERTS_FILE     = "data/alerts.json"

ALERT_DROP_PCT  = -15.0
MIN_EXPECTED_ITEMS = 25

USE_YAHOO_PRICES = False  # requested: keep disabled

WEEKLYPAYERS_HOME = "https://weeklypayers.com/"
WEEKLYPAYERS_CAL  = "https://weeklypayers.com/calendar/"

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

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

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

def dedupe_by_ticker(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for it in items:
        t = (it.get("ticker") or "").upper().strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(it)
    return out


# =========================
# WeeklyPayers parsing
# =========================
def weeklypayers_parse_weekly_table() -> List[Dict]:
    """
    Parse WeeklyPayers weekly dividend ETFs table from homepage.
    Expected columns (as shown in your screenshot):
      Ticker | Fund Manager | Current Price | Last Dividend | Ann. Yield % | ...
    We only need: ticker, manager, current_price, last_dividend (dist), and optionally name.
    """
    soup = fetch_soup(WEEKLYPAYERS_HOME)

    # Find a table that looks like the "Weekly Dividend ETFs" table
    target = None
    for tbl in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        blob = " ".join(headers)
        if ("ticker" in blob and "fund manager" in blob and "current price" in blob and "last dividend" in blob):
            target = tbl
            break

    if not target:
        # Fallback: take the first table with "Ticker" and "Last Dividend"
        for tbl in soup.find_all("table"):
            headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
            blob = " ".join(headers)
            if ("ticker" in blob and "last dividend" in blob):
                target = tbl
                break

    if not target:
        return []

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in target.find_all("th")]

    def idx_of(needle: str) -> Optional[int]:
        for i, h in enumerate(headers):
            if needle in h:
                return i
        return None

    i_ticker  = idx_of("ticker")
    i_mgr     = idx_of("fund manager") or idx_of("manager")
    i_price   = idx_of("current price") or idx_of("price")
    i_lastdiv = idx_of("last dividend") or idx_of("dividend")
    i_name    = idx_of("fund") or idx_of("name")  # may not exist

    out = []
    for tr in target.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i):
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        ticker = cell(i_ticker)
        if not ticker:
            continue

        # sometimes first "td" is a + expander, shifting columns; try to recover
        # If ticker doesn't look like a symbol, attempt shift by 1
        if not re.fullmatch(r"[A-Z]{1,6}", ticker.upper()):
            if len(tds) >= 2:
                maybe = norm_space(tds[1].get_text(" ", strip=True))
                if re.fullmatch(r"[A-Z]{1,6}", maybe.upper()):
                    # shift all indices by +1
                    def cell_shifted(i):
                        if i is None:
                            return None
                        return cell(i + 1)
                    ticker = cell_shifted(i_ticker) or ticker
                    mgr = cell_shifted(i_mgr)
                    price = cell_shifted(i_price)
                    last_div = cell_shifted(i_lastdiv)
                    name = cell_shifted(i_name)
                else:
                    mgr = cell(i_mgr)
                    price = cell(i_price)
                    last_div = cell(i_lastdiv)
                    name = cell(i_name)
            else:
                mgr = cell(i_mgr)
                price = cell(i_price)
                last_div = cell(i_lastdiv)
                name = cell(i_name)
        else:
            mgr = cell(i_mgr)
            price = cell(i_price)
            last_div = cell(i_lastdiv)
            name = cell(i_name)

        ticker_u = ticker.upper().strip()
        px = _parse_float(price)
        dist = _parse_float(last_div)

        out.append({
            "ticker": ticker_u,
            "issuer": mgr or "Other",
            "name": name,
            "share_price": px,                    # WeeklyPayers price
            "distribution_per_share": dist,       # WeeklyPayers last dividend
            "notes": f"Source: {WEEKLYPAYERS_HOME}"
        })

    return dedupe_by_ticker(out)


def weeklypayers_parse_calendar_week() -> Dict[str, Dict]:
    """
    Parse WeeklyPayers calendar page.
    Your screenshot shows:
      - Pink tags = Ex/Record
      - Green tags = Payment
    We’ll capture dates per ticker:
      ex_record_dates: [YYYY-MM-DD...]
      payment_dates:   [YYYY-MM-DD...]
    Then, for each ticker, pick the nearest upcoming date for ex_dividend_date and pay_date.
    """
    soup = fetch_soup(WEEKLYPAYERS_CAL)

    # Attempt to detect the "week currently shown" dates by reading the day headers:
    # e.g., "Tuesday Jan 20" ... This is a best-effort.
    # We'll scan each day column for a header containing a month/day.
    day_blocks = []

    # Common structures: tables, div grids. We'll just find elements that look like day columns.
    # Heuristic: a container that contains many tickers and has a date-ish header.
    date_header_regex = re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b", re.IGNORECASE)

    candidates = soup.find_all(["td", "div"], limit=5000)
    for el in candidates:
        txt = norm_space(el.get_text(" ", strip=True))
        # day blocks tend to be "dense"
        if len(txt) < 30:
            continue
        if date_header_regex.search(txt) and re.search(r"\b[A-Z]{3,6}\b", txt):
            # likely a day cell
            day_blocks.append(el)

    # If heuristic fails, fallback to scanning the whole page for date blocks via table cells
    if not day_blocks:
        day_blocks = soup.find_all("td")

    # We need a way to associate tickers with the specific day date.
    # We’ll find a date string inside each block, parse it, then find ticker tags inside.
    ticker_map = defaultdict(lambda: {"ex_record_dates": [], "payment_dates": [], "source_url": WEEKLYPAYERS_CAL})

    # Build a "current year" guess.
    current_year = datetime.now().year

    def parse_month_day_to_iso(month_day: str) -> Optional[str]:
        # month_day could be "Jan 23" or "January 23" etc.
        md = month_day.strip()
        # If calendar is January and you're in late year, this can be off; but good enough.
        for fmt in ("%b %d", "%B %d"):
            try:
                d = datetime.strptime(md, fmt).date().replace(year=current_year)
                return d.isoformat()
            except Exception:
                pass
        return None

    # Classes may not be stable; so we also use the legend words if present.
    # Best effort: if element has style/background or class names containing "payment" or "ex"
    for block in day_blocks:
        block_text = norm_space(block.get_text(" ", strip=True))

        # Find a date token like "Jan 23" or "January 23"
        m = re.search(r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2}\b",
                      block_text, flags=re.IGNORECASE)
        day_iso = parse_month_day_to_iso(m.group(0)) if m else None

        # Find tickers in this block
        tickers = re.findall(r"\b[A-Z]{3,6}\b", block_text)
        if not tickers:
            continue

        # If we cannot parse a date for this block, skip adding dates (still okay)
        if not day_iso:
            continue

        # Now attempt to classify each ticker as ex/record vs payment using HTML hints:
        # - if the ticker appears inside an element whose class contains "payment" => payment
        # - if class contains "ex" or "record" => ex/record
        # Otherwise, add to both lists as a safe fallback.
        # This prevents “all dates missing” even if classes change.
        per_ticker_class = defaultdict(set)

        # Search descendants for short nodes (likely ticker tags)
        for node in block.find_all(["span", "div", "a", "p"]):
            ttxt = norm_space(node.get_text(" ", strip=True))
            if not re.fullmatch(r"[A-Z]{3,6}", ttxt):
                continue
            cls = " ".join(node.get("class") or []).lower()
            style = (node.get("style") or "").lower()
            hint = cls + " " + style
            per_ticker_class[ttxt].add(hint)

        for t in set(tickers):
            hints = " ".join(per_ticker_class.get(t, []))
            if "pay" in hints or "payment" in hints or "green" in hints:
                ticker_map[t]["payment_dates"].append(day_iso)
            elif "ex" in hints or "record" in hints or "pink" in hints:
                ticker_map[t]["ex_record_dates"].append(day_iso)
            else:
                # unknown tag type; add to both as conservative fallback
                ticker_map[t]["ex_record_dates"].append(day_iso)
                ticker_map[t]["payment_dates"].append(day_iso)

    # Deduplicate + sort
    for t, v in ticker_map.items():
        v["ex_record_dates"] = sorted(set(v["ex_record_dates"]))
        v["payment_dates"] = sorted(set(v["payment_dates"]))

    return dict(ticker_map)


# =========================
# History + metrics (unchanged)
# =========================
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
        snap_date = (snap.get("generated_at","")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency","")).lower() != "weekly":
                continue
            ticker = it.get("ticker")
            if not ticker:
                continue
            timeline[ticker].append({
                "run_date": snap_date,
                "ex_div": it.get("ex_dividend_date"),
                "price": it.get("share_price") or it.get("price_proxy"),
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


# =========================
# Build items (WeeklyPayers-only)
# =========================
def build_items() -> List[Dict]:
    # 1) Universe + price + last dividend all from WeeklyPayers homepage table
    base_items = weeklypayers_parse_weekly_table()
    print(f"[weeklypayers] parsed homepage rows={len(base_items)}")

    # 2) Dates from WeeklyPayers calendar
    cal_map = weeklypayers_parse_calendar_week()
    print(f"[weeklypayers] parsed calendar tickers={len(cal_map)}")

    items: List[Dict] = []
    for b in base_items:
        t = b["ticker"]
        issuer = b.get("issuer") or "Other"
        px = b.get("share_price")
        dist = b.get("distribution_per_share")

        # choose dates (best effort):
        cal = cal_map.get(t, {})
        ex_dates = cal.get("ex_record_dates") or []
        pay_dates = cal.get("payment_dates") or []

        # pick "next" date (>= today) if possible, else last known
        today_iso = date.today().isoformat()

        def pick_next(dates: List[str]) -> Optional[str]:
            if not dates:
                return None
            future = [d for d in dates if d >= today_iso]
            return future[0] if future else dates[-1]

        ex_dividend_date = pick_next(ex_dates)
        pay_date = pick_next(pay_dates)

        row = {
            "ticker": t,
            "name": b.get("name"),
            "issuer": issuer,
            "reference_asset": None,
            "frequency": "Weekly",

            "distribution_per_share": dist,
            "declaration_date": None,
            "ex_dividend_date": ex_dividend_date,
            "record_date": ex_dividend_date,  # WeeklyPayers labels "Ex/Record" together
            "pay_date": pay_date,

            # Prices: WeeklyPayers only
            "share_price": px,
            "price_proxy": px,  # keep filled so UI math always has something

            # Derived columns
            "div_pct_per_share": None,
            "payout_per_1000": None,
            "annualized_yield_pct": None,     # dist*52/price * 100
            "monthly_income_per_1000": None,

            # Comparison columns
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

            "notes": b.get("notes") or ""
        }

        # derived calculations (requested formula dist*52/price)
        if px is not None and dist is not None and px > 0:
            row["div_pct_per_share"] = (dist / px) * 100.0
            row["payout_per_1000"] = (1000.0 / px) * dist
            row["annualized_yield_pct"] = (dist * 52.0 / px) * 100.0
            row["monthly_income_per_1000"] = (row["payout_per_1000"] * 52.0) / 12.0

        # add calendar source note
        if cal and cal.get("source_url"):
            row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + f"Calendar: {cal['source_url']}")

        items.append(row)

    items = dedupe_by_ticker(items)

    # Safety net: if scrape fails badly, restore prior snapshot instead of wiping
    if len(items) < MIN_EXPECTED_ITEMS:
        prev = read_json_if_exists(Path(OUTFILE_BACKUP), None)
        if isinstance(prev, dict) and isinstance(prev.get("items"), list) and len(prev["items"]) >= MIN_EXPECTED_ITEMS:
            items = prev["items"]
            print(f"[fallback] restored previous snapshot from {OUTFILE_BACKUP}, count={len(items)}")
        elif isinstance(prev, list) and len(prev) >= MIN_EXPECTED_ITEMS:
            items = prev
            print(f"[fallback] restored previous snapshot(list) from {OUTFILE_BACKUP}, count={len(items)}")
        else:
            print("[fallback] no valid previous snapshot found; keeping small result")

    return items


# =========================
# Main
# =========================
def main():
    items = build_items()
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "items": items
    }

    # Write history first (so comparisons can look back)
    write_history_snapshot(payload)

    # Compute comparisons using history (including today's snapshot)
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
    print("NOTE: Prices are WeeklyPayers-only (USE_YAHOO_PRICES=False). Annualized yield = dist*52/price.")

if __name__ == "__main__":
    main()
