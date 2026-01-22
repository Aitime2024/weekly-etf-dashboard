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
OUTFILE_BACKUP  = "data/items.json"         # used as fallback + history comparisons
ALERTS_FILE     = "data/alerts.json"
ALERT_DROP_PCT  = -15.0

UA = {
    "User-Agent": "weekly-etf-dashboard/2.0 (+github-actions)",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_FETCH_CACHE: Dict[str, str] = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35

YAHOO_OVERRIDES_FILE = Path("data/yahoo_symbol_overrides.json")
MANUAL_TICKERS_FILE  = Path("data/manual_tickers.json")

# If scraping fails, don't wipe your dataset:
MIN_EXPECTED_ITEMS = 25

# =========================
# Small helpers
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
    # sometimes includes footnotes like "0.1234*"
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
    # normalize common variants
    t = t.replace("Sept.", "Sep.").replace("Sept ", "Sep ")
    # Some tables show "01/19/2026" etc
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except Exception:
            pass
    # Try to extract "Month dd, yyyy" substring
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
    """
    Cached + throttled fetch to reduce rate limits.
    """
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


# =========================
# Yahoo price fetching (ALL ETFs)
# =========================
def _chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def yahoo_quote_prices(symbols: List[str]) -> Dict[str, Optional[float]]:
    """
    Uses Yahoo's quote endpoint to get regularMarketPrice for a list of symbols.
    Returns mapping symbol -> price or None.
    """
    out: Dict[str, Optional[float]] = {}
    if not symbols:
        return out

    # Yahoo endpoint supports many symbols; still chunk for safety
    base = "https://query1.finance.yahoo.com/v7/finance/quote?symbols="
    for batch in _chunk(symbols, 50):
        url = base + ",".join(batch)
        try:
            r = requests.get(url, timeout=20, headers=UA)
            r.raise_for_status()
            data = r.json()
            results = (((data or {}).get("quoteResponse") or {}).get("result") or [])
            found = set()
            for row in results:
                sym = row.get("symbol")
                px = row.get("regularMarketPrice")
                if sym:
                    found.add(sym)
                    try:
                        out[sym] = float(px) if px is not None else None
                    except Exception:
                        out[sym] = None
            # mark missing ones as None
            for sym in batch:
                if sym not in found and sym not in out:
                    out[sym] = None
        except Exception:
            # On a transient error, mark this batch as None so we can fallback later
            for sym in batch:
                out[sym] = None

    return out

def get_price_proxy_stooq(ticker: str) -> Optional[float]:
    """
    Fallback price proxy via Stooq (when Yahoo fails).
    """
    symbol = ticker.lower() + ".us"
    url = f"https://stooq.com/q/l/?s={symbol}&i=d"
    try:
        r = requests.get(url, timeout=20, headers=UA)
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        if len(lines) < 2:
            return None
        cols = lines[1].split(",")
        if len(cols) < 6:
            return None
        close_str = cols[5]
        return float(close_str) if close_str and close_str != "N/A" else None
    except Exception:
        return None

def build_yahoo_symbol_overrides(all_tickers: List[str]) -> Dict[str, str]:
    """
    Writes/updates data/yahoo_symbol_overrides.json so it contains an entry for *every* ETF.
    Default mapping is ticker -> ticker.

    If you ever need special mappings, you can edit this file manually.
    """
    existing = read_json_if_exists(YAHOO_OVERRIDES_FILE, {})
    if not isinstance(existing, dict):
        existing = {}
    out = dict(existing)

    for t in sorted(set(all_tickers)):
        if t not in out:
            out[t] = t  # default 1:1 mapping

    # Also remove obviously invalid keys
    for k in list(out.keys()):
        if not isinstance(k, str) or not isinstance(out[k], str):
            out.pop(k, None)

    write_json(YAHOO_OVERRIDES_FILE, out)
    return out


# =========================
# Discovery
# =========================
def yieldmax_discover_weekly_tight() -> List[Dict]:
    """
    Discover YieldMax tickers that are weekly from their Our ETFs page (table includes frequency).
    """
    url = "https://yieldmaxetfs.com/our-etfs/"
    soup = fetch_soup(url)

    items = []
    for table in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers:
            continue
        blob = " ".join(headers)
        if "ticker" not in blob or "distribution frequency" not in blob:
            continue

        def find_idx(needle: str):
            for i, h in enumerate(headers):
                if needle in h:
                    return i
            return None

        idx_ticker = find_idx("ticker")
        idx_name   = find_idx("etf name")
        idx_ref    = find_idx("reference asset")
        idx_freq   = find_idx("distribution frequency")

        if idx_ticker is None or idx_freq is None:
            continue

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue

            def cell(i):
                if i is None or i >= len(tds):
                    return None
                return norm_space(tds[i].get_text(" ", strip=True)) or None

            ticker = cell(idx_ticker)
            freq = cell(idx_freq)
            if not ticker or not freq:
                continue
            if freq.strip().lower() != "weekly":
                continue

            items.append({
                "ticker": ticker.upper(),
                "issuer": "YieldMax",
                "frequency": "Weekly",
                "name": cell(idx_name),
                "reference_asset": cell(idx_ref),
                "notes": "Discovered via YieldMax Our ETFs"
            })

    return dedupe(items)

def roundhill_discover_weeklypay() -> List[Dict]:
    url = "https://www.roundhillinvestments.com/weeklypay-etfs"
    soup = fetch_soup(url)
    text = soup.get_text("\n", strip=True)
    tickers = sorted(set(re.findall(r"\b[A-Z]{3,5}W\b", text)))
    items = [{
        "ticker": t,
        "issuer": "Roundhill",
        "frequency": "Weekly",
        "name": None,
        "reference_asset": None,
        "notes": "Discovered via Roundhill WeeklyPay"
    } for t in tickers]
    return dedupe(items)

def graniteshares_discover_yieldboost() -> List[Dict]:
    """
    Keep your existing GraniteShares discovery via product guide PDF regex scan,
    but also allow manual_tickers.json to carry the real universe.
    """
    pdf_url = "https://graniteshares.com/media/us4pi2qq/graniteshares-product-guide.pdf"
    try:
        r = requests.get(pdf_url, timeout=30, headers=UA)
        r.raise_for_status()
        blob = r.content.decode("latin-1", errors="ignore")
    except Exception:
        return []

    candidates = sorted(set(re.findall(r"\b[A-Z]{3,5}Y{1,2}\b", blob)))

    def is_weekly_near_ticker(t: str) -> bool:
        return re.search(rf"{t}.{{0,200}}Weekly", blob, flags=re.IGNORECASE | re.DOTALL) is not None

    tickers = [t for t in candidates if is_weekly_near_ticker(t)]

    items = [{
        "ticker": t,
        "issuer": "GraniteShares",
        "frequency": "Weekly",
        "name": None,
        "reference_asset": None,
        "notes": "Discovered via GraniteShares product guide (weekly-only)"
    } for t in tickers]

    return dedupe(items)

def load_manual_tickers() -> List[Dict]:
    """
    data/manual_tickers.json supports entries like:
    [
      {"ticker":"AMYY","issuer":"GraniteShares","name":"YieldBOOST AMD","reference_asset":"AMD"},
      ...
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


# =========================
# Enrichment: distributions/dates
# =========================
def yieldmax_fund_url(ticker: str) -> str:
    return f"https://yieldmaxetfs.com/our-etfs/{ticker.lower()}/"

def yieldmax_parse_fund_page(ticker: str) -> Dict:
    """
    Parses YieldMax fund page distribution table (best effort):
    returns keys: distribution_per_share, declaration_date, ex_dividend_date, record_date, pay_date, source_url
    """
    url = yieldmax_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Find a table that looks like a distribution table
    target = None
    for tbl in soup.find_all("table"):
        ths = tbl.find_all("th")
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in ths]
        blob = " ".join(headers)
        if ("ex" in blob and "record" in blob and "pay" in blob) and ("distribution" in blob or "amount" in blob):
            target = tbl
            break

    if not target:
        # fallback: scan text labels (less reliable)
        text = soup.get_text("\n", strip=True)
        def grab(pat):
            m = re.search(pat, text, flags=re.IGNORECASE)
            return m.group(1).strip() if m else None
        return {
            "source_url": url,
            "declaration_date": _parse_date_to_iso(grab(r"Declaration Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})")),
            "ex_dividend_date": _parse_date_to_iso(grab(r"Ex(?:-| )Dividend Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})")),
            "record_date": _parse_date_to_iso(grab(r"Record Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})")),
            "pay_date": _parse_date_to_iso(grab(r"Pay(?:ment)? Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})")),
            "distribution_per_share": _parse_float(grab(r"Distribution(?:\s+per\s+Share)?[:\s]+\$?\s*([0-9]*\.[0-9]+)")),
        }

    # Map header indexes
    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in target.find_all("th")]
    def idx_contains(*needles) -> Optional[int]:
        for i, h in enumerate(headers):
            if all(n in h for n in needles):
                return i
        return None

    i_decl = idx_contains("declaration")
    i_ex   = idx_contains("ex")
    i_rec  = idx_contains("record")
    i_pay  = idx_contains("pay")
    i_dist = None
    for i, h in enumerate(headers):
        if "distribution" in h and ("share" in h or "per" in h or "amount" in h):
            i_dist = i
            break
    if i_dist is None:
        for i, h in enumerate(headers):
            if "amount" in h:
                i_dist = i
                break

    # Use first data row
    tr = None
    tbody = target.find("tbody")
    if tbody:
        tr = tbody.find("tr")
    if not tr:
        trs = target.find_all("tr")
        tr = trs[1] if len(trs) > 1 else None
    if not tr:
        return {"source_url": url}

    tds = [norm_space(td.get_text(" ", strip=True)) for td in tr.find_all("td")]

    def cell(i):
        if i is None or i >= len(tds):
            return None
        return tds[i]

    out = {"source_url": url}
    out["declaration_date"] = _parse_date_to_iso(cell(i_decl))
    out["ex_dividend_date"] = _parse_date_to_iso(cell(i_ex))
    out["record_date"]      = _parse_date_to_iso(cell(i_rec))
    out["pay_date"]         = _parse_date_to_iso(cell(i_pay))
    out["distribution_per_share"] = _parse_float(cell(i_dist))
    return out

def yieldmax_weekly_distributions_and_dates(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = yieldmax_parse_fund_page(t)
        if info:
            out[t] = info
    return out


# Roundhill
def roundhill_fund_url(ticker: str) -> str:
    return f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"

def _parse_roundhill_table(soup: BeautifulSoup, heading_text: str):
    h = None
    for tag in soup.find_all(["h1","h2","h3","h4","h5"]):
        if heading_text.lower() in tag.get_text(" ", strip=True).lower():
            h = tag
            break
    if not h:
        return None
    return h.find_next("table")

def roundhill_weekly_calendar_and_latest_dist(ticker: str) -> Dict:
    url = roundhill_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Prefer Distribution History (includes amount)
    hist = _parse_roundhill_table(soup, "Distribution History")
    cal  = _parse_roundhill_table(soup, "Distribution Calendar")

    def to_iso_guess(d): return _parse_date_to_iso(d)

    if hist:
        rows = hist.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td","th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            # Expected: Declaration, Ex Date, Record Date, Pay Date, Amount Paid
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = to_iso_guess(vals[0])
                out["ex_dividend_date"] = to_iso_guess(vals[1])
                out["record_date"]      = to_iso_guess(vals[2])
                out["pay_date"]         = to_iso_guess(vals[3])
            if len(vals) >= 5:
                out["distribution_per_share"] = _parse_float(vals[4])
            return out

    if cal:
        rows = cal.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td","th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = to_iso_guess(vals[0])
                out["ex_dividend_date"] = to_iso_guess(vals[1])
                out["record_date"]      = to_iso_guess(vals[2])
                out["pay_date"]         = to_iso_guess(vals[3])
            out["distribution_per_share"] = None
            return out

    return {}

def roundhill_weekly_distributions_and_dates(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = roundhill_weekly_calendar_and_latest_dist(t)
        if info:
            out[t] = info
    return out


# GraniteShares (distribution page)
def graniteshares_yieldboost_distribution_table() -> Tuple[Dict[str, Dict], int]:
    """
    Parse GraniteShares distribution table:
    returns (map, row_count_found)
    """
    url = "https://graniteshares.com/institutional/us/en-us/underlyings/distribution/"
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}, 0

    table = None
    for t in soup.find_all("table"):
        hdr = " ".join([norm_space(th.get_text(" ", strip=True)) for th in t.find_all("th")]).lower()
        if "ticker" in hdr and "distribution per share" in hdr and ("payment date" in hdr or "pay date" in hdr):
            table = t
            break
    if not table:
        return {}, 0

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]

    def idx_of(needle: str) -> Optional[int]:
        for i, h in enumerate(headers):
            if needle in h:
                return i
        return None

    idx_ticker = idx_of("ticker")
    idx_freq   = idx_of("frequency")
    idx_dist   = idx_of("distribution per share")
    idx_ex     = idx_of("ex-date") or idx_of("ex date")
    idx_rec    = idx_of("record")
    idx_pay    = idx_of("payment") or idx_of("pay")

    out: Dict[str, Dict] = {}
    rows_found = 0

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i):
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        ticker = (cell(idx_ticker) or "").upper()
        if not ticker:
            continue

        freq = (cell(idx_freq) or "")
        if freq and freq.strip().lower() != "weekly":
            continue

        rows_found += 1
        out[ticker] = {
            "source_url": url,
            "distribution_per_share": _parse_float(cell(idx_dist)),
            "ex_dividend_date": _parse_date_to_iso(cell(idx_ex)),
            "record_date": _parse_date_to_iso(cell(idx_rec)),
            "pay_date": _parse_date_to_iso(cell(idx_pay)),
            "declaration_date": None,
        }

    return out, rows_found


# =========================
# History + metrics
# =========================
def write_history_snapshot(payload):
    hist_dir = Path("data/history")
    hist_dir.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = hist_dir / f"{day}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
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

        # last 8 ex-div events
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
# Build items (main pipeline)
# =========================
def build_items() -> List[Dict]:
    # 1) Discover universe (KEEP existing logic)
    discovered: List[Dict] = []
    discovered += yieldmax_discover_weekly_tight()
    discovered += graniteshares_discover_yieldboost()
    discovered += roundhill_discover_weeklypay()

    # Manual tickers always included
    discovered += load_manual_tickers()

    discovered = dedupe(discovered)

    print(
        f"[discovery] YieldMax={len([d for d in discovered if d.get('issuer')=='YieldMax'])} "
        f"GraniteShares={len([d for d in discovered if d.get('issuer')=='GraniteShares'])} "
        f"Roundhill={len([d for d in discovered if d.get('issuer')=='Roundhill'])} "
        f"Total={len(discovered)}"
    )

    all_tickers = [d["ticker"] for d in discovered if d.get("ticker")]
    overrides = build_yahoo_symbol_overrides(all_tickers)

    # 2) Yahoo prices for ALL tickers (with overrides)
    yahoo_symbols = [overrides.get(t, t) for t in all_tickers]
    yahoo_map = yahoo_quote_prices(sorted(set(yahoo_symbols)))

    # 3) Distribution + dates maps
    ym_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "YieldMax"]
    rh_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "Roundhill"]
    gs_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "GraniteShares"]

    ym_map = yieldmax_weekly_distributions_and_dates(ym_tickers)
    rh_map = roundhill_weekly_distributions_and_dates(rh_tickers)
    gs_map, gs_rows = graniteshares_yieldboost_distribution_table()

    print(
        f"[enrich] YieldMax fund pages={len(ym_map)} "
        f"Roundhill fund pages={len(rh_map)} "
        f"GraniteShares dist rows={gs_rows}"
    )

    # 4) Build final rows
    items: List[Dict] = []
    for d in discovered:
        ticker = d["ticker"]
        issuer = d.get("issuer") or "Other"

        # Yahoo price (preferred)
        sym = overrides.get(ticker, ticker)
        share_price = yahoo_map.get(sym)

        # Fallback to stooq if Yahoo failed
        price_proxy = share_price
        if price_proxy is None:
            price_proxy = get_price_proxy_stooq(ticker)

        row = {
            "ticker": ticker,
            "name": d.get("name"),
            "issuer": issuer,
            "reference_asset": d.get("reference_asset"),
            "frequency": "Weekly",

            # Core distribution/dates
            "distribution_per_share": None,
            "declaration_date": None,
            "ex_dividend_date": None,
            "record_date": None,
            "pay_date": None,

            # Prices
            "share_price": share_price,     # Yahoo
            "price_proxy": price_proxy,     # Yahoo or Stooq fallback

            # Derived columns (new)
            "div_pct_per_share": None,      # distribution/share_price * 100
            "payout_per_1000": None,        # (1000/share_price)*distribution
            "annualized_yield_pct": None,   # weekly dist * 52 / share_price * 100
            "monthly_income_per_1000": None,# annual payout / 12

            # existing columns used by your UI comparisons
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

        # Fill issuer-specific distribution/dates
        info = None
        if issuer == "YieldMax":
            info = ym_map.get(ticker)
        elif issuer == "Roundhill":
            info = rh_map.get(ticker)
        elif issuer == "GraniteShares":
            info = gs_map.get(ticker)

        if info:
            if info.get("distribution_per_share") is not None:
                row["distribution_per_share"] = info.get("distribution_per_share")
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    row[k] = info.get(k)

            if info.get("source_url"):
                row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + str(info["source_url"]))

        # Derived calculations (requires price + dist)
        px = row["share_price"] if row["share_price"] is not None else row["price_proxy"]
        dist = row["distribution_per_share"]

        if px is not None and dist is not None and px > 0:
            row["div_pct_per_share"] = (dist / px) * 100.0
            row["payout_per_1000"] = (1000.0 / px) * dist
            row["annualized_yield_pct"] = (dist * 52.0 / px) * 100.0
            row["monthly_income_per_1000"] = (row["payout_per_1000"] * 52.0) / 12.0

        items.append(row)

    # Weekly-only filter (keep it strict)
    items_weekly = [x for x in items if str(x.get("frequency", "")).lower() == "weekly"]

    # Safety net: if scraping failed badly, restore previous snapshot (do not wipe)
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

    # Write primary outputs
    Path("data").mkdir(exist_ok=True)

    with open(OUTFILE_PRIMARY, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with open(OUTFILE_BACKUP, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # Alerts
    alerts = generate_alerts(items)
    with open(ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": payload["generated_at"],
            "threshold_drop_pct": ALERT_DROP_PCT,
            "alerts": alerts
        }, f, indent=2)

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_BACKUP} with {len(items)} items; alerts={len(alerts)}")
    print(f"Wrote Yahoo overrides: {YAHOO_OVERRIDES_FILE}")

if __name__ == "__main__":
    main()
