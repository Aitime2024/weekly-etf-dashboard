import json, re, statistics
from datetime import datetime, timezone, date
from pathlib import Path
from collections import defaultdict
from math import isnan
from typing import List, Dict, Optional
import requests
import time
from bs4 import BeautifulSoup
def _parse_date_to_iso(s: str) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    import datetime as _dt
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return _dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None

def _parse_float(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None



OUTFILE = "data/weekly_etfs.json"
ALERT_DROP_PCT = -15.0
UA = {"User-Agent": "weekly-etf-dashboard/1.0"}

_FETCH_CACHE = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35

def fetch_text(url: str) -> str:
    global _LAST_FETCH_AT
    if url in _FETCH_CACHE:
        return _FETCH_CACHE[url]

    # polite throttle to reduce rate-limits
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

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

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

def get_price_proxy_stooq(ticker: str) -> Optional[float]:
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
# --- YieldMax PR parsing (weekly distributions + dates) ---
def yieldmax_get_latest_pr_links(limit=6) -> List[str]:
    """
    Pull recent YieldMax news links and return likely weekly distribution PR links.
    Source: YieldMax News page (their site). If it changes, fallback to empty.
    """
    news_url = "https://yieldmaxetfs.com/news/"
    try:
        soup = fetch_soup(news_url)
    except Exception:
        return []

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        if "yieldmaxetfs.com" in href and "announces-weekly-distributions" in href.lower():
            links.append(href)
    # de-dupe preserve order
    out = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= limit:
            break
    return out

def yieldmax_parse_pr(url: str) -> Dict:
    """
    Parse a YieldMax weekly distribution PR page and return:
      - dates: declaration / ex-div / record / pay (best-effort)
      - per-ticker distribution values
    Works on YieldMax-hosted PR pages (preferred).
    """
    html = fetch_text(url)
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\\n", strip=True)

    # Try to find date line patterns
    # Common PR phrasing often includes "Ex-Date", "Record Date", "Pay Date" or similar.
    def find_date(label_patterns):
        for pat in label_patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    # Best-effort ISO normalization: accept "January 17, 2026" etc.
    def to_iso(d):
        if not d:
            return None
        try:
            # datetime.strptime can't handle all formats without locale; do a small mapper
            import datetime as _dt
            d = d.replace("Sept.", "Sep.").replace("Sept ", "Sep ")
            for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
                try:
                    return _dt.datetime.strptime(d, fmt).date().isoformat()
                except Exception:
                    pass
        except Exception:
            return None
        return None

    declaration_raw = find_date([r"Declaration Date[:\\s]+([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})"])
    ex_raw = find_date([r"Ex[-\\s]?Date[:\\s]+([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})",
                        r"Ex[-\\s]?Dividend Date[:\\s]+([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})"])
    record_raw = find_date([r"Record Date[:\\s]+([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})"])
    pay_raw = find_date([r"Pay(?:ment)? Date[:\\s]+([A-Za-z]+\\s+\\d{1,2},\\s+\\d{4})"])

    dates = {
        "declaration_date": to_iso(declaration_raw),
        "ex_dividend_date": to_iso(ex_raw),
        "record_date": to_iso(record_raw),
        "pay_date": to_iso(pay_raw),
    }

    # Distribution table parsing: look for TICKER + Weekly + $0.1234 patterns
    dists = {}
    for m in re.finditer(r"\\b([A-Z]{3,5})\\b\\s+Weekly\\s+\\$([0-9]*\\.[0-9]+)", text, flags=re.IGNORECASE):
        t = m.group(1).upper()
        dists[t] = float(m.group(2))

    return {"dates": dates, "dists": dists, "url": url}

def yieldmax_weekly_distributions_and_dates() -> Dict[str, Dict]:
    """
    Returns dict keyed by ticker with {distribution_per_share, dates..., source_url}.
    We parse a small set of latest PRs (Group 1/2 etc.) and merge results.
    """
    out = {}
    for url in yieldmax_get_latest_pr_links(limit=8):
        try:
            parsed = yieldmax_parse_pr(url)
        except Exception:
            continue
        dates = parsed.get("dates") or {}
        for t, dist in (parsed.get("dists") or {}).items():
            out[t] = {
                "distribution_per_share": dist,
                "declaration_date": dates.get("declaration_date"),
                "ex_dividend_date": dates.get("ex_dividend_date"),
                "record_date": dates.get("record_date"),
                "pay_date": dates.get("pay_date"),
                "source_url": parsed.get("url"),
            }
    return out


# --- Roundhill distributions + dates (from fund pages) ---
def roundhill_fund_url(ticker: str) -> str:
    return f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"

def _parse_roundhill_table(soup: BeautifulSoup, heading_text: str):
    # Find a heading containing heading_text, then the next table
    h = None
    for tag in soup.find_all(["h1","h2","h3","h4","h5"]):
        if heading_text.lower() in tag.get_text(" ", strip=True).lower():
            h = tag
            break
    if not h:
        return None
    # find next table after heading
    nxt = h.find_next("table")
    return nxt

def roundhill_weekly_calendar_and_latest_dist(ticker: str) -> Dict:
    """
    Returns latest distribution record from Roundhill fund page:
      - declaration / ex / record / pay / amount (best effort)
    """
    url = roundhill_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Distribution History table contains Amount Paid
    # Try to parse the first row in Distribution History
    hist_table = _parse_roundhill_table(soup, "Distribution History")
    if not hist_table:
        # Some pages may only have calendar; fallback to calendar table (no amount)
        cal_table = _parse_roundhill_table(soup, "Distribution Calendar")
        if not cal_table:
            return {}
        rows = cal_table.find_all("tr")
        if len(rows) < 2:
            return {}
        tds = rows[1].find_all(["td","th"])
        vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
        # Expect: Declaration, Ex Date, Record Date, Pay Date
        def to_iso_guess(d):
            try:
                import datetime as _dt
                for fmt in ("%B %d, %Y", "%b %d, %Y"):
                    try:
                        return _dt.datetime.strptime(d, fmt).date().isoformat()
                    except Exception:
                        pass
            except Exception:
                return None
            return None
        if len(vals) >= 4:
            return {
                "declaration_date": to_iso_guess(vals[0]),
                "ex_dividend_date": to_iso_guess(vals[1]),
                "record_date": to_iso_guess(vals[2]),
                "pay_date": to_iso_guess(vals[3]),
                "distribution_per_share": None,
                "source_url": url
            }
        return {}

    # Parse rows
    rows = hist_table.find_all("tr")
    if len(rows) < 2:
        return {}
    # Use first data row
    tds = rows[1].find_all(["td","th"])
    vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]

    def to_iso_guess(d):
        try:
            import datetime as _dt
            for fmt in ("%B %d, %Y", "%b %d, %Y"):
                try:
                    return _dt.datetime.strptime(d, fmt).date().isoformat()
                except Exception:
                    pass
        except Exception:
            return None
        return None

    # Expect: Declaration, Ex Date, Record Date, Pay Date, Amount Paid
    out = {"source_url": url}
    if len(vals) >= 4:
        out["declaration_date"] = to_iso_guess(vals[0])
        out["ex_dividend_date"] = to_iso_guess(vals[1])
        out["record_date"] = to_iso_guess(vals[2])
        out["pay_date"] = to_iso_guess(vals[3])
    if len(vals) >= 5:
        amt = vals[4].replace("$","").replace(",","").strip()
        try:
            out["distribution_per_share"] = float(amt)
        except Exception:
            out["distribution_per_share"] = None
    return out

def roundhill_weekly_distributions_and_dates(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = roundhill_weekly_calendar_and_latest_dist(t)
        if info:
            out[t] = info
    return out

# --- GraniteShares YieldBOOST distributions + dates (official distribution page) ---
def graniteshares_yieldboost_distribution_table() -> Dict[str, Dict]:
    """
    Parse GraniteShares distribution table (official site) for YieldBOOST weekly ETFs.
    Returns per ticker:
      - distribution_per_share, ex_dividend_date, record_date, pay_date
    """
    url = "https://graniteshares.com/institutional/us/en-us/underlyings/distribution/"
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Find table containing 'Ticker' and 'Distribution per Share'
    table = None
    for t in soup.find_all("table"):
        hdr = " ".join([norm_space(th.get_text(" ", strip=True)) for th in t.find_all("th")]).lower()
        if "ticker" in hdr and "distribution per share" in hdr and "payment date" in hdr:
            table = t
            break
    if not table:
        return {}

    def to_iso_guess(d):
        if not d:
            return None
        d = norm_space(d)
        try:
            import datetime as _dt
            for fmt in ("%B %d, %Y", "%b %d, %Y"):
                try:
                    return _dt.datetime.strptime(d, fmt).date().isoformat()
                except Exception:
                    pass
        except Exception:
            return None
        return None

    # Map header indexes
    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
    def idx_of(needle):
        for i, h in enumerate(headers):
            if needle in h:
                return i
        return None
    idx_ticker = idx_of("ticker")
    idx_freq = idx_of("frequency")
    idx_dist = idx_of("distribution per share")
    idx_exrec = idx_of("ex-date")
    idx_pay = idx_of("payment date")

    out = {}
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        def cell(i):
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        ticker = cell(idx_ticker)
        freq = (cell(idx_freq) or "")
        if not ticker:
            continue
        if freq.strip().lower() != "weekly":
            continue

        dist_raw = cell(idx_dist)
        dist = None
        if dist_raw:
            dist_raw = dist_raw.replace("$","").replace(",","").strip()
            try:
                dist = float(dist_raw)
            except Exception:
                dist = None

        exrec_raw = cell(idx_exrec)
        pay_raw = cell(idx_pay)

        ex_iso = to_iso_guess(exrec_raw)
        pay_iso = to_iso_gues
# --- GraniteShares YieldBOOST fund-page parsing (weekly detection + calendar parsing) ---
def graniteshares_yieldboost_fund_url(ticker: str) -> str:
    return f"https://graniteshares.com/institutional/us/en-us/etfs/{ticker.lower()}/"

def graniteshares_parse_yieldboost_fund_page(ticker: str) -> Dict:
    """
    Parse GraniteShares YieldBOOST fund page for:
      - Distribution Frequency (Weekly/Monthly/etc.)
      - Latest Distribution Amount ($/Share)
      - Ex Date / Record Date / Pay Date (from Distribution Calendar table)
      - Next Expected Distribution Date (tile, if present)
    """
    url = graniteshares_yieldboost_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    text = soup.get_text("\n", strip=True)

    # Frequency tile
    freq = None
    m = re.search(r"Distribution Frequency\s+([A-Za-z]+)", text, flags=re.IGNORECASE)
    if m:
        freq = m.group(1).strip().title()

    # Next expected date tile (optional)
    next_expected_iso = None
    m = re.search(r"Next Expected Distribution Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.IGNORECASE)
    if m:
        next_expected_iso = None
        nd = m.group(1).strip()
        next_expected_iso = _parse_date_to_iso(nd)

    # Fallback tiles
    latest_amt = None
    m = re.search(r"Latest Distribution Amount\s*\$?\s*([0-9]*\.[0-9]+)", text, flags=re.IGNORECASE)
    if m:
        latest_amt = _parse_float(m.group(1))

    latest_tile_pay_iso = None
    m = re.search(r"Latest Distribution Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.IGNORECASE)
    if m:
        latest_tile_pay_iso = _parse_date_to_iso(m.group(1).strip())

    # Preferred: Distribution Calendar table
    ex_iso = rec_iso = pay_iso = None

    tables = soup.find_all("table")
    for tbl in tables:
        ths = tbl.find_all("th")
        headers = [h.get_text(" ", strip=True).lower() for h in ths]
        if not headers:
            continue

        has_ex = any("ex date" in h for h in headers)
        has_rec = any("record date" in h for h in headers)
        has_pay = any("pay date" in h for h in headers)
        has_share = any("$/share" in h.replace(" ", "") or "$ / share" in h or "share" in h for h in headers)

        if not (has_ex and has_rec and has_pay and has_share):
            continue

        tbody = tbl.find("tbody")
        tr = tbody.find("tr") if tbody else None
        if not tr:
            trs = tbl.find_all("tr")
            tr = trs[1] if len(trs) > 1 else None
        if not tr:
            continue

        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        def idx(sub):
            for i, h in enumerate(headers):
                if sub in h:
                    return i
            return None

        i_ex = idx("ex date")
        i_rec = idx("record date")
        i_pay = idx("pay date")

        # $/Share header varies; pick first header containing "share"
        i_share = None
        for i, h in enumerate(headers):
            if "$/share" in h.replace(" ", "") or "share" in h:
                i_share = i
                break

        if i_ex is not None and i_ex < len(tds):
            ex_iso = _parse_date_to_iso(tds[i_ex])
        if i_rec is not None and i_rec < len(tds):
            rec_iso = _parse_date_to_iso(tds[i_rec])
        if i_pay is not None and i_pay < len(tds):
            pay_iso = _parse_date_to_iso(tds[i_pay])
        if i_share is not None and i_share < len(tds):
            latest_amt = _parse_float(tds[i_share])

        break

    out = {"source_url": url}
    if freq:
        out["frequency"] = freq
    if latest_amt is not None:
        out["distribution_per_share"] = latest_amt
    if ex_iso:
        out["ex_dividend_date"] = ex_iso
    if rec_iso:
        out["record_date"] = rec_iso
    if pay_iso:
        out["pay_date"] = pay_iso
    elif latest_tile_pay_iso:
        out["pay_date"] = latest_tile_pay_iso
    if next_expected_iso:
        out["next_expected_distribution_date"] = next_expected_iso
    return out

def graniteshares_yieldboost_from_fund_pages(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = graniteshares_parse_yieldboost_fund_page(t)
        if info:
            out[t] = info
    return out


urce_url": url}
    if freq:
        out["frequency"] = freq
    if latest_amt is not None:
        out["distribution_per_share"] = latest_amt
    if latest_date:
        out["pay_date"] = latest_date
    if next_date:
        out["next_expected_distribution_date"] = next_date
    return out

def graniteshares_yieldboost_from_fund_pages(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = graniteshares_parse_yieldboost_fund_page(t)
        if info:
            out[t] = info
    return out



# --- Discovery (Weekly-only) ---
def yieldmax_discover_weekly_tight() -> List[Dict]:
    url = "https://yieldmaxetfs.com/our-etfs/"
    soup = fetch_soup(url)

    items = []
    for table in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers:
            continue
        if "ticker" not in " ".join(headers) or "distribution frequency" not in " ".join(headers):
            continue

        def find_idx(needle: str):
            for i, h in enumerate(headers):
                if needle in h:
                    return i
            return None

        idx_ticker = find_idx("ticker")
        idx_name = find_idx("etf name")
        idx_ref = find_idx("reference asset")
        idx_freq = find_idx("distribution frequency")

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
                "ticker": ticker,
                "issuer": "YieldMax",
                "frequency": "Weekly",
                "name": cell(idx_name),
                "reference_asset": cell(idx_ref),
                "notes": "Discovered via YieldMax Our ETFs (tight)"
            })

    return dedupe(items)

def roundhill_discover_weeklypay() -> List[Dict]:
    url = "https://www.roundhillinvestments.com/weeklypay-etfs"
    soup = fetch_soup(url)
    text = soup.get_text("\n", strip=True)
    tickers = sorted(set(re.findall(r"\b[A-Z]{3,5}W\b", text)))
    items = [{
        "ticker": t, "issuer": "Roundhill", "frequency": "Weekly",
        "name": None, "reference_asset": None,
        "notes": "Discovered via Roundhill WeeklyPay"
    } for t in tickers]
    return dedupe(items)

def graniteshares_discover_yieldboost() -> List[Dict]:
    # Primary: product guide PDF (regex scan)
    pdf_url = "https://graniteshares.com/media/us4pi2qq/graniteshares-product-guide.pdf"
    try:
        r = requests.get(pdf_url, timeout=30, headers=UA)
        r.raise_for_status()
        blob = r.content.decode("latin-1", errors="ignore")
    except Exception:
        return []

    tickers = sorted(set(re.findall(r"\b[A-Z]{3,5}YY\b", blob)))

    # Weekly-only filter: require "Weekly" near ticker in PDF text
    def is_weekly_near_ticker(t: str) -> bool:
        return re.search(rf"{t}.{{0,120}}Weekly", blob, flags=re.IGNORECASE | re.DOTALL) is not None

    tickers = [t for t in tickers if is_weekly_near_ticker(t)]

    items = [{
        "ticker": t, "issuer": "GraniteShares", "frequency": "Weekly",
        "name": None, "reference_asset": None,
        "notes": "Discovered via GraniteShares YieldBOOST (weekly-only)"
    } for t in tickers]
    return dedupe(items)

# --- Calculations ---
def pct_change(new, old):
    if new is None or old is None:
        return None
    try:
        new, old = float(new), float(old)
        if old == 0 or isnan(old):
            return None
        return (new - old) / old * 100
    except Exception:
        return None

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

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

def compute_ex_div_comparisons(current_items):
    history = load_history(45)
    timeline = defaultdict(list)
    for snap in history:
        snap_date = (snap.get("generated_at","")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency","")).lower() != "weekly":
                continue
            timeline[it["ticker"]].append({
                "run_date": snap_date,
                "ex_div": it.get("ex_dividend_date"),
                "price": it.get("price_proxy"),
                "dist": it.get("distribution_per_share"),
                "nav": it.get("nav_official"),
            })

    today = date.today()

    for it in current_items:
        t = it["ticker"]
        rows = timeline.get(t, [])
        if not rows:
            continue
        rows.sort(key=lambda x: x["run_date"])
        rows = [r for r in rows if r["ex_div"]]
        if len(rows) < 2:
            continue

        latest = rows[-1]
        latest_ex = date.fromisoformat(latest["ex_div"])

        def find_prior(days_back):
            for r in reversed(rows[:-1]):
                ex = date.fromisoformat(r["ex_div"])
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

        # last 8 ex-div events (distinct ex-div dates)
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

def build_items():
    discovered = []
    discovered += yieldmax_discover_weekly_tight()
    discovered += graniteshares_discover_yieldboost()
    discovered += roundhill_discover_weeklypay()
    discovered = dedupe(discovered)

    # Pull latest YieldMax weekly PR info (distribution + dates)
    ymd = yieldmax_weekly_distributions_and_dates()
    # Roundhill + GraniteShares official distribution sources
    roundhill_tickers = [d['ticker'] for d in discovered if d.get('issuer') == 'Roundhill']
    rhd = roundhill_weekly_distributions_and_dates(roundhill_tickers)
    gsd = graniteshares_yieldboost_distribution_table()
    granite_tickers = [d['ticker'] for d in discovered if d.get('issuer') == 'GraniteShares']
    gsd_fund = graniteshares_yieldboost_from_fund_pages(granite_tickers)

    items = []
    for d in discovered:
        items.append({
            "ticker": d["ticker"],
            "name": d.get("name"),
            "issuer": d.get("issuer"),
            "reference_asset": d.get("reference_asset"),
            "distribution_per_share": None,
            "frequency": "Weekly",
            "declaration_date": None,
            "ex_dividend_date": None,
            "record_date": None,
            "pay_date": None,
            "nav_official": None,
            "price_proxy": get_price_proxy_stooq(d["ticker"]),
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
        })
        # If this is a YieldMax ticker and we have PR data, fill distribution + dates
        if d.get("issuer") == "YieldMax" and d["ticker"] in ymd:
            info = ymd[d["ticker"]]
            if info.get("distribution_per_share") is not None:
                items[-1]["distribution_per_share"] = info["distribution_per_share"]
            # dates (best-effort)
            for k in ["declaration_date","ex_dividend_date","record_date","pay_date"]:
                if info.get(k):
                    items[-1][k] = info[k]
            if info.get("source_url"):
                items[-1]["notes"] = (items[-1].get("notes","") + " | " if items[-1].get("notes") else "") + "YieldMax PR"


    # final safety net: weekly-only
    items = [x for x in items if str(x.get("frequency","")).lower() == "weekly"]
    return items


        # If this is a Roundhill ticker and we have fund-page calendar/history data, fill it
        if d.get("issuer") == "Roundhill" and d["ticker"] in rhd:
            info = rhd[d["ticker"]]
            if info.get("distribution_per_share") is not None:
                items[-1]["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date","ex_dividend_date","record_date","pay_date"]:
                if info.get(k):
                    items[-1][k] = info[k]
            if info.get("source_url"):
                items[-1]["notes"] = (items[-1].get("notes","") + " | " if items[-1].get("notes") else "") + "Roundhill site"

        # If this is a GraniteShares ticker, prefer YieldBOOST fund-page calendar data (frequency + amount + ex/record/pay)
        if d.get("issuer") == "GraniteShares" and d["ticker"] in gsd_fund:
            info = gsd_fund[d["ticker"]]
            if info.get("frequency"):
                items[-1]["frequency"] = info["frequency"]
            if info.get("distribution_per_share") is not None:
                items[-1]["distribution_per_share"] = info["distribution_per_share"]
            if info.get("ex_dividend_date"):
                items[-1]["ex_dividend_date"] = info["ex_dividend_date"]
            if info.get("record_date"):
                items[-1]["record_date"] = info["record_date"]
            if info.get("pay_date"):
                items[-1]["pay_date"] = info["pay_date"]
            if info.get("next_expected_distribution_date"):
                items[-1]["next_expected_distribution_date"] = info["next_expected_distribution_date"]
                notes = items[-1].get("notes", "")

if info.get("next_expected_distribution_date"):
    notes = (notes + " | " if notes else "") + f"Next exp: {info['next_expected_distribution_date']}"

notes = (notes + " | " if notes else "") + "GraniteShares fund page"

items[-1]["notes"] = notes
def main():
    items = build_items()
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "items": items
    }

    # history first (so comparisons can look back)
    write_history_snapshot(payload)

    # compute comparisons using history (including today's snapshot)
    compute_ex_div_comparisons(items)
    payload["items"] = items

    # write primary output
    Path("data").mkdir(exist_ok=True)
    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # alerts
    alerts = generate_alerts(items)
    with open("data/alerts.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": payload["generated_at"],
            "threshold_drop_pct": ALERT_DROP_PCT,
            "alerts": alerts
        }, f, indent=2)

    print(f"Wrote {OUTFILE} with {len(items)} items; alerts={len(alerts)}")

if __name__ == "__main__":
    main()
