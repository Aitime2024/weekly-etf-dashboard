import json
import re
import statistics
import time
from datetime import datetime, timezone, date
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup


# ============================================================
# Config
# ============================================================

OUTFILE_PRIMARY = "data/weekly_etfs.json"   # what the dashboard should read
OUTFILE_LEGACY  = "data/items.json"        # cache / fallback / compatibility
ALERT_DROP_PCT = -15.0

UA = {
    "User-Agent": "weekly-etf-dashboard/1.0 (+github-actions; bot)"
}

_FETCH_CACHE: Dict[str, str] = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35


# ============================================================
# Utility helpers
# ============================================================

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def pct_change(new, old) -> Optional[float]:
    try:
        if new is None or old is None:
            return None
        new = float(new)
        old = float(old)
        if old == 0:
            return None
        return ((new - old) / old) * 100.0
    except Exception:
        return None

def _parse_float(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace("$", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def _parse_date_to_iso(s: str) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    # normalize some common variants
    s = s.replace("Sept.", "Sep.").replace("Sept ", "Sep ")
    import datetime as _dt
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()

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


# ============================================================
# Fetch helpers (throttled + cached)
# ============================================================

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


# ============================================================
# Price proxy (best-effort)
# ============================================================

def get_price_proxy_stooq(ticker: str) -> Optional[float]:
    """
    Stooq daily CSV endpoint: close is column 5 in second line.
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
        if not close_str or close_str == "N/A":
            return None
        return float(close_str)
    except Exception:
        return None


# ============================================================
# YieldMax discovery + PR enrichment
# ============================================================

def yieldmax_discover_weekly_from_our_etfs() -> List[Dict]:
    """
    Scrape https://yieldmaxetfs.com/our-etfs/ for weekly distribution rows.
    """
    url = "https://yieldmaxetfs.com/our-etfs/"
    try:
        soup = fetch_soup(url)
    except Exception:
        return []

    items: List[Dict] = []
    for table in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers:
            continue

        header_join = " ".join(headers)
        if "ticker" not in header_join or "distribution frequency" not in header_join:
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
                "ticker": ticker.upper(),
                "issuer": "YieldMax",
                "frequency": "Weekly",
                "name": cell(idx_name),
                "reference_asset": cell(idx_ref),
                "notes": "Discovered via YieldMax Our ETFs"
            })

    return dedupe(items)

def yieldmax_get_latest_pr_links(limit=10) -> List[str]:
    """
    Pull recent YieldMax news links and return likely weekly distribution PR links.
    """
    news_url = "https://yieldmaxetfs.com/news/"
    try:
        soup = fetch_soup(news_url)
    except Exception:
        return []

    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        hlow = href.lower()
        if "yieldmaxetfs.com" in href and ("announces-weekly-distributions" in hlow or "weekly-distributions" in hlow):
            links.append(href)

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
    html = fetch_text(url)
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    def find_date(patterns):
        for pat in patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    declaration_raw = find_date([r"Declaration Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"])
    ex_raw = find_date([
        r"Ex[-\s]?Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        r"Ex[-\s]?Dividend Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"
    ])
    record_raw = find_date([r"Record Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"])
    pay_raw = find_date([r"Pay(?:ment)? Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"])

    dates = {
        "declaration_date": _parse_date_to_iso(declaration_raw),
        "ex_dividend_date": _parse_date_to_iso(ex_raw),
        "record_date": _parse_date_to_iso(record_raw),
        "pay_date": _parse_date_to_iso(pay_raw),
    }

    # Common PR tables have patterns like: TICKER Weekly $0.1234
    dists: Dict[str, float] = {}
    for m in re.finditer(r"\b([A-Z]{3,5})\b\s+Weekly\s+\$([0-9]*\.[0-9]+)", text, flags=re.IGNORECASE):
        t = m.group(1).upper()
        dists[t] = float(m.group(2))

    return {"dates": dates, "dists": dists, "url": url}

def yieldmax_weekly_distributions_and_dates() -> Dict[str, Dict]:
    """
    Returns dict keyed by ticker with distribution_per_share + dates.
    """
    out: Dict[str, Dict] = {}
    for url in yieldmax_get_latest_pr_links(limit=12):
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


# ============================================================
# Roundhill discovery + fund-page enrichment
# ============================================================

def roundhill_discover_weeklypay() -> List[Dict]:
    url = "https://www.roundhillinvestments.com/weeklypay-etfs"
    try:
        soup = fetch_soup(url)
    except Exception:
        return []

    text = soup.get_text("\n", strip=True)
    # Most WeeklyPay end with W (TSLW, NFLW, etc.)
    tickers = sorted(set(re.findall(r"\b[A-Z]{3,5}W\b", text)))

    items = [{
        "ticker": t.upper(),
        "issuer": "Roundhill",
        "frequency": "Weekly",
        "name": None,
        "reference_asset": None,
        "notes": "Discovered via Roundhill WeeklyPay page"
    } for t in tickers]

    return dedupe(items)

def roundhill_fund_url(ticker: str) -> str:
    return f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"

def _parse_roundhill_table(soup: BeautifulSoup, heading_text: str):
    h = None
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
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

    def to_iso_guess(d):
        return _parse_date_to_iso(d)

    # Prefer Distribution History (has amount)
    hist_table = _parse_roundhill_table(soup, "Distribution History")
    if hist_table:
        rows = hist_table.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td", "th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            # best-effort mapping
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = to_iso_guess(vals[0])
                out["ex_dividend_date"] = to_iso_guess(vals[1])
                out["record_date"] = to_iso_guess(vals[2])
                out["pay_date"] = to_iso_guess(vals[3])
            if len(vals) >= 5:
                out["distribution_per_share"] = _parse_float(vals[4])
            return out

    # Fallback: Distribution Calendar (no amount)
    cal_table = _parse_roundhill_table(soup, "Distribution Calendar")
    if cal_table:
        rows = cal_table.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td", "th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
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

def roundhill_weekly_distributions_and_dates(tickers: List[str]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for t in tickers:
        info = roundhill_weekly_calendar_and_latest_dist(t)
        if info:
            out[t] = info
    return out


# ============================================================
# GraniteShares: manual weekly tickers + fund-page enrichment
# ============================================================

# Your January 2026 list + a couple already seen (QDTY/QVHY etc may be new)
GRANITE_WEEKLY_TICKERS = [
    "AMYY","AZYY","BBYY","COYY","FBYY","HOYY","IOYY","MAAY","MTYY","NVYY","PLYY",
    "QBY","RGYY","SEMY","SMYY","TQQY","TSYY","YSPY",
    "XBTY","YBST","YBTY"
]

# Reference asset mapping (best-effort from your list)
GRANITE_REF_ASSET = {
    "AMYY":"AMD",
    "AZYY":"AMZN",
    "BBYY":"BABA",
    "COYY":"COIN",
    "FBYY":"META",
    "HOYY":"HOOD",
    "IOYY":"GOOGL",
    "MAAY":"MA",
    "MTYY":"MSTR",
    "NVYY":"NVDA",
    "PLYY":"PYPL",
    "QBY":"QCOM",
    "RGYY":"RGTI",
    "SEMY":"Semiconductor",
    "SMYY":"SMCI",
    "TQQY":"TQQQ",
    "TSYY":"TSLA",
    "YSPY":"S&P 500",
    "XBTY":"Bitcoin",
    "YBST":"Single Stock Universe",
    "YBTY":"Top Yielders",
}

def graniteshares_discover_weekly() -> List[Dict]:
    """
    Uses a known list (most reliable). Also merges optional data/manual_tickers.json.
    """
    items: List[Dict] = []

    for t in GRANITE_WEEKLY_TICKERS:
        items.append({
            "ticker": t.upper(),
            "issuer": "GraniteShares",
            "frequency": "Weekly",
            "name": None,
            "reference_asset": GRANITE_REF_ASSET.get(t.upper()),
            "notes": "GraniteShares weekly list (manual)"
        })

    # Optional: user-maintained file in repo: data/manual_tickers.json
    # Format:
    # [
    #   {"ticker":"SOMETH","issuer":"GraniteShares","reference_asset":"XYZ"},
    #   ...
    # ]
    try:
        p = Path("data/manual_tickers.json")
        if p.exists():
            manual = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(manual, list):
                for m in manual:
                    t = str(m.get("ticker","")).upper().strip()
                    issuer = m.get("issuer","GraniteShares")
                    if not t:
                        continue
                    items.append({
                        "ticker": t,
                        "issuer": issuer,
                        "frequency": "Weekly",
                        "name": None,
                        "reference_asset": m.get("reference_asset") or GRANITE_REF_ASSET.get(t),
                        "notes": "Manually added (data/manual_tickers.json)"
                    })
    except Exception:
        pass

    return dedupe(items)

def graniteshares_yieldboost_fund_url(ticker: str) -> str:
    return f"https://graniteshares.com/institutional/us/en-us/etfs/{ticker.lower()}/"

def graniteshares_parse_fund_page(ticker: str) -> Dict:
    """
    Best-effort parsing:
    - Distribution Frequency (Weekly)
    - Latest Distribution Amount ($/share)
    - Ex Date / Record Date / Pay Date from Distribution Calendar table if present
    """
    url = graniteshares_yieldboost_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    text = soup.get_text("\n", strip=True)

    # Frequency
    freq = None
    m = re.search(r"Distribution Frequency\s+([A-Za-z]+)", text, flags=re.IGNORECASE)
    if m:
        freq = m.group(1).strip().title()

    # Latest Distribution Amount
    latest_amt = None
    m = re.search(r"Latest Distribution Amount\s*\$?\s*([0-9]*\.[0-9]+)", text, flags=re.IGNORECASE)
    if m:
        latest_amt = _parse_float(m.group(1))

    # Sometimes tile has a "Latest Distribution Date" (treat as pay_date fallback)
    latest_tile_pay_iso = None
    m = re.search(r"Latest Distribution Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.IGNORECASE)
    if m:
        latest_tile_pay_iso = _parse_date_to_iso(m.group(1))

    # Try Distribution Calendar table
    ex_iso = rec_iso = pay_iso = None

    for tbl in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        if not headers:
            continue

        has_ex  = any("ex date" in h for h in headers)
        has_rec = any("record date" in h for h in headers)
        has_pay = any("pay date" in h for h in headers)
        has_share = any("share" in h for h in headers)  # $/Share column varies

        if not (has_ex and has_rec and has_pay and has_share):
            continue

        # First data row
        rows = tbl.find_all("tr")
        if len(rows) < 2:
            continue
        tds = [norm_space(td.get_text(" ", strip=True)) for td in rows[1].find_all("td")]
        if not tds:
            continue

        def idx_contains(substr: str):
            for i, h in enumerate(headers):
                if substr in h:
                    return i
            return None

        i_ex  = idx_contains("ex date")
        i_rec = idx_contains("record date")
        i_pay = idx_contains("pay date")

        # pick first header containing "share"
        i_share = None
        for i, h in enumerate(headers):
            if "share" in h:
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

    return out

def graniteshares_from_fund_pages(tickers: List[str]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for t in tickers:
        info = graniteshares_parse_fund_page(t)
        if info:
            out[t] = info
    return out


# ============================================================
# History + comparisons (ex-div timeline)
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
        snap_date = (snap.get("generated_at","")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency","")).lower() != "weekly":
                continue
            if not it.get("ticker"):
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
        t = it.get("ticker")
        if not t:
            continue
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

        # last 8 distinct ex-div dates
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


# ============================================================
# Alerts
# ============================================================

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
# Build items (THIS is where indentation must be correct)
# ============================================================

def build_items() -> List[Dict]:
    discovered: List[Dict] = []

    # Discovery
    discovered += yieldmax_discover_weekly_from_our_etfs()
    discovered += roundhill_discover_weeklypay()
    discovered += graniteshares_discover_weekly()

    discovered = dedupe(discovered)

    # Debug discovery counts (shows up in GitHub Actions logs)
    print(
        f"[discovery] YieldMax={len([d for d in discovered if d.get('issuer')=='YieldMax'])} "
        f"GraniteShares={len([d for d in discovered if d.get('issuer')=='GraniteShares'])} "
        f"Roundhill={len([d for d in discovered if d.get('issuer')=='Roundhill'])} "
        f"Total={len(discovered)}"
    )

    # Enrichment sources
    ymd = yieldmax_weekly_distributions_and_dates()
    rh_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "Roundhill"]
    rhd = roundhill_weekly_distributions_and_dates(rh_tickers)

    gs_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "GraniteShares"]
    gsd_fund = graniteshares_from_fund_pages(gs_tickers)

    print(
        f"[enrich] YieldMax PR rows={len(ymd)} "
        f"Roundhill fund pages={len(rhd)} "
        f"Granite fund pages={len(gsd_fund)}"
    )

    # Build the normalized item schema
    items: List[Dict] = []
    for d in discovered:
        t = d["ticker"]
        issuer = d.get("issuer")

        item = {
            "ticker": t,
            "name": d.get("name"),
            "issuer": issuer,
            "reference_asset": d.get("reference_asset"),
            "distribution_per_share": None,
            "frequency": d.get("frequency") or "Weekly",
            "declaration_date": None,
            "ex_dividend_date": None,
            "record_date": None,
            "pay_date": None,
            "nav_official": None,
            "price_proxy": get_price_proxy_stooq(t),
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

        # -----------------------------
        # YieldMax enrichment (PR)
        # -----------------------------
        if issuer == "YieldMax" and t in ymd:
            info = ymd[t]
            if info.get("distribution_per_share") is not None:
                item["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    item[k] = info[k]
            if info.get("source_url"):
                item["notes"] = (item["notes"] + (" | " if item["notes"] else "") + info["source_url"])

        # -----------------------------
        # Roundhill enrichment (fund page)
        # -----------------------------
        if issuer == "Roundhill" and t in rhd:
            info = rhd[t]
            if info.get("distribution_per_share") is not None:
                item["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    item[k] = info[k]
            if info.get("source_url"):
                item["notes"] = (item["notes"] + (" | " if item["notes"] else "") + info["source_url"])

        # -----------------------------
        # GraniteShares enrichment (fund page)
        # -----------------------------
        if issuer == "GraniteShares" and t in gsd_fund:
            info = gsd_fund[t]
            # Some pages might not be weekly; keep whatever they say but don't drop the item
            if info.get("distribution_per_share") is not None:
                item["distribution_per_share"] = info["distribution_per_share"]
            # Frequency if detected
            if info.get("frequency"):
                item["frequency"] = info["frequency"]
            for k in ["ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    item[k] = info[k]
            if info.get("source_url"):
                item["notes"] = (item["notes"] + (" | " if item["notes"] else "") + info["source_url"])

        items.append(item)

    # -----------------------------
    # FINAL safety net: weekly-only (but do NOT wipe GraniteShares)
    # -----------------------------
    items_weekly = [
        x for x in items
        if str(x.get("frequency", "")).lower() == "weekly" or x.get("issuer") == "GraniteShares"
    ]

    # -----------------------------
    # NO-NUKE fallback: if something broke, restore last good dataset
    # -----------------------------
    if len(items_weekly) < 20:
        print(f"[fallback] items dropped to {len(items_weekly)}; attempting restore from {OUTFILE_LEGACY}")
        try:
            prev = json.loads(Path(OUTFILE_LEGACY).read_text(encoding="utf-8"))
            if isinstance(prev, dict) and "items" in prev and isinstance(prev["items"], list):
                items_weekly = prev["items"]
                print(f"[fallback] restored {len(items_weekly)} items from legacy cache dict")
            elif isinstance(prev, list):
                items_weekly = prev
                print(f"[fallback] restored {len(items_weekly)} items from legacy cache list")
        except Exception as e:
            print(f"[fallback] restore failed: {e}")

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

    # history first
    write_history_snapshot(payload)

    # comparisons
    compute_ex_div_comparisons(items)
    payload["items"] = items

    Path("data").mkdir(exist_ok=True)

    # write primary output
    Path(OUTFILE_PRIMARY).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    # write legacy cache
    Path(OUTFILE_LEGACY).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # alerts
    alerts = generate_alerts(items)
    Path("data/alerts.json").write_text(json.dumps({
        "generated_at": payload["generated_at"],
        "threshold_drop_pct": ALERT_DROP_PCT,
        "alerts": alerts
    }, indent=2), encoding="utf-8")

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_LEGACY} with {len(items)} items; alerts={len(alerts)}")

if __name__ == "__main__":
    main()
