# scraper.py
# Weekly Pay ETF Dashboard scraper (YieldMax + Roundhill + GraniteShares YieldBOOST)
# - Discovers weekly ETFs from issuer list pages
# - Optionally merges manual tickers from data/manual_tickers.json
# - Enriches distribution + dates from issuer distribution sources / fund pages
# - Pulls share prices for ALL tickers from Yahoo (batch)
# - Writes data/weekly_etfs.json (frontend reads this) + data/items.json (backup/fallback)
# - Keeps history snapshots + alerts

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

# ----------------------------
# Config
# ----------------------------

OUTFILE_PRIMARY = "data/weekly_etfs.json"
OUTFILE_LEGACY = "data/items.json"          # keep for fallback / compatibility
ALERTS_FILE = "data/alerts.json"
ALERT_DROP_PCT = -15.0

UA = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) weekly-etf-dashboard/1.0",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}

_FETCH_CACHE: Dict[str, str] = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35


# ----------------------------
# Helpers
# ----------------------------

def clamp(x, a, b):
    return max(a, min(b, x))


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
    return re.sub(r"\s+", " ", str(s or "")).strip()


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
    s = norm_space(s)
    import datetime as _dt

    # common formats we see on issuer sites
    fmts = (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
    )
    for fmt in fmts:
        try:
            return _dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass

    # last-resort: try to extract "Mon dd, yyyy"
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})", s)
    if m:
        try:
            return _dt.datetime.strptime(f"{m.group(1)} {m.group(2)}, {m.group(3)}", "%B %d, %Y").date().isoformat()
        except Exception:
            try:
                return _dt.datetime.strptime(f"{m.group(1)} {m.group(2)}, {m.group(3)}", "%b %d, %Y").date().isoformat()
            except Exception:
                pass
    return None


def read_json_if_exists(path: str, default=None):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def fetch_text(url: str) -> str:
    global _LAST_FETCH_AT
    if url in _FETCH_CACHE:
        return _FETCH_CACHE[url]

    # polite throttle
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
        t = (it.get("ticker") or "").upper().strip()
        iss = norm_space(it.get("issuer") or "")
        if not t:
            continue
        key = (t, iss)
        if key in seen:
            continue
        seen.add(key)
        it["ticker"] = t
        out.append(it)
    return out


# ----------------------------
# Yahoo prices (batch)
# ----------------------------

def yahoo_batch_quotes(tickers: List[str]) -> Dict[str, float]:
    """
    Fetch prices for many tickers from Yahoo in batches.
    Returns { TICKER: price }
    """
    out: Dict[str, float] = {}
    if not tickers:
        return out

    base = "https://query1.finance.yahoo.com/v7/finance/quote?symbols="
    headers = {"User-Agent": UA["User-Agent"], "Accept": "application/json"}

    tickers = [t.upper().strip() for t in tickers if t]
    BATCH = 80

    for i in range(0, len(tickers), BATCH):
        batch = tickers[i:i + BATCH]
        try:
            r = requests.get(base + ",".join(batch), headers=headers, timeout=20)
            r.raise_for_status()
            data = r.json().get("quoteResponse", {}).get("result", [])
        except Exception:
            continue

        for q in data:
            sym = (q.get("symbol") or "").upper()
            price = q.get("regularMarketPrice")
            if sym and price is not None:
                try:
                    out[sym] = float(price)
                except Exception:
                    pass

    return out


# ----------------------------
# YieldMax discovery + enrichment
# ----------------------------

def yieldmax_discover_weekly_from_our_etfs() -> List[Dict]:
    """
    Discover weekly YieldMax ETFs from https://yieldmaxetfs.com/our-etfs/
    This page typically contains a table with Distribution Frequency.
    """
    url = "https://yieldmaxetfs.com/our-etfs/"
    soup = fetch_soup(url)

    items: List[Dict] = []
    for table in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers:
            continue
        header_blob = " ".join(headers)
        if "ticker" not in header_blob or "distribution" not in header_blob:
            continue

        def find_idx(needle: str):
            for i, h in enumerate(headers):
                if needle in h:
                    return i
            return None

        idx_ticker = find_idx("ticker")
        idx_name = find_idx("etf")
        idx_ref = find_idx("reference")
        idx_freq = find_idx("frequency")
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
            freq = (cell(idx_freq) or "")
            if not ticker:
                continue
            if freq.strip().lower() != "weekly":
                continue

            items.append({
                "ticker": ticker,
                "issuer": "YieldMax",
                "frequency": "Weekly",
                "name": cell(idx_name),
                "reference_asset": cell(idx_ref),
                "notes": "Discovered via YieldMax Our ETFs",
            })

    return dedupe(items)


def yieldmax_fund_url(ticker: str) -> str:
    return f"https://yieldmaxetfs.com/our-etfs/{ticker.lower()}/"


def yieldmax_parse_fund_page_latest(ticker: str) -> Dict:
    """
    Best-effort parse a YieldMax fund page for latest distribution row.
    Many pages include a distribution table with dates and amount.
    We try to find any table whose headers include ex-dividend/pay/record and $/share or distribution.
    """
    url = yieldmax_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Try to find a table with relevant headers
    for tbl in soup.find_all("table"):
        ths = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        if not ths:
            continue

        has_ex = any("ex" in h and "div" in h for h in ths) or any("ex-" in h for h in ths) or any("ex date" in h for h in ths)
        has_pay = any("pay" in h for h in ths)
        has_rec = any("record" in h for h in ths)
        has_amt = any("amount" in h for h in ths) or any("distribution" in h for h in ths) or any("per share" in h for h in ths) or any("$" in h and "share" in h for h in ths)

        if not (has_ex and has_pay and has_amt):
            continue

        # pick first data row
        tbody = tbl.find("tbody")
        row = tbody.find("tr") if tbody else None
        if not row:
            trs = tbl.find_all("tr")
            row = trs[1] if len(trs) > 1 else None
        if not row:
            continue

        tds = [norm_space(td.get_text(" ", strip=True)) for td in row.find_all("td")]
        if not tds:
            continue

        def idx_contains(substrs):
            for i, h in enumerate(ths):
                for s in substrs:
                    if s in h:
                        return i
            return None

        i_decl = idx_contains(["declaration"])
        i_ex = idx_contains(["ex-div", "ex dividend", "ex-date", "ex date"])
        i_rec = idx_contains(["record"])
        i_pay = idx_contains(["pay"])
        i_amt = idx_contains(["amount", "distribution", "per share", "$/share", "share"])

        out = {"source_url": url}
        if i_amt is not None and i_amt < len(tds):
            out["distribution_per_share"] = _parse_float(tds[i_amt])
        if i_decl is not None and i_decl < len(tds):
            out["declaration_date"] = _parse_date_to_iso(tds[i_decl])
        if i_ex is not None and i_ex < len(tds):
            out["ex_dividend_date"] = _parse_date_to_iso(tds[i_ex])
        if i_rec is not None and i_rec < len(tds):
            out["record_date"] = _parse_date_to_iso(tds[i_rec])
        if i_pay is not None and i_pay < len(tds):
            out["pay_date"] = _parse_date_to_iso(tds[i_pay])

        # if we got anything useful, return
        if any(out.get(k) is not None for k in ["distribution_per_share", "ex_dividend_date", "pay_date", "record_date", "declaration_date"]):
            return out

    return {}


def yieldmax_enrich_from_fund_pages(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = yieldmax_parse_fund_page_latest(t)
        if info:
            out[t] = info
    return out


# ----------------------------
# Roundhill discovery + enrichment
# ----------------------------

def roundhill_discover_weeklypay() -> List[Dict]:
    """
    Discover Roundhill WeeklyPay tickers from:
    https://www.roundhillinvestments.com/weeklypay-etfs
    """
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
        "notes": "Discovered via Roundhill WeeklyPay",
    } for t in tickers]

    return dedupe(items)


def roundhill_fund_url(ticker: str) -> str:
    return f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"


def _roundhill_find_table_by_heading(soup: BeautifulSoup, heading_text: str):
    h = None
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
        if heading_text.lower() in tag.get_text(" ", strip=True).lower():
            h = tag
            break
    if not h:
        return None
    return h.find_next("table")


def roundhill_weekly_calendar_and_latest_dist(ticker: str) -> Dict:
    """
    Parse Roundhill fund page for latest distribution record.
    Prefers Distribution History (with amount), else Distribution Calendar.
    """
    url = roundhill_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    def to_iso_guess(d):
        return _parse_date_to_iso(d)

    hist = _roundhill_find_table_by_heading(soup, "Distribution History")
    if hist:
        rows = hist.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td", "th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            # Often: Declaration, Ex Date, Record Date, Pay Date, Amount Paid
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = to_iso_guess(vals[0])
                out["ex_dividend_date"] = to_iso_guess(vals[1])
                out["record_date"] = to_iso_guess(vals[2])
                out["pay_date"] = to_iso_guess(vals[3])
            if len(vals) >= 5:
                out["distribution_per_share"] = _parse_float(vals[4])
            if any(out.get(k) is not None for k in ["distribution_per_share", "ex_dividend_date", "pay_date"]):
                return out

    cal = _roundhill_find_table_by_heading(soup, "Distribution Calendar")
    if cal:
        rows = cal.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td", "th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = to_iso_guess(vals[0])
                out["ex_dividend_date"] = to_iso_guess(vals[1])
                out["record_date"] = to_iso_guess(vals[2])
                out["pay_date"] = to_iso_guess(vals[3])
            if any(out.get(k) is not None for k in ["ex_dividend_date", "pay_date"]):
                return out

    return {}


def roundhill_weekly_distributions_and_dates(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = roundhill_weekly_calendar_and_latest_dist(t)
        if info:
            out[t] = info
    return out


# ----------------------------
# GraniteShares discovery + enrichment
# ----------------------------

def graniteshares_discover_yieldboost_from_pdf() -> List[Dict]:
    """
    Discover GraniteShares YieldBOOST tickers by scanning product guide PDF
    and requiring 'Weekly' near the ticker.
    """
    pdf_url = "https://graniteshares.com/media/us4pi2qq/graniteshares-product-guide.pdf"
    try:
        r = requests.get(pdf_url, timeout=30, headers=UA)
        r.raise_for_status()
        blob = r.content.decode("latin-1", errors="ignore")
    except Exception:
        return []

    candidates = sorted(set(re.findall(r"\b[A-Z]{3,5}Y{1,2}\b", blob)))

    def is_weekly_near(t: str) -> bool:
        return re.search(rf"{t}.{{0,220}}Weekly", blob, flags=re.IGNORECASE | re.DOTALL) is not None

    tickers = [t for t in candidates if is_weekly_near(t)]
    items = [{
        "ticker": t,
        "issuer": "GraniteShares",
        "frequency": "Weekly",
        "name": None,
        "reference_asset": None,
        "notes": "Discovered via GraniteShares product guide PDF (weekly)",
    } for t in tickers]

    return dedupe(items)


def graniteshares_distribution_page_url() -> str:
    return "https://graniteshares.com/institutional/us/en-us/underlyings/distribution/"


def graniteshares_parse_distribution_table() -> Dict[str, Dict]:
    """
    Parse GraniteShares official distribution table page.
    Returns per ticker: distribution_per_share + dates (ex/record/pay if available)
    """
    url = graniteshares_distribution_page_url()
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Find the most relevant table
    best = None
    best_score = -1

    for tbl in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        if not headers:
            continue
        blob = " ".join(headers)

        score = 0
        if "ticker" in blob:
            score += 2
        if "frequency" in blob:
            score += 1
        if "distribution" in blob:
            score += 2
        if "payment" in blob or "pay" in blob:
            score += 1
        if "ex" in blob:
            score += 1

        if score > best_score:
            best = tbl
            best_score = score

    if not best:
        return {}

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in best.find_all("th")]

    def idx_of(preds: List[str]) -> Optional[int]:
        for i, h in enumerate(headers):
            for p in preds:
                if p in h:
                    return i
        return None

    i_ticker = idx_of(["ticker"])
    i_freq = idx_of(["frequency"])
    i_dist = idx_of(["distribution per share", "distribution", "amount"])
    i_ex = idx_of(["ex-date", "ex date", "ex-div", "ex dividend"])
    i_rec = idx_of(["record"])
    i_pay = idx_of(["payment date", "pay date", "payment", "pay"])

    out: Dict[str, Dict] = {}

    for tr in best.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i):
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        t = (cell(i_ticker) or "")
        if not t:
            continue
        t = t.upper().strip()

        freq = (cell(i_freq) or "")
        if freq and freq.strip().lower() != "weekly":
            # only keep weekly
            continue

        info = {"source_url": url}
        info["distribution_per_share"] = _parse_float(cell(i_dist)) if cell(i_dist) else None
        info["ex_dividend_date"] = _parse_date_to_iso(cell(i_ex)) if cell(i_ex) else None
        info["record_date"] = _parse_date_to_iso(cell(i_rec)) if cell(i_rec) else None
        info["pay_date"] = _parse_date_to_iso(cell(i_pay)) if cell(i_pay) else None

        # If at least one field found, store
        if any(info.get(k) is not None for k in ["distribution_per_share", "ex_dividend_date", "pay_date", "record_date"]):
            out[t] = info

    return out


# ----------------------------
# Manual tickers
# ----------------------------

def load_manual_tickers() -> List[Dict]:
    """
    data/manual_tickers.json (optional) format:
    [
      {"ticker":"AMYY","issuer":"GraniteShares","name":"YieldBOOST AMD","reference_asset":"AMD"},
      ...
    ]
    """
    p = Path("data/manual_tickers.json")
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

    out = []
    if isinstance(data, list):
        for m in data:
            if not isinstance(m, dict):
                continue
            t = (m.get("ticker") or "").upper().strip()
            if not t:
                continue
            out.append({
                "ticker": t,
                "issuer": m.get("issuer") or "Other",
                "frequency": "Weekly",
                "name": m.get("name"),
                "reference_asset": m.get("reference_asset"),
                "notes": "Manually added",
            })
    return dedupe(out)


# ----------------------------
# History + comparisons (optional but useful)
# ----------------------------

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
        snap_date = (snap.get("generated_at", "")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency", "")).lower() != "weekly":
                continue
            timeline[it["ticker"]].append({
                "run_date": snap_date,
                "ex_div": it.get("ex_dividend_date"),
                "price": it.get("share_price") or it.get("price_proxy"),
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
            it["dist_chg_ex_1w_pct"] = pct_change(latest["dist"], prev_w["dist"])
            it["nav_chg_ex_1w_pct"] = pct_change(latest["nav"], prev_w["nav"])

        if prev_m:
            it["price_chg_ex_1m_pct"] = pct_change(latest["price"], prev_m["price"])
            it["dist_chg_ex_1m_pct"] = pct_change(latest["dist"], prev_m["dist"])
            it["nav_chg_ex_1m_pct"] = pct_change(latest["nav"], prev_m["nav"])

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
                "message": f"{it['ticker']} distribution down {w:.2f}% vs prior ex-div week",
            })
        if m is not None and m <= ALERT_DROP_PCT:
            alerts.append({
                "ticker": it["ticker"],
                "type": "DIVIDEND_DROP_VS_1M",
                "pct": round(m, 2),
                "ex_dividend_date": it.get("ex_dividend_date"),
                "message": f"{it['ticker']} distribution down {m:.2f}% vs prior ex-div month",
            })
    return alerts


# ----------------------------
# Build items (core)
# ----------------------------

def build_items() -> List[Dict]:
    discovered: List[Dict] = []

    # Discover from issuer sites
    discovered += yieldmax_discover_weekly_from_our_etfs()
    discovered += roundhill_discover_weeklypay()
    discovered += graniteshares_discover_yieldboost_from_pdf()

    # Manual tickers (optional)
    discovered += load_manual_tickers()

    # De-dupe
    discovered = dedupe(discovered)

    # Debug discovery counts (visible in GitHub Actions "Run scraper" log)
    ym_n = len([d for d in discovered if d.get("issuer") == "YieldMax"])
    rh_n = len([d for d in discovered if d.get("issuer") == "Roundhill"])
    gs_n = len([d for d in discovered if d.get("issuer") == "GraniteShares"])
    print(f"[discovery] YieldMax={ym_n} GraniteShares={gs_n} Roundhill={rh_n} Total={len(discovered)}")

    # Prices for ALL discovered tickers (Yahoo)
    all_tickers = [d["ticker"] for d in discovered if d.get("ticker")]
    yahoo_prices = yahoo_batch_quotes(all_tickers)

    # Enrichment maps
    ym_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "YieldMax"]
    rh_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "Roundhill"]
    gs_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "GraniteShares"]

    ym_map = yieldmax_enrich_from_fund_pages(ym_tickers)
    rh_map = roundhill_weekly_distributions_and_dates(rh_tickers)
    gs_map = graniteshares_parse_distribution_table()

    print(f"[enrich] YieldMax fund pages={len(ym_map)} Roundhill fund pages={len(rh_map)} GraniteShares dist rows={len(gs_map)}")

    items: List[Dict] = []

    for d in discovered:
        t = (d.get("ticker") or "").upper().strip()
        issuer = d.get("issuer") or "Other"

        row = {
            "ticker": t,
            "name": d.get("name"),
            "issuer": issuer,
            "reference_asset": d.get("reference_asset"),

            "distribution_per_share": None,
            "frequency": "Weekly",
            "declaration_date": None,
            "ex_dividend_date": None,
            "record_date": None,
            "pay_date": None,

            "nav_official": None,

            # Price fields
            "price_proxy": None,     # kept for compatibility
            "share_price": None,     # new field (frontend can use this)
            "div_pct_per_share": None,

            # comparison fields (filled later)
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

            "notes": d.get("notes") or "",
        }

        # Set Yahoo price for ALL issuers
        price = yahoo_prices.get(t)
        row["price_proxy"] = price
        row["share_price"] = price

        # Enrich per issuer
        if issuer == "YieldMax" and t in ym_map:
            info = ym_map[t]
            if info.get("distribution_per_share") is not None:
                row["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    row[k] = info[k]
            if info.get("source_url"):
                row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + info["source_url"])

        elif issuer == "Roundhill" and t in rh_map:
            info = rh_map[t]
            if info.get("distribution_per_share") is not None:
                row["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    row[k] = info[k]
            if info.get("source_url"):
                row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + info["source_url"])

        elif issuer == "GraniteShares" and t in gs_map:
            info = gs_map[t]
            if info.get("distribution_per_share") is not None:
                row["distribution_per_share"] = info["distribution_per_share"]
            for k in ["ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    row[k] = info[k]
            if info.get("source_url"):
                row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + info["source_url"])

        # Compute div % / share (weekly distribution percent of current share price)
        dist = row.get("distribution_per_share")
        if dist is not None and price:
            try:
                row["div_pct_per_share"] = round((float(dist) / float(price)) * 100.0, 2)
            except Exception:
                row["div_pct_per_share"] = None

        # Optional frontend compatibility aliases (harmless if unused)
        row["distributionPerShare"] = row["distribution_per_share"]
        row["declaration"] = row["declaration_date"]
        row["exDividend"] = row["ex_dividend_date"]
        row["record"] = row["record_date"]
        row["pay"] = row["pay_date"]
        row["price"] = row["share_price"]

        items.append(row)

    # Weekly-only safety filter (keep weekly)
    items_weekly = [x for x in items if str(x.get("frequency", "")).lower() == "weekly"]

    # Do-not-nuke fallback: if scrape collapses, restore last good snapshot
    if len(items_weekly) < 20:
        prev = read_json_if_exists(OUTFILE_LEGACY, None)
        if isinstance(prev, dict) and isinstance(prev.get("items"), list) and len(prev["items"]) >= 20:
            items_weekly = prev["items"]
            print(f"[fallback] restored previous snapshot from {OUTFILE_LEGACY}, count={len(items_weekly)}")
        else:
            print("[fallback] no prior snapshot found (or too small); keeping current results")

    return items_weekly


# ----------------------------
# Main
# ----------------------------

def main():
    items = build_items()

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "items": items,
    }

    # history first (so comparisons can look back)
    write_history_snapshot(payload)

    # compute comparisons using history (including today's snapshot)
    compute_ex_div_comparisons(items)
    payload["items"] = items

    Path("data").mkdir(exist_ok=True)

    # write primary output (frontend reads this)
    with open(OUTFILE_PRIMARY, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # write legacy/backup output (used for fallback)
    with open(OUTFILE_LEGACY, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # alerts
    alerts = generate_alerts(items)
    with open(ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": payload["generated_at"],
            "threshold_drop_pct": ALERT_DROP_PCT,
            "alerts": alerts,
        }, f, indent=2)

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_LEGACY} with {len(items)} items; alerts={len(alerts)}")


if __name__ == "__main__":
    main()
