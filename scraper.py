import json, re, statistics, time
from datetime import datetime, timezone, date
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup


# ----------------------------
# Config
# ----------------------------
OUTFILE_ITEMS = "data/items.json"          # primary output for dashboard + fallback
OUTFILE_LEGACY = "data/weekly_etfs.json"   # keep this too, in case your UI points at it
ALERT_DROP_PCT = -15.0
UA = {"User-Agent": "weekly-etf-dashboard/1.0"}

_FETCH_CACHE = {}
_LAST_FETCH_AT = 0.0
_MIN_FETCH_INTERVAL_SEC = 0.35


# ----------------------------
# Helpers
# ----------------------------
def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct_change(new, old) -> Optional[float]:
    if new is None or old is None:
        return None
    try:
        new = float(new)
        old = float(old)
        if old == 0:
            return None
        return (new - old) / old * 100.0
    except Exception:
        return None


def _parse_date_to_iso(s: str) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    import datetime as _dt
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y"):
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
    return re.sub(r"\s+", " ", (s or "")).strip()


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


# ----------------------------
# YieldMax: discovery + PR parsing
# ----------------------------
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
                "ticker": ticker.upper(),
                "issuer": "YieldMax",
                "frequency": "Weekly",
                "name": cell(idx_name),
                "reference_asset": cell(idx_ref),
                "notes": "Discovered via YieldMax Our ETFs (tight)"
            })

    return dedupe(items)


def yieldmax_get_latest_pr_links(limit=8) -> List[str]:
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

    def find_date(label_patterns):
        for pat in label_patterns:
            m = re.search(pat, text, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    def to_iso(d):
        return _parse_date_to_iso(d)

    declaration_raw = find_date([r"Declaration Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"])
    ex_raw = find_date([
        r"Ex[-\s]?Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        r"Ex[-\s]?Dividend Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"
    ])
    record_raw = find_date([r"Record Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"])
    pay_raw = find_date([r"Pay(?:ment)? Date[:\s]+([A-Za-z]+\s+\d{1,2},\s+\d{4})"])

    dates = {
        "declaration_date": to_iso(declaration_raw),
        "ex_dividend_date": to_iso(ex_raw),
        "record_date": to_iso(record_raw),
        "pay_date": to_iso(pay_raw),
    }

    dists = {}
    for m in re.finditer(r"\b([A-Z]{3,5})\b\s+Weekly\s+\$([0-9]*\.[0-9]+)", text, flags=re.IGNORECASE):
        t = m.group(1).upper()
        dists[t] = float(m.group(2))

    return {"dates": dates, "dists": dists, "url": url}


def yieldmax_weekly_distributions_and_dates() -> Dict[str, Dict]:
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


# ----------------------------
# Roundhill: discovery + fund page parsing
# ----------------------------
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

    hist_table = _parse_roundhill_table(soup, "Distribution History")
    if not hist_table:
        cal_table = _parse_roundhill_table(soup, "Distribution Calendar")
        if not cal_table:
            return {}
        rows = cal_table.find_all("tr")
        if len(rows) < 2:
            return {}
        tds = rows[1].find_all(["td", "th"])
        vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
        if len(vals) >= 4:
            return {
                "declaration_date": _parse_date_to_iso(vals[0]),
                "ex_dividend_date": _parse_date_to_iso(vals[1]),
                "record_date": _parse_date_to_iso(vals[2]),
                "pay_date": _parse_date_to_iso(vals[3]),
                "distribution_per_share": None,
                "source_url": url
            }
        return {}

    rows = hist_table.find_all("tr")
    if len(rows) < 2:
        return {}

    tds = rows[1].find_all(["td", "th"])
    vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]

    out = {"source_url": url}
    if len(vals) >= 4:
        out["declaration_date"] = _parse_date_to_iso(vals[0])
        out["ex_dividend_date"] = _parse_date_to_iso(vals[1])
        out["record_date"] = _parse_date_to_iso(vals[2])
        out["pay_date"] = _parse_date_to_iso(vals[3])
    if len(vals) >= 5:
        out["distribution_per_share"] = _parse_float(vals[4])
    return out


def roundhill_weekly_distributions_and_dates(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = roundhill_weekly_calendar_and_latest_dist(t)
        if info:
            out[t] = info
    return out


# ----------------------------
# GraniteShares: discovery + official distribution table + fund pages
# ----------------------------
def graniteshares_discover_yieldboost() -> List[Dict]:
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
        "notes": "Discovered via GraniteShares YieldBOOST (weekly-only)"
    } for t in tickers]

    return dedupe(items)


def graniteshares_yieldboost_distribution_table() -> Dict[str, Dict]:
    url = "https://graniteshares.com/institutional/us/en-us/underlyings/distribution/"
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    table = None
    for t in soup.find_all("table"):
        hdr = " ".join([norm_space(th.get_text(" ", strip=True)) for th in t.find_all("th")]).lower()
        if "ticker" in hdr and "distribution per share" in hdr and "payment date" in hdr:
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
    idx_freq = idx_of("frequency")
    idx_dist = idx_of("distribution per share")
    idx_ex = idx_of("ex-date") or idx_of("ex date") or idx_of("ex")
    idx_rec = idx_of("record date")
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

        ticker = (cell(idx_ticker) or "").upper()
        if not ticker:
            continue

        freq = (cell(idx_freq) or "")
        if freq.strip().lower() != "weekly":
            continue

        dist = _parse_float(cell(idx_dist))
        ex_iso = _parse_date_to_iso(cell(idx_ex))
        rec_iso = _parse_date_to_iso(cell(idx_rec))
        pay_iso = _parse_date_to_iso(cell(idx_pay))

        out[ticker] = {
            "frequency": "Weekly",
            "distribution_per_share": dist,
            "ex_dividend_date": ex_iso,
            "record_date": rec_iso,
            "pay_date": pay_iso,
            "source_url": url
        }

    return out


def graniteshares_yieldboost_fund_url(ticker: str) -> str:
    return f"https://graniteshares.com/institutional/us/en-us/etfs/{ticker.lower()}/"


def graniteshares_parse_yieldboost_fund_page(ticker: str) -> Dict:
    url = graniteshares_yieldboost_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    text = soup.get_text("\n", strip=True)

    freq = None
    m = re.search(r"Distribution Frequency\s+([A-Za-z]+)", text, flags=re.IGNORECASE)
    if m:
        freq = m.group(1).strip().title()

    next_expected_iso = None
    m = re.search(r"Next Expected Distribution Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.IGNORECASE)
    if m:
        next_expected_iso = _parse_date_to_iso(m.group(1).strip())

    latest_amt = None
    m = re.search(r"Latest Distribution Amount\s*\$?\s*([0-9]*\.[0-9]+)", text, flags=re.IGNORECASE)
    if m:
        latest_amt = _parse_float(m.group(1))

    latest_tile_pay_iso = None
    m = re.search(r"Latest Distribution Date\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.IGNORECASE)
    if m:
        latest_tile_pay_iso = _parse_date_to_iso(m.group(1).strip())

    ex_iso = rec_iso = pay_iso = None

    for tbl in soup.find_all("table"):
        ths = tbl.find_all("th")
        headers = [norm_space(h.get_text(" ", strip=True)).lower() for h in ths]
        if not headers:
            continue

        has_ex = any("ex date" in h for h in headers)
        has_rec = any("record date" in h for h in headers)
        has_pay = any("pay date" in h for h in headers)
        has_share = any("share" in h for h in headers)

        if not (has_ex and has_rec and has_pay and has_share):
            continue

        tbody = tbl.find("tbody")
        tr = tbody.find("tr") if tbody else None
        if not tr:
            trs = tbl.find_all("tr")
            tr = trs[1] if len(trs) > 1 else None
        if not tr:
            continue

        tds = [norm_space(td.get_text(" ", strip=True)) for td in tr.find_all("td")]

        def idx(sub):
            for i, h in enumerate(headers):
                if sub in h:
                    return i
            return None

        i_ex = idx("ex date")
        i_rec = idx("record date")
        i_pay = idx("pay date")

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
            out[t.upper()] = info
    return out


# ----------------------------
# History + analytics
# ----------------------------
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
        snap_date = (snap.get("generated_at", "")[:10] or "")
        for it in snap.get("items", []):
            if str(it.get("frequency", "")).lower() != "weekly":
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


# ----------------------------
# Build items (main pipeline)
# ----------------------------
def build_items() -> List[Dict]:
    discovered = []
    discovered += yieldmax_discover_weekly_tight()
    discovered += graniteshares_discover_yieldboost()
    discovered += roundhill_discover_weeklypay()
    discovered = dedupe(discovered)

    # enrichment sources
    ymd = yieldmax_weekly_distributions_and_dates()

    roundhill_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "Roundhill"]
    rhd = roundhill_weekly_distributions_and_dates(roundhill_tickers)

    gsd_table = graniteshares_yieldboost_distribution_table()
    granite_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "GraniteShares"]
    gsd_fund = graniteshares_yieldboost_from_fund_pages(granite_tickers)

    items: List[Dict] = []

    for d in discovered:
        ticker = d["ticker"].upper()
        issuer = d.get("issuer")

        it = {
            "ticker": ticker,
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
            "price_proxy": get_price_proxy_stooq(ticker),

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

        # YieldMax fill
        if issuer == "YieldMax" and ticker in ymd:
            info = ymd[ticker]
            if info.get("distribution_per_share") is not None:
                it["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    it[k] = info[k]
            if info.get("source_url"):
                it["notes"] = (it["notes"] + (" | " if it["notes"] else "") + info["source_url"])

        # Roundhill fill
        if issuer == "Roundhill" and ticker in rhd:
            info = rhd[ticker]
            if info.get("distribution_per_share") is not None:
                it["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    it[k] = info[k]
            if info.get("source_url"):
                it["notes"] = (it["notes"] + (" | " if it["notes"] else "") + info["source_url"])

        # GraniteShares fill: prefer fund page (usually richer), fallback to distribution table
        if issuer == "GraniteShares":
            info = gsd_fund.get(ticker) or gsd_table.get(ticker) or {}
            if info.get("frequency"):
                it["frequency"] = info.get("frequency")
            if info.get("distribution_per_share") is not None:
                it["distribution_per_share"] = info["distribution_per_share"]
            for k in ["ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    it[k] = info[k]
            # sometimes fund page has next_expected_distribution_date; store it in notes
            if info.get("next_expected_distribution_date"):
                it["notes"] = (it["notes"] + (" | " if it["notes"] else "") +
                               f"Next exp: {info['next_expected_distribution_date']}")
            if info.get("source_url"):
                it["notes"] = (it["notes"] + (" | " if it["notes"] else "") + info["source_url"])

        items.append(it)

    # FINAL SAFETY NET: keep weekly items, but do NOT wipe GraniteShares if frequency missing
    filtered = [
        x for x in items
        if (
            str(x.get("frequency", "")).lower() == "weekly"
            or x.get("issuer") == "GraniteShares"
        )
    ]
    items = filtered

    # DO NOT NUKE DATASET fallback: if something blocked and result is tiny, reuse last good
    if len(items) < 20:
        for p in [Path(OUTFILE_ITEMS), Path(OUTFILE_LEGACY)]:
            try:
                if p.exists():
                    prev = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(prev, dict) and "items" in prev and isinstance(prev["items"], list):
                        items = prev["items"]
                        break
                    if isinstance(prev, list):
                        items = prev
                        break
            except Exception:
                pass

    return items


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

    Path("data").mkdir(exist_ok=True)

    # write primary output
    with open(OUTFILE_ITEMS, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # write legacy output too
    with open(OUTFILE_LEGACY, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # alerts
    alerts = generate_alerts(items)
    with open("data/alerts.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": payload["generated_at"],
            "threshold_drop_pct": ALERT_DROP_PCT,
            "alerts": alerts
        }, f, indent=2)

    print(f"Wrote {OUTFILE_ITEMS} with {len(items)} items; alerts={len(alerts)}")


if __name__ == "__main__":
    main()
