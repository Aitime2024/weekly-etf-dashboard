import json
import re
import time
import random
import statistics
from datetime import datetime, timezone, date
from pathlib import Path
from collections import defaultdict, OrderedDict
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ============================================================
# Primary Sources (WeeklyPayers)
# ============================================================
WEEKLYPAYERS_ROOT = "https://weeklypayers.com/"
WEEKLYPAYERS_CAL  = "https://weeklypayers.com/calendar/"

# ============================================================
# Outputs
# ============================================================
OUTFILE_PRIMARY = "data/weekly_etfs.json"   # UI reads this
OUTFILE_BACKUP  = "data/items.json"         # fallback + history comparisons
ALERTS_FILE     = "data/alerts.json"
ALERT_DROP_PCT  = -15.0

# If scraping fails, don't wipe your dataset:
MIN_EXPECTED_ITEMS = 25

UA = {
    "User-Agent": "weekly-etf-dashboard/3.1 (+github-actions)",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ============================================================
# Prices: OFF (as requested)
# ============================================================
USE_YAHOO_PRICES = False  # <<< integrated per your request

# ============================================================
# HTTP: retries + per-domain throttle + bounded cache
# ============================================================
_SESSION: Optional[requests.Session] = None
_FETCH_CACHE: "OrderedDict[str, str]" = OrderedDict()
_FETCH_CACHE_MAX = 200
_LAST_FETCH_AT_BY_DOMAIN: Dict[str, float] = {}
_MIN_FETCH_INTERVAL_SEC = 0.35
_JITTER_SEC = 0.08


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    _SESSION = s
    return s


def _throttle(url: str) -> None:
    domain = urlparse(url).netloc.lower()
    now = time.time()
    last = _LAST_FETCH_AT_BY_DOMAIN.get(domain, 0.0)
    dt = now - last
    min_dt = _MIN_FETCH_INTERVAL_SEC + random.random() * _JITTER_SEC
    if dt < min_dt:
        time.sleep(min_dt - dt)
    _LAST_FETCH_AT_BY_DOMAIN[domain] = time.time()


def http_get(url: str, *, timeout: int = 30, allow_cache: bool = True) -> requests.Response:
    s = _get_session()
    if allow_cache and url in _FETCH_CACHE:
        r = requests.Response()
        r.status_code = 200
        r._content = _FETCH_CACHE[url].encode("utf-8", errors="ignore")
        r.url = url
        r.headers["Content-Type"] = "text/html; charset=utf-8"
        return r

    _throttle(url)
    r = s.get(url, timeout=timeout, headers=UA)
    r.raise_for_status()

    if allow_cache:
        _FETCH_CACHE[url] = r.text
        _FETCH_CACHE.move_to_end(url)
        while len(_FETCH_CACHE) > _FETCH_CACHE_MAX:
            _FETCH_CACHE.popitem(last=False)

    return r


def fetch_text(url: str) -> str:
    return http_get(url, timeout=30, allow_cache=True).text


def fetch_soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_text(url), "lxml")


# ============================================================
# Helpers
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
    m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2}[,\s]+\d{4})", t)
    if m:
        x = m.group(1).replace("  ", " ")
        if "," not in x and re.search(r"\s\d{4}$", x):
            x = re.sub(r"(\w+\s+\d{1,2})\s+(\d{4})$", r"\1, \2", x)
        return _parse_date_to_iso(x)
    return None


def read_json_if_exists(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


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


def normalize_ticker(t: str) -> Optional[str]:
    t = (t or "").strip().upper()
    if not t:
        return None
    if not re.match(r"^[A-Z][A-Z0-9.\-]{0,9}$", t):
        return None
    return t


# ============================================================
# WeeklyPayers scraping (PRIMARY)
# ============================================================
def _find_best_table(soup: BeautifulSoup, required_keywords: List[str]) -> Optional[BeautifulSoup]:
    for tbl in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]
        if not headers:
            continue
        blob = " ".join(headers)
        if all(k.lower() in blob for k in required_keywords):
            return tbl
    return None


def weeklypayers_discover_universe() -> List[Dict]:
    soup = fetch_soup(WEEKLYPAYERS_ROOT)
    items: List[Dict] = []

    tbl = _find_best_table(soup, required_keywords=["ticker"])
    if tbl:
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]

        def idx_of(keyword: str) -> Optional[int]:
            for i, h in enumerate(headers):
                if keyword in h:
                    return i
            return None

        i_ticker = idx_of("ticker")
        i_name = idx_of("name") or idx_of("fund") or idx_of("etf")
        i_issuer = idx_of("issuer") or idx_of("provider") or idx_of("sponsor")

        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue

            def cell(i) -> Optional[str]:
                if i is None or i >= len(tds):
                    return None
                return norm_space(tds[i].get_text(" ", strip=True)) or None

            t = normalize_ticker(cell(i_ticker) or "")
            if not t:
                continue

            items.append({
                "ticker": t,
                "issuer": cell(i_issuer) or "WeeklyPayers",
                "name": cell(i_name),
                "frequency": "Weekly",
                "notes": "Discovered via weeklypayers.com",
                "source_urls": [WEEKLYPAYERS_ROOT],
            })

    if len(items) < 5:
        text = soup.get_text("\n", strip=True)
        candidates = sorted(set(re.findall(r"\b[A-Z]{2,6}\b", text)))

        for c in candidates:
            if c in {"ETF", "ETFS", "DIV", "NAV", "USD", "FAQ", "HOME", "ABOUT", "BLOG"}:
                continue
            if not re.match(r"^[A-Z]{2,6}$", c):
                continue
            t = normalize_ticker(c)
            if not t:
                continue
            items.append({
                "ticker": t,
                "issuer": "WeeklyPayers",
                "name": None,
                "frequency": "Weekly",
                "notes": "Discovered via weeklypayers.com (regex fallback)",
                "source_urls": [WEEKLYPAYERS_ROOT],
            })

    return dedupe(items)


def weeklypayers_calendar_latest() -> Dict[str, Dict]:
    soup = fetch_soup(WEEKLYPAYERS_CAL)

    tbl = (
        _find_best_table(soup, ["ticker", "ex"]) or
        _find_best_table(soup, ["ticker", "pay"]) or
        _find_best_table(soup, ["ticker", "record"]) or
        _find_best_table(soup, ["ticker", "distribution"]) or
        _find_best_table(soup, ["ticker", "amount"])
    )
    if not tbl:
        return {}

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in tbl.find_all("th")]

    def idx_any(*needles) -> Optional[int]:
        for i, h in enumerate(headers):
            if any(n in h for n in needles):
                return i
        return None

    idx_ticker = idx_any("ticker", "symbol")
    idx_decl   = idx_any("declaration")
    idx_ex     = idx_any("ex", "ex-date", "ex date")
    idx_record = idx_any("record")
    idx_pay    = idx_any("pay", "payment", "payable")

    idx_amt = None
    for i, h in enumerate(headers):
        if ("amount" in h) or ("distribution" in h and ("per" in h or "share" in h or "amount" in h)):
            idx_amt = i
            break

    out: Dict[str, Dict] = {}

    def rank_key(row: Dict) -> Tuple[str, str]:
        return (row.get("ex_dividend_date") or "", row.get("pay_date") or "")

    for tr in tbl.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i) -> Optional[str]:
            if i is None or i >= len(tds):
                return None
            return norm_space(tds[i].get_text(" ", strip=True)) or None

        t = normalize_ticker(cell(idx_ticker) or "")
        if not t:
            continue

        row = {
            "source_url": WEEKLYPAYERS_CAL,
            "distribution_per_share": _parse_float(cell(idx_amt)) if idx_amt is not None else None,
            "declaration_date": _parse_date_to_iso(cell(idx_decl)),
            "ex_dividend_date": _parse_date_to_iso(cell(idx_ex)),
            "record_date": _parse_date_to_iso(cell(idx_record)),
            "pay_date": _parse_date_to_iso(cell(idx_pay)),
        }

        if t not in out or rank_key(row) > rank_key(out[t]):
            out[t] = row

    return out


# ============================================================
# History + metrics
# ============================================================
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
            ticker = it.get("ticker")
            if not ticker:
                continue
            timeline[ticker].append({
                "run_date": snap_date,
                "ex_div": it.get("ex_dividend_date"),
                "price": it.get("price"),
                "dist": it.get("distribution_per_share"),
                "nav": it.get("nav_official"),
            })

    today = datetime.now(timezone.utc).date()

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
    seen = set()
    out = []
    for a in alerts:
        key = (a.get("ticker"), a.get("type"), a.get("ex_dividend_date"))
        if key in seen:
            continue
        seen.add(key)
        out.append(a)
    return out


# ============================================================
# Build items (main pipeline) — WeeklyPayers-only
# ============================================================
def _load_prev_snapshot() -> Optional[Dict]:
    prev = read_json_if_exists(Path(OUTFILE_BACKUP), None)
    if isinstance(prev, dict) and isinstance(prev.get("items"), list):
        return prev
    if isinstance(prev, list):
        return {"items": prev}
    return None


def build_items() -> List[Dict]:
    prev_payload = _load_prev_snapshot()

    discovered = weeklypayers_discover_universe()
    discovered = dedupe(discovered)
    print(f"[weeklypayers] discovered universe: {len(discovered)} tickers")

    cal_map = weeklypayers_calendar_latest()
    print(f"[weeklypayers] calendar rows mapped: {len(cal_map)} tickers")

    items: List[Dict] = []
    for d in discovered:
        ticker = d["ticker"]
        info = cal_map.get(ticker, {})

        # No external price source (per request)
        price = None
        price_source = None

        row = {
            "ticker": ticker,
            "name": d.get("name"),
            "issuer": d.get("issuer") or "WeeklyPayers",
            "reference_asset": d.get("reference_asset"),
            "frequency": "Weekly",

            # WeeklyPayers calendar (primary)
            "distribution_per_share": info.get("distribution_per_share"),
            "declaration_date": info.get("declaration_date"),
            "ex_dividend_date": info.get("ex_dividend_date"),
            "record_date": info.get("record_date"),
            "pay_date": info.get("pay_date"),

            # Sources
            "source_urls": sorted(set((d.get("source_urls") or []) + ([info.get("source_url")] if info.get("source_url") else []))),

            # Price fields kept for UI compatibility (null)
            "share_price": None,
            "price_proxy": None,
            "price": price,
            "price_source": price_source,

            # Derived: ONLY compute if price is present
            "div_pct_per_share": None,
            "payout_per_1000": None,
            # Annualized yield must be: dist * 52 / price * 100  (as requested)
            "annualized_yield_pct": None,
            "monthly_income_per_1000": None,

            # Comparisons
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

            "notes": d.get("notes") or "",
        }

        px = row["price"]
        dist = row["distribution_per_share"]
        if px is not None and dist is not None and px > 0:
            row["div_pct_per_share"] = (dist / px) * 100.0
            row["payout_per_1000"] = (1000.0 / px) * dist
            row["annualized_yield_pct"] = (dist * 52.0 / px) * 100.0  # <<< dist*52/price
            row["monthly_income_per_1000"] = (row["payout_per_1000"] * 52.0) / 12.0

        items.append(row)

    items_weekly = [x for x in items if str(x.get("frequency", "")).lower() == "weekly"]

    # Safety net
    if len(items_weekly) < MIN_EXPECTED_ITEMS:
        if prev_payload and isinstance(prev_payload.get("items"), list) and len(prev_payload["items"]) >= MIN_EXPECTED_ITEMS:
            items_weekly = prev_payload["items"]
            print(f"[fallback] restored previous snapshot from {OUTFILE_BACKUP}, count={len(items_weekly)}")
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

    # Write history first
    write_history_snapshot(payload)

    # Comparisons
    compute_ex_div_comparisons(items)
    payload["items"] = items

    Path("data").mkdir(exist_ok=True)

    with open(OUTFILE_PRIMARY, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with open(OUTFILE_BACKUP, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    alerts = generate_alerts(items)
    with open(ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": payload["generated_at"],
            "threshold_drop_pct": ALERT_DROP_PCT,
            "alerts": alerts
        }, f, indent=2)

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_BACKUP} with {len(items)} items; alerts={len(alerts)}")
    print("NOTE: Prices disabled (USE_YAHOO_PRICES=False). Yield fields requiring price will be null unless you add a WeeklyPayers-based price source.")


if __name__ == "__main__":
    main()
