import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup


# =========================
# Config (defaults)
# =========================
DEFAULT_SOURCES = {
    "graniteshares_distribution": "https://graniteshares.com/institutional/us/en-us/underlyings/distribution/",
    "roundhill_weeklypay_list": "https://www.roundhillinvestments.com/weeklypay-etfs",
    "yieldmax_our_etfs": "https://yieldmaxetfs.com/our-etfs/",
    # YieldMax fund pages are derived from ticker using:
    # https://yieldmaxetfs.com/our-etfs/{ticker.lower()}/
}

UA = {"User-Agent": "weekly-etf-dashboard/1.0"}
OUTFILE_PRIMARY = "data/weekly_etfs.json"   # dashboard reads this
OUTFILE_BACKUP = "data/items.json"          # last-good snapshot backup
MIN_FETCH_INTERVAL_SEC = 0.35               # polite throttle


# =========================
# Utilities
# =========================
_FETCH_CACHE: Dict[str, str] = {}
_LAST_FETCH_AT = 0.0


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
    s = str(s).strip()
    import datetime as _dt
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return _dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None


def _parse_mmddyyyy_to_iso(s: str) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    import datetime as _dt
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return _dt.datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return _parse_date_to_iso(s)


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


def fetch_text(url: str) -> str:
    global _LAST_FETCH_AT
    if url in _FETCH_CACHE:
        return _FETCH_CACHE[url]

    now = time.time()
    dt = now - _LAST_FETCH_AT
    if dt < MIN_FETCH_INTERVAL_SEC:
        time.sleep(MIN_FETCH_INTERVAL_SEC - dt)

    r = requests.get(url, timeout=30, headers=UA)
    r.raise_for_status()
    text = r.text
    _FETCH_CACHE[url] = text
    _LAST_FETCH_AT = time.time()
    return text


def fetch_soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_text(url), "lxml")


def read_json_if_exists(path: str, default):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_sources() -> Dict[str, str]:
    """
    Optional config file to avoid rewriting code:
      data/sources.json

    Example:
    {
      "graniteshares_distribution": "...",
      "roundhill_weeklypay_list": "...",
      "yieldmax_our_etfs": "..."
    }
    """
    cfg = read_json_if_exists("data/sources.json", {})
    merged = dict(DEFAULT_SOURCES)
    if isinstance(cfg, dict):
        for k, v in cfg.items():
            if isinstance(v, str) and v.strip():
                merged[k] = v.strip()
    return merged


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


# =========================
# Manual tickers (optional)
# =========================
def load_manual_tickers() -> List[Dict]:
    """
    data/manual_tickers.json can be either:
    1) simple list:
       [{"ticker":"AMYY","issuer":"GraniteShares"}]
    2) richer:
       [{"ticker":"AMYY","issuer":"GraniteShares","name":"...","reference_asset":"AMD"}]

    We use it to ensure tickers appear even if discovery fails.
    """
    data = read_json_if_exists("data/manual_tickers.json", [])
    out = []
    if isinstance(data, list):
        for x in data:
            if not isinstance(x, dict):
                continue
            t = str(x.get("ticker", "")).upper().strip()
            issuer = x.get("issuer")
            if not t or not issuer:
                continue
            out.append({
                "ticker": t,
                "issuer": issuer,
                "frequency": "Weekly",
                "name": x.get("name"),
                "reference_asset": x.get("reference_asset"),
                "notes": (x.get("notes") or "Manually added")
            })
    return out


# =========================
# GraniteShares: distribution table (PRIMARY)
# =========================
def graniteshares_distribution_map(url: str) -> Dict[str, Dict]:
    """
    Returns per ticker:
      distribution_per_share, ex_dividend_date, record_date, pay_date, frequency
    """
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    table = None
    for t in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in t.find_all("th")]
        h = " ".join(headers)
        if "ticker" in h and "distribution per share" in h and ("payment date" in h or "pay" in h):
            table = t
            break
    if not table:
        return {}

    headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]

    def find_idx_contains(*needles):
        for i, h in enumerate(headers):
            if all(n in h for n in needles):
                return i
        return None

    idx_ticker = find_idx_contains("ticker")
    idx_freq = find_idx_contains("frequency")
    idx_dist = find_idx_contains("distribution", "per", "share")
    idx_ex = find_idx_contains("ex")
    idx_record = find_idx_contains("record")
    idx_pay = find_idx_contains("payment") if find_idx_contains("payment") is not None else find_idx_contains("pay")

    out: Dict[str, Dict] = {}
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

        freq = (cell(idx_freq) or "").strip().title() or None
        dist = _parse_float(cell(idx_dist))
        exd = _parse_date_to_iso(cell(idx_ex))
        rec = _parse_date_to_iso(cell(idx_record))
        pay = _parse_date_to_iso(cell(idx_pay))

        out[ticker] = {
            "frequency": freq,
            "distribution_per_share": dist,
            "ex_dividend_date": exd,
            "record_date": rec,
            "pay_date": pay,
            "source_url": url
        }
    return out


# =========================
# Roundhill: WeeklyPay list + fund pages
# =========================
def roundhill_discover_weekly(url: str) -> List[Dict]:
    soup = fetch_soup(url)
    text = soup.get_text("\n", strip=True)
    tickers = sorted(set(re.findall(r"\b[A-Z]{3,5}W\b", text)))
    return [{
        "ticker": t,
        "issuer": "Roundhill",
        "frequency": "Weekly",
        "name": None,
        "reference_asset": None,
        "notes": "Discovered via Roundhill WeeklyPay"
    } for t in tickers]


def roundhill_fund_url(ticker: str) -> str:
    return f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"


def roundhill_latest_dist_and_dates(ticker: str) -> Dict:
    """
    Scrape first row of Distribution History if present (preferred),
    else first row of Distribution Calendar.
    """
    url = roundhill_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    # Find a heading and then the next table
    def table_after_heading(heading_text: str):
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
            if heading_text.lower() in tag.get_text(" ", strip=True).lower():
                return tag.find_next("table")
        return None

    hist = table_after_heading("Distribution History")
    if hist:
        rows = hist.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td", "th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            # common order: Declared, Ex, Record, Pay, Amount
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = _parse_date_to_iso(vals[0])
                out["ex_dividend_date"] = _parse_date_to_iso(vals[1])
                out["record_date"] = _parse_date_to_iso(vals[2])
                out["pay_date"] = _parse_date_to_iso(vals[3])
            if len(vals) >= 5:
                out["distribution_per_share"] = _parse_float(vals[4])
            return out

    cal = table_after_heading("Distribution Calendar")
    if cal:
        rows = cal.find_all("tr")
        if len(rows) >= 2:
            tds = rows[1].find_all(["td", "th"])
            vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
            out = {"source_url": url}
            if len(vals) >= 4:
                out["declaration_date"] = _parse_date_to_iso(vals[0])
                out["ex_dividend_date"] = _parse_date_to_iso(vals[1])
                out["record_date"] = _parse_date_to_iso(vals[2])
                out["pay_date"] = _parse_date_to_iso(vals[3])
            out["distribution_per_share"] = None
            return out

    return {}


def roundhill_map(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = roundhill_latest_dist_and_dates(t)
        if info:
            out[t] = info
    return out


# =========================
# YieldMax: Our ETFs discovery + per-fund distribution table
# =========================
def yieldmax_discover_weekly(url: str) -> List[Dict]:
    soup = fetch_soup(url)

    items = []
    for table in soup.find_all("table"):
        headers = [norm_space(th.get_text(" ", strip=True)).lower() for th in table.find_all("th")]
        if not headers:
            continue
        joined = " ".join(headers)
        if "ticker" not in joined or "distribution frequency" not in joined:
            continue

        def idx(needle: str) -> Optional[int]:
            for i, h in enumerate(headers):
                if needle in h:
                    return i
            return None

        idx_ticker = idx("ticker")
        idx_name = idx("etf name")
        idx_ref = idx("reference asset")
        idx_freq = idx("distribution frequency")
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

            ticker = (cell(idx_ticker) or "").upper()
            freq = (cell(idx_freq) or "")
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
                "notes": "Discovered via YieldMax Our ETFs"
            })

    return dedupe(items)


def yieldmax_fund_url(ticker: str) -> str:
    return f"https://yieldmaxetfs.com/our-etfs/{ticker.lower()}/"


def yieldmax_latest_dist_and_dates(ticker: str) -> Dict:
    """
    Scrape first row of the YieldMax distribution table on the fund page.
    Expected headers include:
      Distribution Per Share, Declared Date, Ex Date, Record Date, Payable Date
    Dates commonly appear as mm/dd/yyyy.
    """
    url = yieldmax_fund_url(ticker)
    try:
        soup = fetch_soup(url)
    except Exception:
        return {}

    table = None
    for t in soup.find_all("table"):
        hdrs = [norm_space(th.get_text(" ", strip=True)).lower() for th in t.find_all("th")]
        h = " ".join(hdrs)
        if ("distribution per share" in h and "declared" in h and "ex" in h and "record" in h and "payable" in h):
            table = t
            break
    if not table:
        return {}

    rows = table.find_all("tr")
    if len(rows) < 2:
        return {}

    tds = rows[1].find_all(["td", "th"])
    vals = [norm_space(x.get_text(" ", strip=True)) for x in tds]
    if len(vals) < 5:
        return {}

    return {
        "distribution_per_share": _parse_float(vals[0]),
        "declaration_date": _parse_mmddyyyy_to_iso(vals[1]),
        "ex_dividend_date": _parse_mmddyyyy_to_iso(vals[2]),
        "record_date": _parse_mmddyyyy_to_iso(vals[3]),
        "pay_date": _parse_mmddyyyy_to_iso(vals[4]),
        "source_url": url
    }


def yieldmax_map(tickers: List[str]) -> Dict[str, Dict]:
    out = {}
    for t in tickers:
        info = yieldmax_latest_dist_and_dates(t)
        if info:
            out[t] = info
    return out


# =========================
# Build & write
# =========================
def build_items() -> List[Dict]:
    sources = load_sources()

    # Discovery from your three links
    discovered: List[Dict] = []
    discovered += yieldmax_discover_weekly(sources["yieldmax_our_etfs"])
    discovered += roundhill_discover_weekly(sources["roundhill_weeklypay_list"])

    # Keep your manual tickers as a safety net (and for GraniteShares tickers list)
    discovered += load_manual_tickers()

    discovered = dedupe(discovered)

    # Enrichment maps
    gs_map = graniteshares_distribution_map(sources["graniteshares_distribution"])

    rh_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "Roundhill"]
    rh_map = roundhill_map(rh_tickers)

    ym_tickers = [d["ticker"] for d in discovered if d.get("issuer") == "YieldMax"]
    ym_map = yieldmax_map(ym_tickers)

    print(
        f"[discovery] YieldMax={len([d for d in discovered if d.get('issuer')=='YieldMax'])} "
        f"GraniteShares={len([d for d in discovered if d.get('issuer')=='GraniteShares'])} "
        f"Roundhill={len([d for d in discovered if d.get('issuer')=='Roundhill'])} "
        f"Total={len(discovered)}"
    )
    print(
        f"[enrich] YieldMax fund pages={len(ym_map)} "
        f"Roundhill fund pages={len(rh_map)} "
        f"GraniteShares dist rows={len(gs_map)}"
    )

    items: List[Dict] = []

    # IMPORTANT: merge happens INSIDE this loop (this is what fixes blank columns)
    for d in discovered:
        t = d["ticker"]
        issuer = d.get("issuer")

        row = {
            "ticker": t,
            "issuer": issuer,
            "name": d.get("name"),
            "reference_asset": d.get("reference_asset"),
            "frequency": "Weekly",
            "distribution_per_share": None,
            "declaration_date": None,
            "ex_dividend_date": None,
            "record_date": None,
            "pay_date": None,
            "nav_official": None,
            "price_proxy": get_price_proxy_stooq(t),
            "notes": d.get("notes") or ""
        }

        # GraniteShares: authoritative distribution table
        if issuer == "GraniteShares" and t in gs_map:
            info = gs_map[t]
            # keep only weekly rows if frequency exists
            freq = (info.get("frequency") or "").strip().lower()
            if freq:
                row["frequency"] = info.get("frequency") or row["frequency"]
            row["distribution_per_share"] = info.get("distribution_per_share")
            row["ex_dividend_date"] = info.get("ex_dividend_date")
            row["record_date"] = info.get("record_date")
            row["pay_date"] = info.get("pay_date")
            row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + info.get("source_url", "")).strip()

        # Roundhill: fund page table
        if issuer == "Roundhill" and t in rh_map:
            info = rh_map[t]
            if info.get("distribution_per_share") is not None:
                row["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    row[k] = info[k]
            row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + info.get("source_url", "")).strip()

        # YieldMax: fund page distribution table (ULTY is one example; same pattern for all)
        if issuer == "YieldMax" and t in ym_map:
            info = ym_map[t]
            if info.get("distribution_per_share") is not None:
                row["distribution_per_share"] = info["distribution_per_share"]
            for k in ["declaration_date", "ex_dividend_date", "record_date", "pay_date"]:
                if info.get(k):
                    row[k] = info[k]
            row["notes"] = (row["notes"] + (" | " if row["notes"] else "") + info.get("source_url", "")).strip()
           
            # ---- Frontend compatibility fields ----
            row["distributionPerShare"] = row["distribution_per_share"]
            row["declaration"] = row["declaration_date"]
            row["exDividend"] = row["ex_dividend_date"]
            row["record"] = row["record_date"]
            row["pay"] = row["pay_date"]
            row["price"] = row["price_proxy"]
        items.append(row)

    # Weekly only
    items = [x for x in items if str(x.get("frequency", "")).lower() == "weekly"]

    # Safety net: if we somehow end up tiny, keep last good snapshot
    if len(items) < 20:
        prev = read_json_if_exists(OUTFILE_BACKUP, None)
        if isinstance(prev, dict) and isinstance(prev.get("items"), list) and len(prev["items"]) >= 20:
            items = prev["items"]
            print(f"[fallback] restored previous snapshot, count={len(items)}")
    
    return items


def main():
    items = build_items()
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "items": items
    }

    Path("data").mkdir(parents=True, exist_ok=True)
    Path(OUTFILE_PRIMARY).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    Path(OUTFILE_BACKUP).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {OUTFILE_PRIMARY} and {OUTFILE_BACKUP} with {len(items)} items")


if __name__ == "__main__":
    main()
