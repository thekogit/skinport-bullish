#!/usr/bin/env python3
"""
skinport_bullish_full.py

Fetch Skinport items, compute bullish score, and write results either to:
 - skinport_bullish.csv  (CSV mode)
 - skinport_bullish.html (HTML mode)

Features:
 - Accept-Encoding: br required by Skinport API for items & sales endpoints
 - caching with requests_cache if available; file-cache fallback
 - robust parsing and error handling
 - interactive filters: category & weapon tokens (many keywords included)
 - smarter matching (avoid matching cases/charms/stickers that include token)
 - default output: HTML
 - adds 30d average and 7d_vs_30d ratio; includes 30d change in bullish score calculation
"""
import sys
import math
import time
import json
import hashlib
import urllib.parse
import re
from pathlib import Path
import csv
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests

# Optional: requests_cache speeds up and simplifies caching if installed
try:
    import requests_cache
except Exception:
    requests_cache = None

API_BASE = "https://api.skinport.com/v1"
HEADERS = {
    # Skinport API requires brotli encoding header for /items and /sales/history endpoints
    "Accept-Encoding": "br",
    "User-Agent": "skinport-bullish-script/2.0"
}

CACHE_DIR = Path.home() / ".skinport_skin_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_CACHE_TTL = 300  # seconds

SUPPORTED = {"usd", "eur", "pln", "gbp"}

# Output filenames in working directory
OUT_CSV = Path.cwd() / "skinport_bullish.csv"
OUT_HTML = Path.cwd() / "skinport_bullish.html"

# Hidden canonical master CSV used to enable reliable merges and HTML generation
MASTER_CSV = CACHE_DIR / "skinport_bullish_master.csv"

CSV_FIELDS = [
    "Name",
    "Skinport_URL",
    "Steam_URL",
    "Price",
    "Currency",
    "SalesThisWeek",
    "7d_avg",
    "24h_avg",
    "30d_avg",
    "7d_vs_30d",
    "GrowthRatio",
    "BullishScore",
    "LastUpdated"
]

# ---------- Expanded keywords ----------
CATEGORY_KEYWORDS = {
    "knife": [
        "knife", "karambit", "butterfly", "bayonet", "m9 bayonet", "m9", "flip knife", "flip",
        "gut knife", "gut", "huntsman", "stiletto", "stiletto knife", "shadow daggers",
        "skeleton knife", "talon", "talon knife", "ursus", "ursus knife", "navaja", "navaja knife",
        "paracord", "survival knife", "balisong", "balisong knife", "bowie", "bowie knife",
        "classic knife", "falchion", "falchion knife", "kukri", "kukri knife", "daggers"
    ],
    "gloves": [
        "gloves", "hand wrap", "handwraps", "motorcycle gloves", "moto", "specialist gloves",
        "driver gloves", "sport gloves", "hydra", "sport", "tactical gloves", "bloodhound gloves",
        "studded gloves", "leather gloves"
    ],
    "rifle": [
        "rifle", "ak-47", "ak47", "ak 47", "m4a4", "m4a1", "m4a1-s", "aug", "sg 553", "sg-553",
        "galil", "famas", "scar", "g3sg1", "ak"
    ],
    "smg": [
        "p90", "mp7", "mp9", "mac-10", "mac10", "ump-45", "ump45", "mp5", "mp5-sd", "pp-bizon"
    ],
    "sniper": [
        "awp", "ssg 08", "ssg-08", "negev", "g3sg1"
    ],
    "pistol": [
        "desert eagle", "deagle", "usp-s", "usp", "glock-18", "glock", "p250", "five-seven",
        "fiveseven", "cz75", "r8", "dual berettas", "tec-9", "revolver"
    ],
}

WEAPON_KEYWORDS = {
    "ak-47": ["ak-47", "ak47", "ak 47"],
    "m4a4": ["m4a4", "m4 a4"],
    "m4a1-s": ["m4a1-s", "m4a1s", "m4 a1 s", "m4a1 s"],
    "butterfly knife": ["butterfly", "butterfly knife", "balisong"],
    "karambit": ["karambit"],
    "bayonet": ["bayonet", "m9 bayonet", "m9"],
    "falchion": ["falchion"]
}

# Build combined lookup
ALL_FILTER_KEYWORDS: Dict[str, List[str]] = {}
for k, v in CATEGORY_KEYWORDS.items():
    ALL_FILTER_KEYWORDS[k] = v.copy()
for k, v in WEAPON_KEYWORDS.items():
    ALL_FILTER_KEYWORDS[k] = v.copy()

# Exclusion tokens per category to avoid non-weapon items
CATEGORY_EXCLUSIONS: Dict[str, List[str]] = {
    "knife": ["case", "weapon case", "case key", "key", "container", "package", "pack",
              "charm", "sticker", "souvenir", "pin", "patch", "music kit", "coin", "tag",
              "crate", "skin case", "case key"],
    "gloves": ["case", "charm", "sticker", "souvenir", "patch", "key", "case key"],
    "rifle": ["case", "charm", "sticker", "souvenir", "patch", "key"],
    "smg": ["case", "charm", "sticker", "souvenir", "patch"],
    "sniper": ["case", "charm", "sticker", "souvenir", "patch"],
    "pistol": ["case", "charm", "sticker", "souvenir", "patch"],
}

# ---------- utilities ----------
def normalize_currency(cur: Optional[str]) -> str:
    if not cur:
        return "USD"
    cur = cur.strip().lower()
    if cur not in SUPPORTED:
        print(f"Unsupported currency '{cur}'. Defaulting to USD.")
        return "USD"
    return cur.upper()

def maybe_float(s: Optional[str]):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def maybe_int(s: Optional[str]):
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None

def safe_avg(obj: Optional[Dict[str, Any]]):
    if not obj:
        return None
    return maybe_float(obj.get("avg"))

def safe_volume(obj: Optional[Dict[str, Any]]):
    if not obj:
        return 0
    return maybe_int(obj.get("volume") or obj.get("vol") or obj.get("count") or 0)

def compute_bullish_score(avg_24h: float, avg_7d: float, avg_30d: float, vol_7d: int) -> float:
    """
    Combine short-term and medium-term momentum:
      - growth_short = avg_24h / avg_7d  (or fallback)
      - growth_med   = avg_7d  / avg_30d (or fallback)
      - combined = 0.6*growth_short + 0.4*growth_med
      - score = combined * (1 + log10(1 + vol_7d))
    """
    avg_24h = avg_24h or 0.0
    avg_7d = avg_7d or 0.0
    avg_30d = avg_30d or 0.0
    vol_7d = vol_7d or 0

    if avg_7d and avg_7d > 0:
        growth_short = (avg_24h / avg_7d) if avg_24h not in (None, 0) else 0.0
    else:
        growth_short = 2.0 if (avg_24h and avg_24h > 0) else 0.0

    if avg_30d and avg_30d > 0:
        growth_med = (avg_7d / avg_30d) if avg_7d not in (None, 0) else 0.0
    else:
        # neutral medium-term growth if no 30d data but there's 7d activity
        growth_med = 1.0 if (avg_7d and avg_7d > 0) else 0.0

    combined = 0.6 * growth_short + 0.4 * growth_med
    return float(combined * (1 + math.log10(1 + vol_7d)))

def make_skinport_url(name: str) -> str:
    return f"https://skinport.com/market?search={urllib.parse.quote(name)}"

def make_steam_url(name: str) -> str:
    return f"https://steamcommunity.com/market/listings/730/{urllib.parse.quote(name)}"

# ---------- simple file cache (fallback if requests_cache not available) ----------
def _cache_key_for(url: str, params: Optional[Dict[str, Any]]):
    key = url
    if params:
        param_pairs = []
        for k in sorted(params.keys()):
            param_pairs.append(f"{k}={params[k]}")
        key += "?" + "&".join(param_pairs)
    return hashlib.sha256(key.encode("utf8")).hexdigest()

def load_cache(key: str, ttl: int = DEFAULT_CACHE_TTL):
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    mtime = path.stat().st_mtime
    if time.time() - mtime > ttl:
        return None
    try:
        return json.loads(path.read_text(encoding="utf8"))
    except Exception:
        return None

def save_cache(key: str, data: Any):
    path = CACHE_DIR / f"{key}.json"
    try:
        path.write_text(json.dumps(data), encoding="utf8")
    except Exception:
        pass

# ---------- fetch with caching & retries ----------
def install_requests_cache_if_available(expire_after: int = DEFAULT_CACHE_TTL) -> bool:
    if requests_cache is None:
        print("requests_cache not available — using simple file cache.")
        return False
    try:
        requests_cache.install_cache("skinport_cache", expire_after=expire_after)
        print(f"requests_cache installed (expire_after={expire_after}s).")
        return True
    except Exception:
        print("Failed to install requests_cache; falling back to file cache.")
        return False

def http_get_with_cache(url: str, params: Optional[Dict[str, Any]] = None, ttl: int = DEFAULT_CACHE_TTL, max_retries: int = 3):
    use_requests_cache = requests_cache is not None

    backoff = 1
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not use_requests_cache:
                key = _cache_key_for(url, params)
                save_cache(key, data)
            return data
        except requests.RequestException:
            if attempt == 1 and not use_requests_cache:
                key = _cache_key_for(url, params)
                cached = load_cache(key, ttl=ttl)
                if cached is not None:
                    return cached
            if attempt >= max_retries:
                raise
            time.sleep(backoff)
            backoff *= 2

# ---------- CSV helpers ----------
def write_csv(path: Path, rows: List[Dict[str, Any]]):
    with path.open("w", newline="", encoding="utf8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows:
            out = {k: (r.get(k, "") if r.get(k, "") is not None else "") for k in CSV_FIELDS}
            writer.writerow(out)

def read_csv_to_map(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    out = {}
    with path.open("r", encoding="utf8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Name")
            if name:
                out[name] = row
    return out

# ---------- HTML generator ----------
def escape_html(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def generate_html(rows: List[Dict[str, Any]], out_path: Path, title: str = "Skinport Bullish Skins"):
    html_rows = []
    for r in rows:
        skin_anchor = f'<a href="{escape_html(r.get("Skinport_URL"))}" target="_blank" rel="noopener">Skinport</a>'
        steam_anchor = f'<a href="{escape_html(r.get("Steam_URL"))}" target="_blank" rel="noopener">Steam</a>'
        html_rows.append(
            "<tr>"
            f"<td>{escape_html(r.get('Name'))}</td>"
            f"<td>{skin_anchor}</td>"
            f"<td>{steam_anchor}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('Price'))}</td>"
            f"<td>{escape_html(r.get('Currency'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('SalesThisWeek'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('7d_avg'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('24h_avg'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('30d_avg'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('7d_vs_30d'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('GrowthRatio'))}</td>"
            f"<td style='text-align:right'>{escape_html(r.get('BullishScore'))}</td>"
            "</tr>"
        )

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{escape_html(title)}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial; padding: 18px; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 1600px; }}
  th, td {{ border: 1px solid #ddd; padding: 8px; }}
  th {{ background: #f4f4f4; text-align: left; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  td a {{ text-decoration: none; color: inherit; background: #e8f0ff; padding: 3px 6px; border-radius: 4px; }}
</style>
</head>
<body>
<h1>{escape_html(title)}</h1>
<p>Generated: {datetime.now(timezone.utc).isoformat()}</p>
<table>
<thead>
<tr>
<th>Name</th><th>Skinport</th><th>Steam</th><th>Price</th><th>Currency</th><th>Sales(7d)</th><th>7d_avg</th><th>24h_avg</th><th>30d_avg</th><th>7d_vs_30d</th><th>Growth</th><th>Bullish</th>
</tr>
</thead>
<tbody>
{''.join(html_rows)}
</tbody>
</table>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf8")
    print(f"Wrote HTML to {out_path}")

# ---------- filter helpers ----------
def parse_filters(raw: str) -> List[str]:
    """Accept comma-separated filters. Normalize to known keywords or plain tokens."""
    if not raw:
        return []
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    expanded: List[str] = []
    for p in parts:
        if p in ALL_FILTER_KEYWORDS:
            expanded.extend(ALL_FILTER_KEYWORDS[p])
        else:
            expanded.append(p)
    return sorted(set(expanded))

def _has_word_token(name: str, token: str) -> bool:
    """
    Match token as standalone unit in name, handling hyphens and spaces.
    Uses lookarounds to avoid matching within words.
    """
    try:
        tok = re.escape(token)
        pattern = r'(?<!\w)' + tok + r'(?!\w)'
        return re.search(pattern, name, flags=re.IGNORECASE) is not None
    except re.error:
        return token.lower() in name.lower()

def matches_filters(name: str, filters: List[str]) -> bool:
    """
    Smart matching:
     - If no filters: accept all
     - If filters provided: token matches using _has_word_token
     - Use category-specific exclusions to avoid cases/charms/stickers
     - For knives: require a knife-specific token (or explicit word 'knife') plus not excluded
    """
    if not filters:
        return True

    for tok in filters:
        if _has_word_token(name, tok):
            # if token maps to category/weapon keywords, validate exclusions
            for cat, keywords in ALL_FILTER_KEYWORDS.items():
                if tok in keywords or tok == cat:
                    excl = CATEGORY_EXCLUSIONS.get(cat, [])
                    if any(_has_word_token(name, ex) for ex in excl):
                        # excluded for this category
                        break
                    return True
            # token matched but not in category map -> avoid general exclusions
            general_exclusions = ["case", "charm", "sticker", "souvenir", "patch", "key", "container", "package", "crate", "coin", "music kit"]
            if any(_has_word_token(name, ex) for ex in general_exclusions):
                return False
            return True

    # For knife queries, also accept items containing a star '★' only if they also include a knife-specific token
    knife_tokens = set(ALL_FILTER_KEYWORDS.get("knife", []))
    if any(tok in knife_tokens for tok in filters):
        if "★" in name:
            # require at least one knife-specific token (excluding the generic 'knife' token if needed)
            knife_specific_tokens = [t for t in ALL_FILTER_KEYWORDS.get("knife", []) if t and t != "knife"]
            for kt in knife_specific_tokens + ["knife"]:
                if _has_word_token(name, kt):
                    if not any(_has_word_token(name, ex) for ex in CATEGORY_EXCLUSIONS.get("knife", [])):
                        return True
    return False

# ---------- main ----------
def main():
    print("=== Skinport -> CSV/HTML (single file mode) ===")
    install_requests_cache_if_available(expire_after=DEFAULT_CACHE_TTL)

    cur_input = input("Currency (usd, pln, eur, gbp) [default: usd]: ").strip()
    currency = normalize_currency(cur_input or "usd")

    min_price_raw = input("Minimum price (blank => 0): ").strip()
    max_price_raw = input("Maximum price (blank => no upper limit): ").strip()
    min_sales_raw = input("Minimum sales in the last week (blank => ignored): ").strip()

    min_price = maybe_float(min_price_raw) if min_price_raw != "" else 0.0
    max_price = maybe_float(max_price_raw) if max_price_raw != "" else None
    min_sales_week = maybe_int(min_sales_raw) if min_sales_raw != "" else None

    print("\nFilter by category or weapon. Examples: knife, gloves, rifle, ak-47, m4a4, m4a1-s, butterfly knife")
    raw_filter = input("Enter comma-separated categories/weapon tokens to include (blank => all): ").strip()
    filter_tokens = parse_filters(raw_filter)

    # default output is HTML now
    output_format = input("Output format (csv / html) [html]: ").strip().lower() or "html"
    if output_format not in ("csv", "html"):
        print("Unknown output format; defaulting to html.")
        output_format = "html"

    write_mode = input("Write mode: overwrite / merge [overwrite]: ").strip().lower() or "overwrite"
    if write_mode not in ("overwrite", "merge"):
        print("Unknown mode; defaulting to overwrite.")
        write_mode = "overwrite"

    # Fetch data
    try:
        items = http_get_with_cache(f"{API_BASE}/items", params={"currency": currency, "tradable": 0})
        if not isinstance(items, list):
            items = items.get("items") if isinstance(items, dict) else []
    except Exception as e:
        print("Error fetching /v1/items:", e)
        sys.exit(1)

    try:
        sales = http_get_with_cache(f"{API_BASE}/sales/history", params={"currency": currency})
        if not isinstance(sales, list):
            sales = sales.get("history") if isinstance(sales, dict) else []
    except Exception as e:
        print("Error fetching /v1/sales/history:", e)
        sys.exit(1)

    sales_map = {s.get("market_hash_name"): s for s in sales if s.get("market_hash_name")}

    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()
    for it in items:
        name = it.get("market_hash_name") or it.get("name") or it.get("market_hash") or ""
        if not name:
            continue

        # filter by category/weapon tokens
        if not matches_filters(name, filter_tokens):
            continue

        price = maybe_float(it.get("min_price") or it.get("price") or None)
        if price is None:
            continue
        if price < (min_price or 0.0):
            continue
        if (max_price is not None) and (price > max_price):
            continue

        s = sales_map.get(name)
        last7 = s.get("last_7_days") if s else None
        last24 = s.get("last_24_hours") if s else None

        # attempt to find 30-day data under common keys
        last30 = None
        if s:
            for k in ("last_30_days", "last_30", "last_30d"):
                if k in s:
                    last30 = s.get(k)
                    break

        vol7 = safe_volume(last7)
        if (min_sales_week is not None) and (vol7 < min_sales_week):
            continue

        avg7 = safe_avg(last7) or 0.0
        avg24 = safe_avg(last24) or 0.0
        avg30 = safe_avg(last30) or 0.0

        # growth ratio (24h vs 7d) preserved for compatibility
        growth_ratio = round((avg24 / avg7) if avg7 else (999.0 if avg24 > 0 else 0.0), 3)
        # 7d_vs_30d ratio
        vs30 = round((avg7 / avg30) if avg30 else (999.0 if avg7 > 0 else 0.0), 3)

        score = compute_bullish_score(avg24, avg7, avg30, vol7)

        rows.append({
            "Name": name,
            "Skinport_URL": make_skinport_url(name),
            "Steam_URL": make_steam_url(name),
            "Price": f"{round(price, 2):.2f}",
            "Currency": it.get("currency") or currency,
            "SalesThisWeek": str(vol7),
            "7d_avg": f"{round(avg7, 2):.2f}",
            "24h_avg": f"{round(avg24, 2):.2f}",
            "30d_avg": f"{round(avg30, 2):.2f}",
            "7d_vs_30d": f"{vs30:.3f}",
            "GrowthRatio": f"{growth_ratio:.3f}",
            "BullishScore": f"{round(score, 4):.4f}",
            "LastUpdated": now
        })

    if not rows:
        print("No skins matched your filters. No file written.")
        return

    # sort rows by BullishScore descending (numerical)
    try:
        rows.sort(key=lambda r: float(r.get("BullishScore", 0)), reverse=True)
    except Exception:
        pass

    # ---------- handle write/merge logic ----------
    if output_format == "csv":
        target_path = OUT_CSV
    else:
        target_path = OUT_HTML

    if write_mode == "overwrite":
        write_csv(MASTER_CSV, rows)
        if output_format == "csv":
            write_csv(target_path, rows)
            print(f"Wrote {len(rows)} rows to {target_path} (overwritten).")
        else:
            generate_html(rows, target_path)
        return

    # merge mode
    master_map: Dict[str, Dict[str, Any]] = {}
    if MASTER_CSV.exists():
        master_map = read_csv_to_map(MASTER_CSV)
    else:
        if output_format == "csv" and OUT_CSV.exists():
            master_map = read_csv_to_map(OUT_CSV)
        else:
            master_map = {}

    for r in rows:
        master_map[r["Name"]] = r

    merged_rows = list(master_map.values())

    # sort merged
    try:
        merged_rows.sort(key=lambda r: float(r.get("BullishScore", 0)), reverse=True)
    except Exception:
        pass

    write_csv(MASTER_CSV, merged_rows)

    if output_format == "csv":
        write_csv(target_path, merged_rows)
        print(f"Merged {len(rows)} new/updated rows into {target_path} (total rows now: {len(merged_rows)}).")
    else:
        generate_html(merged_rows, target_path)
        print(f"Merged {len(rows)} new/updated rows into {target_path} (total rows now: {len(merged_rows)}).")

if __name__ == "__main__":
    main()
