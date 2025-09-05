# main.py
import sys
import math
import requests
from tabulate import tabulate

API_BASE = "https://api.skinport.com/v1"
HEADERS = {
    # Brotli encoding is required by Skinport docs for these endpoints.
    "Accept-Encoding": "br",
    "User-Agent": "skinport-bullish-script/1.0"
}

SUPPORTED = {"usd", "eur", "pln", "gbp"}

def get_input_currency():
    cur = input("Currency (usd, pln, eur, gbp): ").strip().lower()
    if cur not in SUPPORTED:
        print(f"Unsupported currency '{cur}'. Defaulting to usd.")
        return "USD"
    return cur.upper()

def get_float(prompt):
    while True:
        try:
            return float(input(prompt).strip())
        except ValueError:
            print("Please enter a valid number.")

def get_int(prompt):
    while True:
        try:
            return int(input(prompt).strip())
        except ValueError:
            print("Please enter a valid integer.")

def fetch_items(currency):
    params = {"currency": currency, "tradable": 0}  # tradable: 0 or 1 per docs example
    resp = requests.get(f"{API_BASE}/items", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_sales_history(currency):
    params = {"currency": currency}
    resp = requests.get(f"{API_BASE}/sales/history", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def build_sales_map(sales_list):
    # Map by market_hash_name for quick lookups
    m = {}
    for s in sales_list:
        name = s.get("market_hash_name")
        if not name:
            continue
        m[name] = s
    return m

def safe_avg(obj, key):
    # obj may be None or have 'avg' as None
    if not obj:
        return None
    return obj.get("avg")

def safe_volume(obj, key):
    if not obj:
        return 0
    return obj.get("volume") or 0

def compute_bullish_score(avg_24h, avg_7d, vol_7d):
    # If 7d avg missing: if 24h exists treat as strong signal; else 0.
    if avg_7d and avg_7d > 0:
        growth_ratio = (avg_24h / avg_7d) if avg_24h not in (None, 0) else 0.0
    else:
        # No 7-day average to compare to:
        growth_ratio = 2.0 if (avg_24h and avg_24h > 0) else 0.0
    # scale by log(volume+1) to favour items with meaningful volume
    score = growth_ratio * (1 + math.log10(1 + vol_7d))
    return score

def main():
    print("=== Skinport Bullish Skins Analyzer ===")
    currency = get_input_currency()
    min_price = get_float("Minimum price: ")
    max_price = get_float("Maximum price: ")
    min_sales_week = get_int("Minimum sales in the last week: ")

    try:
        print("Fetching items (this uses API and requires 'Accept-Encoding: br')...")
        items = fetch_items(currency)
    except requests.HTTPError as e:
        print("Failed fetching /v1/items:", e)
        try:
            print("Response body:", e.response.text)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print("Network/error fetching items:", e)
        sys.exit(1)

    try:
        print("Fetching sales history (aggregated stats)...")
        sales = fetch_sales_history(currency)
    except requests.HTTPError as e:
        print("Failed fetching /v1/sales/history:", e)
        try:
            print("Response body:", e.response.text)
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print("Network/error fetching sales history:", e)
        sys.exit(1)

    sales_map = build_sales_map(sales)

    rows = []
    for it in items:
        name = it.get("market_hash_name")
        if not name:
            continue
        # price field from /v1/items: use 'min_price' as representative
        min_price_item = it.get("min_price")
        # skip items missing a price or outside price range
        if min_price_item is None:
            continue
        if not (min_price <= min_price_item <= max_price):
            continue

        s = sales_map.get(name)
        last7 = s.get("last_7_days") if s else None
        last24 = s.get("last_24_hours") if s else None

        vol7 = safe_volume(last7, "volume")
        if vol7 < min_sales_week:
            continue

        avg7 = safe_avg(last7, "avg") or 0.0
        avg24 = safe_avg(last24, "avg") or 0.0

        # Compute metric
        score = compute_bullish_score(avg24, avg7, vol7)

        rows.append({
            "Name": name,
            "MinPrice": min_price_item,
            "Currency": it.get("currency", currency),
            "7d_vol": vol7,
            "7d_avg": round(avg7, 2) if avg7 else None,
            "24h_avg": round(avg24, 2) if avg24 else None,
            "SalesGrowthRatio": round((avg24 / avg7) if avg7 else (avg24 and 999 or 0), 3),
            "BullishScore": round(score, 4)
        })

    if not rows:
        print("No skins matched your filters.")
        return

    # sort by BullishScore desc
    rows_sorted = sorted(rows, key=lambda r: r["BullishScore"], reverse=True)

    # Print a tidy table
    print("\nMost bullish skins (sorted):\n")
    print(tabulate(rows_sorted, headers="keys", tablefmt="fancy_grid", floatfmt=".2f"))

if __name__ == "__main__":
    main()
