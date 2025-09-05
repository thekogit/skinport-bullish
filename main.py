import sys
import math
import requests
from tabulate import tabulate
import urllib.parse

API_BASE = "https://api.skinport.com/v1"
HEADERS = {
    "Accept-Encoding": "br",
    "User-Agent": "skinport-bullish-script/1.4"
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
    resp = requests.get(f"{API_BASE}/items", headers=HEADERS, params={"currency": currency, "tradable": 0}, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_sales_history(currency):
    resp = requests.get(f"{API_BASE}/sales/history", headers=HEADERS, params={"currency": currency}, timeout=30)
    resp.raise_for_status()
    return resp.json()

def build_sales_map(sales_list):
    return {s["market_hash_name"]: s for s in sales_list if s.get("market_hash_name")}

def safe_avg(obj):
    return obj.get("avg") if obj else None

def safe_volume(obj):
    return obj.get("volume", 0) if obj else 0

def compute_bullish_score(avg_24h, avg_7d, vol_7d):
    if avg_7d and avg_7d > 0:
        growth_ratio = (avg_24h / avg_7d) if avg_24h not in (None, 0) else 0.0
    else:
        growth_ratio = 2.0 if (avg_24h and avg_24h > 0) else 0.0
    return growth_ratio * (1 + math.log10(1 + vol_7d))

def make_skinport_url(name):
    return f"https://skinport.com/market?search={urllib.parse.quote(name)}"

def make_steam_url(name):
    return f"https://steamcommunity.com/market/listings/730/{urllib.parse.quote(name)}"

def truncate(text, width=40):
    return (text[:width-3] + "...") if len(text) > width else text

def main():
    print("=== Skinport Bullish Skins Analyzer ===")
    currency = get_input_currency()
    min_price = get_float("Minimum price: ")
    max_price = get_float("Maximum price: ")
    min_sales_week = get_int("Minimum sales in the last week: ")

    try:
        items = fetch_items(currency)
        sales = fetch_sales_history(currency)
    except Exception as e:
        print("Error fetching data:", e)
        sys.exit(1)

    sales_map = build_sales_map(sales)
    rows = []

    for it in items:
        name = it.get("market_hash_name")
        if not name:
            continue
        price = it.get("min_price")
        if price is None or not (min_price <= price <= max_price):
            continue

        s = sales_map.get(name)
        last7 = s.get("last_7_days") if s else None
        last24 = s.get("last_24_hours") if s else None

        vol7 = safe_volume(last7)
        if vol7 < min_sales_week:
            continue

        avg7 = safe_avg(last7) or 0.0
        avg24 = safe_avg(last24) or 0.0
        score = compute_bullish_score(avg24, avg7, vol7)

        rows.append({
            "Name": truncate(name, 30),
            "Price": round(price,2),
            "SalesThisWeek": vol7,
            "7d_avg": round(avg7,2),
            "24h_avg": round(avg24,2),
            "GrowthRatio": round((avg24 / avg7) if avg7 else 999,3),
            "BullishScore": round(score,4),
            "SkinportURL": truncate(make_skinport_url(name), 50),
            "SteamURL": truncate(make_steam_url(name), 50)
        })

    if not rows:
        print("No skins matched your filters.")
        return

    rows_sorted = sorted(rows, key=lambda r: r["BullishScore"], reverse=True)

    # Table with main stats only
    table_rows = [{k: r[k] for k in ["Name","Price","SalesThisWeek","7d_avg","24h_avg","GrowthRatio","BullishScore"]} for r in rows_sorted]
    print("\nMost bullish skins (sorted):\n")
    print(tabulate(table_rows, headers="keys", tablefmt="fancy_grid", floatfmt=".2f", stralign="left"))

    # URLs separately
    print("\nSkin URLs:\n")
    for idx, r in enumerate(rows_sorted,1):
        print(f"{idx}. {r['Name']} -> Skinport: {r['SkinportURL']} | Steam: {r['SteamURL']}")

if __name__ == "__main__":
    main()
