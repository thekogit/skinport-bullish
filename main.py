#!/usr/bin/env python3

import sys
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any

from config import (
    API_BASE, SUPPORTED_GAMES, STEAM_SOURCES, MIN_VOL_FLOOR, MIN_PROFIT_PERCENTAGE,
    MAX_CANDIDATES, OUT_CSV, OUT_HTML, MASTER_CSV
)
from utils import (
    normalize_currency, set_global_currency, set_global_game, maybe_float, maybe_int,
    safe_avg, safe_volume, http_get_with_cache, install_requests_cache_if_available,
    make_skinport_url, make_steam_url, AIOHTTP_AVAILABLE, TQDM_AVAILABLE, tqdm
)
from cache_manager import load_cache, save_cache
from steam_api import batch_fetch_steam_prices_async, batch_fetch_steam_prices_sync
from data_analysis import compute_fee_aware_arbitrage_opportunity, compute_bullish_score, compute_enhanced_explosiveness
from output_generator import write_csv, read_csv_to_map, generate_html_with_candidates
from filters import parse_filters, matches_filters, get_available_filters



def extract_price_history(sales_obj, days=90):
    """
    Extract price history from Skinport sales data using available periods.
    Creates a 4-point chart from 90d, 30d, 7d, and 24h average prices.

    Args:
        sales_obj: Sales data object from Skinport API
        days: Maximum days to include (default 90)

    Returns:
        List of dicts with 'date' and 'price' keys for Chart.js
    """
    if not sales_obj:
        return []

    history = []

    # Extract average prices from the 4 available periods in Skinport API
    # This creates a simple 4-point trend chart
    periods = [
        ('last_90_days', '90d ago'),
        ('last_30_days', '30d ago'),
        ('last_7_days', '7d ago'),
        ('last_24_hours', '24h ago')
    ]

    for period_key, label in periods:
        if period_key in sales_obj:
            avg_price = safe_avg(sales_obj.get(period_key))
            if avg_price is not None and avg_price > 0:
                history.append({
                    'date': label,
                    'price': round(avg_price, 2)
                })

    return history

PUMP_RISK_MAX = 60.0

def display_game_selection():
    print("="*70)
    print("🎮 Select Game(s)")
    print("="*70)
    print("  1. CS2 (Counter-Strike 2)")
    print("  2. Dota 2")
    print("  3. TF2 (Team Fortress 2)")
    print("  4. Rust")
    print("  5. All Games (CS2 + Dota 2 + TF2 + Rust)")
    print("="*70)
    print()

    while True:
        choice = input("Select game (1/2/3/4/5) [1]: ").strip() or "1"
        if choice == "1":
            return ["cs2"]
        elif choice == "2":
            return ["dota2"]
        elif choice == "3":
            return ["tf2"]
        elif choice == "4":
            return ["rust"]
        elif choice == "5":
            return ["cs2", "dota2", "tf2", "rust"]
        else:
            print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")


def display_filter_help(game: str):
    print(f"\nAvailable filters for {SUPPORTED_GAMES[game]['name']}:")
    categories = get_available_filters(game)
    for category, filters in categories.items():
        print(f"  {category}:")
        filter_list = ", ".join(filters[:8])
        if len(filters) > 8:
            filter_list += f" ... ({len(filters)-8} more)"
        print(f"    {filter_list}")


def process_game(game: str, currency: str, min_price: float, max_price: float,
                 min_sales_week: int, fetch_steam: bool, filter_tokens: List[str]) -> tuple:
    game_config = SUPPORTED_GAMES[game]
    game_name = game_config["name"]
    app_id = game_config["app_id"]

    # Set global game context
    set_global_game(game, app_id)

    print()
    print("="*70)
    print(f"🎮 Processing {game_name} (App ID: {app_id})")
    print("="*70)
    print()

    # Fetch data from Skinport
    print(f"📡 Fetching {game_name} data from Skinport...")
    try:
        # Get items
        params = {"currency": currency, "tradable": 0, "app_id": app_id}
        items_raw = http_get_with_cache(f"{API_BASE}/items", params=params)

        if isinstance(items_raw, dict):
            for k in ["items", "results", "data"]:
                if k in items_raw and isinstance(items_raw[k], list):
                    items = items_raw[k]
                    break
            else:
                items = items_raw.get("items", []) if isinstance(items_raw.get("items"), list) else []
        elif isinstance(items_raw, list):
            items = items_raw
        else:
            items = []

        # Get sales
        params_sales = {"currency": currency, "app_id": app_id}
        sales_raw = http_get_with_cache(f"{API_BASE}/sales/history", params=params_sales)

        if isinstance(sales_raw, dict):
            sales = (sales_raw.get("history") or sales_raw.get("sales") or
                    sales_raw.get("data") or [])
        elif isinstance(sales_raw, list):
            sales = sales_raw
        else:
            sales = []

    except Exception as e:
        print(f"Error fetching {game_name} data: {e}")
        return [], [], {}

    print(f"✅ Fetched {len(items)} items and {len(sales)} sales records")

    # Build sales map
    sales_map = {}
    for s in sales:
        key = s.get("market_hash_name") or s.get("market_hash") or s.get("name")
        if key:
            sales_map[key] = s

    # Process items
    rows: List[Dict[str, Any]] = []
    item_names_for_steam = []
    now = datetime.now(timezone.utc).isoformat()

    print(f"🔍 Processing {game_name} items with filters...")

    if TQDM_AVAILABLE:
        progress_bar = tqdm(items, desc=f"Processing {game_name}", unit="items")
    else:
        progress_bar = tqdm(total=len(items), desc=f"Processing {game_name}", unit="items")

    try:
        for i, it in enumerate(items if TQDM_AVAILABLE else range(len(items))):
            if not TQDM_AVAILABLE:
                it = items[i]
            progress_bar.update(1)

            name = it.get("market_hash_name") or it.get("name") or it.get("market_hash") or ""
            if not name:
                continue

            # Apply filters
            if not matches_filters(name, filter_tokens, item=it, game=game):
                continue

            price = maybe_float(
                it.get("suggested_price") or  # Suggested price (most accurate for display)
                it.get("min_price") or        # Minimum listing price (fallback)
                it.get("price") or            # Generic price field (fallback)
                None
            )

            # Optional: Verify currency matches
            item_currency = it.get("currency", currency).upper()
            if item_currency != currency.upper():
                if TQDM_AVAILABLE:
                    progress_bar.write(f"Warning: Currency mismatch for {name}: {item_currency} vs {currency}")
            if price is None:
                continue

            if price < (min_price or 0.0):
                continue
            if max_price is not None and price > max_price:
                continue

            # Sales data
            s = sales_map.get(name)
            last_7 = s.get("last_7_days") if s else None
            last_24 = s.get("last_24_hours") if s else None
            last_30 = s.get("last_30_days") if s else None

            vol_7 = safe_volume(last_7)
            if min_sales_week is not None and vol_7 < min_sales_week:
                continue

            avg_7 = safe_avg(last_7) or 0.0
            avg_24 = safe_avg(last_24) or 0.0
            avg_30 = safe_avg(last_30)

            if avg_7 > 0:
                growth_ratio = round(avg_24 / avg_7, 3)
            else:
                growth_ratio = round(999.0 if avg_24 > 0 else 0.0, 3)

            if avg_30 is not None and avg_30 > 0:
                vs_30 = round(avg_7 / avg_30, 3)
            else:
                vs_30 = None

            score = compute_bullish_score(
                avg_24, avg_7,
                avg_30 if avg_30 is not None else 0.0,
                vol_7
            )

            explosiveness, pump_risk = compute_enhanced_explosiveness(
                avg_24, avg_7, avg_30, vol_7, price, it, s, name
            )

            row = {
                "Name": name,
                "Game": game_name,
                "Skinport_URL": make_skinport_url(name, app_id),
                "Steam_URL": make_steam_url(name, app_id),
                "Skinport_Price": f"{round(price, 2):.2f}",
                "Steam_Price": "",
                "Price_Diff_Pct": "",
                "Currency": it.get("currency") or currency,
                "Skinport_Sales7d": str(vol_7),
                "Steam_Sales7d": "",
                "Skinport_7d_avg": f"{round(avg_7, 2):.2f}",
                "Skinport_24h_avg": f"{round(avg_24, 2):.2f}",
                "Skinport_30d_avg": f"{round(avg_30, 2):.2f}" if avg_30 is not None else "",
                "Steam_Explosiveness": "",
                "Skinport_7d_vs_30d": f"{vs_30:.3f}" if vs_30 is not None else "",
                "Skinport_GrowthRatio": f"{growth_ratio:.3f}",
                "Skinport_BullishScore": f"{round(score, 4):.4f}",
                "Skinport_Explosiveness": f"{explosiveness:.2f}",
                "PumpRisk": f"{pump_risk:.1f}",
                "Arbitrage_Opportunity": "",
                "Candidate": "",
                "LastUpdated": now,
                "Steam_Source": "",
                "Fee_Aware_Profit": "",
                "Net_Steam_Proceeds": "",
                "PriceHistory": extract_price_history(s)
            }
            rows.append(row)
            item_names_for_steam.append(name)
    finally:
        progress_bar.close()

    print(f"✅ Processed {len(rows)} {game_name} items matching criteria")

    # Fetch Steam data
    steam_data_map = {}
    if fetch_steam and item_names_for_steam:
        try:
            if AIOHTTP_AVAILABLE:
                steam_data_map = asyncio.run(batch_fetch_steam_prices_async(item_names_for_steam, app_id, currency))
            else:
                steam_data_map = batch_fetch_steam_prices_sync(item_names_for_steam, app_id, currency)
        except Exception as e:
            print(f"Error in Steam fetching: {e}")
            import traceback
            traceback.print_exc()

    return rows, item_names_for_steam, steam_data_map


def main():
    print("="*70)
    print("🚀 High-Speed Fee-Aware Skinport + Steam Market Analysis")
    print("   Multi-Game Support: CS2, Dota 2, TF2, Rust")
    print("="*70)

    if AIOHTTP_AVAILABLE:
        print("⚡ Concurrent fetching: ENABLED (aiohttp available)")
        print("🚀 Features: Async requests, Smart caching, Adaptive rate limiting")
    else:
        print("📊 Sequential fetching: ENABLED (install aiohttp for concurrent mode)")
        print("🚀 Features: Smart caching, Rate limiting, Fee-aware arbitrage")

    print("💰 Platform fees: Skinport 0% buying + Steam 15% selling")

    if not TQDM_AVAILABLE:
        print("📊 Install tqdm for better progress bars: pip install tqdm")

    print()
    print("🔧 Steam sources configured:")
    for source, config in STEAM_SOURCES.items():
        status = "✅ Enabled" if config["enabled"] else "❌ Disabled"
        delay = config["initial_delay"]
        print(f"   {config['name']}: {status} (delay: {delay}s)")

    install_requests_cache_if_available()

    # Game selection with new options
    selected_games = display_game_selection()

    # Currency selection
    cur_input = input("\nCurrency (usd, pln, eur, gbp) [default: usd]: ").strip()
    currency = normalize_currency(cur_input or "usd")
    set_global_currency(currency)

    # Price and volume filters
    min_price_raw = input("Minimum price (blank => 0): ").strip()
    max_price_raw = input("Maximum price (blank => no limit): ").strip()
    min_sales_raw = input("Minimum sales last week (blank => ignored): ").strip()

    min_price = maybe_float(min_price_raw) if min_price_raw else 0.0
    max_price = maybe_float(max_price_raw) if max_price_raw else None
    min_sales_week = maybe_int(min_sales_raw) if min_sales_raw else None

    # Fetch Steam data option
    if AIOHTTP_AVAILABLE:
        fetch_steam = input("Fetch Steam data with concurrent requests? (y/n) [y]: ").strip().lower() != "n"
    else:
        fetch_steam = input("Fetch Steam data sequentially? (y/n) [y]: ").strip().lower() != "n"

    # Game-specific filter setup
    game_filters = {}
    for game in selected_games:
        game_name = SUPPORTED_GAMES[game]["name"]

        print()
        print("="*70)
        print(f"🎯 Filter Setup for {game_name}")
        print("="*70)

        show_help = input(f"Show available filters for {game_name}? (y/n) [n]: ").strip().lower() == "y"
        if show_help:
            display_filter_help(game)

        raw_filter = input(f"\nFilters for {game_name} (comma-separated, blank => all): ").strip()
        game_filters[game] = parse_filters(raw_filter, game)

        if game_filters[game]:
            print(f"Active filters: {", ".join(game_filters[game][:10])}", end="")
            if len(game_filters[game]) > 10:
                print(f" ... ({len(game_filters[game])-10} more)")
            else:
                print()

    # Output options
    output_format = input("\nOutput format (csv / html) [html]: ").strip().lower() or "html"
    if output_format not in ["csv", "html"]:
        print("Unknown format, defaulting to html.")
        output_format = "html"

    write_mode = input("Write mode (overwrite / merge) [overwrite]: ").strip().lower() or "overwrite"
    if write_mode not in ["overwrite", "merge"]:
        print("Unknown mode, defaulting to overwrite.")
        write_mode = "overwrite"

    # Process each selected game
    all_rows = []
    all_steam_data_maps = {}

    for game in selected_games:
        rows, item_names, steam_data_map = process_game(
            game, currency, min_price, max_price, min_sales_week, fetch_steam, game_filters[game]
        )
        all_rows.extend(rows)
        all_steam_data_maps.update(steam_data_map)

    if not all_rows:
        print("\n❌ No items matched your criteria across all selected games.")
        return

    # Apply Steam data and calculate arbitrage
    print("\n💰 Applying Steam data and calculating fee-aware arbitrage...")
    steam_data_applied = 0

    if TQDM_AVAILABLE:
        apply_progress = tqdm(all_rows, desc="Applying fee-aware arbitrage", unit="items")
    else:
        apply_progress = tqdm(total=len(all_rows), desc="Applying fee-aware arbitrage", unit="items")

    try:
        for i, row in enumerate(all_rows if TQDM_AVAILABLE else range(len(all_rows))):
            if not TQDM_AVAILABLE:
                row = all_rows[i]
            apply_progress.update(1)

            item_name = row["Name"]
            if item_name in all_steam_data_maps:
                steam_data = all_steam_data_maps[item_name]
                if steam_data.get("current_price"):
                    steam_price = steam_data["current_price"]
                    row["Steam_Price"] = f"{steam_price:.2f}"

                    skinport_price = float(row["Skinport_Price"])
                    arbitrage_opp, profit_pct, breakdown = compute_fee_aware_arbitrage_opportunity(
                        skinport_price, steam_price, int(row["Skinport_Sales7d"]), currency
                    )

                    row["Arbitrage_Opportunity"] = arbitrage_opp
                    row["Fee_Aware_Profit"] = f"{profit_pct:.1f}%"
                    row["Net_Steam_Proceeds"] = f"{breakdown['net_steam_proceeds']:.2f}"

                    raw_diff_pct = ((skinport_price - steam_price) / steam_price * 100) if steam_price > 0 else 0
                    row["Price_Diff_Pct"] = f"{raw_diff_pct:.1f}%"
                    steam_data_applied += 1

                    row["Steam_Sales7d"] = str(steam_data.get("sales_7d", 0))
                    row["Steam_Explosiveness"] = f"{steam_data.get('explosiveness', 0.0):.1f}"

                    source_name = steam_data.get("source", "unknown")
                    if source_name in STEAM_SOURCES:
                        row["Steam_Source"] = STEAM_SOURCES[source_name]["name"]
                    else:
                        row["Steam_Source"] = source_name.replace("_", " ").title()
    finally:
        apply_progress.close()

    if fetch_steam:
        print(f"✅ Applied fee-aware Steam arbitrage data to {steam_data_applied}/{len(all_rows)} items")
    print(f"💱 Currency consistency: All prices in {currency}")

    # Analyze candidates
    print("\n🎯 Analyzing fee-aware candidates across all games...")

    try:
        all_rows.sort(key=lambda r: float(r.get("Fee_Aware_Profit", "0%").rstrip("%") or 0), reverse=True)
    except Exception:
        pass

    vol_req = min_sales_week if min_sales_week is not None else MIN_VOL_FLOOR
    candidates: List[Dict[str, Any]] = []

    for r in all_rows:
        try:
            fee_aware_profit = float(r.get("Fee_Aware_Profit", "0%").rstrip("%") or 0)
            vol_7 = int(r.get("Skinport_Sales7d") or 0)
            pump_risk = float(r.get("PumpRisk", 0))
            arbitrage_opp = r.get("Arbitrage_Opportunity", "")
        except Exception:
            r["Candidate"] = ""
            continue

        if (fee_aware_profit >= MIN_PROFIT_PERCENTAGE and vol_7 >= vol_req and pump_risk <= PUMP_RISK_MAX):
            if arbitrage_opp in ["EXCELLENT_BUY", "GOOD_BUY", "GOOD_BUY_LOW_VOL"]:
                r["Candidate"] = "YES"
                candidates.append(r)
        else:
            r["Candidate"] = ""

        if len(candidates) >= MAX_CANDIDATES:
            break

    candidates.sort(key=lambda r: float(r.get("Fee_Aware_Profit", "0%").rstrip("%") or 0), reverse=True)

    # Display results
    if candidates:
        print(f"\n🎯 Fee-Aware Top Candidates • {len(candidates)} item(s):")
        for i, c in enumerate(candidates, 1):
            pump_risk = float(c.get("PumpRisk", 0))
            risk_indicator = ("🔴 HIGH" if pump_risk >= 60 else
                            "🟠 HIGH" if pump_risk >= 40 else
                            "🟡 MED" if pump_risk >= 25 else
                            "🟢 LOW-MED" if pump_risk >= 15 else "✅ LOW")

            print(f"   {i:2d}. [{c['Game']}] {c['Name']}")
            print(f"      💰 Skinport: {c['Skinport_Price']} {currency} • Steam: {c['Steam_Price']} {currency}")
            print(f"      💸 Fee-aware profit: {c['Fee_Aware_Profit']} ({c['Arbitrage_Opportunity']})")
            print(f"      📊 Risk: {c['PumpRisk']}({risk_indicator}) | Vol: {c['Skinport_Sales7d']} | Source: {c['Steam_Source']}")
    else:
        print("\n❌ No fee-aware candidates found.")

    # Write output
    print(f"\n📄 Writing {output_format.upper()} output...")

    target_path = OUT_CSV if output_format == "csv" else OUT_HTML

    if write_mode == "overwrite":
        write_csv(MASTER_CSV, all_rows)

        if output_format == "csv":
            write_csv(target_path, all_rows)
            print(f"📄 Wrote {len(all_rows)} rows to {target_path}")
        else:
            generate_html_with_candidates(
                candidates, all_rows, target_path,
                title=f"Fee-Aware Analysis • {' + '.join([SUPPORTED_GAMES[g]['name'] for g in selected_games])}"
            )
    else:
        # Merge mode
        master_map: Dict[str, Dict[str, Any]] = {}
        if MASTER_CSV.exists():
            master_map = read_csv_to_map(MASTER_CSV)

        for r in all_rows:
            master_map[r["Name"]] = r

        merged_rows = list(master_map.values())
        try:
            merged_rows.sort(key=lambda r: float(r.get("Fee_Aware_Profit", "0%").rstrip("%") or 0), reverse=True)
        except Exception:
            pass

        write_csv(MASTER_CSV, merged_rows)

        if output_format == "csv":
            write_csv(target_path, merged_rows)
            print(f"📄 Merged into {target_path} (total: {len(merged_rows)} rows)")
        else:
            generate_html_with_candidates(
                candidates, merged_rows, target_path,
                title=f"Fee-Aware Analysis • {' + '.join([SUPPORTED_GAMES[g]['name'] for g in selected_games])}"
            )

    processing_mode = "concurrent" if AIOHTTP_AVAILABLE else "sequential"
    print(f"\n🎉 High-speed {processing_mode} analysis complete!")
    print(f"📊 Processed {len(all_rows)} items across {len(selected_games)} game(s)")


if __name__ == "__main__":
    main()
