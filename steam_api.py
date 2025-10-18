#!/usr/bin/env python3

import asyncio
import random
import urllib.parse
import re
import math
import time
import threading
import json
from typing import Dict, Any, List

import requests

from config import (
    STEAM_SOURCES, STEAM_HEADERS, STEAM_CACHE_TTL, MAX_CONCURRENT_STEAM_REQUESTS,
    REQUEST_CHUNK_SIZE, ADAPTIVE_DELAY_MULTIPLIER, DELAY_RECOVERY_FACTOR
)

from cache_manager import load_cache, save_cache, load_cache_batch, _cache_key_for_steam
from utils import clean_price_string, AIOHTTP_AVAILABLE, TQDM_AVAILABLE, tqdm, _rate_limit_lock

if AIOHTTP_AVAILABLE:
    import aiohttp

# Get currency info from utils
import utils


def adaptive_delay_adjustment(source: str, success: bool, rate_limited: bool = False):
    with _rate_limit_lock:
        config = STEAM_SOURCES[source]
        if rate_limited:
            config["rate_limit_count"] += 1
            config["current_delay"] = min(
                config["current_delay"] * ADAPTIVE_DELAY_MULTIPLIER,
                config["max_delay"]
            )
        elif success:
            config["current_delay"] = max(
                config["current_delay"] * DELAY_RECOVERY_FACTOR,
                config["initial_delay"]
            )


# Async Steam fetching functions with RETRY LOGIC
if AIOHTTP_AVAILABLE:
    async def fetch_steam_price_direct_async(session, skin_name: str, semaphore, app_id: int = 730, 
                                            currency: str = "USD", max_retries: int = 5) -> Dict[str, Any]:
        # Removed: global steam_currency_id (now using utils.steam_currency_id)

        cache_key = _cache_key_for_steam("steam_direct", skin_name, currency, app_id)
        cached_data = load_cache(cache_key, STEAM_CACHE_TTL)
        if cached_data:
            return cached_data

        steam_data = {
            "current_price": None,
            "sales_7d": 0,
            "explosiveness": 0.0,
            "source": "steam_direct",
            "currency": currency,
            "app_id": app_id
        }

        # RETRY LOOP - Keep trying until success or max retries
        for retry_attempt in range(max_retries):
            async with semaphore:
                try:
                    # Exponential backoff for retries
                    if retry_attempt > 0:
                        backoff_delay = min(30.0, 5.0 * (2 ** (retry_attempt - 1)))
                        await asyncio.sleep(backoff_delay)
                        print(f"🔄 Retry {retry_attempt}/{max_retries-1} for: {skin_name[:30]}")

                    current_delay = STEAM_SOURCES["steam_direct"]["current_delay"]
                    await asyncio.sleep(current_delay + random.uniform(0.1, 0.5))

                    headers = random.choice(STEAM_HEADERS)
                    url = STEAM_SOURCES["steam_direct"]["base_url"]
                    params = {
                        "appid": str(app_id),
                        "market_hash_name": skin_name,
                        "currency": str(utils.steam_currency_id)
                    }

                    async with session.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=STEAM_SOURCES["steam_direct"]["timeout"])
                    ) as response:
                        # Check for rate limiting - RETRY if hit
                        if response.status == 429:
                            adaptive_delay_adjustment("steam_direct", False, rate_limited=True)
                            STEAM_SOURCES["steam_direct"]["error_count"] += 1
                            if retry_attempt == 0:
                                print(f"⚠️  Rate limited for: {skin_name[:30]} - will retry")
                            continue  # Retry this request

                        if response.status != 200:
                            STEAM_SOURCES["steam_direct"]["error_count"] += 1
                            if retry_attempt == 0:
                                print(f"❌ Status {response.status} for: {skin_name[:30]} - will retry")
                            continue  # Retry this request

                        # Read and parse JSON
                        try:
                            text_content = await response.text()
                            data = json.loads(text_content)
                        except json.JSONDecodeError as je:
                            if retry_attempt == 0:
                                print(f"❌ JSON error for {skin_name[:30]} - will retry")
                            STEAM_SOURCES["steam_direct"]["error_count"] += 1
                            continue  # Retry this request

                        # Parse the successful response
                        if data.get("success") and data.get("lowest_price"):
                            steam_price = clean_price_string(data["lowest_price"], currency)
                            if steam_price:
                                steam_data["current_price"] = steam_price

                                # Get volume
                                if data.get("volume"):
                                    volume_str = str(data["volume"]).replace(",", "").replace(".", "")
                                    try:
                                        steam_data["sales_7d"] = int(volume_str)
                                    except:
                                        steam_data["sales_7d"] = 0

                                # Calculate explosiveness
                                base_explosiveness = min(50.0, steam_price * 0.1)
                                volume_factor = min(2.0, math.log10(1 + steam_data["sales_7d"]) / 2.0) if steam_data["sales_7d"] > 0 else 1.0
                                steam_data["explosiveness"] = round(base_explosiveness * volume_factor, 2)

                                adaptive_delay_adjustment("steam_direct", True)
                                STEAM_SOURCES["steam_direct"]["success_count"] += 1
                                save_cache(cache_key, steam_data)

                                if retry_attempt > 0:
                                    print(f"✅ Retry succeeded for: {skin_name[:30]}")

                                return steam_data  # SUCCESS!

                except asyncio.TimeoutError:
                    STEAM_SOURCES["steam_direct"]["error_count"] += 1
                    if retry_attempt == 0:
                        print(f"⏱️  Timeout for: {skin_name[:30]} - will retry")
                    continue  # Retry this request

                except aiohttp.ClientError as ce:
                    STEAM_SOURCES["steam_direct"]["error_count"] += 1
                    if retry_attempt == 0:
                        print(f"🔌 Connection error for {skin_name[:30]} - will retry")
                    continue  # Retry this request

                except Exception as e:
                    STEAM_SOURCES["steam_direct"]["error_count"] += 1
                    if retry_attempt == 0:
                        print(f"💥 Error for {skin_name[:30]}: {type(e).__name__} - will retry")
                    continue  # Retry this request

        # All retries exhausted
        print(f"❌ Failed after {max_retries} retries: {skin_name[:30]}")
        return steam_data


    async def fetch_steam_price_render_async(session, skin_name: str, semaphore, app_id: int = 730,
                                            currency: str = "USD", max_retries: int = 5) -> Dict[str, Any]:
        # Removed: global steam_currency_id (now using utils.steam_currency_id)

        cache_key = _cache_key_for_steam("steam_render", skin_name, currency, app_id)
        cached_data = load_cache(cache_key, STEAM_CACHE_TTL)
        if cached_data:
            return cached_data

        steam_data = {
            "current_price": None,
            "sales_7d": 0,
            "explosiveness": 0.0,
            "source": "steam_render",
            "currency": currency,
            "app_id": app_id
        }

        # RETRY LOOP
        for retry_attempt in range(max_retries):
            async with semaphore:
                try:
                    # Exponential backoff for retries
                    if retry_attempt > 0:
                        backoff_delay = min(30.0, 5.0 * (2 ** (retry_attempt - 1)))
                        await asyncio.sleep(backoff_delay)

                    current_delay = STEAM_SOURCES["steam_render"]["current_delay"]
                    await asyncio.sleep(current_delay + random.uniform(0.1, 0.5))

                    headers = random.choice(STEAM_HEADERS)
                    encoded_name = urllib.parse.quote(skin_name)
                    url = f"https://steamcommunity.com/market/listings/{app_id}/{encoded_name}/render"
                    params = {
                        "start": "0",
                        "count": "1",
                        "currency": str(utils.steam_currency_id),
                        "format": "json"
                    }

                    async with session.get(
                        url,
                        params=params,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=STEAM_SOURCES["steam_render"]["timeout"])
                    ) as response:
                        # Check for rate limiting - RETRY if hit
                        if response.status == 429:
                            adaptive_delay_adjustment("steam_render", False, rate_limited=True)
                            STEAM_SOURCES["steam_render"]["error_count"] += 1
                            if retry_attempt == 0:
                                print(f"⚠️  Rate limited (render) for: {skin_name[:30]} - will retry")
                            continue  # Retry

                        if response.status != 200:
                            STEAM_SOURCES["steam_render"]["error_count"] += 1
                            continue  # Retry

                        # Read and parse JSON
                        try:
                            text_content = await response.text()
                            data = json.loads(text_content)
                        except json.JSONDecodeError:
                            STEAM_SOURCES["steam_render"]["error_count"] += 1
                            continue  # Retry

                        # Parse the successful response
                        if data.get("success") and data.get("results_html"):
                            html_content = data["results_html"]

                            # Extract price based on currency
                            if currency == "PLN":
                                price_match = re.search(r'([0-9,.]+)\s*z', html_content)
                            elif currency == "EUR":
                                price_match = re.search(r'€\s*([0-9,.]+)|([0-9,.]+)\s*€', html_content)
                            elif currency == "GBP":
                                price_match = re.search(r'£\s*([0-9,.]+)', html_content)
                            else:  # USD
                                price_match = re.search(r'\$\s*([0-9,.]+)', html_content)

                            if price_match:
                                price_str = price_match.group(1) or (price_match.group(2) if len(price_match.groups()) > 1 else price_match.group(1))
                                steam_price = clean_price_string(price_str, currency)

                                if steam_price:
                                    steam_data["current_price"] = steam_price

                                    # Get volume
                                    if data.get("total_count"):
                                        steam_data["sales_7d"] = min(int(data["total_count"]), 1000)

                                    # Calculate explosiveness
                                    base_explosiveness = min(50.0, steam_price * 0.1)
                                    volume_factor = min(2.0, math.log10(1 + steam_data["sales_7d"]) / 2.0) if steam_data["sales_7d"] > 0 else 1.0
                                    steam_data["explosiveness"] = round(base_explosiveness * volume_factor, 2)

                                    adaptive_delay_adjustment("steam_render", True)
                                    STEAM_SOURCES["steam_render"]["success_count"] += 1
                                    save_cache(cache_key, steam_data)

                                    if retry_attempt > 0:
                                        print(f"✅ Retry succeeded (render) for: {skin_name[:30]}")

                                    return steam_data  # SUCCESS!

                except asyncio.TimeoutError:
                    STEAM_SOURCES["steam_render"]["error_count"] += 1
                    continue  # Retry
                except aiohttp.ClientError:
                    STEAM_SOURCES["steam_render"]["error_count"] += 1
                    continue  # Retry
                except Exception:
                    STEAM_SOURCES["steam_render"]["error_count"] += 1
                    continue  # Retry

        # All retries exhausted
        return steam_data


    async def fetch_steam_price_multi_source_async(session, skin_name: str, semaphore, app_id: int = 730, 
                                                   currency: str = "USD", max_retries: int = 5) -> Dict[str, Any]:
        # Try direct API first with retries
        if STEAM_SOURCES["steam_direct"]["enabled"]:
            result = await fetch_steam_price_direct_async(session, skin_name, semaphore, app_id, currency, max_retries)
            if result.get("current_price"):
                return result

        # Fallback to render API with retries
        if STEAM_SOURCES["steam_render"]["enabled"]:
            result = await fetch_steam_price_render_async(session, skin_name, semaphore, app_id, currency, max_retries)
            if result.get("current_price"):
                return result

        # Both failed after all retries
        return {
            "current_price": None,
            "sales_7d": 0,
            "explosiveness": 0.0,
            "source": "none",
            "currency": currency,
            "app_id": app_id
        }


    async def batch_fetch_steam_prices_async(item_names: List[str], app_id: int = 730, currency: str = "USD") -> Dict[str, Dict[str, Any]]:
        print(f"\n⚡ Fast-fetching Steam prices in {currency} (App ID: {app_id})")
        print(f"   Processing in chunks of 8 items with automatic retry")
        print(f"   Concurrent requests per chunk: {MAX_CONCURRENT_STEAM_REQUESTS}")

        # Batch check cache
        print("📦 Checking cache for existing data...")
        cached_data_direct, items_to_fetch_direct = load_cache_batch(item_names, "steam_direct", currency, app_id)
        cached_data_render, items_to_fetch_render = load_cache_batch(item_names, "steam_render", currency, app_id)

        steam_data_map = {}
        for item_name in item_names:
            if item_name in cached_data_direct:
                steam_data_map[item_name] = cached_data_direct[item_name]
            elif item_name in cached_data_render:
                steam_data_map[item_name] = cached_data_render[item_name]

        items_to_fetch = [name for name in item_names if name not in steam_data_map]
        cache_hit_rate = ((len(item_names) - len(items_to_fetch)) / len(item_names)) * 100 if item_names else 0
        print(f"   Cache hit rate: {cache_hit_rate:.1f}% ({len(item_names) - len(items_to_fetch)}/{len(item_names)})")

        if not items_to_fetch:
            print("✅ All items found in cache!")
            return steam_data_map

        print(f"🚀 Fetching {len(items_to_fetch)} items in chunks of 8 with retry...")

        # Create semaphore and connector
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_STEAM_REQUESTS)
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_STEAM_REQUESTS * 2,
            limit_per_host=MAX_CONCURRENT_STEAM_REQUESTS,
            ttl_dns_cache=300,
            use_dns_cache=True
        )

        successful_fetches = 0

        if TQDM_AVAILABLE:
            progress_bar = tqdm(
                total=len(items_to_fetch),
                desc=f"Concurrent Steam fetch ({currency})",
                unit="items"
            )
        else:
            progress_bar = tqdm(total=len(items_to_fetch), desc=f"Concurrent fetch ({currency})", unit="items")

        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                # Process in CHUNKS OF 8 with delays between chunks
                chunk_size = 8
                for chunk_start in range(0, len(items_to_fetch), chunk_size):
                    chunk_end = min(chunk_start + chunk_size, len(items_to_fetch))
                    chunk_items = items_to_fetch[chunk_start:chunk_end]

                    if chunk_start > 0:
                        # Wait between chunks to avoid overwhelming Steam
                        inter_chunk_delay = 4.5
                        print(f"\n⏸️  Waiting {inter_chunk_delay}s before next chunk...")
                        await asyncio.sleep(inter_chunk_delay)

                    print(f"\n📦 Processing chunk {chunk_start//chunk_size + 1}/{(len(items_to_fetch) + chunk_size - 1)//chunk_size} ({len(chunk_items)} items)")

                    tasks = [
                        fetch_steam_price_multi_source_async(session, item_name, semaphore, app_id, currency, max_retries=5)
                        for item_name in chunk_items
                    ]

                    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for item_name, result in zip(chunk_items, chunk_results):
                        if isinstance(result, Exception):
                            print(f"💥 Exception for {item_name[:30]}: {type(result).__name__}")
                            steam_data_map[item_name] = {
                                "current_price": None,
                                "sales_7d": 0,
                                "explosiveness": 0.0,
                                "source": "error",
                                "currency": currency,
                                "app_id": app_id
                            }
                        else:
                            steam_data_map[item_name] = result
                            if result.get("current_price"):
                                successful_fetches += 1

                        progress_bar.update(1)
                        if TQDM_AVAILABLE:
                            current_success_rate = (successful_fetches / (chunk_end)) * 100 if chunk_end > 0 else 0
                            progress_bar.set_postfix({
                                'Success': f'{successful_fetches}',
                                'Rate': f'{current_success_rate:.1f}%'
                            })
            finally:
                progress_bar.close()
                await connector.close()

        success_rate_final = (successful_fetches / len(items_to_fetch)) * 100 if items_to_fetch else 100
        print(f"\n✅ Concurrent Steam fetching complete!")
        print(f"💥 Success: {successful_fetches}/{len(items_to_fetch)} ({success_rate_final:.1f}%) in {currency}")
        print(f"📊 Total processed: {len(steam_data_map)} items ({cache_hit_rate:.1f}% from cache)")

        # Print diagnostic info
        print(f"\n📈 Diagnostic Summary:")
        print(f"   Direct API successes: {STEAM_SOURCES['steam_direct']['success_count']}")
        print(f"   Direct API errors: {STEAM_SOURCES['steam_direct']['error_count']}")
        print(f"   Render API successes: {STEAM_SOURCES['steam_render']['success_count']}")
        print(f"   Render API errors: {STEAM_SOURCES['steam_render']['error_count']}")

        return steam_data_map

def fetch_steam_price_direct_sync(skin_name: str, app_id: int = 730, currency: str = "USD") -> Dict[str, Any]:

    cache_key = _cache_key_for_steam("steam_direct", skin_name, currency, app_id)
    cached_data = load_cache(cache_key, STEAM_CACHE_TTL)
    if cached_data:
        return cached_data

    steam_data = {
        "current_price": None,
        "sales_7d": 0,
        "explosiveness": 0.0,
        "source": "steam_direct",
        "currency": currency,
        "app_id": app_id
    }

    try:
        time.sleep(STEAM_SOURCES["steam_direct"]["current_delay"] + random.uniform(0.1, 0.3))

        headers = random.choice(STEAM_HEADERS)
        url = STEAM_SOURCES["steam_direct"]["base_url"]
        params = {
            "appid": str(app_id),
            "market_hash_name": skin_name,
            "currency": str(utils.steam_currency_id)
        }

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=STEAM_SOURCES["steam_direct"]["timeout"]
        )

        if response.status_code in [429, 403]:
            adaptive_delay_adjustment("steam_direct", False, rate_limited=True)
            STEAM_SOURCES["steam_direct"]["error_count"] += 1
            return steam_data

        response.raise_for_status()
        data = response.json()

        if data.get("success") and data.get("lowest_price"):
            steam_price = clean_price_string(data["lowest_price"], currency)
            if steam_price:
                steam_data["current_price"] = steam_price

                if data.get("volume"):
                    volume_str = str(data["volume"]).replace(",", "").replace(".", "")
                    try:
                        steam_data["sales_7d"] = int(volume_str)
                    except:
                        steam_data["sales_7d"] = 0

                base_explosiveness = min(50.0, steam_price * 0.1)
                volume_factor = min(2.0, math.log10(1 + steam_data["sales_7d"]) / 2.0) if steam_data["sales_7d"] > 0 else 1.0
                steam_data["explosiveness"] = round(base_explosiveness * volume_factor, 2)

                adaptive_delay_adjustment("steam_direct", True)
                STEAM_SOURCES["steam_direct"]["success_count"] += 1
                save_cache(cache_key, steam_data)

    except Exception:
        STEAM_SOURCES["steam_direct"]["error_count"] += 1

    return steam_data


def batch_fetch_steam_prices_sync(item_names: List[str], app_id: int = 730, currency: str = "USD") -> Dict[str, Dict[str, Any]]:
    print(f"\n🐌 Fetching Steam prices sequentially in {currency} (App ID: {app_id})")
    print("💡 Install aiohttp for much faster concurrent fetching: pip install aiohttp")

    cached_data_direct, items_to_fetch = load_cache_batch(item_names, "steam_direct", currency, app_id)
    steam_data_map = cached_data_direct.copy()

    cache_hit_rate = ((len(item_names) - len(items_to_fetch)) / len(item_names)) * 100 if item_names else 0
    print(f"   Cache hit rate: {cache_hit_rate:.1f}%")

    if not items_to_fetch:
        return steam_data_map

    successful_fetches = 0

    if TQDM_AVAILABLE:
        progress_bar = tqdm(items_to_fetch, desc=f"Sequential Steam fetch ({currency})", unit="items")
    else:
        progress_bar = tqdm(total=len(items_to_fetch), desc=f"Sequential fetch ({currency})", unit="items")

    try:
        for i, item_name in enumerate(items_to_fetch if TQDM_AVAILABLE else range(len(items_to_fetch))):
            if not TQDM_AVAILABLE:
                item_name = items_to_fetch[i]

            progress_bar.update(1)

            result = fetch_steam_price_direct_sync(item_name, app_id, currency)
            steam_data_map[item_name] = result

            if result.get("current_price"):
                successful_fetches += 1

            if TQDM_AVAILABLE:
                progress_bar.set_postfix({'Success': successful_fetches})
    finally:
        progress_bar.close()

    success_rate = (successful_fetches / len(items_to_fetch)) * 100 if items_to_fetch else 100
    print(f"✅ Sequential fetching complete: {successful_fetches}/{len(items_to_fetch)} ({success_rate:.1f}%)")

    return steam_data_map
    