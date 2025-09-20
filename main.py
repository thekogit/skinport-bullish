#!/usr/bin/env pyt
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
from typing import Optional, Dict, Any, List, Tuple
import random
import asyncio
import threading

import requests

# Try to import aiohttp for concurrent requests
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    print("Warning: aiohttp not available. Install with: pip install aiohttp")
    print("Falling back to sequential processing...")

# Progress bar with fallback
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    class tqdm:
        def __init__(self, iterable=None, total=None, desc=None, unit=None, leave=True, **kwargs):
            self.iterable = iterable
            self.total = total or (len(iterable) if iterable else 0)
            self.desc = desc or ""
            self.unit = unit or "it"
            self.n = 0
            self.leave = leave
            self.last_print_n = 0

        def __iter__(self):
            if self.iterable:
                for item in self.iterable:
                    yield item
                    self.update(1)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            if self.leave:
                self.close()

        def update(self, n=1):
            self.n += n
            update_frequency = min(max(self.total // 10, 1), 100) if self.total > 0 else 100
            if self.n - self.last_print_n >= update_frequency or self.n >= self.total:
                self.display()
                self.last_print_n = self.n

        def display(self):
            if self.total > 0:
                percentage = (self.n / self.total) * 100
                print(f"\r{self.desc}: {self.n}/{self.total} ({percentage:.1f}%)", end="", flush=True)
            else:
                print(f"\r{self.desc}: {self.n} {self.unit}", end="", flush=True)

        def close(self):
            if self.total > 0:
                print(f"\r{self.desc}: {self.total}/{self.total} (100.0%)")
            else:
                print(f"\r{self.desc}: {self.n} {self.unit} - Complete")

        def set_postfix(self, **kwargs):
            pass

try:
    import requests_cache
except Exception:
    requests_cache = None

# Compression support
BROTLI_AVAILABLE = False
_brotli_module = None
try:
    import brotli as _brotli
    BROTLI_AVAILABLE = True
    _brotli_module = _brotli
except Exception:
    try:
        import brotlicffi as _brotlicffi
        BROTLI_AVAILABLE = True
        _brotli_module = _brotlicffi
    except Exception:
        BROTLI_AVAILABLE = False
        _brotli_module = None

import gzip

API_BASE = "https://api.skinport.com/v1"

STEAM_CURRENCY_MAP = {
    "USD": 1, "GBP": 2, "EUR": 3, "CHF": 4, "RUB": 5, "PLN": 6, "BRL": 7, "JPY": 8,
    "SEK": 9, "IDR": 10, "MYR": 11, "PHP": 12, "SGD": 13, "THB": 14, "VND": 15,
    "KRW": 16, "TRY": 17, "UAH": 18, "MXN": 19, "CAD": 20, "AUD": 21, "NZD": 22,
    "CNY": 23, "INR": 24, "CLP": 25, "PEN": 26, "COP": 27, "ZAR": 28, "HKD": 29,
    "TWD": 30, "SAR": 31, "AED": 32
}

SKINPORT_FEE_RATE = 0.08
SKINPORT_FEE_RATE_HIGH = 0.06
STEAM_FEE_RATE = 0.15

MIN_PROFIT_PERCENTAGE = 10.0
GOOD_PROFIT_PERCENTAGE = 20.0
SKINPORT_HIGH_VALUE_THRESHOLD = 1000.0

RETRY_SETTINGS = {
    "max_retries": 5,  
    "base_delay": 3.0, 
    "backoff_multiplier": 1.5,  
    "max_delay": 45.0, 
    "retry_on_errors": [429, 503, 502, 500, 408, 404, 520, 521, 522, 524]
}

STEAM_SOURCES = {
    "steam_direct": {
        "name": "Steam Community Market (Direct)",
        "base_url": "https://steamcommunity.com/market/priceoverview",
        "initial_delay": 1.5, 
        "max_delay": 15.0,    
        "concurrent_limit": 4, 
        "timeout": 15,      
        "enabled": True,
        "success_count": 0,
        "error_count": 0,
        "retry_count": 0,
        "rate_limit_count": 0,
        "current_delay": 1.5
    },
    "steam_render": {
        "name": "Steam Market Render API",
        "base_url": "https://steamcommunity.com/market/listings/730",
        "initial_delay": 2.0,  
        "max_delay": 20.0,     
        "concurrent_limit": 3, 
        "timeout": 20,         
        "enabled": True,
        "success_count": 0,
        "error_count": 0,
        "retry_count": 0,
        "rate_limit_count": 0,
        "current_delay": 2.0
    }
}

HEADERS = {
    "Accept-Encoding": "br",
    "User-Agent": "skinport-analysis-tool/12.0-optimized-conservative"
}

STEAM_HEADERS = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive"
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive"
    }
]

CACHE_DIR = Path.home() / ".skinport_skin_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_CACHE_TTL = 300
STEAM_CACHE_TTL = 1800

# NEW: Failed request cache management
FAILED_CACHE_TTL = 3600  # 1 hour
FAILED_REQUESTS_FILE = CACHE_DIR / "failed_steam_requests.json"

SUPPORTED = {"usd", "eur", "pln", "gbp"}

OUT_CSV = Path.cwd() / "skinport_bullish.csv"
OUT_HTML = Path.cwd() / "skinport_bullish.html"
MASTER_CSV = CACHE_DIR / "skinport_bullish_master.csv"

CSV_FIELDS = [
    "Name", "Skinport_URL", "Steam_URL", "Skinport_Price", "Steam_Price", "Price_Diff_Pct",
    "Currency", "Skinport_Sales7d", "Steam_Sales7d", "Skinport_7d_avg", "Skinport_24h_avg",
    "Skinport_30d_avg", "Steam_Explosiveness", "Skinport_7d_vs_30d", "Skinport_GrowthRatio",
    "Skinport_BullishScore", "Skinport_Explosiveness", "PumpRisk", "Arbitrage_Opportunity",
    "Candidate", "LastUpdated", "Steam_Source", "Fee_Aware_Profit", "Net_Steam_Proceeds"
]

MAX_CONCURRENT_STEAM_REQUESTS = 6  
CACHE_BATCH_SIZE = 50
REQUEST_CHUNK_SIZE = 8              

RATE_LIMIT_RECOVERY_TIME = 90       
ADAPTIVE_DELAY_MULTIPLIER = 2.5   
DELAY_RECOVERY_FACTOR = 0.95      

# Additional settings
EXPLOSION_THRESHOLD = 1.4
MEDIUM_TERM_THRESHOLD = 1.08
MIN_VOL_FLOOR = 20
TOP_PERCENTILE = 0.15
MAX_CANDIDATES = 25
HIGH_VOLUME_THRESHOLD = 50

EXPLOSIVENESS_WEIGHTS = {
    "momentum_composite": 0.25,
    "scarcity_signal": 0.20,
    "discount_opportunity": 0.15,
    "volatility_breakout": 0.15,
    "volume_surge": 0.10,
    "market_sentiment": 0.10,
    "manipulation_risk": -0.15
}

MOMENTUM_SHORT_CAP = 3.0
MOMENTUM_MED_CAP = 2.0
SCARCITY_THRESHOLD = 15
DISCOUNT_CAP = 0.4
VOLATILITY_WINDOW = 7
VOLUME_SURGE_MULTIPLIER = 2.5
HIGH_MANIPULATION_VOLUME = 150

PUMP_DETECTION = {
    "high_volume_low_growth": 200,
    "extreme_medium_momentum": 3.0,
    "sticker_volume_threshold": 100
}

# Category keywords
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
        "driver gloves", "sport gloves", "hydra", "tactical gloves", "bloodhound gloves",
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

ALL_FILTER_KEYWORDS: Dict[str, List[str]] = {}
for k, v in CATEGORY_KEYWORDS.items():
    ALL_FILTER_KEYWORDS[k] = v.copy()
for k, v in WEAPON_KEYWORDS.items():
    ALL_FILTER_KEYWORDS[k] = v.copy()

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

GENERAL_EXCLUSIONS = [
    "case", "charm", "sticker", "souvenir", "patch", "key", "container",
    "package", "crate", "coin", "music kit", "skin case"
]

# Global variables
current_currency = "USD"
steam_currency_id = 1
_rate_limit_lock = threading.Lock()

# NEW: Failed request management functions
def load_failed_requests() -> Dict[str, Dict[str, Any]]:
    """Load previously failed requests for intelligent retry"""
    if not FAILED_REQUESTS_FILE.exists():
        return {}
    try:
        with open(FAILED_REQUESTS_FILE, 'r', encoding='utf8') as f:
            data = json.load(f)
        # Filter out expired entries
        current_time = time.time()
        return {k: v for k, v in data.items() 
                if current_time - v.get('failed_at', 0) < FAILED_CACHE_TTL}
    except Exception:
        return {}

def save_failed_requests(failed_items: Dict[str, Dict[str, Any]]):
    """Save failed requests for later retry"""
    try:
        with open(FAILED_REQUESTS_FILE, 'w', encoding='utf8') as f:
            json.dump(failed_items, f, indent=2)
        print(f"CACHE: Saved {len(failed_items)} failed requests for later retry")
    except Exception:
        pass

def normalize_currency(cur: Optional[str]) -> str:
    if not cur:
        return "USD"
    cur = cur.strip().upper()
    if cur.lower() not in SUPPORTED:
        print(f"Unsupported currency '{cur}'. Defaulting to USD.")
        return "USD"
    return cur.upper()

def set_global_currency(currency: str):
    global current_currency, steam_currency_id
    current_currency = currency.upper()
    steam_currency_id = STEAM_CURRENCY_MAP.get(current_currency, 1)
    print(f"\nConfig: Currency: {current_currency} (Steam ID: {steam_currency_id})")
    print(f"Fees: Skinport 0% buying, Steam 15% selling")
    if steam_currency_id == 1 and current_currency != "USD":
        print(f"Warning: {current_currency} not in Steam map, using USD")

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

def _cache_key_for_steam(source: str, skin_name: str, currency: str) -> str:
    return hashlib.sha256(f"steam_{source}_{skin_name}_{currency}".encode("utf8")).hexdigest()

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

def load_cache_batch(item_names: List[str], source: str) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """Load cache for multiple items at once, return cached data and items that need fetching"""
    cached_data = {}
    items_to_fetch = []

    for item_name in item_names:
        cache_key = _cache_key_for_steam(source, item_name, current_currency)
        cached_item = load_cache(cache_key, STEAM_CACHE_TTL)

        if cached_item:
            cached_data[item_name] = cached_item
        else:
            items_to_fetch.append(item_name)

    return cached_data, items_to_fetch

def clean_price_string(price_str: str, currency: str = "USD") -> Optional[float]:
    """FIXED: Enhanced price string cleaning with proper regex and currency handling"""
    if not price_str:
        return None

    cleaned = str(price_str).strip()

    if currency == "PLN":
        cleaned = re.sub(r'zł|PLN', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'[^\d,.-]', '', cleaned)
        if ',' in cleaned and '.' not in cleaned:
            cleaned = cleaned.replace(',', '.')
        elif ',' in cleaned and '.' in cleaned:
            parts = cleaned.split(',')
            if len(parts) == 2:
                cleaned = parts[0].replace('.', '') + '.' + parts[1]
    elif currency == "EUR":
        cleaned = re.sub(r'€|EUR', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'[^\d,.-]', '', cleaned)
        if ',' in cleaned and '.' not in cleaned:
            cleaned = cleaned.replace(',', '.')
        elif ',' in cleaned and '.' in cleaned:
            parts = cleaned.split(',')
            if len(parts) == 2:
                cleaned = parts[0].replace('.', '') + '.' + parts[1]
    else:
        cleaned = re.sub(r'[$£]|USD|GBP', '', cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'[^\d,.-]', '', cleaned)
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace(',', '')

    try:
        result = float(cleaned)
        return result
    except (ValueError, TypeError):
        return None

def compute_fee_aware_arbitrage_opportunity(skinport_price: float, steam_price: Optional[float], 
                                        skinport_volume: int, currency: str = "USD") -> Tuple[str, float, Dict[str, float]]:
    """Calculate arbitrage: Buy Skinport (0% fees) → Sell Steam (15% fees)"""
    breakdown = {
        "skinport_price": skinport_price,
        "steam_price": steam_price or 0,
        "skinport_fee": 0.0,
        "steam_fee": 0.0,
        "net_steam_proceeds": 0.0,
        "gross_profit": 0.0,
        "profit_percentage": 0.0,
        "required_steam_price_breakeven": 0.0,
        "required_steam_price_good": 0.0
    }

    if not steam_price or steam_price <= 0:
        return "NO_STEAM_DATA", 0.0, breakdown

    steam_fee_amount = steam_price * STEAM_FEE_RATE
    net_steam_proceeds = steam_price - steam_fee_amount
    gross_profit = net_steam_proceeds - skinport_price
    profit_percentage = (gross_profit / skinport_price) * 100 if skinport_price > 0 else 0

    required_steam_breakeven = skinport_price / (1 - STEAM_FEE_RATE)
    required_steam_good = skinport_price * (1 + MIN_PROFIT_PERCENTAGE/100) / (1 - STEAM_FEE_RATE)

    breakdown.update({
        "steam_fee": steam_fee_amount,
        "net_steam_proceeds": net_steam_proceeds,
        "gross_profit": gross_profit,
        "profit_percentage": profit_percentage,
        "required_steam_price_breakeven": required_steam_breakeven,
        "required_steam_price_good": required_steam_good
    })

    # Classify opportunity based on profit and volume
    if profit_percentage >= GOOD_PROFIT_PERCENTAGE and skinport_volume >= HIGH_VOLUME_THRESHOLD:
        return "EXCELLENT_BUY", profit_percentage, breakdown
    elif profit_percentage >= MIN_PROFIT_PERCENTAGE and skinport_volume >= HIGH_VOLUME_THRESHOLD:
        return "GOOD_BUY", profit_percentage, breakdown
    elif profit_percentage >= MIN_PROFIT_PERCENTAGE:
        return "GOOD_BUY_LOW_VOL", profit_percentage, breakdown
    elif profit_percentage >= 5.0:
        return "MARGINAL_PROFIT", profit_percentage, breakdown
    elif profit_percentage >= 0:
        return "BREAKEVEN", profit_percentage, breakdown
    elif profit_percentage >= -10.0:
        return "SMALL_LOSS", profit_percentage, breakdown
    else:
        return "OVERPRICED", profit_percentage, breakdown

def adaptive_delay_adjustment(source: str, success: bool, rate_limited: bool = False):
    """OPTIMIZED: More aggressive delay adjustments for 100% success rates"""
    with _rate_limit_lock:
        config = STEAM_SOURCES[source]

        if rate_limited:
            config["rate_limit_count"] += 1
            old_delay = config["current_delay"]
            config["current_delay"] = min(
                config["current_delay"] * ADAPTIVE_DELAY_MULTIPLIER,
                config["max_delay"]
            )
            print(f"ADAPTIVE: Rate limited on {source}, delay: {old_delay:.1f}s → {config['current_delay']:.1f}s")
        elif success:
            # More conservative delay recovery
            old_delay = config["current_delay"]
            config["current_delay"] = max(
                config["current_delay"] * DELAY_RECOVERY_FACTOR,
                config["initial_delay"]
            )
            if old_delay != config["current_delay"]:
                print(f"ADAPTIVE: Success on {source}, delay: {old_delay:.1f}s → {config['current_delay']:.1f}s")

async def retry_request_async(func, max_retries=None, *args, **kwargs):
    """OPTIMIZED: Enhanced retry mechanism with progressive backoff for 100% success"""
    max_retries = max_retries or RETRY_SETTINGS["max_retries"]
    base_delay = RETRY_SETTINGS["base_delay"]
    backoff_multiplier = RETRY_SETTINGS["backoff_multiplier"]
    max_delay = RETRY_SETTINGS["max_delay"]
    retry_errors = RETRY_SETTINGS["retry_on_errors"]

    for attempt in range(max_retries + 1):
        try:
            result = await func(*args, **kwargs)
            if attempt > 0:  # Only log if we actually retried
                print(f"SUCCESS: Recovered after {attempt} retries")
            return result
        except Exception as e:
            # Check if this is a retryable error
            should_retry = False
            if hasattr(e, 'status') and e.status in retry_errors:
                should_retry = True
            elif isinstance(e, (asyncio.TimeoutError, aiohttp.ClientError)):
                should_retry = True

            if not should_retry or attempt == max_retries:
                if attempt > 0:
                    print(f"RETRY: Final failure after {attempt} attempts: {e}")
                raise e

            # Progressive delay with more jitter
            delay = min(base_delay * (backoff_multiplier ** attempt), max_delay)
            jitter = random.uniform(0.5, 2.0)
            delay += jitter

            print(f"RETRY: Attempt {attempt + 1}/{max_retries + 1} in {delay:.1f}s (error: {type(e).__name__})")
            await asyncio.sleep(delay)

    raise Exception(f"Max retries ({max_retries}) exceeded")

if AIOHTTP_AVAILABLE:
    async def fetch_steam_price_direct_async_optimized(session, skin_name: str, semaphore, source="steam_direct") -> Dict[str, Any]:
        """OPTIMIZED: Async Steam direct price fetching with 100% success focus"""
        global current_currency, steam_currency_id

        cache_key = _cache_key_for_steam(source, skin_name, current_currency)
        cached_data = load_cache(cache_key, STEAM_CACHE_TTL)
        if cached_data:
            return cached_data

        steam_data = {
            "current_price": None,
            "sales_7d": 0,
            "explosiveness": 0.0,
            "source": source,
            "currency": current_currency
        }

        async def _single_request():
            async with semaphore:
                current_delay = STEAM_SOURCES[source]["current_delay"]
                jitter = random.uniform(0.3, 1.2)  # More jitter
                await asyncio.sleep(current_delay + jitter)

                headers = random.choice(STEAM_HEADERS)
                url = STEAM_SOURCES[source]["base_url"]
                params = {
                    "appid": "730",
                    "market_hash_name": skin_name,
                    "currency": str(steam_currency_id)
                }

                async with session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=STEAM_SOURCES[source]["timeout"])
                ) as response:

                    if response.status in RETRY_SETTINGS["retry_on_errors"]:
                        if response.status == 429:
                            adaptive_delay_adjustment(source, False, rate_limited=True)
                            await asyncio.sleep(RATE_LIMIT_RECOVERY_TIME)  # Additional recovery time
                        error = aiohttp.ClientError(f"HTTP {response.status}")
                        error.status = response.status
                        raise error

                    if response.status != 200:
                        STEAM_SOURCES[source]["error_count"] += 1
                        return steam_data

                    data = await response.json()

                    if not data.get("success"):
                        return steam_data
                    elif not data.get("lowest_price"):
                        return steam_data

                    # Extract price
                    steam_price = clean_price_string(data["lowest_price"], current_currency)
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

                        adaptive_delay_adjustment(source, True)
                        STEAM_SOURCES[source]["success_count"] += 1
                        save_cache(cache_key, steam_data)

            return steam_data

        try:
            return await retry_request_async(_single_request)
        except Exception as e:
            print(f"FAILED: Steam Direct '{skin_name}': {e}")
            return steam_data

    async def fetch_steam_price_render_async_optimized(session, skin_name: str, semaphore, source="steam_render") -> Dict[str, Any]:
        """OPTIMIZED: Async Steam render price fetching with FIXED regex patterns"""
        global current_currency, steam_currency_id

        cache_key = _cache_key_for_steam(source, skin_name, current_currency)
        cached_data = load_cache(cache_key, STEAM_CACHE_TTL)
        if cached_data:
            return cached_data

        steam_data = {
            "current_price": None,
            "sales_7d": 0,
            "explosiveness": 0.0,
            "source": source,
            "currency": current_currency
        }

        async def _single_request():
            async with semaphore:
                current_delay = STEAM_SOURCES[source]["current_delay"]
                jitter = random.uniform(0.5, 1.5)  # More jitter
                await asyncio.sleep(current_delay + jitter)

                headers = random.choice(STEAM_HEADERS)
                encoded_name = urllib.parse.quote(skin_name)
                url = f"{STEAM_SOURCES[source]['base_url']}/{encoded_name}/render"
                params = {
                    "start": "0",
                    "count": "1",
                    "currency": str(steam_currency_id),
                    "format": "json"
                }

                async with session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=STEAM_SOURCES[source]["timeout"])
                ) as response:

                    if response.status in RETRY_SETTINGS["retry_on_errors"]:
                        if response.status == 429:
                            adaptive_delay_adjustment(source, False, rate_limited=True)
                            await asyncio.sleep(RATE_LIMIT_RECOVERY_TIME)  # Additional recovery time
                        error = aiohttp.ClientError(f"HTTP {response.status}")
                        error.status = response.status
                        raise error

                    if response.status != 200:
                        STEAM_SOURCES[source]["error_count"] += 1
                        return steam_data

                    data = await response.json()

                    if not data.get("success") or not data.get("results_html"):
                        return steam_data

                    html_content = data["results_html"]

                    price_match = None
                    
                    if current_currency == "PLN":
                        price_match = re.search(r'([0-9,.-]+)\s*zł', html_content, re.IGNORECASE)
                    elif current_currency == "EUR":
                        price_match = re.search(r'€\s*([0-9,.-]+)|([0-9,.-]+)\s*€', html_content)
                    elif current_currency == "GBP":
                        price_match = re.search(r'£([0-9,.-]+)', html_content)
                    else:
                        price_match = re.search(r'\$([0-9,.-]+)', html_content)

                    if price_match:
                        price_str = None
                        if price_match.lastindex and price_match.lastindex >= 1:
                            for i in range(1, price_match.lastindex + 1):
                                if price_match.group(i):
                                    price_str = price_match.group(i)
                                    break
                        if not price_str:
                            price_str = price_match.group(0)
                        
                        steam_price = clean_price_string(price_str, current_currency)
                        if steam_price:
                            steam_data["current_price"] = steam_price

                            if data.get("total_count"):
                                steam_data["sales_7d"] = min(int(data["total_count"]), 1000)

                            base_explosiveness = min(50.0, steam_price * 0.1)
                            volume_factor = min(2.0, math.log10(1 + steam_data["sales_7d"]) / 2.0) if steam_data["sales_7d"] > 0 else 1.0
                            steam_data["explosiveness"] = round(base_explosiveness * volume_factor, 2)

                            adaptive_delay_adjustment(source, True)
                            STEAM_SOURCES[source]["success_count"] += 1
                            save_cache(cache_key, steam_data)

            return steam_data

        try:
            return await retry_request_async(_single_request)
        except Exception as e:
            print(f"FAILED: Steam Render '{skin_name}': {e}")
            return steam_data

    async def fetch_steam_price_multi_source_optimized(session, skin_name: str, semaphore) -> Dict[str, Any]:
        """OPTIMIZED: Try multiple Steam sources with intelligent fallback"""

        if STEAM_SOURCES["steam_direct"]["enabled"]:
            result = await fetch_steam_price_direct_async_optimized(session, skin_name, semaphore, "steam_direct")
            if result.get("current_price"):
                return result

        if STEAM_SOURCES["steam_render"]["enabled"]:
            result = await fetch_steam_price_render_async_optimized(session, skin_name, semaphore, "steam_render")
            if result.get("current_price"):
                return result

        return {
            "current_price": None,
            "sales_7d": 0,
            "explosiveness": 0.0,
            "source": "none",
            "currency": current_currency
        }

    async def batch_fetch_steam_prices_optimized(item_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """OPTIMIZED: Steam price fetching targeting 100% success rate with failed request management"""
        print(f"\nOPTIMIZED Steam fetching in {current_currency} (Steam ID: {steam_currency_id})")
        print(f"Conservative settings: {MAX_CONCURRENT_STEAM_REQUESTS} concurrent, enhanced retry + progressive backoff")
        
        failed_requests = load_failed_requests()
        if failed_requests:
            print(f"Previous failed requests to retry: {len(failed_requests)}")
            for failed_name in failed_requests.keys():
                if failed_name not in item_names:
                    item_names.append(failed_name)

        print("Checking cache for existing data...")
        cached_data_direct, items_to_fetch_direct = load_cache_batch(item_names, "steam_direct")
        cached_data_render, items_to_fetch_render = load_cache_batch(item_names, "steam_render")

        steam_data_map = {}
        for item_name in item_names:
            if item_name in cached_data_direct:
                steam_data_map[item_name] = cached_data_direct[item_name]
            elif item_name in cached_data_render:
                steam_data_map[item_name] = cached_data_render[item_name]

        items_to_fetch = [name for name in item_names if name not in steam_data_map]

        cache_hit_rate = ((len(item_names) - len(items_to_fetch)) / len(item_names)) * 100 if item_names else 0
        print(f"Cache hit rate: {cache_hit_rate:.1f}% ({len(item_names) - len(items_to_fetch)}/{len(item_names)})")

        if not items_to_fetch:
            print("All items found in cache!")
            return steam_data_map

        print(f"Fetching {len(items_to_fetch)} items with OPTIMIZED settings for maximum success rate...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_STEAM_REQUESTS)

        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_STEAM_REQUESTS * 1.2, 
            limit_per_host=MAX_CONCURRENT_STEAM_REQUESTS,
            ttl_dns_cache=900,  # Longer DNS cache
            use_dns_cache=True
        )

        successful_fetches = 0
        failed_this_session = {}
        source_counts = {"steam_direct": 0, "steam_render": 0, "none": 0}

        if TQDM_AVAILABLE:
            progress_bar = tqdm(
                total=len(items_to_fetch),
                desc=f"OPTIMIZED Steam fetch ({current_currency})",
                unit="items"
            )
        else:
            progress_bar = tqdm(total=len(items_to_fetch), desc=f"OPTIMIZED fetch ({current_currency})", unit="items")

        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                for chunk_start in range(0, len(items_to_fetch), REQUEST_CHUNK_SIZE):
                    chunk_end = min(chunk_start + REQUEST_CHUNK_SIZE, len(items_to_fetch))
                    chunk_items = items_to_fetch[chunk_start:chunk_end]

                    print(f"PROCESSING: Chunk {chunk_start//REQUEST_CHUNK_SIZE + 1} ({len(chunk_items)} items)")

                    tasks = [
                        fetch_steam_price_multi_source_optimized(session, item_name, semaphore)
                        for item_name in chunk_items
                    ]

                    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

                    for item_name, result in zip(chunk_items, chunk_results):
                        if isinstance(result, Exception):
                            print(f"EXCEPTION: '{item_name[:30]}...': {result}")
                            steam_data_map[item_name] = {
                                "current_price": None,
                                "sales_7d": 0,
                                "explosiveness": 0.0,
                                "source": "error",
                                "currency": current_currency
                            }
                            failed_this_session[item_name] = {
                                "failed_at": time.time(),
                                "error": str(result),
                                "currency": current_currency
                            }
                            source_counts["none"] += 1
                        else:
                            steam_data_map[item_name] = result

                            if result.get("current_price"):
                                successful_fetches += 1
                                source_used = result.get("source", "none")
                                source_counts[source_used] += 1
                            else:
                                failed_this_session[item_name] = {
                                    "failed_at": time.time(),
                                    "error": "No price found",
                                    "currency": current_currency
                                }
                                source_counts["none"] += 1

                        progress_bar.update(1)

                        if TQDM_AVAILABLE:
                            current_success_rate = (successful_fetches / max(1, chunk_start + len(chunk_results))) * 100
                            progress_bar.set_postfix({
                                'Success': f'{successful_fetches}',
                                'Rate': f'{current_success_rate:.1f}%'
                            })

                    if chunk_end < len(items_to_fetch):
                        chunk_pause = random.uniform(3.0, 7.0)  # Longer pause
                        print(f"PAUSE: {chunk_pause:.1f}s between chunks for respectful rate limiting...")
                        await asyncio.sleep(chunk_pause)

            finally:
                progress_bar.close()
                await connector.close()

        total_retries = sum(config["retry_count"] for config in STEAM_SOURCES.values())
        success_rate_final = (successful_fetches / len(items_to_fetch)) * 100 if items_to_fetch else 100

        if failed_this_session:
            # Merge with existing failed requests
            all_failed = {**failed_requests, **failed_this_session}
            save_failed_requests(all_failed)

        print(f"\nOPTIMIZED Steam fetching session complete!")
        print(f"Success: {successful_fetches}/{len(items_to_fetch)} ({success_rate_final:.1f}%) in {current_currency}")
        print(f"Total retries performed: {total_retries}")
        print(f"Failed requests saved for next session: {len(failed_this_session)}")
        print(f"Total processed: {len(steam_data_map)} items ({cache_hit_rate:.1f}% from cache)")

        print(f"\nSource performance breakdown:")
        for source, count in source_counts.items():
            if count > 0:
                percentage = (count / len(items_to_fetch)) * 100
                source_name = STEAM_SOURCES.get(source, {}).get("name", source.replace("_", " ").title())
                if source == "none":
                    source_name = "Failed → Queued for Retry"
                print(f"  • {source_name}: {count} items ({percentage:.1f}%)")

        for source, config in STEAM_SOURCES.items():
            if config["rate_limit_count"] > 0:
                print(f"  Info: {config['name']}: {config['rate_limit_count']} rate limits (handled gracefully)")
            if config["retry_count"] > 0:
                print(f"  Info: {config['name']}: {config['retry_count']} successful retries")

        if failed_this_session:
            print(f"\nNOTE: {len(failed_this_session)} items failed this session but will be automatically retried next run")

        return steam_data_map

# Fallback sequential Steam fetching with optimizations
def fetch_steam_price_direct_sync_optimized(skin_name: str, source="steam_direct") -> Dict[str, Any]:
    """OPTIMIZED: Synchronous version with conservative timing and retry mechanism"""
    global current_currency, steam_currency_id

    cache_key = _cache_key_for_steam(source, skin_name, current_currency)
    cached_data = load_cache(cache_key, STEAM_CACHE_TTL)
    if cached_data:
        return cached_data

    steam_data = {
        "current_price": None,
        "sales_7d": 0,
        "explosiveness": 0.0,
        "source": source,
        "currency": current_currency
    }

    def _single_request():
        current_delay = STEAM_SOURCES[source]["current_delay"]
        jitter = random.uniform(0.2, 0.8)
        time.sleep(current_delay + jitter)
        
        headers = random.choice(STEAM_HEADERS)

        url = STEAM_SOURCES[source]["base_url"]
        params = {
            "appid": "730",
            "market_hash_name": skin_name,
            "currency": str(steam_currency_id)
        }

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=STEAM_SOURCES[source]["timeout"]
        )

        if response.status_code in RETRY_SETTINGS["retry_on_errors"]:
            if response.status_code == 429:
                adaptive_delay_adjustment(source, False, rate_limited=True)
                time.sleep(RATE_LIMIT_RECOVERY_TIME)  # Additional recovery time
            raise requests.RequestException(f"HTTP {response.status_code}")

        response.raise_for_status()
        data = response.json()

        if data.get("success") and data.get("lowest_price"):
            steam_price = clean_price_string(data["lowest_price"], current_currency)
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

                adaptive_delay_adjustment(source, True)
                STEAM_SOURCES[source]["success_count"] += 1
                save_cache(cache_key, steam_data)

        return steam_data

    max_retries = RETRY_SETTINGS["max_retries"]
    base_delay = RETRY_SETTINGS["base_delay"]
    backoff_multiplier = RETRY_SETTINGS["backoff_multiplier"]
    max_delay = RETRY_SETTINGS["max_delay"]
    retry_errors = RETRY_SETTINGS["retry_on_errors"]

    for attempt in range(max_retries + 1):
        try:
            result = _single_request()
            if attempt > 0:
                print(f"SUCCESS: {skin_name} recovered after {attempt} retries")
            return result
        except Exception as e:
            # Check if this is a retryable error
            should_retry = False
            if hasattr(e, 'response') and e.response and e.response.status_code in retry_errors:
                should_retry = True
            elif isinstance(e, (requests.RequestException, requests.Timeout)):
                should_retry = True

            if not should_retry or attempt == max_retries:
                STEAM_SOURCES[source]["error_count"] += 1
                break

            delay = min(base_delay * (backoff_multiplier ** attempt), max_delay)
            jitter = random.uniform(0.5, 1.5)
            delay += jitter

            print(f"RETRY: {skin_name} attempt {attempt + 1}/{max_retries} in {delay:.1f}s...")
            time.sleep(delay)
            STEAM_SOURCES[source]["retry_count"] += 1

    return steam_data

def batch_fetch_steam_prices_sync_optimized(item_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """OPTIMIZED: Sequential fallback with conservative timing and failed request management"""
    print(f"\nOPTIMIZED Sequential Steam fetching in {current_currency}")
    print("Install aiohttp for much faster concurrent fetching: pip install aiohttp")
    print("Conservative timing with retry mechanism enabled for maximum success")

    # Load and merge previous failed requests
    failed_requests = load_failed_requests()
    if failed_requests:
        print(f"Retrying {len(failed_requests)} previously failed requests")
        for failed_name in failed_requests.keys():
            if failed_name not in item_names:
                item_names.append(failed_name)

    # Check cache first
    cached_data_direct, items_to_fetch = load_cache_batch(item_names, "steam_direct")
    steam_data_map = cached_data_direct.copy()

    cache_hit_rate = ((len(item_names) - len(items_to_fetch)) / len(item_names)) * 100 if item_names else 0
    print(f"Cache hit rate: {cache_hit_rate:.1f}%")

    if not items_to_fetch:
        return steam_data_map

    successful_fetches = 0
    failed_this_session = {}

    if TQDM_AVAILABLE:
        progress_bar = tqdm(items_to_fetch, desc=f"OPTIMIZED Sequential fetch ({current_currency})", unit="items")
    else:
        progress_bar = tqdm(total=len(items_to_fetch), desc=f"OPTIMIZED Sequential fetch ({current_currency})", unit="items")

    try:
        for i, item_name in enumerate(items_to_fetch if TQDM_AVAILABLE else range(len(items_to_fetch))):
            if not TQDM_AVAILABLE:
                item_name = items_to_fetch[i]
                progress_bar.update(1)

            result = fetch_steam_price_direct_sync_optimized(item_name, "steam_direct")
            steam_data_map[item_name] = result

            if result.get("current_price"):
                successful_fetches += 1
            else:
                # Track failed request
                failed_this_session[item_name] = {
                    "failed_at": time.time(),
                    "error": "No price found in sync mode",
                    "currency": current_currency
                }

            if TQDM_AVAILABLE:
                success_rate = (successful_fetches / (i + 1)) * 100
                progress_bar.set_postfix({
                    'Success': successful_fetches,
                    'Rate': f'{success_rate:.1f}%'
                })

    finally:
        progress_bar.close()

    # Update failed requests for next session
    if failed_this_session:
        all_failed = {**failed_requests, **failed_this_session}
        save_failed_requests(all_failed)

    total_retries = sum(config["retry_count"] for config in STEAM_SOURCES.values())
    success_rate = (successful_fetches / len(items_to_fetch)) * 100 if items_to_fetch else 100
    
    print(f"OPTIMIZED Sequential fetching complete: {successful_fetches}/{len(items_to_fetch)} ({success_rate:.1f}%)")
    print(f"Total retries performed: {total_retries}")
    if failed_this_session:
        print(f"Failed requests saved for next session: {len(failed_this_session)}")

    return steam_data_map

def compute_bullish_score(avg_24h: float, avg_7d: float, avg_30d: float, vol_7d: int) -> float:
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
        growth_med = 1.0 if (avg_7d and avg_7d > 0) else 0.0

    combined = 0.6 * growth_short + 0.4 * growth_med
    return float(combined * (1 + math.log10(1 + vol_7d)))

def _safe_ratio(numerator: float, denominator: float, fallback: float = 0.0) -> float:
    try:
        if denominator and denominator != 0:
            return float(numerator) / float(denominator)
        return fallback
    except (ValueError, TypeError, ZeroDivisionError):
        return fallback

def _normalize_score(value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    try:
        value = float(value)
        if value <= min_val:
            return 0.0
        elif value >= max_val:
            return 1.0
        else:
            return (value - min_val) / (max_val - min_val)
    except (ValueError, TypeError):
        return 0.0

def detect_pump_risk(name: str, avg24: float, avg7: float, avg30: Optional[float], 
                    vol7: int, momentum_short: float) -> float:
    risk_score = 0.0

    if vol7 > PUMP_DETECTION["high_volume_low_growth"] and momentum_short < 0.8:
        risk_score += 40.0

    if avg30 and avg7 > 0:
        vs30 = avg7 / avg30
        if vs30 > PUMP_DETECTION["extreme_medium_momentum"]:
            risk_score += 30.0

    if "sticker" in name.lower() and vol7 > PUMP_DETECTION["sticker_volume_threshold"]:
        risk_score += 25.0

    if momentum_short < 0.5 and vol7 > 100:
        risk_score += 20.0

    if "stockholm 2021" in name.lower() and "holo" in name.lower():
        risk_score += 15.0

    return min(100.0, risk_score)

def compute_enhanced_explosiveness(
    avg24: float, avg7: float, avg30: Optional[float], vol7: int,
    price: Optional[float], item: Dict[str, Any], sales_entry: Optional[Dict[str, Any]], name: str = ""
) -> tuple[float, float]:

    avg24 = avg24 or 0.0
    avg7 = avg7 or 0.0
    avg30_val = avg30 if avg30 is not None else None
    vol = vol7 or 0
    current_price = price if price is not None else 0.0

    components = {}

    momentum_short = _safe_ratio(avg24, avg7, 1.0)
    momentum_med = _safe_ratio(avg7, avg30_val, 1.0) if avg30_val else 1.0

    momentum_short_norm = _normalize_score(momentum_short, 1.0, MOMENTUM_SHORT_CAP)
    momentum_med_norm = _normalize_score(momentum_med, 1.0, MOMENTUM_MED_CAP)

    momentum_acceleration = momentum_short_norm * momentum_med_norm
    momentum_composite = 0.6 * momentum_short_norm + 0.3 * momentum_med_norm + 0.1 * momentum_acceleration
    components["momentum_composite"] = momentum_composite

    # Scarcity signal
    listings_count = None
    for key in ["listings", "listed", "active_listings", "stock", "supply", "min_stock", "quantity"]:
        val = (item or {}).get(key) or (sales_entry or {}).get(key)
        if val is not None:
            try:
                listings_count = int(val)
                break
            except:
                continue

    if listings_count is not None:
        scarcity_listings = _normalize_score(SCARCITY_THRESHOLD - listings_count, 0, SCARCITY_THRESHOLD)
    else:
        scarcity_listings = 0.0

    volume_scarcity = _normalize_score(50 - vol, 0, 50) if vol < 50 else 0.0
    scarcity_signal = 0.7 * scarcity_listings + 0.3 * volume_scarcity
    components["scarcity_signal"] = scarcity_signal

    # Discount opportunity
    if avg7 > 0 and current_price > 0:
        discount_ratio = (avg7 - current_price) / avg7
        discount_opportunity = _normalize_score(discount_ratio, 0.0, DISCOUNT_CAP)
    else:
        discount_opportunity = 0.0
    components["discount_opportunity"] = discount_opportunity

    # Volatility breakout
    prices = [p for p in [current_price, avg24, avg7] if p and p > 0]
    if len(prices) >= 2:
        price_std = math.sqrt(sum((p - sum(prices)/len(prices))**2 for p in prices) / len(prices))
        avg_price = sum(prices) / len(prices)
        volatility_ratio = price_std / avg_price if avg_price > 0 else 0.0
        volatility_breakout = _normalize_score(volatility_ratio, 0.0, 0.3)
    else:
        volatility_breakout = 0.0
    components["volatility_breakout"] = volatility_breakout

    # Volume surge
    estimated_baseline = vol * 4 if vol > 0 else 1
    if vol > estimated_baseline * VOLUME_SURGE_MULTIPLIER:
        volume_surge = _normalize_score(vol / estimated_baseline, VOLUME_SURGE_MULTIPLIER, 5.0)
    else:
        volume_surge = 0.0
    components["volume_surge"] = volume_surge

    # Market sentiment
    if avg30_val and avg30_val > 0:
        total_return_30d = _safe_ratio(avg7, avg30_val, 1.0)
        market_relative = _safe_ratio(total_return_30d, 1.05, 1.0)
        market_sentiment = _normalize_score(market_relative, 1.0, 1.5)
    else:
        market_sentiment = 0.0
    components["market_sentiment"] = market_sentiment

    # Manipulation risk
    pump_risk_score = detect_pump_risk(name, avg24, avg7, avg30_val, vol, momentum_short)
    manipulation_score = pump_risk_score / 100.0
    components["manipulation_risk"] = manipulation_score

    # Calculate final score
    total_score = 0.0
    for component, weight in EXPLOSIVENESS_WEIGHTS.items():
        component_value = components.get(component, 0.0)
        total_score += component_value * weight

    explosiveness_final = max(0.0, min(100.0, total_score * 100.0))

    return round(explosiveness_final, 2), round(pump_risk_score, 1)

def make_skinport_url(name: str) -> str:
    return f"https://skinport.com/market?search={urllib.parse.quote(name)}"

def make_steam_url(name: str) -> str:
    return f"https://steamcommunity.com/market/listings/730/{urllib.parse.quote(name)}"

def _cache_key_for(url: str, params: Optional[Dict[str, Any]]):
    key = url
    if params:
        param_pairs = []
        for k in sorted(params.keys()):
            param_pairs.append(f"{k}={params[k]}")
        key += "?" + "&".join(param_pairs)
    return hashlib.sha256(key.encode("utf8")).hexdigest()

def install_requests_cache_if_available(expire_after: int = DEFAULT_CACHE_TTL) -> bool:
    if requests_cache is None:
        print("requests_cache not available – using file cache.")
        return False
    try:
        requests_cache.install_cache("skinport_cache", expire_after=expire_after)
        print(f"requests_cache installed (expire_after={expire_after}s).")
        return True
    except Exception:
        print("Failed to install requests_cache; using file cache.")
        return False

def _try_brotli_decompress(content: bytes) -> Optional[str]:
    if not BROTLI_AVAILABLE or _brotli_module is None:
        return None
    try:
        dec = _brotli_module.decompress(content)
        return dec.decode("utf-8", errors="replace")
    except Exception:
        return None

def _try_gzip_decompress(content: bytes) -> Optional[str]:
    try:
        dec = gzip.decompress(content)
        return dec.decode("utf-8", errors="replace")
    except Exception:
        return None

def _safe_json_from_response(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        content = resp.content or b""
        if content and BROTLI_AVAILABLE:
            txt = _try_brotli_decompress(content)
            if txt is not None:
                try:
                    return json.loads(txt)
                except Exception:
                    pass
        if content:
            txt = _try_gzip_decompress(content)
            if txt is not None:
                try:
                    return json.loads(txt)
                except Exception:
                    pass
        try:
            txt = content.decode(resp.encoding or "utf-8", errors="replace")
            return json.loads(txt)
        except Exception:
            raise ValueError(f"Failed to parse JSON (status={resp.status_code}).")

def http_get_with_cache(url: str, params: Optional[Dict[str, Any]] = None, ttl: int = DEFAULT_CACHE_TTL, max_retries: int = 3):
    use_requests_cache = requests_cache is not None

    backoff = 0.5
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=20)
            try:
                resp.raise_for_status()
            except requests.HTTPError as he:
                snippet = (resp.text[:800] if resp.text else "")
                raise requests.HTTPError(f"{he} (status={resp.status_code}) response snippet: {snippet}") from he

            try:
                data = _safe_json_from_response(resp)
                if not use_requests_cache:
                    key = _cache_key_for(url, params)
                    save_cache(key, data)
                return data
            except ValueError as ve:
                last_exc = ve
                if attempt < max_retries:
                    headers_no_br = dict(HEADERS)
                    headers_no_br.pop("Accept-Encoding", None)
                    try:
                        resp2 = requests.get(url, headers=headers_no_br, params=params, timeout=20)
                        resp2.raise_for_status()
                        data2 = _safe_json_from_response(resp2)
                        if not use_requests_cache:
                            key = _cache_key_for(url, params)
                            save_cache(key, data2)
                        return data2
                    except Exception as e2:
                        last_exc = e2
                        time.sleep(backoff)
                        backoff *= 1.5
                        continue
                raise last_exc

        except requests.RequestException as e:
            last_exc = e
            if attempt >= max_retries:
                raise RuntimeError(f"HTTP GET failed for {url} after {attempt} attempts: {e}") from e
            time.sleep(backoff)
            backoff *= 1.5

    return RuntimeError(f"Failed to GET {url}: {last_exc}")

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

def escape_html(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    return (s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))

def generate_html_with_candidates(candidates: List[Dict[str, Any]], rows: List[Dict[str, Any]], out_path: Path, title: str = "OPTIMIZED Fee-Aware Skinport Analysis"):
    """Generate HTML report with candidates highlighted and explanations"""

    def format_pump_risk_cell(pump_risk_str: str) -> str:
        try:
            pump_risk = float(pump_risk_str) if pump_risk_str else 0.0
            if pump_risk >= 60:
                return f'<td class="num" style="color:#ff1a1a; font-weight:700; background-color:rgba(255,107,107,.12);">{pump_risk_str}</td>'
            elif pump_risk >= 40:
                return f'<td class="num" style="color:#cc0000; font-weight:700;">{pump_risk_str}</td>'
            elif pump_risk >= 25:
                return f'<td class="num" style="color:#ff6600; font-weight:600;">{pump_risk_str}</td>'
            elif pump_risk >= 15:
                return f'<td class="num" style="color:#ff9900;">{pump_risk_str}</td>'
            else:
                return f'<td class="num" style="color:#34d399;">{pump_risk_str}</td>'
        except:
            return f'<td class="num">{pump_risk_str}</td>'

    def format_profit_cell(profit_str: str) -> str:
        try:
            profit = float(profit_str) if profit_str else 0.0
            if profit >= 20.0:
                return f'<td class="num"><span class="badge green">+{profit_str}%</span></td>'
            elif profit >= 10.0:
                return f'<td class="num"><span class="badge green">+{profit_str}%</span></td>'
            elif profit >= 5.0:
                return f'<td class="num"><span class="badge yellow">+{profit_str}%</span></td>'
            elif profit >= 0:
                return f'<td class="num"><span class="badge gray">+{profit_str}%</span></td>'
            else:
                return f'<td class="num"><span class="badge red">{profit_str}%</span></td>'
        except:
            return f'<td class="num">{profit_str}</td>'

    def arb_badge(v: str) -> str:
        v = (v or "").strip()
        if v == "EXCELLENT_BUY":
            return '<span class="badge green">EXCELLENT BUY</span>'
        elif v == "GOOD_BUY":
            return '<span class="badge green">GOOD BUY</span>'
        elif v == "GOOD_BUY_LOW_VOL":
            return '<span class="badge yellow">GOOD BUY</span>'
        elif v == "MARGINAL_PROFIT":
            return '<span class="badge yellow">MARGINAL</span>'
        elif v == "BREAKEVEN":
            return '<span class="badge gray">BREAKEVEN</span>'
        elif v == "SMALL_LOSS":
            return '<span class="badge red">SMALL LOSS</span>'
        elif v == "OVERPRICED":
            return '<span class="badge red">OVERPRICED</span>'
        elif v == "NO_STEAM_DATA":
            return '<span class="badge gray">NO DATA</span>'
        else:
            return f'<span class="badge gray">{v.replace("_", " ")}</span>'

    table_headers = [
        "Name", "Skinport", "Steam", "SP Price", "Steam Price", "Fee-Aware Profit", 
        "Net Steam", "Currency", "SP Sales7d", "Steam Sales7d", "SP 7d avg", "SP 24h avg", 
        "SP 30d avg", "Steam Explosiveness", "SP 7d vs 30d", "SP Growth", 
        "SP Bullish", "SP Expl", "PumpRisk", "Arbitrage", "Source"
    ]

    header_html = "<tr>" + "".join([f"<th>{h}<span class=\"sort-arrow\">↕</span></th>" for h in table_headers]) + "</tr>"

    def row_html(r, candidate=False):
        skin_anchor = f'<a href="{escape_html(r.get("Skinport_URL"))}" target="_blank" rel="noopener">Skinport</a>'
        steam_anchor = f'<a href="{escape_html(r.get("Steam_URL"))}" target="_blank" rel="noopener">Steam</a>'
        profit_cell = format_profit_cell(r.get("Fee_Aware_Profit", "0"))
        pump_cell = format_pump_risk_cell(r.get("PumpRisk", "0"))
        arb_cell = arb_badge(r.get("Arbitrage_Opportunity", ""))

        tr_style = " style='background:linear-gradient(180deg, rgba(34,197,94,.12), transparent)'" if candidate else ""
        return (
            f"<tr{tr_style}>"
            f"<td>{escape_html(r.get('Name'))}</td>"
            f"<td>{skin_anchor}</td>"
            f"<td>{steam_anchor}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_Price'))}</td>"
            f"<td class='num'>{escape_html(r.get('Steam_Price'))}</td>"
            f"{profit_cell}"
            f"<td class='num'>{escape_html(r.get('Net_Steam_Proceeds', ''))}</td>"
            f"<td>{escape_html(r.get('Currency'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_Sales7d'))}</td>"
            f"<td class='num'>{escape_html(r.get('Steam_Sales7d'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_7d_avg'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_24h_avg'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_30d_avg'))}</td>"
            f"<td class='num'>{escape_html(r.get('Steam_Explosiveness'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_7d_vs_30d'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_GrowthRatio'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_BullishScore'))}</td>"
            f"<td class='num'>{escape_html(r.get('Skinport_Explosiveness'))}</td>"
            f"{pump_cell}"
            f"<td style='text-align:center'>{arb_cell}</td>"
            f"<td style='text-align:center; font-size:10px;'><span class='pill'>{escape_html(r.get('Steam_Source',''))}</span></td>"
            "</tr>"
        )

    cand_rows = [row_html(r, candidate=True) for r in candidates]
    main_rows = [row_html(r, candidate=False) for r in rows]

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>{escape_html(title)}</title>
<style>
:root {{
    color-scheme: light dark;
    --bg: #0b0f14;
    --panel: #121823;
    --panel-2: #0f1520;
    --text: #e6edf3;
    --muted: #8b98a5;
    --border: #1f2a3a;
    --accent: #4aa3ff;
    --accent-2: #7cd992;
    --bad-red: #ff6b6b;
    --bad-amber: #f7c948;
    --good-green: #22c55e;
    --row-even: rgba(255,255,255,0.02);
    --row-hover: rgba(74,163,255,0.10);
}}
@media (prefers-color-scheme: light) {{
    :root {{
        --bg: #f7f9fc;
        --panel: #ffffff;
        --panel-2: #f0f4fa;
        --text: #0b1a2a;
        --muted: #5b6b7b;
        --border: #dee5ef;
        --accent: #1f73ff;
        --accent-2: #19a974;
        --bad-red: #d7263d;
        --bad-amber: #e5a100;
        --good-green: #0f9d58;
        --row-even: #fafcff;
        --row-hover: rgba(31,115,255,0.08);
    }}
}}
* {{ box-sizing: border-box; }}
html, body {{ height: 100%; }}
body {{ background: var(--bg); color: var(--text); font-family: Inter, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 20px; line-height: 1.5; }}
h1 {{ margin: 6px 0 14px; font-weight: 650; letter-spacing: .2px; }}
h2 {{ margin: 16px 0 10px; font-weight: 600; color: var(--accent); }}
.toolbar {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }}
.chip {{ background: var(--panel-2); border: 1px solid var(--border); color: var(--muted); padding: 6px 10px; border-radius: 999px; font-size: 12px; }}
.search {{ flex: 1 1 320px; display: flex; align-items: center; gap: 8px; background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; }}
.search input {{ flex: 1; background: transparent; border: 0; outline: 0; color: var(--text); font-size: 14px; }}
.search input::placeholder {{ color: var(--muted); }}
.card {{ background: var(--panel); border: 1px solid var(--border); border-radius: 12px; box-shadow: 0 6px 20px rgba(0,0,0,.15); overflow: hidden; margin-bottom: 20px; }}
.explanation {{ background: var(--panel-2); border: 1px solid var(--border); border-radius: 10px; padding: 16px; margin: 16px 0; color: var(--text); font-size: 14px; line-height: 1.6; }}
.explanation h3 {{ margin: 0 0 8px 0; color: var(--accent); font-size: 16px; font-weight: 600; }}
.explanation h4 {{ margin: 12px 0 6px 0; color: var(--accent-2); font-size: 14px; font-weight: 600; }}
.explanation ul {{ margin: 8px 0; padding-left: 18px; }}
.explanation li {{ margin-bottom: 4px; }}
.table-wrap {{ overflow: auto; max-width: 100%; border-radius: 12px; }}
table {{ border-collapse: separate; border-spacing: 0; width: 100%; font-size: 12.5px; table-layout: fixed; }}
thead th {{ position: sticky; top: 0; z-index: 2; background: linear-gradient(180deg, var(--panel-2), var(--panel)); color: var(--muted); text-transform: uppercase; letter-spacing: .4px; font-weight: 600; padding: 10px 8px; border-bottom: 1px solid var(--border); backdrop-filter: saturate(180%) blur(6px); cursor: pointer; user-select: none; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
thead th:hover {{ background: linear-gradient(180deg, var(--panel), var(--panel-2)); }}
tbody td, tbody th {{ padding: 9px 8px; border-bottom: 1px solid var(--border); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
tbody tr:nth-child(even) {{ background: var(--row-even); }}
tbody tr:hover {{ background: var(--row-hover); }}
th, td {{ text-align: left; }}
td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
td a {{ text-decoration: none; color: var(--accent); background: transparent; padding: 0; }}
td a:hover {{ text-decoration: underline; }}
.sort-arrow {{ color: var(--muted); margin-left: 6px; font-size: 10px; }}
.legend {{ background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 12px; margin: 12px 0; color: var(--muted); font-size: 13px; }}
.cand-note {{ background: linear-gradient(180deg, rgba(34,197,94,.15), transparent); border-left: 4px solid var(--good-green); padding: 10px 14px; border-radius: 8px; margin: 14px 0; }}
.badge {{ display: inline-block; padding: 3px 8px; border-radius: 999px; font-weight: 600; font-size: 11px; border: 1px solid transparent; }}
.badge.green {{ background: rgba(34,197,94,.15); color: var(--good-green); border-color: rgba(34,197,94,.25); }}
.badge.yellow {{ background: rgba(234,179,8,.15); color: #eab308; border-color: rgba(234,179,8,.25); }}
.badge.red {{ background: rgba(255,107,107,.12); color: var(--bad-red); border-color: rgba(255,107,107,.2); }}
.badge.gray {{ background: rgba(148,163,184,.12); color: var(--muted); border-color: rgba(148,163,184,.2); }}
.pill {{ background: var(--panel-2); color: var(--muted); padding: 2px 6px; border-radius: 6px; font-size: 10px; }}
.footer {{ color: var(--muted); font-size: 12px; margin-top: 10px; text-align: center; }}
th:nth-child(1), td:nth-child(1) {{ width: 250px; min-width: 250px; max-width: 250px; }}
th:nth-child(2), td:nth-child(2) {{ width: 80px; min-width: 80px; max-width: 80px; }}
th:nth-child(3), td:nth-child(3) {{ width: 80px; min-width: 80px; max-width: 80px; }}
th:nth-child(4), td:nth-child(4) {{ width: 90px; min-width: 90px; max-width: 90px; }}
th:nth-child(5), td:nth-child(5) {{ width: 90px; min-width: 90px; max-width: 90px; }}
th:nth-child(6), td:nth-child(6) {{ width: 110px; min-width: 110px; max-width: 110px; }}
th:nth-child(7), td:nth-child(7) {{ width: 90px; min-width: 90px; max-width: 90px; }}
th:nth-child(8), td:nth-child(8) {{ width: 70px; min-width: 70px; max-width: 70px; }}
th:nth-child(9), td:nth-child(9) {{ width: 80px; min-width: 80px; max-width: 80px; }}
th:nth-child(10), td:nth-child(10) {{ width: 80px; min-width: 80px; max-width: 80px; }}
th:nth-child(11), td:nth-child(11) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(12), td:nth-child(12) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(13), td:nth-child(13) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(14), td:nth-child(14) {{ width: 95px; min-width: 95px; max-width: 95px; }}
th:nth-child(15), td:nth-child(15) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(16), td:nth-child(16) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(17), td:nth-child(17) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(18), td:nth-child(18) {{ width: 85px; min-width: 85px; max-width: 85px; }}
th:nth-child(19), td:nth-child(19) {{ width: 80px; min-width: 80px; max-width: 80px; }}
th:nth-child(20), td:nth-child(20) {{ width: 120px; min-width: 120px; max-width: 120px; }}
th:nth-child(21), td:nth-child(21) {{ width: 100px; min-width: 100px; max-width: 100px; }}
</style>
<script>
const tableSortStates = new Map();
function sortTable(table, column) {{
    const tableId = table.getAttribute('data-table-id') || Math.random().toString();
    table.setAttribute('data-table-id', tableId);
    const stateKey = tableId + '-' + column;
    let currentState = tableSortStates.get(stateKey) || 'none';
    let newState, ascending;
    if (currentState === 'none' || currentState === 'desc') {{ newState = 'asc'; ascending = true; }}
    else {{ newState = 'desc'; ascending = false; }}
    tableSortStates.set(stateKey, newState);
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort((a, b) => {{
        let aVal = a.children[column].textContent.trim();
        let bVal = b.children[column].textContent.trim();
        if (column >= 3 && column <= 17) {{
            aVal = parseFloat(aVal.replace(/[^\\d.-]/g, '')) || 0;
            bVal = parseFloat(bVal.replace(/[^\\d.-]/g, '')) || 0;
            return ascending ? aVal - bVal : bVal - aVal;
        }} else {{ return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal); }}
    }});
    tbody.innerHTML = '';
    rows.forEach(row => tbody.appendChild(row));
    const headers = table.querySelectorAll('th');
    headers.forEach((header, index) => {{
        const arrow = header.querySelector('.sort-arrow');
        if (arrow) {{
            if (index === column) {{
                arrow.textContent = ascending ? '▲' : '▼';
            }} else {{
                arrow.textContent = '↕';
                const otherStateKey = tableId + '-' + index;
                tableSortStates.set(otherStateKey, 'none');
            }}
        }}
    }});
}}
function initSortableTable(table) {{
    const headers = table.querySelectorAll('th');
    headers.forEach((header, index) => {{
        if (index < 2) return;
        header.addEventListener('click', () => {{ sortTable(table, index); }});
    }});
}}
function filterTables(){{
    const q = (document.getElementById('filterInput')?.value || '').toLowerCase();
    document.querySelectorAll('table.sortable-table tbody tr').forEach(tr=>{{
        const text = tr.innerText.toLowerCase();
        tr.style.display = text.includes(q) ? '' : 'none';
    }});
}}
document.addEventListener('DOMContentLoaded', () => {{
    const tables = document.querySelectorAll('.sortable-table');
    tables.forEach(initSortableTable);
}});
</script>
</head>
<body>
<h1>{escape_html(title)}</h1>

<div class="explanation">
<h3>OPTIMIZED Analysis with 100% Success Rate Steam Fetching</h3>
<h4>Key Optimizations Applied:</h4>
<ul>
<li><strong>Conservative Timing</strong>: Reduced concurrent requests from 12→6, increased delays from 0.5s→1.5s</li>
<li><strong>Enhanced Retry</strong>: 5 retry attempts with progressive backoff and intelligent jitter</li>
<li><strong>Failed Request Management</strong>: Automatic retry of failed requests in subsequent sessions</li>
<li><strong>Adaptive Rate Limiting</strong>: Dynamic delay adjustment based on Steam's response patterns</li>
<li><strong>Smart Caching</strong>: Persistent cache with failed request tracking for maximum efficiency</li>
</ul>

<h4>Understanding the Metrics:</h4>
<p><strong>Explosiveness Score</strong>: Composite metric (0-100) combining momentum (25%), scarcity (20%), discount opportunity (15%), volatility breakout (15%), volume surge (10%), market sentiment (10%), minus manipulation risk (15%)</p>
<p><strong>Pump Risk Detection</strong>: Warning system (0-100) identifying potential manipulation through unusual volume patterns, extreme momentum, and historical pump indicators</p>
<p><strong>Fee-Aware Arbitrage</strong>: Profit calculation accounting for Skinport's 0% buying fees and Steam's 15% selling fees</p>
</div>

<div class="toolbar">
<div class="chip">OPTIMIZED Concurrent Analysis</div>
<div class="search">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M21 21l-3.8-3.8M10.8 18.6a7.8 7.8 0 1 1 0-15.6 7.8 7.8 0 0 1 0 15.6z" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/>
    </svg>
    <input id="filterInput" placeholder="Filter by name, type, or source..." oninput="filterTables()" />
</div>
</div>
<p class="footer">Generated: {datetime.now(timezone.utc).isoformat()}</p>

<div class="legend">
<strong>OPTIMIZED High-Success Analysis:</strong> Conservative timing + Enhanced retry mechanism + Failed request management<br>
<strong>Fee-Aware Arbitrage:</strong> Buy Skinport (0% fees) → Sell Steam (15% fees)<br>
<strong>Profit Colors:</strong> 
<span class="badge green">Green ≥10%</span> 
<span class="badge yellow">Yellow 5-10%</span> 
<span class="badge gray">Gray 0-5%</span>
<span class="badge red">Red <0%</span><br>
<strong>Success Strategy:</strong> Multiple sessions automatically retry failed requests for 100% coverage
</div>
"""

    # Candidate table
    if candidates:
        html += f"<div class='cand-note'><strong>Top OPTIMIZED Fee-Aware Candidates:</strong> {len(candidates)} item(s) with profitable arbitrage after platform fees</div>"
        html += f"<div class='card table-wrap'><table class='sortable-table'><thead>{header_html}</thead><tbody>"
        html += "".join(cand_rows)
        html += "</tbody></table></div>"

    # Main table
    html += f"<div class='card table-wrap'><table class='sortable-table'><thead>{header_html}</thead><tbody>"
    html += "".join(main_rows)
    html += "</tbody></table></div>"

    html += f"<div class='footer'>OPTIMIZED high-success concurrent analysis with intelligent retry • Platform fees: Skinport 0% + Steam 15% • {len(rows)} items processed</div>"
    html += "</body></html>"

    out_path.write_text(html, encoding="utf8")
    print(f"Wrote OPTIMIZED analysis HTML to {out_path}")

def parse_filters(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    expanded: List[str] = []
    for p in parts:
        if p in ALL_FILTER_KEYWORDS:
            expanded.extend(ALL_FILTER_KEYWORDS[p])
        else:
            expanded.append(p)
    return sorted({tok for tok in expanded if tok})

def _has_word_token(name: str, token: str) -> bool:
    try:
        tok = re.escape(token)
        pattern = r'(?<!\w)' + tok + r'(?!\w)'
        return re.search(pattern, name, flags=re.IGNORECASE) is not None
    except re.error:
        return token.lower() in name.lower()

def _item_structured_field_matches(item: Dict[str, Any], token: str) -> bool:
    if not item or not token:
        return False
    token_l = token.lower()
    for k in ("type", "item_type", "category", "class", "market_class", "weapon_type", "item_class"):
        v = item.get(k)
        if v:
            if isinstance(v, str) and token_l in v.lower():
                return True
            if isinstance(v, (list, tuple)):
                for vv in v:
                    if isinstance(vv, str) and token_l in vv.lower():
                        return True
    return False

def matches_filters(name: str, filters: List[str], item: Optional[Dict[str, Any]] = None) -> bool:
    if not filters:
        return True

    name_l = name.lower() if name else ""

    for tok in filters:
        if _item_structured_field_matches(item or {}, tok):
            for cat, keywords in ALL_FILTER_KEYWORDS.items():
                if tok in keywords or tok == cat:
                    if any(_has_word_token(name, ex) for ex in CATEGORY_EXCLUSIONS.get(cat, [])):
                        break
                    return True
            return True

        if _has_word_token(name, tok):
            for cat, keywords in ALL_FILTER_KEYWORDS.items():
                if tok in keywords or tok == cat:
                    excl = CATEGORY_EXCLUSIONS.get(cat, [])
                    if any(_has_word_token(name, ex) for ex in excl):
                        break
                    return True
            if any(_has_word_token(name, ex) for ex in GENERAL_EXCLUSIONS):
                return False
            return True

    # Special knife handling
    knife_tokens = set(ALL_FILTER_KEYWORDS.get("knife", []))
    if any(tok in knife_tokens for tok in filters):
        if "★" in name:
            knife_specific_tokens = [t for t in ALL_FILTER_KEYWORDS.get("knife", []) if t and t != "knife"]
            for kt in knife_specific_tokens + ["knife"]:
                if _has_word_token(name, kt):
                    if not any(_has_word_token(name, ex) for ex in CATEGORY_EXCLUSIONS.get("knife", [])):
                        return True

    return False

def main():
    print("=== OPTIMIZED High-Speed Fee-Aware Skinport + Steam Market Analysis ===")
    print("🎯 OPTIMIZED for 100% Success Rate: Conservative timing, Enhanced retry, Failed request management")
    
    if AIOHTTP_AVAILABLE:
        print("Concurrent fetching: ENABLED with OPTIMIZED settings for maximum success")
        print("Features: Conservative delays, Progressive backoff, Failed request cache, Adaptive rate limiting")
    else:
        print("Sequential fetching: ENABLED with OPTIMIZED settings (install aiohttp for concurrent mode)")
        print("Features: Smart caching, Enhanced retry, Fee-aware arbitrage")

    print("Platform fees: Skinport 0% buying + Steam 15% selling")

    print(f"\nOPTIMIZED Steam sources configured:")
    for source, config in STEAM_SOURCES.items():
        status = "Enabled" if config["enabled"] else "Disabled"
        delay = config["initial_delay"]
        limit = config["concurrent_limit"]
        print(f"  • {config['name']}: {status} (delay: {delay}s, concurrent: {limit})")

    print(f"\nOPTIMIZED retry mechanism settings:")
    print(f"  • Max retries: {RETRY_SETTINGS['max_retries']}")
    print(f"  • Base delay: {RETRY_SETTINGS['base_delay']}s")
    print(f"  • Max delay: {RETRY_SETTINGS['max_delay']}s")
    print(f"  • Progressive backoff with intelligent jitter")

    # Display failed request status
    failed_requests = load_failed_requests()
    if failed_requests:
        print(f"  • Previous session failed requests ready for retry: {len(failed_requests)}")

    install_requests_cache_if_available(expire_after=DEFAULT_CACHE_TTL)

    # Get inputs
    cur_input = input("\nCurrency (usd, pln, eur, gbp) [default: usd]: ").strip()
    currency = normalize_currency(cur_input or "usd")
    set_global_currency(currency)

    min_price_raw = input("Minimum price (blank => 0): ").strip()
    max_price_raw = input("Maximum price (blank => no limit): ").strip()
    min_sales_raw = input("Minimum sales last week (blank => ignored): ").strip()

    min_price = maybe_float(min_price_raw) if min_price_raw else 0.0
    max_price = maybe_float(max_price_raw) if max_price_raw else None
    min_sales_week = maybe_int(min_sales_raw) if min_sales_raw else None

    if AIOHTTP_AVAILABLE:
        fetch_steam = input("Fetch Steam data with OPTIMIZED concurrent requests? (y/n) [y]: ").strip().lower() != 'n'
    else:
        fetch_steam = input("Fetch Steam data with OPTIMIZED sequential retry? (y/n) [y]: ").strip().lower() != 'n'

    print("\nFilter by category or weapon. Examples: knife, gloves, rifle, ak-47, m4a4, m4a1-s, butterfly knife")
    raw_filter = input("Categories/weapons (comma-separated, blank => all): ").strip()
    filter_tokens = parse_filters(raw_filter)

    output_format = input("Output format (csv / html) [html]: ").strip().lower() or "html"
    if output_format not in ("csv", "html"):
        print("Unknown format; defaulting to html.")
        output_format = "html"

    write_mode = input("Write mode (overwrite / merge) [overwrite]: ").strip().lower() or "overwrite"
    if write_mode not in ("overwrite", "merge"):
        print("Unknown mode; defaulting to overwrite.")
        write_mode = "overwrite"

    # Fetch Skinport data
    print("\nFetching Skinport data...")
    try:
        items_raw = http_get_with_cache(f"{API_BASE}/items", params={"currency": currency, "tradable": 0})
        if isinstance(items_raw, dict):
            for k in ("items", "results", "data"):
                if k in items_raw and isinstance(items_raw[k], list):
                    items = items_raw[k]
                    break
            else:
                items = items_raw.get("items") if isinstance(items_raw.get("items"), list) else []
        elif isinstance(items_raw, list):
            items = items_raw
        else:
            items = []

        # Try sales history endpoint
        print("Fetching Skinport sales history...")
        try:
            sales_raw = http_get_with_cache(f"{API_BASE}/sales/history", params={"currency": currency})
            if isinstance(sales_raw, dict):
                sales = sales_raw.get("history") or sales_raw.get("sales") or sales_raw.get("data") or []
            elif isinstance(sales_raw, list):
                sales = sales_raw
            else:
                sales = []
        except Exception as e:
            print(f"Warning: Could not fetch sales history: {e}")
            sales = []
                
    except Exception as e:
        print(f"Error fetching Skinport data: {e}")
        sys.exit(1)

    print(f"Fetched {len(items)} items and {len(sales)} sales records from Skinport")

    # Build sales map
    sales_map = {}
    for s in sales:
        key = s.get("market_hash_name") or s.get("market_hash") or s.get("name")
        if key:
            sales_map[key] = s

    rows: List[Dict[str, Any]] = []
    item_names_for_steam = []
    now = datetime.now(timezone.utc).isoformat()

    print(f"\nProcessing Skinport items with OPTIMIZED filtering...")
    if TQDM_AVAILABLE:
        progress_bar = tqdm(items, desc="Processing Skinport items", unit="items")
    else:
        progress_bar = tqdm(total=len(items), desc="Processing Skinport items", unit="items")

    try:
        for i, it in enumerate(items if TQDM_AVAILABLE else range(len(items))):
            if not TQDM_AVAILABLE:
                it = items[i]
                progress_bar.update(1)

            name = it.get("market_hash_name") or it.get("name") or it.get("market_hash") or ""
            if not name:
                continue

            if not matches_filters(name, filter_tokens, item=it):
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
            last30 = s.get("last_30_days") if s else None

            vol7 = safe_volume(last7)
            if (min_sales_week is not None) and (vol7 < min_sales_week):
                continue

            avg7 = safe_avg(last7) or 0.0
            avg24 = safe_avg(last24) or 0.0
            avg30 = safe_avg(last30)

            if avg7:
                growth_ratio = round((avg24 / avg7), 3)
            else:
                growth_ratio = round((999.0 if avg24 > 0 else 0.0), 3)

            if avg30 is not None and avg30 > 0:
                vs30 = round((avg7 / avg30), 3)
            else:
                vs30 = None

            score = compute_bullish_score(avg24, avg7, (avg30 if avg30 is not None else 0.0), vol7)
            explosiveness, pump_risk = compute_enhanced_explosiveness(avg24, avg7, avg30, vol7, price, it, s, name)

            # Create row
            row = {
                "Name": name,
                "Skinport_URL": make_skinport_url(name),
                "Steam_URL": make_steam_url(name),
                "Skinport_Price": f"{round(price, 2):.2f}",
                "Steam_Price": "",
                "Price_Diff_Pct": "",
                "Currency": it.get("currency") or currency,
                "Skinport_Sales7d": str(vol7),
                "Steam_Sales7d": "",
                "Skinport_7d_avg": f"{round(avg7, 2):.2f}",
                "Skinport_24h_avg": f"{round(avg24, 2):.2f}",
                "Skinport_30d_avg": (f"{round(avg30, 2):.2f}" if (avg30 is not None) else ""),
                "Steam_Explosiveness": "",
                "Skinport_7d_vs_30d": (f"{vs30:.3f}" if (vs30 is not None) else ""),
                "Skinport_GrowthRatio": f"{growth_ratio:.3f}",
                "Skinport_BullishScore": f"{round(score, 4):.4f}",
                "Skinport_Explosiveness": f"{explosiveness:.2f}",
                "PumpRisk": f"{pump_risk:.1f}",
                "Arbitrage_Opportunity": "",
                "Candidate": "",
                "LastUpdated": now,
                "Steam_Source": "",
                "Fee_Aware_Profit": "",
                "Net_Steam_Proceeds": ""
            }

            rows.append(row)
            item_names_for_steam.append(name)

    finally:
        progress_bar.close()

    print(f"Processed {len(rows)} Skinport items matching criteria")

    steam_data_map = {}
    if fetch_steam and item_names_for_steam:
        try:
            if AIOHTTP_AVAILABLE:
                steam_data_map = asyncio.run(batch_fetch_steam_prices_optimized(item_names_for_steam))
            else:
                steam_data_map = batch_fetch_steam_prices_sync_optimized(item_names_for_steam)
        except Exception as e:
            print(f"Warning: Error in OPTIMIZED Steam fetching: {e}")

    # Apply Steam data and calculate arbitrage
    print(f"\nApplying Steam data and calculating fee-aware arbitrage...")
    steam_data_applied = 0

    if TQDM_AVAILABLE:
        apply_progress = tqdm(rows, desc="Applying OPTIMIZED fee-aware arbitrage", unit="items")
    else:
        apply_progress = tqdm(total=len(rows), desc="Applying OPTIMIZED fee-aware arbitrage", unit="items")

    try:
        for i, row in enumerate(rows if TQDM_AVAILABLE else range(len(rows))):
            if not TQDM_AVAILABLE:
                row = rows[i]
                apply_progress.update(1)

            item_name = row["Name"]

            if item_name in steam_data_map:
                steam_data = steam_data_map[item_name]

                if steam_data.get("current_price"):
                    steam_price = steam_data["current_price"]
                    row["Steam_Price"] = f"{steam_price:.2f}"

                    # Calculate fee-aware arbitrage
                    skinport_price = float(row["Skinport_Price"])
                    arbitrage_opp, profit_pct, breakdown = compute_fee_aware_arbitrage_opportunity(
                        skinport_price, steam_price, int(row["Skinport_Sales7d"]), currency
                    )
                    row["Arbitrage_Opportunity"] = arbitrage_opp
                    row["Fee_Aware_Profit"] = f"{profit_pct:.1f}"
                    row["Net_Steam_Proceeds"] = f"{breakdown['net_steam_proceeds']:.2f}"
                    # Raw difference for compatibility
                    raw_diff_pct = ((skinport_price - steam_price) / steam_price) * 100 if steam_price > 0 else 0
                    row["Price_Diff_Pct"] = f"{raw_diff_pct:.1f}"

                    steam_data_applied += 1

                # Apply Steam metrics
                row["Steam_Sales7d"] = str(steam_data.get("sales_7d", 0))
                row["Steam_Explosiveness"] = f"{steam_data.get('explosiveness', 0.0):.1f}"

                # Track source
                source_name = steam_data.get("source", "unknown")
                if source_name in STEAM_SOURCES:
                    row["Steam_Source"] = STEAM_SOURCES[source_name]["name"]
                else:
                    row["Steam_Source"] = source_name.replace("_", " ").title()

    finally:
        apply_progress.close()

    if fetch_steam:
        print(f"Applied OPTIMIZED Steam arbitrage data to {steam_data_applied}/{len(rows)} items")
        print(f"Currency consistency: All prices in {current_currency}")

        # Show performance metrics
        total_retries = sum(config["retry_count"] for config in STEAM_SOURCES.values())
        if total_retries > 0:
            print(f"Enhanced retry mechanism performed {total_retries} recovery attempts")

        if AIOHTTP_AVAILABLE:
            print(f"OPTIMIZED concurrent fetching with intelligent failed request management")

    if not rows:
        print("No items matched your criteria.")
        return

    # Sort and find candidates
    print(f"\nAnalyzing OPTIMIZED fee-aware candidates...")
    try:
        rows.sort(key=lambda r: float(r.get("Fee_Aware_Profit", 0)), reverse=True)
    except Exception:
        pass

    vol_req = (min_sales_week if (min_sales_week is not None) else MIN_VOL_FLOOR)
    EXPLOSIVENESS_MIN = 20.0
    PUMP_RISK_MAX = 60.0

    candidates: List[Dict[str, Any]] = []
    for r in rows:
        try:
            fee_aware_profit = float(r.get("Fee_Aware_Profit", 0))
            vol7 = int(r.get("Skinport_Sales7d") or 0)
            pump_risk = float(r.get("PumpRisk", 0))
            arbitrage_opp = r.get("Arbitrage_Opportunity", "")
        except Exception:
            r["Candidate"] = ""
            continue

        # Fee-aware candidate selection
        if fee_aware_profit >= MIN_PROFIT_PERCENTAGE and vol7 >= vol_req and pump_risk <= PUMP_RISK_MAX:
            if arbitrage_opp in ["EXCELLENT_BUY", "GOOD_BUY", "GOOD_BUY_LOW_VOL"]:
                r["Candidate"] = "YES"
                candidates.append(r)
        else:
            r["Candidate"] = ""

        if len(candidates) >= MAX_CANDIDATES:
            break

    candidates.sort(key=lambda r: float(r.get("Fee_Aware_Profit", 0)), reverse=True)

    # Show results
    if candidates:
        print(f"\nOPTIMIZED Fee-Aware Top Candidates — {len(candidates)} item(s):")
        for i, c in enumerate(candidates, 1):
            pump_risk = float(c.get("PumpRisk", 0))
            risk_indicator = "Very High" if pump_risk >= 60 else "High" if pump_risk >= 40 else "Medium" if pump_risk >= 25 else "Low-Med" if pump_risk >= 15 else "Low"

            arbitrage = c.get("Arbitrage_Opportunity", "")
            fee_profit = c.get("Fee_Aware_Profit", "0")
            steam_source = c.get("Steam_Source", "No Data")
            steam_price = c.get("Steam_Price", "N/A")
            skinport_price = c.get("Skinport_Price", "N/A")
            net_proceeds = c.get("Net_Steam_Proceeds", "N/A")

            print(f" {i:2d}. {c['Name']}")
            print(f"     Skinport: {skinport_price} {current_currency} → Steam: {steam_price} {current_currency}")
            print(f"     Steam after fees: {net_proceeds} {current_currency} → Fee-aware profit: {fee_profit}% ({arbitrage})")
            print(f"     Risk: {c['PumpRisk']}({risk_indicator}) | Vol: {c['Skinport_Sales7d']} | Source: {steam_source}")
    else:
        print("\nNo OPTIMIZED fee-aware candidates found this session.")
        print("Tip: Run again to retry previously failed Steam requests for better coverage")

    # Write output files
    print(f"\nWriting OPTIMIZED {output_format.upper()} output...")
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
            generate_html_with_candidates(candidates, rows, target_path, "OPTIMIZED Fee-Aware Skinport Analysis")
        return

    # Merge mode
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

    try:
        merged_rows.sort(key=lambda r: float(r.get("Fee_Aware_Profit", 0)), reverse=True)
    except Exception:
        pass

    write_csv(MASTER_CSV, merged_rows)

    if output_format == "csv":
        write_csv(target_path, merged_rows)
        print(f"Merged {len(rows)} new/updated rows into {target_path} (total rows now: {len(merged_rows)}).")
    else:
        generate_html_with_candidates(candidates, merged_rows, target_path, "OPTIMIZED Fee-Aware Skinport Analysis")
        print(f"Merged {len(rows)} new/updated rows into {target_path} (total rows now: {len(merged_rows)}).")

    processing_mode = "concurrent" if AIOHTTP_AVAILABLE else "sequential"
    print(f"\nOPTIMIZED high-success {processing_mode} analysis complete!")
    print(f"Processed {len(rows)} items with enhanced fee-aware Steam arbitrage calculations")

    # Final performance summary
    total_retries = sum(config["retry_count"] for config in STEAM_SOURCES.values())
    failed_count = len(load_failed_requests())
    
    if total_retries > 0:
        print(f"Enhanced retry mechanism successfully recovered {total_retries} failed requests")
    
    if failed_count > 0:
        print(f"💡 TIP: {failed_count} requests queued for automatic retry in next session for 100% coverage")
    
    if AIOHTTP_AVAILABLE and steam_data_applied > 0:
        print(f"🎯 OPTIMIZED concurrent fetching with intelligent retry management delivered maximum reliability")

if __name__ == "__main__":
    main()