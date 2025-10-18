#!/usr/bin/env python3

from pathlib import Path

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Game Configuration
SUPPORTED_GAMES = {
    "cs2": {
        "name": "Counter-Strike 2",
        "app_id": 730,
        "steam_app_id": 730,
        "default_categories": ["knife", "gloves", "rifle", "pistol", "smg", "sniper", "shotgun"]
    },
    "dota2": {
        "name": "Dota 2",
        "app_id": 570,
        "steam_app_id": 570,
        "default_categories": ["immortal", "arcana", "hero", "courier", "ward", "treasure"]
    },
    "tf2": {
        "name": "Team Fortress 2",
        "app_id": 440,
        "steam_app_id": 440,
        "default_categories": ["unusual", "strange", "weapon", "hat", "misc", "taunt"]
    },
    "rust": {
        "name": "Rust",
        "app_id": 252490,
        "steam_app_id": 252490,
        "default_categories": ["clothing", "weapon", "building", "tool", "decoration"]
    }
}

# API Configuration
API_BASE = "https://api.skinport.com/v1"

# Steam Currency Mapping
STEAM_CURRENCY_MAP = {
    "USD": 1, "GBP": 2, "EUR": 3, "CHF": 4, "RUB": 5, "PLN": 6, "BRL": 7, "JPY": 8,
    "SEK": 9, "IDR": 10, "MYR": 11, "PHP": 12, "SGD": 13, "THB": 14, "VND": 15,
    "KRW": 16, "TRY": 17, "UAH": 18, "MXN": 19, "CAD": 20, "AUD": 21, "NZD": 22,
    "CNY": 23, "INR": 24, "CLP": 25, "PEN": 26, "COP": 27, "ZAR": 28, "HKD": 29,
    "TWD": 30, "SAR": 31, "AED": 32
}

# Platform fees
SKINPORT_FEE_RATE = 0.08
SKINPORT_FEE_RATE_HIGH = 0.06
STEAM_FEE_RATE = 0.15

# Arbitrage thresholds
MIN_PROFIT_PERCENTAGE = 10.0
GOOD_PROFIT_PERCENTAGE = 20.0
SKINPORT_HIGH_VALUE_THRESHOLD = 1000.0

# Steam API configuration 
STEAM_SOURCES = {
    "steam_direct": {
        "name": "Steam Community Market (Direct)",
        "base_url": "https://steamcommunity.com/market/priceoverview",
        "initial_delay": 2.0,
        "max_delay": 10.0,
        "concurrent_limit": 3,
        "timeout": 15,
        "enabled": True,
        "success_count": 0,
        "error_count": 0,
        "rate_limit_count": 0,
        "current_delay": 2.0
    },
    "steam_render": {
        "name": "Steam Market Render API",
        "base_url_template": "https://steamcommunity.com/market/listings/{app_id}",
        "initial_delay": 2.5,
        "max_delay": 12.0,
        "concurrent_limit": 3,
        "timeout": 20,
        "enabled": True,
        "success_count": 0,
        "error_count": 0,
        "rate_limit_count": 0,
        "current_delay": 2.5
    }
}

# HTTP Headers for Skinport API
HEADERS = {
    "Accept-Encoding": "br",
    "User-Agent": "skinport-analysis-tool/13.0-multi-game"
}

# Realistic browser headers for Steam API
STEAM_HEADERS = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://steamcommunity.com/market/",
        "X-Requested-With": "XMLHttpRequest",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Cache-Control": "no-cache"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://steamcommunity.com/market/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Accept": "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://steamcommunity.com/market/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://steamcommunity.com/market/"
    }
]

# Cache settings
CACHE_DIR = Path.home() / ".skinport_skin_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_CACHE_TTL = 43200   # 12 hours
STEAM_CACHE_TTL = 43200     # 12 hours

# Supported currencies
SUPPORTED = {"usd", "eur", "pln", "gbp"}

# Output file paths
OUT_CSV = Path.cwd() / "skinport_bullish.csv"
OUT_HTML = Path.cwd() / "skinport_bullish.html"
MASTER_CSV = CACHE_DIR / "skinport_bullish_master.csv"

# CSV fields
CSV_FIELDS = [
    "Name", "Game", "Skinport_URL", "Steam_URL", "Skinport_Price", "Steam_Price", "Price_Diff_Pct",
    "Currency", "Skinport_Sales7d", "Steam_Sales7d", "Skinport_7d_avg", "Skinport_24h_avg",
    "Skinport_30d_avg", "Steam_Explosiveness", "Skinport_7d_vs_30d", "Skinport_GrowthRatio",
    "Skinport_BullishScore", "Skinport_Explosiveness", "PumpRisk", "Arbitrage_Opportunity",
    "Candidate", "LastUpdated", "Steam_Source", "Fee_Aware_Profit", "Net_Steam_Proceeds"
]

# Concurrent fetching settings
MAX_CONCURRENT_STEAM_REQUESTS = 3
CACHE_BATCH_SIZE = 50
REQUEST_CHUNK_SIZE = 15

# Rate limiting settings
RATE_LIMIT_RECOVERY_TIME = 60
ADAPTIVE_DELAY_MULTIPLIER = 2.0
DELAY_RECOVERY_FACTOR = 0.95

# Analysis settings
EXPLOSION_THRESHOLD = 1.4
MEDIUM_TERM_THRESHOLD = 1.08
MIN_VOL_FLOOR = 20
TOP_PERCENTILE = 0.15
MAX_CANDIDATES = 25
HIGH_VOLUME_THRESHOLD = 50
PUMP_RISK_MAX = 60.0

# Explosiveness calculation weights
EXPLOSIVENESS_WEIGHTS = {
    "momentum_composite": 0.25,
    "scarcity_signal": 0.20,
    "discount_opportunity": 0.15,
    "volatility_breakout": 0.15,
    "volume_surge": 0.10,
    "market_sentiment": 0.10,
    "manipulation_risk": -0.15
}

# Analysis thresholds
MOMENTUM_SHORT_CAP = 3.0
MOMENTUM_MED_CAP = 2.0
SCARCITY_THRESHOLD = 15
DISCOUNT_CAP = 0.4
VOLATILITY_WINDOW = 7
VOLUME_SURGE_MULTIPLIER = 2.5
HIGH_MANIPULATION_VOLUME = 150

# Pump detection settings
PUMP_DETECTION = {
    "high_volume_low_growth": 200,
    "extreme_medium_momentum": 3.0,
    "sticker_volume_threshold": 100
}

# Global variables for runtime state
current_currency = "USD"
steam_currency_id = 1
current_game = "cs2"  # Default game
current_app_id = 730  # Default to CS2
