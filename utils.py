#!/usr/bin/env python3


import re
import math
import time
import json
import gzip
import hashlib
import urllib.parse
import threading
from typing import Optional, Dict, Any
from pathlib import Path

import requests

from config import (
    SUPPORTED, STEAM_CURRENCY_MAP, HEADERS, DEFAULT_CACHE_TTL,
    SUPPORTED_GAMES
)

# Import optional dependencies with fallbacks
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from tqdm import tqdm as tqdm_real
    TQDM_AVAILABLE = True
    tqdm = tqdm_real
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

# Thread lock for rate limiting
_rate_limit_lock = threading.Lock()

# Global state variables
current_currency = "USD"
steam_currency_id = 1
current_game = "cs2"
current_app_id = 730

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
    print(f"\n💱 Currency: {current_currency} (Steam ID: {steam_currency_id})")
    print(f"💰 Fees: Skinport 0% buying, Steam 15% selling")
    if steam_currency_id == 1 and current_currency != "USD":
        print(f"  ⚠️  Warning: {current_currency} not in Steam map, using USD")

def set_global_game(game: str, app_id: int):
    global current_game, current_app_id
    
    current_game = game
    current_app_id = app_id
    
    game_name = SUPPORTED_GAMES.get(game, {}).get("name", game.upper())
    print(f"🎮 Game: {game_name} (App ID: {app_id})")

def maybe_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def maybe_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None

def safe_avg(obj: Optional[Dict[str, Any]]) -> Optional[float]:
    if not obj:
        return None
    return maybe_float(obj.get("avg"))

def safe_volume(obj: Optional[Dict[str, Any]]) -> int:
    if not obj:
        return 0
    return maybe_int(obj.get("volume") or obj.get("vol") or obj.get("count") or 0) or 0

def clean_price_string(price_str: str, currency: str = "USD") -> Optional[float]:
    if not price_str:
        return None

    cleaned = price_str.strip()

    if currency == "PLN":
        cleaned = re.sub(r'z|PLN', '', cleaned).strip()
        cleaned = re.sub(r'[^\d,.-]', '', cleaned)
        if ',' in cleaned and '.' not in cleaned:
            cleaned = cleaned.replace(',', '.')
    elif currency == "EUR":
        cleaned = re.sub(r'€|EUR', '', cleaned).strip()
        cleaned = re.sub(r'[^\d,.-]', '', cleaned)
        if ',' in cleaned and '.' not in cleaned:
            cleaned = cleaned.replace(',', '.')
    else:
        cleaned = re.sub(r'[$]|USD|GBP', '', cleaned).strip()
        cleaned = re.sub(r'[^\d,.-]', '', cleaned)
        if ',' in cleaned and '.' in cleaned:
            cleaned = cleaned.replace(',', '')

    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None

def make_skinport_url(name: str, app_id: int = 730) -> str:
    if app_id == 570:  # Dota 2
        return f"https://skinport.com/market/dota2?search={urllib.parse.quote(name)}"
    elif app_id == 440:  # Team Fortress 2
        return f"https://skinport.com/market/tf2?search={urllib.parse.quote(name)}"
    elif app_id == 252490:  # Rust
        return f"https://skinport.com/market/rust?search={urllib.parse.quote(name)}"
    else:  # CS2
        return f"https://skinport.com/market?search={urllib.parse.quote(name)}"


def make_steam_url(name: str, app_id: int = 730) -> str:
    return f"https://steamcommunity.com/market/listings/{app_id}/{urllib.parse.quote(name)}"

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

def _cache_key_for(url: str, params: Optional[Dict[str, Any]]) -> str:
    key = url
    if params:
        param_pairs = []
        for k in sorted(params.keys()):
            param_pairs.append(f"{k}={params[k]}")
        key += "?" + "&".join(param_pairs)
    return hashlib.sha256(key.encode("utf8")).hexdigest()

def http_get_with_cache(url: str, params: Optional[Dict[str, Any]] = None, 
                       ttl: int = DEFAULT_CACHE_TTL, max_retries: int = 3):
    from cache_manager import save_cache
    
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