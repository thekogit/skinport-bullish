#!/usr/bin/env python3

import json
import time
import hashlib
from typing import Any, Dict, List, Tuple
from pathlib import Path

from config import CACHE_DIR, DEFAULT_CACHE_TTL, STEAM_CACHE_TTL

def _cache_key_for_steam(source: str, skin_name: str, currency: str, app_id: int = 730) -> str:
    return hashlib.sha256(f"steam_{source}_{skin_name}_{currency}_{app_id}".encode("utf8")).hexdigest()

def load_cache(key: str, ttl: int = DEFAULT_CACHE_TTL):
    try:
        path = CACHE_DIR / f"{key}.json"

        # Check if cache file exists
        if not path.exists():
            return None

        # Check cache age
        mtime = path.stat().st_mtime
        age = time.time() - mtime

        # If cache is too old, delete it and return None
        if age > ttl:
            try:
                path.unlink()  # Delete expired cache
                print(f"🗑️  Deleted expired cache (age: {age/3600:.1f}h, max: {ttl/3600:.1f}h)")
            except Exception:
                pass
            return None

        # Load and parse cache
        data = json.loads(path.read_text(encoding="utf8"))
        return data

    except json.JSONDecodeError as e:
        print(f"⚠️  Cache corrupted, deleting: {e}")
        try:
            path.unlink()
        except Exception:
            pass
        return None
    except Exception as e:
        print(f"⚠️  Error loading cache: {e}")
        return None

def save_cache(key: str, data: Any):
    try:
        # Ensure cache directory exists
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        path = CACHE_DIR / f"{key}.json"

        # Write cache file
        path.write_text(json.dumps(data, indent=2), encoding="utf8")

    except TypeError as e:
        print(f"⚠️  Cannot serialize data to JSON: {e}")
    except Exception as e:
        print(f"⚠️  Error saving cache: {e}")

def load_cache_batch(item_names: List[str], source: str, currency: str, app_id: int = 730) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    cached_data = {}
    items_to_fetch = []

    for item_name in item_names:
        cache_key = _cache_key_for_steam(source, item_name, currency, app_id)
        cached_item = load_cache(cache_key, STEAM_CACHE_TTL)

        if cached_item:
            cached_data[item_name] = cached_item
        else:
            items_to_fetch.append(item_name)

    return cached_data, items_to_fetch

def clear_expired_cache(max_age_hours: int = 12):
    try:
        if not CACHE_DIR.exists():
            return

        max_age_seconds = max_age_hours * 3600
        current_time = time.time()
        deleted_count = 0

        for cache_file in CACHE_DIR.glob("*.json"):
            try:
                mtime = cache_file.stat().st_mtime
                age = current_time - mtime

                if age > max_age_seconds:
                    cache_file.unlink()
                    deleted_count += 1
            except Exception:
                pass

        if deleted_count > 0:
            print(f"🗑️  Cleared {deleted_count} expired cache files (>{max_age_hours}h old)")

    except Exception as e:
        print(f"⚠️  Error clearing expired cache: {e}")

def get_cache_stats():
    try:
        if not CACHE_DIR.exists():
            return {"total_files": 0, "total_size_mb": 0, "oldest_age_hours": 0}

        cache_files = list(CACHE_DIR.glob("*.json"))
        total_size = sum(f.stat().st_size for f in cache_files)

        if cache_files:
            oldest_mtime = min(f.stat().st_mtime for f in cache_files)
            oldest_age = (time.time() - oldest_mtime) / 3600
        else:
            oldest_age = 0

        return {
            "total_files": len(cache_files),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "oldest_age_hours": round(oldest_age, 1)
        }
    except Exception:
        return {"total_files": 0, "total_size_mb": 0, "oldest_age_hours": 0}
