#!/usr/bin/env python3

import re
from typing import List, Optional, Dict, Any

# ============================================================================
# CS2 FILTERS
# ============================================================================

CS2_FILTER_KEYWORDS = {
    "knife": ["knife", "bayonet", "karambit", "huntsman", "butterfly", "falchion", "gut", "flip", "daggers", "bowie", "shadow", "paracord", "survival", "ursus", "navaja", "stiletto", "talon", "classic"],
    "knives": ["knife", "bayonet", "karambit", "huntsman", "butterfly", "falchion", "gut", "flip", "daggers", "bowie", "shadow", "paracord", "survival", "ursus", "navaja", "stiletto", "talon", "classic"],
    "gloves": ["gloves", "hand wraps", "driver gloves", "sport gloves", "motorcycle gloves", "specialist gloves", "bloodhound gloves"],
    "rifle": ["ak-47", "m4a4", "m4a1-s", "awp", "aug", "sg 553", "famas", "galil ar", "scar-20", "g3sg1"],
    "rifles": ["ak-47", "m4a4", "m4a1-s", "awp", "aug", "sg 553", "famas", "galil ar", "scar-20", "g3sg1"],
    "pistol": ["glock-18", "usp-s", "p2000", "p250", "five-seven", "tec-9", "cz75-auto", "desert eagle", "dual berettas", "r8 revolver"],
    "pistols": ["glock-18", "usp-s", "p2000", "p250", "five-seven", "tec-9", "cz75-auto", "desert eagle", "dual berettas", "r8 revolver"],
    "smg": ["mac-10", "mp9", "mp7", "ump-45", "p90", "pp-bizon", "mp5-sd"],
    "smgs": ["mac-10", "mp9", "mp7", "ump-45", "p90", "pp-bizon", "mp5-sd"],
    "shotgun": ["nova", "xm1014", "sawed-off", "mag-7"],
    "shotguns": ["nova", "xm1014", "sawed-off", "mag-7"],
    "ak": ["ak-47"], "m4": ["m4a4", "m4a1-s"], "awp": ["awp"],
    "case": ["case"], "cases": ["case"], "sticker": ["sticker"], "stickers": ["sticker"]
}

CS2_CATEGORY_EXCLUSIONS: Dict[str, List[str]] = {
    "knife": ["case", "weapon case", "case key", "key", "container", "package", "pack",
              "charm", "sticker", "souvenir", "pin", "patch", "music kit", "coin", "tag"],
    "gloves": ["case", "charm", "sticker", "souvenir", "patch", "key", "case key"],
    "rifle": ["case", "charm", "sticker", "souvenir", "patch", "key"],
    "smg": ["case", "charm", "sticker", "souvenir", "patch"],
    "sniper": ["case", "charm", "sticker", "souvenir", "patch"],
    "pistol": ["case", "charm", "sticker", "souvenir", "patch"],
}

# ============================================================================
# DOTA 2 FILTERS
# ============================================================================

DOTA2_FILTER_KEYWORDS = {
    # Rarity levels
    "arcana": ["arcana"],
    "immortal": ["immortal"],
    "legendary": ["legendary"],
    "mythical": ["mythical"],
    "rare": ["rare"],
    "uncommon": ["uncommon"],
    "common": ["common"],
    # Item types
    "courier": ["courier", "courrier"],
    "ward": ["ward", "observer ward", "sentry ward"],
    "treasure": ["treasure", "chest", "cache"],
    "bundle": ["bundle", "set"],
    "loading": ["loading screen", "loading"],
    "hud": ["hud", "hud skin"],
    "music": ["music pack", "music"],
    "announcer": ["announcer", "announcer pack", "mega-kills"],
    "taunt": ["taunt"],
    # Hero categories (popular heroes)
    "hero": ["anti-mage", "axe", "juggernaut", "phantom assassin", "pudge", "invoker",
             "drow ranger", "crystal maiden", "lina", "mirana", "rubick", "sniper",
             "earthshaker", "sven", "tidehunter", "zeus", "kunkka", "legion commander"],
    # Specific popular heroes
    "pudge": ["pudge"],
    "juggernaut": ["juggernaut", "jugg"],
    "invoker": ["invoker"],
    "pa": ["phantom assassin"],
    "rubick": ["rubick"],
    "sf": ["shadow fiend"],
    "lina": ["lina"],
    "cm": ["crystal maiden"],
    "zeus": ["zeus"],
    # Quality/Special
    "inscribed": ["inscribed"],
    "autographed": ["autographed", "autograph"],
    "genuine": ["genuine"],
    "unusual": ["unusual"],
    # Tournament/Event
    "ti": ["the international", "ti1", "ti2", "ti3", "ti4", "ti5", "ti6", "ti7", "ti8", "ti9", "ti10"],
    "compendium": ["compendium", "battle pass"],
}

DOTA2_CATEGORY_EXCLUSIONS: Dict[str, List[str]] = {
    "arcana": ["fake", "replica"],
    "immortal": ["fake", "replica"],
}

# ============================================================================
# TF2 FILTERS
# ============================================================================

TF2_FILTER_KEYWORDS = {
    # Quality levels
    "unusual": ["unusual"],
    "strange": ["strange"],
    "genuine": ["genuine"],
    "vintage": ["vintage"],
    "unique": ["unique"],
    "haunted": ["haunted"],
    "collector": ["collector"],
    "decorated": ["decorated"],

    # Item types
    "hat": ["hat", "cap", "helm", "helmet", "mask", "bandana", "beret", "beanie"],
    "hats": ["hat", "cap", "helm", "helmet", "mask", "bandana", "beret", "beanie"],
    "misc": ["misc", "badge", "medal", "pin"],
    "taunt": ["taunt"],
    "taunts": ["taunt"],
    "weapon": ["weapon", "scattergun", "rocket launcher", "flamethrower", "grenade launcher", 
               "stickybomb", "minigun", "wrench", "medigun", "sniper rifle", "smg", 
               "knife", "revolver", "shotgun", "pistol", "bat", "bottle"],
    "weapons": ["weapon"],

    # Specific item categories
    "cosmetic": ["cosmetic"],
    "paint": ["paint", "paint can"],
    "tool": ["tool", "name tag", "description tag", "gift wrap"],
    "crate": ["crate", "case", "supply crate"],
    "key": ["key", "mann co. supply crate key"],

    # Class-specific
    "scout": ["scout"],
    "soldier": ["soldier"],
    "pyro": ["pyro"],
    "demoman": ["demoman", "demo"],
    "heavy": ["heavy"],
    "engineer": ["engineer", "engie"],
    "medic": ["medic"],
    "sniper": ["sniper"],
    "spy": ["spy"],
    "multi-class": ["multi-class", "all-class"],

    # Popular items
    "australium": ["australium"],
    "killstreak": ["killstreak"],
    "festive": ["festive"],
    "botkiller": ["botkiller", "bot killer"],
}

TF2_CATEGORY_EXCLUSIONS: Dict[str, List[str]] = {
    "unusual": ["strange unusual"],
    "weapon": ["taunt", "hat", "misc"],
    "hat": ["weapon"],
}

# ============================================================================
# RUST FILTERS
# ============================================================================

RUST_FILTER_KEYWORDS = {
    # Weapon types
    "weapon": ["weapon", "gun"],
    "ak": ["ak-47", "assault rifle"],
    "ak-47": ["ak-47", "assault rifle"],
    "lr": ["lr-300", "lr300"],
    "lr-300": ["lr-300", "lr300"],
    "mp5": ["mp5", "mp5a4"],
    "thompson": ["thompson", "tommy gun"],
    "python": ["python revolver", "python"],
    "revolver": ["python revolver", "revolver"],
    "bolt": ["bolt action rifle", "bolt"],
    "m39": ["m39", "m39 rifle"],
    "semi": ["semi-automatic rifle", "sar"],
    "pump": ["pump shotgun", "pump"],
    "spas": ["spas-12", "spas"],

    # Melee weapons
    "sword": ["salvaged sword", "sword"],
    "cleaver": ["salvaged cleaver", "cleaver"],
    "axe": ["salvaged axe", "stone hatchet", "hatchet"],
    "hammer": ["hammer"],
    "machete": ["machete"],

    # Clothing/Armor
    "clothing": ["clothing", "pants", "shirt", "hoodie", "jacket", "gloves", "boots", "shoes"],
    "hoodie": ["hoodie"],
    "pants": ["pants", "trousers"],
    "shirt": ["shirt", "tshirt", "t-shirt"],
    "jacket": ["jacket", "coat"],
    "gloves": ["gloves", "tactical gloves"],
    "boots": ["boots", "shoes"],
    "mask": ["mask", "bandana", "balaclava"],
    "hat": ["hat", "cap", "beanie", "boonie"],

    # Armor
    "armor": ["armor", "chestplate", "metal chest plate", "road sign"],
    "metal": ["metal", "metal chest plate", "metal facemask"],
    "roadsign": ["road sign", "roadsign"],
    "hazmat": ["hazmat", "hazmat suit"],

    # Building/Construction
    "building": ["door", "wall", "foundation", "floor", "stairs", "roof"],
    "door": ["door", "sheet metal door", "armored door", "garage door"],
    "sign": ["sign", "wooden sign", "neon sign"],
    "rug": ["rug", "carpet"],
    "bed": ["bed", "sleeping bag"],

    # Tools
    "tool": ["tool", "pickaxe", "hatchet", "rock"],

    # Decoration
    "decoration": ["sleeping bag", "rug", "painting", "frame"],
    "sleeping bag": ["sleeping bag", "bag"],

    # Rarity/Special
    "general": ["general"],
    "elite": ["elite"],
    "tempered": ["tempered"],
    "metal": ["metal"],

    # Popular skins
    "glory": ["glory"],
    "tempered": ["tempered"],
    "punishment": ["punishment"],
    "woodland": ["woodland"],
    "abstract": ["abstract"],
}

RUST_CATEGORY_EXCLUSIONS: Dict[str, List[str]] = {
    "weapon": ["door", "clothing", "armor"],
    "clothing": ["weapon", "door"],
}

ALL_CS2_KEYWORDS = CS2_FILTER_KEYWORDS.copy()
ALL_DOTA2_KEYWORDS = DOTA2_FILTER_KEYWORDS.copy()
ALL_TF2_KEYWORDS = TF2_FILTER_KEYWORDS.copy()
ALL_RUST_KEYWORDS = RUST_FILTER_KEYWORDS.copy()

GENERAL_EXCLUSIONS = ["case", "charm", "sticker", "souvenir", "patch", "key"]

def get_keyword_dict(game: str) -> Dict[str, List[str]]:
    if game == "dota2":
        return ALL_DOTA2_KEYWORDS
    elif game == "tf2":
        return ALL_TF2_KEYWORDS
    elif game == "rust":
        return ALL_RUST_KEYWORDS
    else:  # cs2
        return ALL_CS2_KEYWORDS

def get_exclusion_dict(game: str) -> Dict[str, List[str]]:
    if game == "dota2":
        return DOTA2_CATEGORY_EXCLUSIONS
    elif game == "tf2":
        return TF2_CATEGORY_EXCLUSIONS
    elif game == "rust":
        return RUST_CATEGORY_EXCLUSIONS
    else:  # cs2
        return CS2_CATEGORY_EXCLUSIONS

def parse_filters(raw: str, game: str = "cs2") -> List[str]:
    if not raw:
        return []

    keyword_dict = get_keyword_dict(game)
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    expanded: List[str] = []

    for p in parts:
        if p in keyword_dict:
            expanded.extend(keyword_dict[p])
        else:
            expanded.append(p)

    return sorted({tok for tok in expanded if tok})

def _has_word_token(name: str, token: str) -> bool:
    try:
        tok = re.escape(token)
        pattern = r'(?<![a-z])' + tok + r'(?![a-z])'
        return re.search(pattern, name.lower()) is not None
    except Exception:
        return token.lower() in name.lower()

def matches_filters(name: str, filters: List[str], item: Optional[Dict[str, Any]] = None, game: str = "cs2") -> bool:
    if not filters:
        return True

    name_l = name.lower() if name else ""
    keyword_dict = get_keyword_dict(game)
    exclusion_dict = get_exclusion_dict(game)

    for tok in filters:
        if _has_word_token(name, tok):
            # Check exclusions
            for cat, keywords in keyword_dict.items():
                if tok in keywords or tok == cat:
                    excl = exclusion_dict.get(cat, [])
                    if any(_has_word_token(name, ex) for ex in excl):
                        break
            return True
    if game == "cs2" and any(_has_word_token(name, ex) for ex in GENERAL_EXCLUSIONS):
        return False

    return True

    if game == "cs2":
        knife_tokens = set(ALL_CS2_KEYWORDS.get("knife", []))
        if any(tok in knife_tokens for tok in filters):
            if "★" in name:
                return True

    return False

def get_available_filters(game: str = "cs2") -> Dict[str, List[str]]:
    """Return categorized list of available filters for a game"""
    if game == "dota2":
        return {
            "Rarity": ["arcana", "immortal", "legendary", "mythical", "rare"],
            "Type": ["courier", "ward", "treasure", "bundle", "hero"],
            "Popular Heroes": ["pudge", "juggernaut", "invoker", "pa", "rubick"],
            "Quality": ["inscribed", "autographed", "genuine", "unusual"],
            "Events": ["ti", "compendium"]
        }
    elif game == "tf2":
        return {
            "Quality": ["unusual", "strange", "genuine", "vintage", "haunted"],
            "Item Types": ["hat", "weapon", "taunt", "misc", "cosmetic"],
            "Classes": ["scout", "soldier", "pyro", "demoman", "heavy", "engineer", "medic", "sniper", "spy"],
            "Special": ["australium", "killstreak", "festive", "botkiller"],
            "Other": ["crate", "key", "paint", "tool"]
        }
    elif game == "rust":
        return {
            "Weapons": ["ak-47", "lr-300", "mp5", "thompson", "python", "bolt"],
            "Clothing": ["hoodie", "pants", "jacket", "gloves", "boots", "mask"],
            "Armor": ["metal", "roadsign", "hazmat"],
            "Building": ["door", "sign", "rug", "bed"],
            "Tools": ["tool", "pickaxe", "hatchet"],
            "Popular Skins": ["glory", "tempered", "punishment", "woodland"]
        }
    else:  # cs2
        return {
            "Weapons": ["knife", "gloves", "rifle", "pistol", "smg", "sniper"],
            "Specific Weapons": ["ak-47", "m4a4", "m4a1-s", "awp"],
            "Knife Types": ["butterfly", "karambit", "bayonet"],
            "Other": ["case", "sticker"]
        }
