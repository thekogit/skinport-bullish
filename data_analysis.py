#!/usr/bin/env python3


import math
from typing import Dict, Any, List, Tuple, Optional

from config import (
    STEAM_FEE_RATE, MIN_PROFIT_PERCENTAGE, GOOD_PROFIT_PERCENTAGE, 
    HIGH_VOLUME_THRESHOLD, EXPLOSIVENESS_WEIGHTS, MOMENTUM_SHORT_CAP,
    MOMENTUM_MED_CAP, SCARCITY_THRESHOLD, DISCOUNT_CAP, VOLUME_SURGE_MULTIPLIER,
    PUMP_DETECTION
)

def compute_fee_aware_arbitrage_opportunity(skinport_price: float, steam_price: Optional[float], 
                                        skinport_volume: int, currency: str = "USD") -> Tuple[str, float, Dict[str, float]]:
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