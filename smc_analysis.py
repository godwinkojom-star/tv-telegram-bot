"""
SmartFX Signal Bot V2 - SMC Analysis Engine
Clean, fast, and trend-following signal system
"""

from typing import List, Dict, Optional


# =========================
# 📊 BASIC INDICATORS
# =========================

def calculate_ema(prices: List[float], period: int) -> Optional[float]:
    if len(prices) < period:
        return None

    ema = sum(prices[:period]) / period
    multiplier = 2 / (period + 1)

    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


def calculate_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_atr(candles: List[Dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    return sum(trs[-period:]) / period


# =========================
# 📉 MARKET STRUCTURE
# =========================

def get_support_resistance(candles: List[Dict], window: int = 20):
    recent = candles[-window:]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]

    return min(lows), max(highs)


def detect_swing_points(candles: List[Dict], lookback: int = 3):
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    swing_high = max(highs[-lookback * 5:])
    swing_low = min(lows[-lookback * 5:])

    return swing_high, swing_low


# =========================
# 🧠 TREND FILTER (4H)
# =========================

def get_trend_direction(candles_4h: List[Dict]) -> Optional[str]:
    """
    Returns:
    - 'BUY'  → bullish trend
    - 'SELL' → bearish trend
    """

    if len(candles_4h) < 200:
        return None

    closes = [c["close"] for c in candles_4h]
    ema_200 = calculate_ema(closes, 200)

    if not ema_200:
        return None

    return "BUY" if closes[-1] > ema_200 else "SELL"


# =========================
# 📡 MAIN ANALYSIS ENGINE
# =========================

def analyze_candles(candles: List[Dict], trend_4h: str = None):

    if len(candles) < 50:
        return None

    closes = [c["close"] for c in candles]

    ema_fast = calculate_ema(closes, 9)
    ema_slow = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles)

    if None in (ema_fast, ema_slow, rsi, atr):
        return None

    entry = closes[-1]

    support, resistance = get_support_resistance(candles)
    swing_high, swing_low = detect_swing_points(candles)

    # =========================
    # 📊 TREND ALIGNMENT RULE
    # =========================

    if trend_4h is None:
        return None

    # ONLY BUY in bullish trend
    if trend_4h == "BUY":
        if not (ema_fast > ema_slow and rsi > 50):
            return None

        direction = "🟢 BUY"

        sl = entry - (atr * 1.5)
        tp1 = entry + (atr * 1.0)
        tp2 = entry + (atr * 2.0)
        tp3 = entry + (atr * 3.0)

    # ONLY SELL in bearish trend
    elif trend_4h == "SELL":
        if not (ema_fast < ema_slow and rsi < 50):
            return None

        direction = "🔴 SELL"

        sl = entry + (atr * 1.5)
        tp1 = entry - (atr * 1.0)
        tp2 = entry - (atr * 2.0)
        tp3 = entry - (atr * 3.0)

    else:
        return None

    # =========================
    # 🎯 CONFIDENCE SCORE
    # =========================

    confidence = 60

    if abs(ema_fast - ema_slow) > atr * 0.2:
        confidence += 10

    if (trend_4h == "BUY" and rsi > 55) or (trend_4h == "SELL" and rsi < 45):
        confidence += 15

    if entry > support and entry < resistance:
        confidence += 10

    confidence = min(confidence, 100)

    if confidence < 75:
        return None

    # =========================
    # 📤 FINAL SIGNAL OUTPUT
    # =========================

    return {
        "direction": direction,
        "entry": round(entry, 5),
        "sl": round(sl, 5),
        "tp1": round(tp1, 5),
        "tp2": round(tp2, 5),
        "tp3": round(tp3, 5),
        "confidence": confidence,
        "risk": "Low" if atr < entry * 0.01 else "Medium"
    }
