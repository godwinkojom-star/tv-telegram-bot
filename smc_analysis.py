"""
==========================================================
 SmartFX Signal Bot V2.0
 Strategy Engine
 Part 1
==========================================================
"""

from math import fabs


# ==========================================================
# EMA
# ==========================================================

def calculate_ema(prices, period):
    if len(prices) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(prices[:period]) / period

    for price in prices[period:]:
        ema = ((price - ema) * multiplier) + ema

    return ema


# ==========================================================
# RSI
# ==========================================================

def calculate_rsi(closes, period=9):

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


# ==========================================================
# ATR
# ==========================================================

def calculate_atr(candles, period=14):

    if len(candles) < period + 1:
        return None

    tr_values = []

    for i in range(1, len(candles)):

        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )

        tr_values.append(tr)

    return sum(tr_values[-period:]) / period


# ==========================================================
# SUPPORT / RESISTANCE
# ==========================================================

def get_support_resistance(candles, window=30):

    recent = candles[-window:]

    support = min(c["low"] for c in recent)
    resistance = max(c["high"] for c in recent)

    return support, resistance


# ==========================================================
# MARKET STRENGTH
# ==========================================================

def market_strength(candles, period=14):

    closes = [c["close"] for c in candles[-period:]]

    movement = 0

    for i in range(1, len(closes)):
        movement += fabs(closes[i] - closes[i - 1])

    return movement / period


# ==========================================================
# EMA 200 TREND
# ==========================================================

def get_trend_direction(candles_4h):

    if len(candles_4h) < 200:
        return None

    closes = [c["close"] for c in candles_4h]

    ema200 = calculate_ema(closes, 200)

    price = closes[-1]

    if price > ema200:
        return "BUY"

    if price < ema200:
        return "SELL"

    return None

# ==========================================================
# BREAK OF STRUCTURE (BOS)
# ==========================================================

def detect_bos(candles, lookback=10):
    """
    Detects a simple Break of Structure.
    Returns:
        "BUY"
        "SELL"
        None
    """

    if len(candles) < lookback + 5:
        return None

    highs = [c["high"] for c in candles[-lookback:]]
    lows = [c["low"] for c in candles[-lookback:]]

    current_close = candles[-1]["close"]

    if current_close > max(highs[:-1]):
        return "BUY"

    if current_close < min(lows[:-1]):
        return "SELL"

    return None


# ==========================================================
# CHANGE OF CHARACTER (CHOCH)
# ==========================================================

def detect_choch(candles, lookback=15):
    """
    Simple Change of Character detection.
    """

    if len(candles) < lookback + 5:
        return None

    previous = candles[-lookback:-1]

    highest = max(c["high"] for c in previous)
    lowest = min(c["low"] for c in previous)

    current = candles[-1]["close"]

    if current > highest:
        return "BUY"

    if current < lowest:
        return "SELL"

    return None


# ==========================================================
# ORDER BLOCK
# ==========================================================

def detect_order_block(candles, window=20):
    """
    Finds the recent order block zone.
    """

    recent = candles[-window:]

    zone_high = max(c["high"] for c in recent)
    zone_low = min(c["low"] for c in recent)

    return zone_low, zone_high


# ==========================================================
# CHECK IF PRICE IS INSIDE ORDER BLOCK
# ==========================================================

def inside_order_block(price, zone_low, zone_high, buffer=0.002):

    return (
        zone_low * (1 - buffer)
        <= price
        <= zone_high * (1 + buffer)
    )


# ==========================================================
# RISK LEVEL
# ==========================================================

def calculate_risk(entry, atr):

    if atr is None:
        return "🟡 MEDIUM"

    ratio = atr / entry

    if ratio < 0.004:
        return "🟢 LOW"

    if ratio < 0.009:
        return "🟡 MEDIUM"

    return "🔴 HIGH"


# ==========================================================
# CONFIDENCE SCORE
# ==========================================================

def calculate_confidence(
    trend,
    ema_fast,
    ema_slow,
    rsi,
    bos,
    choch,
    direction,
):

    score = 0

    # Higher timeframe trend
    if trend == direction:
        score += 25

    # EMA alignment
    if direction == "BUY" and ema_fast > ema_slow:
        score += 20

    if direction == "SELL" and ema_fast < ema_slow:
        score += 20

    # RSI
    if direction == "BUY" and rsi >= 55:
        score += 15

    if direction == "SELL" and rsi <= 45:
        score += 15

    # BOS
    if bos == direction:
        score += 20

    # CHOCH
    if choch == direction:
        score += 20

    return min(score, 100)


# ==========================================================
# TAKE PROFIT / STOP LOSS
# ==========================================================
# TP ratios: TP1 = 1.5x ATR, TP2 = 2.0x ATR, TP3 = 3.0x ATR
# (updated from the original 1.0 / 2.0 / 3.0 spacing)

def calculate_targets(entry, atr, direction):

    if direction == "BUY":

        sl = entry - atr * 1.5

        tp1 = entry + atr * 1.5
        tp2 = entry + atr * 2.0
        tp3 = entry + atr * 3.0

    else:

        sl = entry + atr * 1.5

        tp1 = entry - atr * 1.5
        tp2 = entry - atr * 2.0
        tp3 = entry - atr * 3.0

    return (
        round(sl, 6),
        round(tp1, 6),
        round(tp2, 6),
        round(tp3, 6),
    )


# ==========================================================
# MAIN ANALYSIS
# ==========================================================

def analyze_candles(candles, trend_4h=None):

    if len(candles) < 50:
        return None

    closes = [c["close"] for c in candles]

    ema_fast = calculate_ema(closes, 9)
    ema_slow = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes, 9)
    atr = calculate_atr(candles)

    if None in (ema_fast, ema_slow, rsi, atr):
        return None

    entry = closes[-1]

    support, resistance = get_support_resistance(candles)

    strength = market_strength(candles)

    # Ignore weak markets
    if strength < atr * 0.30:
        return None

    bos = detect_bos(candles)
    choch = detect_choch(candles)

    zone_low, zone_high = detect_order_block(candles)

    if not inside_order_block(entry, zone_low, zone_high):
        return None

    # ================= BUY =================

    if (
        trend_4h == "BUY"
        and ema_fast > ema_slow
        and rsi >= 55
    ):

        confidence = calculate_confidence(
            trend_4h,
            ema_fast,
            ema_slow,
            rsi,
            bos,
            choch,
            "BUY"
        )

        if confidence >= 80:

            sl, tp1, tp2, tp3 = calculate_targets(
                entry,
                atr,
                "BUY"
            )

            return {
                "direction": "🟢 BUY",
                "entry": round(entry, 6),
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "confidence": confidence,
                "risk": calculate_risk(entry, atr),
                "support": round(support, 6),
                "resistance": round(resistance, 6),
            }

    # ================= SELL =================

    if (
        trend_4h == "SELL"
        and ema_fast < ema_slow
        and rsi <= 45
    ):

        confidence = calculate_confidence(
            trend_4h,
            ema_fast,
            ema_slow,
            rsi,
            bos,
            choch,
            "SELL"
        )

        if confidence >= 80:

            sl, tp1, tp2, tp3 = calculate_targets(
                entry,
                atr,
                "SELL"
            )

            return {
                "direction": "🔴 SELL",
                "entry": round(entry, 6),
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "confidence": confidence,
                "risk": calculate_risk(entry, atr),
                "support": round(support, 6),
                "resistance": round(resistance, 6),
            }

    return None
