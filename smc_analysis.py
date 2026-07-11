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
# SWING HIGH / SWING LOW DETECTION
# ==========================================================

def find_swing_highs_lows(candles, lookback=2):
    """
    Finds real pivot points: a swing high is a candle whose high is
    higher than 'lookback' candles on both sides of it. A swing low
    is the mirror opposite. This is more accurate than just taking
    the highest/lowest price over a window, since it reflects actual
    market structure turning points.
    """

    swing_highs = []
    swing_lows = []

    n = len(candles)

    for i in range(lookback, n - lookback):

        window_before = candles[i - lookback:i]
        window_after = candles[i + 1:i + 1 + lookback]

        current_high = candles[i]["high"]
        current_low = candles[i]["low"]

        if (
            all(current_high > c["high"] for c in window_before)
            and all(current_high > c["high"] for c in window_after)
        ):
            swing_highs.append({"index": i, "price": current_high})

        if (
            all(current_low < c["low"] for c in window_before)
            and all(current_low < c["low"] for c in window_after)
        ):
            swing_lows.append({"index": i, "price": current_low})

    return swing_highs, swing_lows


# ==========================================================
# SUPPORT / RESISTANCE
# ==========================================================

def get_support_resistance(candles, window=30, swing_lookback=2):

    recent = candles[-window:]

    swing_highs, swing_lows = find_swing_highs_lows(recent, lookback=swing_lookback)

    # Prefer the most recent real swing point. Fall back to the simple
    # highest/lowest price in the window if no clear swing was found
    # (e.g. not enough candles, or a strongly one-directional move).
    if swing_lows:
        support = swing_lows[-1]["price"]
    else:
        support = min(c["low"] for c in recent)

    if swing_highs:
        resistance = swing_highs[-1]["price"]
    else:
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
# LIQUIDITY SWEEP
# ==========================================================

def detect_liquidity_sweep(candles, lookback=15):
    """
    Detects a liquidity sweep: a wick pierces beyond a recent
    swing high/low (hunting stop-losses / triggering breakout orders),
    but the candle closes back on the other side - a classic sign of
    a trap before a reversal.
    Returns "BUY" (bullish sweep of a low), "SELL" (bearish sweep of
    a high), or None.
    """

    if len(candles) < lookback + 2:
        return None

    recent = candles[-lookback:-1]
    current = candles[-1]

    recent_high = max(c["high"] for c in recent)
    recent_low = min(c["low"] for c in recent)

    # Bearish sweep: wick pokes above the recent high, but closes back below it
    if current["high"] > recent_high and current["close"] < recent_high:
        return "SELL"

    # Bullish sweep: wick pokes below the recent low, but closes back above it
    if current["low"] < recent_low and current["close"] > recent_low:
        return "BUY"

    return None


# ==========================================================
# ORDER BLOCK
# ==========================================================

def detect_order_block(candles, window=20, impulse_multiplier=1.5):
    """
    Finds the order block zone: the last opposite-colored candle right
    before a strong impulsive move, which is what actually defines an
    order block in SMC. Falls back to the simple high/low range of the
    window if no clear impulsive move is found.
    """

    recent = candles[-window:]

    if len(recent) < 5:
        zone_high = max(c["high"] for c in recent)
        zone_low = min(c["low"] for c in recent)
        return zone_low, zone_high

    bodies = [abs(c["close"] - c["open"]) for c in recent]
    avg_body = sum(bodies) / len(bodies)

    # Walk backwards looking for the most recent strong impulsive candle
    for i in range(len(recent) - 1, 0, -1):

        body = abs(recent[i]["close"] - recent[i]["open"])
        is_bullish_impulse = recent[i]["close"] > recent[i]["open"] and body > avg_body * impulse_multiplier
        is_bearish_impulse = recent[i]["close"] < recent[i]["open"] and body > avg_body * impulse_multiplier

        if is_bullish_impulse or is_bearish_impulse:
            ob_candle = recent[i - 1]
            return ob_candle["low"], ob_candle["high"]

    # No clear impulsive move found - fall back to the simple range
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
    liquidity_sweep,
    direction,
):

    score = 0

    # Higher timeframe trend
    if trend == direction:
        score += 20

    # EMA alignment
    if direction == "BUY" and ema_fast > ema_slow:
        score += 15

    if direction == "SELL" and ema_fast < ema_slow:
        score += 15

    # RSI
    if direction == "BUY" and rsi >= 55:
        score += 15

    if direction == "SELL" and rsi <= 45:
        score += 15

    # BOS
    if bos == direction:
        score += 15

    # CHOCH
    if choch == direction:
        score += 15

    # Liquidity sweep confirming this direction
    if liquidity_sweep == direction:
        score += 20

    return min(score, 100)


# ==========================================================
# TAKE PROFIT / STOP LOSS
# ==========================================================
# TP/SL ratios: SL = 2.0x ATR, TP1 = 2.0x ATR, TP2 = 3.0x ATR, TP3 = 4.0x ATR
# (wider stop loss so normal market noise/wicks don't trigger it early,
# with each target a clean multiple of the risk: 1:1, 1:1.5, 1:2)
#
# TP1 is also checked against support/resistance: if the raw ATR-based
# TP1 would land at or beyond a known resistance (BUY) or support (SELL)
# wall, it's pulled back to give the trade a realistic shot at actually
# reaching it, instead of requiring price to break straight through a
# wall on the very first attempt.

def calculate_targets(entry, atr, direction, support=None, resistance=None):

    if direction == "BUY":

        sl = entry - atr * 2.0

        tp1 = entry + atr * 2.0
        tp2 = entry + atr * 3.0
        tp3 = entry + atr * 4.0

        if resistance is not None and tp1 >= resistance:
            buffer = (resistance - entry) * 0.9
            if buffer > 0:
                tp1 = entry + buffer

    else:

        sl = entry + atr * 2.0

        tp1 = entry - atr * 2.0
        tp2 = entry - atr * 3.0
        tp3 = entry - atr * 4.0

        if support is not None and tp1 <= support:
            buffer = (entry - support) * 0.9
            if buffer > 0:
                tp1 = entry - buffer

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
    liquidity_sweep = detect_liquidity_sweep(candles)

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
            liquidity_sweep,
            "BUY"
        )

        # Hard requirement: either CHOCH or a liquidity sweep must also
        # confirm this direction. Without this, BOS alone can fire the
        # instant a level breaks, with no proof the move actually holds -
        # a classic fakeout setup. This forces real confirmation.
        confirmed = (choch == "BUY") or (liquidity_sweep == "BUY")

        if confidence >= 80 and confirmed:

            sl, tp1, tp2, tp3 = calculate_targets(
                entry,
                atr,
                "BUY",
                support=support,
                resistance=resistance,
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
            liquidity_sweep,
            "SELL"
        )

        confirmed = (choch == "SELL") or (liquidity_sweep == "SELL")

        if confidence >= 80 and confirmed:

            sl, tp1, tp2, tp3 = calculate_targets(
                entry,
                atr,
                "SELL",
                support=support,
                resistance=resistance,
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
