"""
SmartFX structural helpers (shared by V3).

This is NOT the old V2 strategy engine (analyze_candles was removed).
It only provides technical-analysis building blocks imported by
smc_analysis_v3.py after the full old smc_analysis.py was deleted.
"""

def calculate_ema(prices, period):
    if len(prices) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(prices[:period]) / period

    for price in prices[period:]:
        ema = ((price - ema) * multiplier) + ema

    return ema

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

def detect_doji_reversal(candles):
    """
    Checks the most recent candle for a Dragonfly Doji (small body near
    the top, long lower wick, tiny upper wick - a bullish rejection of
    lower prices) or its mirror, a Gravestone Doji (small body near the
    bottom, long upper wick - a bearish rejection of higher prices).
    Returns "BUY", "SELL", or None.
    """

    if not candles:
        return None

    c = candles[-1]

    candle_range = c["high"] - c["low"]

    if candle_range == 0:
        return None

    body = abs(c["close"] - c["open"])
    upper_wick = c["high"] - max(c["close"], c["open"])
    lower_wick = min(c["close"], c["open"]) - c["low"]

    if (
        lower_wick > candle_range * 0.6
        and upper_wick < candle_range * 0.1
        and body < candle_range * 0.2
    ):
        return "BUY"

    if (
        upper_wick > candle_range * 0.6
        and lower_wick < candle_range * 0.1
        and body < candle_range * 0.2
    ):
        return "SELL"

    return None

def is_strong_candle_body(candle, min_body_ratio=0.6):
    candle_range = candle["high"] - candle["low"]

    if candle_range == 0:
        return False

    body = abs(candle["close"] - candle["open"])

    return (body / candle_range) >= min_body_ratio

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

def inside_order_block(price, zone_low, zone_high, buffer=0.002):

    return (
        zone_low * (1 - buffer)
        <= price
        <= zone_high * (1 + buffer)
    )

def calculate_risk(entry, atr):

    if atr is None:
        return "🟡 MEDIUM"

    ratio = atr / entry

    if ratio < 0.004:
        return "🟢 LOW"

    if ratio < 0.009:
        return "🟡 MEDIUM"

    return "🔴 HIGH"

SMC_VERSION = "helpers-only-1.0"
