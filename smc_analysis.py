"""
EMA + RSI Strategy
No external libraries required.
"""

def calculate_ema(prices, period):
    if len(prices) < period:
        return None

    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period

    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def recent_swing_low(candles, lookback=10):
    return min(c["low"] for c in candles[-lookback:])


def recent_swing_high(candles, lookback=10):
    return max(c["high"] for c in candles[-lookback:])


def analyze_candles(candles):
    if len(candles) < 60:
        return None

    closes = [c["close"] for c in candles]

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes)

    if ema20 is None or ema50 is None or rsi is None:
        return None

    entry = closes[-1]

    # BUY SETUP
    if ema20 > ema50 and rsi > 55:
        sl = recent_swing_low(candles)
        risk = entry - sl

        if risk <= 0:
            return None

        return {
            "direction": "BUY",
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(entry * 1.01, 6),
            "tp2": round(entry * 1.02, 6),
            "tp3": round(entry * 1.03, 6),
        }

    # SELL SETUP
    if ema20 < ema50 and rsi < 45:
        sl = recent_swing_high(candles)
        risk = sl - entry

        if risk <= 0:
            return None

        return {
            "direction": "SELL",
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(entry * 0.99, 6),
            "tp2": round(entry * 0.98, 6),
            "tp3": round(entry * 0.97, 6),
        }

    return None
