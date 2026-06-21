"""
SmartFX Signal Bot - FINAL STABLE VERSION
EMA + RSI + ATR + Confidence + Market Filter
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


def market_strength(candles, period=14):
    if len(candles) < period + 1:
        return 0

    closes = [c["close"] for c in candles[-period:]]

    moves = []
    for i in range(1, len(closes)):
        moves.append(abs(closes[i] - closes[i - 1]))

    return sum(moves) / period


def analyze_candles(candles):
    if len(candles) < 60:
        return None

    closes = [c["close"] for c in candles]

    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles)

    if ema20 is None or ema50 is None or rsi is None or atr is None:
        return None

    strength = market_strength(candles)
    if strength < atr * 0.5:
        return None

    entry = closes[-1]

    # BUY SETUP
    if ema20 > ema50 and rsi > 55:
        sl = entry - (atr * 1.5)

        risk = entry - sl
        if risk <= 0:
            return None

        confidence = 0
        if ema20 > ema50:
            confidence += 40
        if rsi > 55:
            confidence += 30
        if entry > ema20:
            confidence += 30

        tp1 = entry + (atr * 1.0)
        tp2 = entry + (atr * 2.0)
        tp3 = entry + (atr * 3.5)

        return {
            "direction": "BUY",
            "confidence": confidence,
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "tp3": round(tp3, 6),
        }

    # SELL SETUP
    if ema20 < ema50 and rsi < 45:
        sl = entry + (atr * 1.5)

        risk = sl - entry
        if risk <= 0:
            return None

        confidence = 0
        if ema20 < ema50:
            confidence += 40
        if rsi < 45:
            confidence += 30
        if entry < ema20:
            confidence += 30

        tp1 = entry - (atr * 1.0)
        tp2 = entry - (atr * 2.0)
        tp3 = entry - (atr * 3.5)

        return {
            "direction": "SELL",
            "confidence": confidence,
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "tp3": round(tp3, 6),
        }

    return None
