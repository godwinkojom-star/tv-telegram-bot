import math


# ---------------- INDICATORS ----------------

def calculate_ema(prices, period):
    if len(prices) < period:
        return None

    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period

    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


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


def market_strength(candles):
    closes = [c["close"] for c in candles[-20:]]

    moves = []
    for i in range(1, len(closes)):
        moves.append(abs(closes[i] - closes[i - 1]))

    return sum(moves) / len(moves) if moves else 0


# ---------------- MAIN STRATEGY ----------------

def analyze_candles(candles, trend_4h=None):

    if len(candles) < 30:
        return None

    closes = [c["close"] for c in candles]

    ema_fast = calculate_ema(closes, 9)
    ema_slow = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles)

    if None in (ema_fast, ema_slow, rsi, atr):
        return None

    entry = closes[-1]

    strength = market_strength(candles)

    # 🔥 relaxed filter so signals actually appear
    if strength < atr * 0.2:
        return None

    # ---------------- BUY ----------------
    if ema_fast > ema_slow and rsi > 50:

        confidence = 60
        if trend_4h == "BUY":
            confidence += 25

        if confidence >= 65:
            return {
                "direction": "🟢 BUY",
                "confidence": confidence,
                "entry": entry,
                "sl": entry - atr,
                "tp1": entry + atr * 0.8,
                "tp2": entry + atr * 1.5,
                "tp3": entry + atr * 2.5,
                "risk": "MEDIUM"
            }

    # ---------------- SELL ----------------
    if ema_fast < ema_slow and rsi < 50:

        confidence = 60
        if trend_4h == "SELL":
            confidence += 25

        if confidence >= 65:
            return {
                "direction": "🔴 SELL",
                "confidence": confidence,
                "entry": entry,
                "sl": entry + atr,
                "tp1": entry - atr * 0.8,
                "tp2": entry - atr * 1.5,
                "tp3": entry - atr * 2.5,
                "risk": "MEDIUM"
            }

    return None


# ---------------- TREND FILTER ----------------

def get_trend_direction(candles_4h):
    if len(candles_4h) < 50:
        return None

    closes = [c["close"] for c in candles_4h]

    ema_200 = calculate_ema(closes, 200)
    if not ema_200:
        return None

    return "BUY" if closes[-1] > ema_200 else "SELL"
