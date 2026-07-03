"""
SmartFX Signal Bot - FIXED STRATEGY ENGINE
"""

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

    gains, losses = [], []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_atr(candles, period=14):
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


def analyze_candles(candles, trend_4h=None):

    closes = [c["close"] for c in candles]
    ema_fast = calculate_ema(closes, 9)
    ema_slow = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes)
    atr = calculate_atr(candles)

    if None in (ema_fast, ema_slow, rsi, atr):
        return None

    entry = closes[-1]

    # ---------------- BUY ----------------
    if ema_fast > ema_slow and rsi > 55:
        confidence = 60

        if trend_4h == "BUY":
            confidence += 25

        if confidence >= 80:
            sl = entry - (atr * 1.5)
            tp1 = entry + (atr * 0.75)
            tp2 = entry + (atr * 1.5)
            tp3 = entry + (atr * 2.5)

            return {
                "direction": "🟢 BUY",
                "confidence": confidence,
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk": "AUTO"
            }

    # ---------------- SELL ----------------
    if ema_fast < ema_slow and rsi < 45:
        confidence = 60

        if trend_4h == "SELL":
            confidence += 25

        if confidence >= 80:
            sl = entry + (atr * 1.5)
            tp1 = entry - (atr * 0.75)
            tp2 = entry - (atr * 1.5)
            tp3 = entry - (atr * 2.5)

            return {
                "direction": "🔴 SELL",
                "confidence": confidence,
                "entry": entry,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "risk": "AUTO"
            }

    return None


def get_trend_direction(candles):
    if len(candles) < 200:
        return None

    closes = [c["close"] for c in candles]
    ema_200 = calculate_ema(closes, 200)

    if closes[-1] > ema_200:
        return "BUY"
    elif closes[-1] < ema_200:
        return "SELL"

    return None
