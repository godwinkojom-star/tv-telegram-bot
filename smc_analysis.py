"""
SmartFX Signal Bot - FINAL VERSION (WITH EMOJIS)
EMA + RSI + ATR + Confidence + Risk Filter
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

    entry = closes[-1]

    # Market filter
    strength = market_strength(candles)
    if strength < atr * 0.5:
        return None

    # Risk level
    risk_level = "🟢 LOW"
    if atr > entry * 0.01:
        risk_level = "🔴 HIGH"
    elif atr > entry * 0.005:
        risk_level = "🟡 MEDIUM"

    # ================= BUY =================
    if ema20 > ema50 and rsi > 60:

        sl = entry - (atr * 1.5)

        confidence = 50

        gap_pct = abs(ema20 - ema50) / entry * 100

        if gap_pct > 1:
            confidence += 15
        elif gap_pct > 0.5:
            confidence += 10

        if rsi > 70:
            confidence += 20
        elif rsi > 65:
            confidence += 15
        elif rsi > 60:
            confidence += 10

        if strength > atr:
            confidence += 15

        if confidence < 80:
            return None
            
        tp1 = entry + atr
        tp2 = entry + (atr * 2)
        tp3 = entry + (atr * 3.5)

        return {
            "direction": "🟢 BUY",
            "confidence": confidence,
            "risk": risk_level,
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "tp3": round(tp3, 6),
        }

    # ================= SELL =================
    if ema20 < ema50 and rsi < 40:

        sl = entry + (atr * 1.5)

        confidence = 50

        gap_pct = abs(ema20 - ema50) / entry * 100

        if gap_pct > 1:
            confidence += 15
        elif gap_pct > 0.5:
            confidence += 10

        if rsi < 30:
            confidence += 20
        elif rsi < 35:
            confidence += 15
        elif rsi < 40:
            confidence += 10

        if strength > atr:
            confidence += 15

        if confidence < 80:
            return None
            
        tp1 = entry - atr
        tp2 = entry - (atr * 2)
        tp3 = entry - (atr * 3.5)

        return {
            "direction": "🔴 SELL",
            "confidence": confidence,
            "risk": risk_level,
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "tp3": round(tp3, 6),
        }

    return None
