"""
SmartFX Signal Bot - UPGRADED HIGH-SPEED VERSION
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
    if not gains:
        return 50
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
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
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

def get_support_resistance(candles, window=20):
    recent_candles = candles[-window:]
    highs = [c["high"] for c in recent_candles]
    lows = [c["low"] for c in recent_candles]
    resistance = max(highs)
    support = min(lows)
    return support, resistance

def analyze_candles(candles, trend_4h=None):
    # --- AUTOMATED ORDER BLOCK DETECTION ---
    recent_swing = candles[-20:]
    highs = [c["high"] for c in recent_swing]
    lows = [c["low"] for c in recent_swing]
    zone_high = max(highs)
    zone_low = min(lows)
    
    current_price = candles[-1]['close']
    buffer = 0.005 
    if not (zone_low * (1 - buffer) <= current_price <= zone_high * (1 + buffer)):
        return None 
    # ----------------------------------------

    closes = [c["close"] for c in candles]
    ema_fast = calculate_ema(closes, 9)
    ema_slow = calculate_ema(closes, 21)
    rsi = calculate_rsi(closes, period=9)
    atr = calculate_atr(candles)

    if ema_fast is None or ema_slow is None or rsi is None or atr is None:
        return None

    entry = closes[-1]
    support, resistance = get_support_resistance(candles, window=20)
    strength = market_strength(candles)
    
    if strength < atr * 0.4: return None

    # Risk level calculation
    risk_level = "🟢 LOW"
    if atr > entry * 0.01:
        risk_level = "🔴 HIGH"
    elif atr > entry * 0.005:
        risk_level = "🟡 MEDIUM"

    # ================= BUY (LONG) =================
    if ema_fast > ema_slow and (rsi > 55):
        confidence = 60
        if trend_4h == "BUY":
            confidence += 25
        
        if confidence >= 80:
            sl = entry - (atr * 1.5)
            tp1 = entry + (atr * 0.75)
            tp2 = entry + (atr * 1.5)
            tp3 = entry + (atr * 2.8)
            return {
                "direction": "🟢 BUY", "confidence": min(confidence, 100),
                "risk": risk_level, "entry": round(entry, 6),
                "sl": round(sl, 6), "tp1": round(tp1, 6),
                "tp2": round(tp2, 6), "tp3": round(tp3, 6),
            }

    # ================= SELL (SHORT) =================
    if ema_fast < ema_slow and (rsi < 45):
        confidence = 60
        if trend_4h == "SELL":
            confidence += 25
            
        if confidence >= 80:
            sl = entry + (atr * 1.5)
            tp1 = entry - (atr * 0.75)
            tp2 = entry - (atr * 1.5)
            tp3 = entry - (atr * 2.8)
            return {
                "direction": "🔴 SELL", "confidence": min(confidence, 100),
                "risk": risk_level, "entry": round(entry, 6),
                "sl": round(sl, 6), "tp1": round(tp1, 6),
                "tp2": round(tp2, 6), "tp3": round(tp3, 6),
            }

    return None
