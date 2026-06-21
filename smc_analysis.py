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

    # Market strength filter
    strength = market_strength(candles)
    if strength < atr * 0.5:
        return None

    # Risk level classification
    risk_level = "LOW"
    if atr > entry * 0.01:
        risk_level = "HIGH"
    elif atr > entry * 0.005:
        risk_level = "MEDIUM"

    # ======================
    # BUY SETUP
    # ======================
    if ema20 > ema50 and rsi > 55:

        sl = entry - (atr * 1.5)
        risk = entry - sl

        if risk <= 0:
            return None

        # Confidence calculation
        confidence = 0
        if ema20 > ema50:
            confidence += 40
        if rsi > 55:
            confidence += 30
        if entry > ema20:
            confidence += 30

        # Take profits (ATR-based)
        tp1 = entry + (atr * 1.0)
        tp2 = entry + (atr * 2.0)
        tp3 = entry + (atr * 3.5)

        return {
            "direction": "BUY",
            "confidence": confidence,
            "risk": risk_level,
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "tp3": round(tp3, 6),
        }

    # ======================
    # SELL SETUP
    # ======================
    if ema20 < ema50 and rsi < 45:

        sl = entry + (atr * 1.5)
        risk = sl - entry

        if risk <= 0:
            return None

        # Confidence calculation
        confidence = 0
        if ema20 < ema50:
            confidence += 40
        if rsi < 45:
            confidence += 30
        if entry < ema20:
            confidence += 30

        # Take profits (ATR-based)
        tp1 = entry - (atr * 1.0)
        tp2 = entry - (atr * 2.0)
        tp3 = entry - (atr * 3.5)

        return {
            "direction": "SELL",
            "confidence": confidence,
            "risk": risk_level,
            "entry": round(entry, 6),
            "sl": round(sl, 6),
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "tp3": round(tp3, 6),
        }

    return None
