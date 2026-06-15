"""
Core SMC (Smart Money Concepts) + Fibonacci analysis logic.
Translated from the Pine Script draft.
"""

def find_swings(highs, lows, swing_length=10):
    """
    Find the most recent confirmed swing high and swing low
    using a simple pivot detection (swing_length bars on each side).
    Returns (swing_high, swing_high_idx, swing_low, swing_low_idx)
    """
    n = len(highs)
    swing_high = None
    swing_high_idx = None
    swing_low = None
    swing_low_idx = None

    for i in range(n - swing_length - 1, swing_length - 1, -1):
        window_highs = highs[i - swing_length: i + swing_length + 1]
        window_lows = lows[i - swing_length: i + swing_length + 1]

        if swing_high is None and highs[i] == max(window_highs):
            swing_high = highs[i]
            swing_high_idx = i

        if swing_low is None and lows[i] == min(window_lows):
            swing_low = lows[i]
            swing_low_idx = i

        if swing_high is not None and swing_low is not None:
            break

    return swing_high, swing_high_idx, swing_low, swing_low_idx


def analyze_candles(candles, swing_length=10):
    """
    candles: list of dicts with keys 'open','high','low','close' (oldest to newest)
    Returns a signal dict if a setup is found, else None.
    """
    if len(candles) < swing_length * 3:
        return None

    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    closes = [c['close'] for c in candles]

    swing_high, sh_idx, swing_low, sl_idx = find_swings(highs, lows, swing_length)

    if swing_high is None or swing_low is None:
        return None

    last_close = closes[-1]
    last_high = highs[-1]
    last_low = lows[-1]
    rng = swing_high - swing_low

    if rng <= 0:
        return None

    bullish_bos = last_close > swing_high
    bearish_bos = last_close < swing_low

    fib_618_bull = swing_high - rng * 0.618
    fib_786_bull = swing_high - rng * 0.786

    fib_618_bear = swing_low + rng * 0.618
    fib_786_bear = swing_low + rng * 0.786

    if bullish_bos and fib_786_bull <= last_low <= fib_618_bull:
        return {
            'direction': 'BUY',
            'entry': round(last_close, 6),
            'sl': round(swing_low - rng * 0.05, 6),
            'tp1': round(swing_high - rng * 0.5, 6),
            'tp2': round(swing_high, 6),
            'tp3': round(swing_high + rng * 0.272, 6),
        }

    if bearish_bos and fib_618_bear <= last_high <= fib_786_bear:
        return {
            'direction': 'SELL',
            'entry': round(last_close, 6),
            'sl': round(swing_high + rng * 0.05, 6),
            'tp1': round(swing_low + rng * 0.5, 6),
            'tp2': round(swing_low, 6),
            'tp3': round(swing_low - rng * 0.272, 6),
        }

    return None
