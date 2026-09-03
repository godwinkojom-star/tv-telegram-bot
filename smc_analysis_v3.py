"""
==========================================================
 SmartFX Signal Bot V3
 Strategy Engine — Multi-Timeframe SMC Pipeline
==========================================================

Architecture (per the blueprint):

    4H  -> Market Bias
    1H  -> SMC Setup (liquidity sweep -> displacement -> BOS/CHoCH)
    15M -> Entry Zone (Order Block + FVG)
    5M  -> Entry Trigger (reaction -> structure shift -> displacement)
    ANTI-CHASE FILTER
    RISK / REWARD
    -> FINAL SIGNAL

Philosophy (from the blueprint, kept verbatim as the design intent):
    Not "how many indicators agree" - instead: is the market in the
    right direction, did a real setup happen, are we entering at the
    right location, is the move still fresh, and is the trade worth
    the risk.

This means V3 is a SEQUENTIAL GATE, not a weighted score. Each stage
either passes or the pipeline stops - there is no point-stacking that
lets a bunch of correlated confirmations inflate a number the way V2's
calculate_confidence() did (see V2's SMC_VERSION 1.1.0 notes + the
SmartFX decision-window analysis: the two heaviest-weighted V2 factors,
adx_strong and bos, actually correlated with a LOWER win rate in real
trade history - a classic symptom of additive scoring rewarding
correlated-but-not-predictive factors). V3 sidesteps that class of bug
by construction: "confidence" here is a REPORTED quality label after
every gate has already passed, never the thing deciding whether a
signal fires.

Reuses generic structural-detection functions from smc_analysis.py
(V2) directly - those (swing highs/lows, BOS, CHoCH, liquidity sweep,
order block, entry-extension check) are plain technical-analysis
building blocks, not part of V2's confidence-scoring logic, so there's
no reason to duplicate them. V2 itself is left completely unmodified -
this file only imports from it.
"""

from smc_analysis import (
    calculate_atr,
    calculate_ema,
    calculate_risk,
    find_swing_highs_lows,
    detect_bos,
    detect_choch,
    detect_liquidity_sweep,
    detect_order_block,
    inside_order_block,
    is_strong_candle_body,
    detect_doji_reversal,
    get_support_resistance,
)

V3_VERSION = "3.0.0-draft1"


# ==========================================================
# STAGE 1 — 4H MARKET BIAS
# ==========================================================

def get_4h_bias(candles_4h, ema_period=200):
    """
    Determines market bias from 4H structure. Structure (BOS) is the
    primary signal, per the blueprint - EMA200 is only supporting
    information, used to break a genuine tie/ambiguous case, never to
    override a clear structural break.

    Returns (direction, source) where direction is "BUY", "SELL", or
    None (unclear/mixed -> no trade, exactly as the blueprint
    specifies), and source is "BOS", "CHOCH", or "EMA200" - tracked
    so we can later check whether the weaker EMA-fallback path
    actually performs differently from real-structure bias, instead
    of just assuming it's fine.
    """
    if len(candles_4h) < 30:
        return None, None

    bos = detect_bos(candles_4h, lookback=20)
    choch = detect_choch(candles_4h, lookback=20)

    # Clear bias: BOS and CHoCH agree (or CHoCH hasn't fired against
    # the BOS direction). Contradiction between the two = genuinely
    # mixed structure -> no trade.
    if bos is not None and choch is not None and bos != choch:
        return None, None

    if bos is not None:
        return bos, "BOS"

    if choch is not None:
        return choch, "CHOCH"

    # No clear structural break at all in the lookback window - fall
    # back to EMA200 as supporting info only, per the blueprint. This
    # is a WEAKER signal than a structural break, used only so the
    # pipeline doesn't starve entirely on quiet/ranging 4H charts.
    if len(candles_4h) >= ema_period:
        closes = [c["close"] for c in candles_4h]
        ema200 = calculate_ema(closes, ema_period)
        if ema200 is not None:
            if closes[-1] > ema200:
                return "BUY", "EMA200"
            if closes[-1] < ema200:
                return "SELL", "EMA200"

    return None, None


# ==========================================================
# STAGE 2 — 1H SMC SETUP
# ==========================================================

def _find_most_recent(candles, detector_fn, search_window=20, min_candles=15, **kwargs):
    """
    Scans backward over the last `search_window` candles, calling
    detector_fn on the candle history truncated at each point, to find
    the most recent index where it fired and what direction it fired.
    Needed because detect_bos/detect_choch/detect_liquidity_sweep only
    ever look at the LAST candle relative to their own lookback - to
    check ordering ("did the sweep happen before the break") we need
    to know WHEN each one most recently fired, not just whether either
    has fired at all somewhere in the past.

    Returns (index, direction) for the most recent hit, or (None, None).
    """
    n = len(candles)
    earliest = max(min_candles, n - search_window)

    for i in range(n - 1, earliest - 1, -1):
        result = detector_fn(candles[: i + 1], **kwargs)
        if result is not None:
            return i, result

    return None, None


def detect_1h_setup(candles_1h, bias, search_window=20):
    """
    Looks for: liquidity sweep -> displacement -> BOS/CHoCH, all in
    the 4H bias direction, on the 1H timeframe.

    Returns a dict:
        {
            "valid": bool,
            "sweep_index": int or None,
            "structure_index": int or None,
            "structure_type": "BOS" or "CHOCH" or None,
            "displacement_ok": bool,
        }
    """
    result = {
        "valid": False,
        "sweep_index": None,
        "structure_index": None,
        "structure_type": None,
        "displacement_ok": False,
    }

    if bias not in ("BUY", "SELL") or len(candles_1h) < 25:
        return result

    sweep_index, sweep_dir = _find_most_recent(
        candles_1h, detect_liquidity_sweep, search_window=search_window
    )

    bos_index, bos_dir = _find_most_recent(
        candles_1h, detect_bos, search_window=search_window
    )
    choch_index, choch_dir = _find_most_recent(
        candles_1h, detect_choch, search_window=search_window
    )

    # Prefer whichever structural break (BOS or CHoCH) is more recent,
    # as long as it agrees with bias.
    structure_index, structure_dir, structure_type = None, None, None
    candidates = []
    if bos_index is not None and bos_dir == bias:
        candidates.append((bos_index, bos_dir, "BOS"))
    if choch_index is not None and choch_dir == bias:
        candidates.append((choch_index, choch_dir, "CHOCH"))
    if candidates:
        structure_index, structure_dir, structure_type = max(candidates, key=lambda c: c[0])

    if sweep_index is None or sweep_dir != bias or structure_index is None:
        return result

    # Sequence matters: the sweep must have happened AT OR BEFORE the
    # structure break that confirms it, not after.
    if sweep_index > structure_index:
        return result

    # Displacement: a real impulsive candle somewhere between the
    # sweep and now, in the bias direction - not just a slow drift.
    displacement_ok = False
    for c in candles_1h[structure_index:]:
        is_bias_colored = (
            (bias == "BUY" and c["close"] > c["open"])
            or (bias == "SELL" and c["close"] < c["open"])
        )
        if is_bias_colored and is_strong_candle_body(c, min_body_ratio=0.55):
            displacement_ok = True
            break

    result.update({
        "valid": displacement_ok,
        "sweep_index": sweep_index,
        "structure_index": structure_index,
        "structure_type": structure_type,
        "displacement_ok": displacement_ok,
    })
    return result


# ==========================================================
# STAGE 3 — 15M ENTRY ZONE (Order Block + FVG)
# ==========================================================

def _detect_fvg_zone(candles, lookback=10):
    """
    Same 3-candle imbalance logic as V2's detect_fair_value_gap, but
    returns the actual price zone (low, high, direction) instead of
    just a direction - needed here to check overlap with the order
    block zone, which the blueprint specifically calls out as
    "especially interesting" when it happens.
    """
    if len(candles) < lookback + 2:
        return None

    recent = candles[-lookback:]

    for i in range(len(recent) - 1, 1, -1):
        first_candle = recent[i - 2]
        third_candle = recent[i]

        if first_candle["high"] < third_candle["low"]:
            return (first_candle["high"], third_candle["low"], "BUY")

        if first_candle["low"] > third_candle["high"]:
            return (third_candle["high"], first_candle["low"], "SELL")

    return None


def get_15m_entry_zone(candles_15m, bias):
    """
    Finds the entry zone: the order block, tightened to the overlap
    with a same-direction FVG when one exists.

    Returns a dict:
        {
            "zone_low": float, "zone_high": float,
            "has_fvg_overlap": bool,
        }
    or None if no usable zone / not enough data.
    """
    if bias not in ("BUY", "SELL") or len(candles_15m) < 20:
        return None

    ob_low, ob_high = detect_order_block(candles_15m)

    fvg = _detect_fvg_zone(candles_15m)
    has_fvg_overlap = False

    if fvg is not None:
        fvg_low, fvg_high, fvg_dir = fvg
        if fvg_dir == bias:
            overlap_low = max(ob_low, fvg_low)
            overlap_high = min(ob_high, fvg_high)
            if overlap_low < overlap_high:
                # Real overlap - tighten the zone to the intersection,
                # per the blueprint ("if they overlap, that's
                # especially interesting").
                ob_low, ob_high = overlap_low, overlap_high
                has_fvg_overlap = True

    return {
        "zone_low": ob_low,
        "zone_high": ob_high,
        "has_fvg_overlap": has_fvg_overlap,
    }


def price_has_returned_to_zone(candles_15m, zone_low, zone_high):
    """
    Checks whether price has actually come back to the entry zone
    (wick touch counts, not just a close inside it) on the most recent
    15M candle. If price ran straight through/away without tagging the
    zone, the blueprint says don't chase it - this is what lets the
    pipeline correctly return "no entry yet" rather than force one.
    """
    if not candles_15m:
        return False

    c = candles_15m[-1]
    return c["low"] <= zone_high and c["high"] >= zone_low


# ==========================================================
# STAGE 4 — 5M ENTRY TRIGGER
# ==========================================================

def detect_5m_trigger(candles_5m, bias, zone_low, zone_high, search_window=8):
    """
    Looks for: reaction at the zone -> liquidity/rejection ->
    structure shift (CHoCH) -> displacement, on the 5M timeframe.

    MACD is deliberately NOT included - per the blueprint, "MACD
    alone can never create a signal." It's left as an optional future
    momentum add-on rather than a gate, so its absence never blocks
    a signal and its presence (if added later) should only ever
    support, not decide.

    Returns a dict:
        {"valid": bool, "reaction_type": str or None}
    """
    result = {"valid": False, "reaction_type": None}

    if bias not in ("BUY", "SELL") or len(candles_5m) < 15:
        return result

    # Reaction: a doji-style rejection candle, or a liquidity sweep,
    # occurring while price is inside/near the zone.
    reaction_type = None
    reaction_index = None

    for i in range(len(candles_5m) - 1, max(0, len(candles_5m) - search_window) - 1, -1):
        c = candles_5m[i]
        touched_zone = c["low"] <= zone_high and c["high"] >= zone_low
        if not touched_zone:
            continue

        doji = detect_doji_reversal(candles_5m[: i + 1])
        if doji == bias:
            reaction_type = "rejection_candle"
            reaction_index = i
            break

        sweep = detect_liquidity_sweep(candles_5m[: i + 1], lookback=10)
        if sweep == bias:
            reaction_type = "liquidity_sweep"
            reaction_index = i
            break

    if reaction_index is None:
        return result

    # Structure shift after the reaction, in bias direction.
    choch_index, choch_dir = _find_most_recent(
        candles_5m, detect_choch, search_window=search_window, min_candles=10, lookback=8
    )
    if choch_index is None or choch_dir != bias or choch_index < reaction_index:
        return result

    # Displacement after the structure shift.
    displacement_ok = False
    for c in candles_5m[choch_index:]:
        is_bias_colored = (
            (bias == "BUY" and c["close"] > c["open"])
            or (bias == "SELL" and c["close"] < c["open"])
        )
        if is_bias_colored and is_strong_candle_body(c, min_body_ratio=0.5):
            displacement_ok = True
            break

    result.update({"valid": displacement_ok, "reaction_type": reaction_type})
    return result


# ==========================================================
# STAGE 5 — ANTI-CHASE FILTER
# ==========================================================

def anti_chase_check(entry, bias, zone_low, zone_high, atr, max_distance_atr=1.2):
    """
    Measures how far price has already travelled from the entry zone,
    in ATR terms - this is the "distance from entry zone" check the
    blueprint's Anti-Chase Filter calls for specifically.

    Deliberately NOT based on raw last-N-candle price change (V2's
    is_entry_extended approach): Stage 4 (5M trigger) requires a real
    displacement candle to confirm entry, and that same candle would
    often trip a raw-drift check too - the two stages would end up
    fighting each other, rejecting valid setups purely because the
    confirmation they demanded also looked like "moving too fast."
    Measuring distance from the zone instead avoids that conflict:
    a strong reaction candle right at the zone is expected and good;
    price already far past the zone by the time of entry is not.

    Returns True if the entry is too far from the zone already
    (should be blocked), False if it's still close enough to be fresh.
    """
    if atr is None or atr <= 0:
        return False

    if bias == "BUY":
        distance = entry - zone_high
    else:
        distance = zone_low - entry

    return distance > atr * max_distance_atr


# ==========================================================
# STAGE 6 — RISK / REWARD (structural)
# ==========================================================

def calculate_structural_targets(entry, bias, zone_low, zone_high, candles_1h, atr,
                                  sl_buffer_atr=0.3, min_rr=1.0):
    """
    Structural SL: beyond the entry-zone invalidation point (not a
    flat ATR multiple like V2), with a small ATR buffer so normal
    noise doesn't stop the trade out right at the zone edge.

    Structural targets: TP1/TP2 come from real 1H swing structure
    (support/resistance), TP3 extends further using ATR as a stand-in
    for "larger higher-timeframe target" when no further 1H structure
    exists.

    Returns (sl, tp1, tp2, tp3) or None if the resulting R:R doesn't
    clear min_rr - "only send the trade if the reward makes sense
    relative to the risk," per the blueprint.
    """
    support, resistance = get_support_resistance(candles_1h, window=40, swing_lookback=2)

    if bias == "BUY":
        sl = zone_low - (atr * sl_buffer_atr)
        risk = entry - sl
        if risk <= 0:
            return None

        tp1 = resistance if resistance > entry else entry + atr * 1.5
        tp3 = tp1 + atr * 2.0
        tp2 = (tp1 + tp3) / 2

    else:
        sl = zone_high + (atr * sl_buffer_atr)
        risk = sl - entry
        if risk <= 0:
            return None

        tp1 = support if support < entry else entry - atr * 1.5
        tp3 = tp1 - atr * 2.0
        tp2 = (tp1 + tp3) / 2

    reward = abs(tp1 - entry)
    if reward < risk * min_rr:
        return None

    return (round(sl, 6), round(tp1, 6), round(tp2, 6), round(tp3, 6))


# ==========================================================
# MAIN ANALYSIS — orchestrates all 6 stages
# ==========================================================

def analyze_v3(candles_4h, candles_1h, candles_15m, candles_5m):
    """
    Runs the full V3 pipeline. Returns a signal dict identical in
    shape to V2's analyze_candles() output (so app.py can call either
    engine interchangeably during the comparison window), or None if
    any stage fails to qualify.
    """
    bias, bias_source = get_4h_bias(candles_4h)
    if bias is None:
        return None

    setup = detect_1h_setup(candles_1h, bias)
    if not setup["valid"]:
        return None

    zone = get_15m_entry_zone(candles_15m, bias)
    if zone is None:
        return None

    if not price_has_returned_to_zone(candles_15m, zone["zone_low"], zone["zone_high"]):
        return None

    trigger = detect_5m_trigger(candles_5m, bias, zone["zone_low"], zone["zone_high"])
    if not trigger["valid"]:
        return None

    atr_5m = calculate_atr(candles_5m)
    if atr_5m is None:
        return None

    entry = candles_5m[-1]["close"]

    if anti_chase_check(entry, bias, zone["zone_low"], zone["zone_high"], atr_5m):
        return None

    targets = calculate_structural_targets(
        entry, bias, zone["zone_low"], zone["zone_high"], candles_1h, atr_5m
    )
    if targets is None:
        return None

    sl, tp1, tp2, tp3 = targets

    # Confidence is REPORTED, not decisive - every stage above already
    # had to pass for us to get here. This just communicates quality
    # among signals that already qualified, so it can't reproduce
    # V2's "sum of correlated bonuses" miscalibration.
    quality_points = 0
    quality_points += 20 if setup["structure_type"] == "BOS" else 15
    quality_points += 15 if zone["has_fvg_overlap"] else 0
    quality_points += 15 if trigger["reaction_type"] == "liquidity_sweep" else 10
    confidence = min(70 + quality_points, 95)  # capped below 100: no stage is ever "certain"

    support, resistance = get_support_resistance(candles_1h, window=40, swing_lookback=2)

    direction_label = "🟢 BUY" if bias == "BUY" else "🔴 SELL"

    return {
        "direction": direction_label,
        "entry": round(entry, 6),
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "confidence": confidence,
        "risk": calculate_risk(entry, atr_5m),
        "atr": round(atr_5m, 6),
        "setup_explanation": (
            f"4H bias {bias} -> 1H {setup['structure_type']} setup "
            f"-> 15M zone ({'OB+FVG overlap' if zone['has_fvg_overlap'] else 'OB only'}) "
            f"-> 5M {trigger['reaction_type']} + CHoCH + displacement"
        ),
        "zone_low": round(zone["zone_low"], 6),
        "zone_high": round(zone["zone_high"], 6),
        "support": round(support, 6),
        "resistance": round(resistance, 6),
        # Per-stage diagnostic breakdown - the V3 equivalent of V2's
        # "factors" dict. V3's pipeline is gated, not scored, so this
        # isn't a set of independent pass/fail confirmations the way
        # V2's was - it's a record of WHICH PATH each qualifying
        # signal took through the pipeline. Without this, a month of
        # live V3 results would only be a win/loss count with no way
        # to see which stage-level choices correlate with outcomes -
        # exactly the blind spot that let V2's adx_strong/bos
        # inversion go unnoticed until we queried real trade history.
        "factors": {
            "bias_source": bias_source,
            "setup_structure_type": setup["structure_type"],
            "setup_sweep_to_structure_gap": setup["structure_index"] - setup["sweep_index"],
            "zone_has_fvg_overlap": zone["has_fvg_overlap"],
            "trigger_reaction_type": trigger["reaction_type"],
            "anti_chase_distance_atr": round(
                abs(
                    (entry - zone["zone_high"]) if bias == "BUY"
                    else (zone["zone_low"] - entry)
                ) / atr_5m,
                3,
            ) if atr_5m else None,
            "rr_tp1": round(abs(tp1 - entry) / abs(entry - sl), 3) if abs(entry - sl) > 0 else None,
        },
    }
