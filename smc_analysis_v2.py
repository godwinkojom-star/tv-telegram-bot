"""
SmartFS Gold V2 - standalone strategy engine.

Purpose
-------
Gold/XAUUSD-only scalping strategy using a sequential, price/structure-first
pipeline:

15M EMA50 + liquidity context
    -> 5M liquidity sweep/reclaim
    -> 1M MSS + displacement
    -> anti-chase / market-condition gates
    -> structural SL + dynamic liquidity TP1/TP2
    -> informational confidence (never a signal gate)

This module intentionally contains NO Telegram, Supabase, dashboard, MT5,
app.py, or legacy-V2 compatibility code.

Input format
------------
Pandas DataFrames with columns: open, high, low, close. A volume column is
optional but is not used by the strategy. Datetime indexes are recommended.

The public entry point is ``analyze_gold_v2(df15, df5, df1, ...)``.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

import math
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class V2Config:
    ema_period: int = 50
    atr_period: int = 14
    swing_left_15m: int = 5
    swing_right_15m: int = 5
    swing_left_5m: int = 2
    swing_right_5m: int = 2
    swing_left_1m: int = 2
    swing_right_1m: int = 2

    # A sweep must move through the level by at least this fraction of recent
    # 5M ATR. This avoids treating microscopic ticks as meaningful sweeps.
    min_sweep_atr: float = 0.10

    # A reclaim is a close back inside the level, not a close beyond it.
    # Small tolerance prevents floating-point noise from deciding the result.
    reclaim_tolerance_atr: float = 0.03

    # 1M entry must occur soon after the 5M sweep/reclaim.
    mss_window_minutes: int = 5

    # Anti-chase: distance from the sweep/reclaim area after MSS.
    max_extension_atr: float = 1.25

    # Displacement: current candle body must be at least this fraction of ATR
    # and body/range must show directional intent.
    displacement_body_atr: float = 0.35
    displacement_body_ratio: float = 0.60

    # SL buffer is adaptive rather than a fixed dollar amount.
    sl_buffer_atr: float = 0.20
    max_sl_atr: float = 2.00

    # Target must leave room for costs/noise. RR is calculated AFTER targets.
    min_tp1_risk_multiple: float = 0.80
    min_tp2_risk_multiple: float = 1.20

    # Volatility gates are deliberately distribution-based rather than fixed
    # dollar thresholds. These are starting parameters, not claimed winners.
    # Spread is intentionally supplied by the execution/data adapter because
    # broker spread units vary; the strategy never invents a broker-specific
    # maximum.
    m1_volatility_floor_quantile: float = 0.20
    m5_volatility_floor_quantile: float = 0.20
    m5_volatility_lookback: int = 100
    m1_volatility_lookback: int = 100

    # Level clustering adapts to volatility.
    level_cluster_atr: float = 0.20

    # Freshness / session context.
    level_lookback_15m: int = 300
    level_lookback_5m: int = 240
    session_extreme_hours: int = 24
    consumed_level_atr: float = 0.15
    m5_range_floor_quantile: float = 0.20


# ---------------------------------------------------------------------------
# Basic validation / helpers
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = ("open", "high", "low", "close")


def _validate_ohlc(df: pd.DataFrame, name: str) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} must be a pandas DataFrame")
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")
    if len(df) < 10:
        raise ValueError(f"{name} needs at least 10 OHLC rows")


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_index()
    for c in REQUIRED_COLUMNS:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out.dropna(subset=list(REQUIRED_COLUMNS))


def _atr(df: pd.DataFrame, period: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def _ema(df: pd.DataFrame, period: int) -> pd.Series:
    return df["close"].ewm(span=period, adjust=False, min_periods=period).mean()


def _body(candle: pd.Series) -> float:
    return abs(float(candle["close"]) - float(candle["open"]))


def _range(candle: pd.Series) -> float:
    return max(float(candle["high"]) - float(candle["low"]), 0.0)


def _directional_displacement(candle: pd.Series, direction: str, atr: float, cfg: V2Config) -> bool:
    if not np.isfinite(atr) or atr <= 0:
        return False
    body = _body(candle)
    rng = _range(candle)
    if body < cfg.displacement_body_atr * atr or rng <= 0:
        return False
    if body / rng < cfg.displacement_body_ratio:
        return False
    if direction == "BUY":
        return float(candle["close"]) > float(candle["open"])
    return float(candle["close"]) < float(candle["open"])


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _last_timestamp(df: pd.DataFrame):
    return df.index[-1] if len(df.index) else None


def _minutes_between(a, b) -> Optional[float]:
    try:
        return abs((pd.Timestamp(b) - pd.Timestamp(a)).total_seconds()) / 60.0
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Swing / liquidity detection
# ---------------------------------------------------------------------------

def _confirmed_swings(df: pd.DataFrame, left: int, right: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []
    if len(df) < left + right + 1:
        return highs, lows

    h = df["high"].to_numpy(dtype=float)
    l = df["low"].to_numpy(dtype=float)
    for i in range(left, len(df) - right):
        hi = h[i]
        lo = l[i]
        if hi >= np.max(h[i - left : i]) and hi >= np.max(h[i + 1 : i + right + 1]):
            highs.append({"index": i, "time": df.index[i], "price": hi, "kind": "swing_high"})
        if lo <= np.min(l[i - left : i]) and lo <= np.min(l[i + 1 : i + right + 1]):
            lows.append({"index": i, "time": df.index[i], "price": lo, "kind": "swing_low"})
    return highs, lows


def _equal_levels(swings: Sequence[Dict[str, Any]], tolerance: float) -> List[Dict[str, Any]]:
    if not swings:
        return []
    result: List[Dict[str, Any]] = []
    used = set()
    for i, s in enumerate(swings):
        if i in used:
            continue
        cluster = [s]
        for j in range(i + 1, len(swings)):
            if abs(swings[j]["price"] - s["price"]) <= tolerance:
                cluster.append(swings[j])
                used.add(j)
        if len(cluster) >= 2:
            price = float(np.mean([x["price"] for x in cluster]))
            result.append({
                "price": price,
                "type": "equal_high" if s["kind"] == "swing_high" else "equal_low",
                "time": cluster[-1]["time"],
                "source_count": len(cluster),
                "fresh": True,
            })
    return result


def build_15m_liquidity_levels(df15: pd.DataFrame, cfg: V2Config) -> List[Dict[str, Any]]:
    """Return meaningful 15M liquidity levels, newest first.

    Includes confirmed swings, equal highs/lows, previous completed calendar
    day extremes, and the most recent completed 24-hour/session extremes when
    timestamps are available. No level is a trade trigger by itself.
    """
    df = _clean(df15)
    atr = _atr(df, cfg.atr_period).iloc[-1]
    atr = float(atr) if np.isfinite(atr) else float((df["high"] - df["low"]).tail(20).mean())
    tolerance = max(atr * cfg.level_cluster_atr, 1e-9)

    highs, lows = _confirmed_swings(df.tail(cfg.level_lookback_15m), cfg.swing_left_15m, cfg.swing_right_15m)
    levels: List[Dict[str, Any]] = []
    for s in highs + lows:
        levels.append({
            "price": float(s["price"]),
            "type": s["kind"],
            "time": s["time"],
            "source_count": 1,
            "fresh": True,
        })
    levels.extend(_equal_levels(highs, tolerance))
    levels.extend(_equal_levels(lows, tolerance))

    # Previous completed calendar day extremes.
    try:
        idx = pd.DatetimeIndex(df.index)
        last_day = idx[-1].normalize()
        prev = df[idx.normalize() < last_day]
        if not prev.empty:
            prev_day = prev[idx[idx.normalize() < last_day].normalize() == idx[idx.normalize() < last_day].normalize().max()]
            levels.append({"price": float(prev_day["high"].max()), "type": "previous_day_high", "time": prev_day.index[-1], "source_count": 1, "fresh": True})
            levels.append({"price": float(prev_day["low"].min()), "type": "previous_day_low", "time": prev_day.index[-1], "source_count": 1, "fresh": True})
    except Exception:
        pass

    # Most recent completed 24-hour/session extremes. This is deliberately
    # descriptive rather than a fixed London/New-York trading restriction.
    try:
        idx = pd.DatetimeIndex(df.index)
        end = idx[-1]
        start = end - pd.Timedelta(hours=cfg.session_extreme_hours)
        prior = df.loc[(idx < end) & (idx >= start)]
        if len(prior) >= 4:
            levels.append({"price": float(prior["high"].max()), "type": "recent_session_high", "time": prior.index[-1], "source_count": 1, "fresh": True})
            levels.append({"price": float(prior["low"].min()), "type": "recent_session_low", "time": prior.index[-1], "source_count": 1, "fresh": True})
    except Exception:
        pass

    return _dedupe_levels(levels, tolerance)


def _dedupe_levels(levels: Sequence[Dict[str, Any]], tolerance: float) -> List[Dict[str, Any]]:
    ordered = sorted(levels, key=lambda x: pd.Timestamp(x["time"]) if x.get("time") is not None else pd.Timestamp.min, reverse=True)
    kept: List[Dict[str, Any]] = []
    for level in ordered:
        if any(abs(float(level["price"]) - float(k["price"])) <= tolerance for k in kept):
            continue
        kept.append(dict(level))
    return kept


def _level_side(level_price: float, current: float) -> str:
    return "ABOVE" if level_price > current else "BELOW"


def _find_nearest_level(levels: Sequence[Dict[str, Any]], current: float, direction: str) -> Optional[Dict[str, Any]]:
    candidates = []
    for level in levels:
        p = float(level["price"])
        if direction == "BUY" and p > current:
            candidates.append(level)
        elif direction == "SELL" and p < current:
            candidates.append(level)
    if not candidates:
        return None
    return min(candidates, key=lambda x: abs(float(x["price"]) - current))


def _level_consumed(level: Dict[str, Any], df15: pd.DataFrame, df5: pd.DataFrame, tolerance: float) -> bool:
    """Return whether a liquidity level was subsequently traded through.

    A target is not considered fresh merely because it is structurally
    important; if later price action has already consumed it, it is removed
    from the target list.
    """
    p = float(level["price"])
    t = level.get("time")
    kind = str(level.get("type", ""))
    frames = []
    for df in (df15, df5):
        d = _clean(df)
        if t is not None:
            try:
                d = d.loc[d.index > t]
            except Exception:
                pass
        if not d.empty:
            frames.append(d)
    if not frames:
        return False

    resistance = ("high" in kind) or kind in {"swing_high", "equal_high", "previous_day_high", "recent_session_high"}
    for d in frames:
        if resistance and float(d["high"].max()) > p + tolerance:
            return True
        if not resistance and float(d["low"].min()) < p - tolerance:
            return True
    return False


def _target_levels(
    df15: pd.DataFrame,
    df5: pd.DataFrame,
    current: float,
    direction: str,
    cfg: V2Config,
    consumed_prices: Optional[Sequence[float]] = None,
) -> List[Dict[str, Any]]:
    """Build opposing, fresh liquidity/structure targets."""
    d15 = _clean(df15)
    d5 = _clean(df5)
    levels = build_15m_liquidity_levels(d15, cfg)
    atr5_series = _atr(d5, cfg.atr_period)
    atr5 = _safe_float(atr5_series.iloc[-1]) if len(atr5_series) else None
    if atr5 is None:
        atr5 = float((d5["high"] - d5["low"]).tail(20).mean())
    atr5 = max(float(atr5), 1e-9)
    tol = max(atr5 * cfg.level_cluster_atr, 1e-9)

    h5, l5 = _confirmed_swings(d5.tail(cfg.level_lookback_5m), cfg.swing_left_5m, cfg.swing_right_5m)
    levels += [
        {"price": float(s["price"]), "type": s["kind"], "time": s["time"], "source_count": 1, "fresh": True}
        for s in h5 + l5
    ]
    levels += _equal_levels(h5, tol)
    levels += _equal_levels(l5, tol)

    opposing: List[Dict[str, Any]] = []
    for level in levels:
        p = float(level["price"])
        if direction == "BUY" and p > current + tol:
            opposing.append(level)
        elif direction == "SELL" and p < current - tol:
            opposing.append(level)

    opposing = _dedupe_levels(opposing, tol)
    consumed = [float(x) for x in (consumed_prices or []) if _safe_float(x) is not None]
    fresh: List[Dict[str, Any]] = []
    for level in opposing:
        if consumed and any(abs(float(level["price"]) - cp) <= cfg.consumed_level_atr * atr5 for cp in consumed):
            continue
        if _level_consumed(level, d15, d5, cfg.consumed_level_atr * atr5):
            continue
        fresh.append({**level, "fresh": True})

    fresh.sort(key=lambda x: abs(float(x["price"]) - current))
    return fresh


# ---------------------------------------------------------------------------
# Trend + sweep + MSS
# ---------------------------------------------------------------------------

def determine_15m_bias(df15: pd.DataFrame, cfg: V2Config) -> Dict[str, Any]:
    df = _clean(df15)
    ema = _ema(df, cfg.ema_period)
    close = float(df["close"].iloc[-1])
    e = _safe_float(ema.iloc[-1])
    prev_e = _safe_float(ema.iloc[-2]) if len(ema) >= 2 else None
    if e is None:
        return {"bias": "NEUTRAL", "ema50": None, "reason": "EMA50_not_ready"}
    slope = (e - prev_e) if prev_e is not None else 0.0
    if close > e and slope >= 0:
        bias = "BUY"
    elif close < e and slope <= 0:
        bias = "SELL"
    else:
        bias = "NEUTRAL"
    return {"bias": bias, "ema50": e, "ema_slope": slope, "close": close}


def _choose_near_level(levels: Sequence[Dict[str, Any]], price: float, direction: str, atr: float) -> Optional[Dict[str, Any]]:
    if not levels:
        return None
    # For a BUY reversal we need resistance/support context below/around price
    # where a downside sweep can occur. For SELL, the inverse.
    tolerance = max(atr * 0.50, 1e-9)
    candidates = []
    for l in levels:
        p = float(l["price"])
        if direction == "BUY" and p <= price + tolerance:
            candidates.append(l)
        elif direction == "SELL" and p >= price - tolerance:
            candidates.append(l)
    if not candidates:
        return None
    return min(candidates, key=lambda x: abs(float(x["price"]) - price))


def detect_5m_sweep_reclaim(
    df5: pd.DataFrame,
    levels15: Sequence[Dict[str, Any]],
    direction: str,
    cfg: V2Config,
) -> Optional[Dict[str, Any]]:
    """Detect a 5M wick-through + close-back-inside liquidity sweep.

    BUY sweeps occur below a meaningful level and reclaim above it.
    SELL sweeps occur above a meaningful level and reclaim below it.
    """
    df = _clean(df5)
    atr = _atr(df, cfg.atr_period)
    if len(df) < cfg.atr_period + 2:
        return None
    candle = df.iloc[-1]
    a = _safe_float(atr.iloc[-1])
    if a is None or a <= 0:
        return None

    price = float(candle["close"])
    level = _choose_near_level(levels15, price, direction, a)
    if level is None:
        return None
    lp = float(level["price"])
    tol = cfg.reclaim_tolerance_atr * a

    if direction == "BUY":
        swept = float(candle["low"]) < lp - cfg.min_sweep_atr * a
        reclaimed = price >= lp - tol and price <= float(candle["high"])
        rejection = (price - float(candle["low"])) / max(_range(candle), 1e-9) >= 0.55
    else:
        swept = float(candle["high"]) > lp + cfg.min_sweep_atr * a
        reclaimed = price <= lp + tol and price >= float(candle["low"])
        rejection = (float(candle["high"]) - price) / max(_range(candle), 1e-9) >= 0.55

    if not (swept and reclaimed and rejection):
        return None

    # A reversal setup requires reclaim. If the sweep candle closes decisively
    # outside the level, treat it as acceptance/breakout instead.
    if direction == "BUY" and price < lp - 0.10 * a:
        return None
    if direction == "SELL" and price > lp + 0.10 * a:
        return None

    # If a following 5M candle exists, sustained closes outside the level
    # invalidate the reversal. During live use the last candle is normally the
    # latest closed candle, so this check only uses a genuinely later candle.
    try:
        pos = df.index.get_loc(candle.name)
        if isinstance(pos, (int, np.integer)) and pos + 1 < len(df):
            following = df.iloc[pos + 1]
            if direction == "BUY" and float(following["close"]) < lp - 0.10 * a:
                return None
            if direction == "SELL" and float(following["close"]) > lp + 0.10 * a:
                return None
    except Exception:
        pass

    return {
        "direction": direction,
        "level": lp,
        "level_type": level.get("type"),
        "sweep_extreme": float(candle["low"] if direction == "BUY" else candle["high"]),
        "reclaim_price": price,
        "time": candle.name,
        "atr5": a,
        "rejection_strength": (price - float(candle["low"])) / max(_range(candle), 1e-9) if direction == "BUY" else (float(candle["high"]) - price) / max(_range(candle), 1e-9),
    }


def _latest_micro_swings(df1: pd.DataFrame, cfg: V2Config) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    return _confirmed_swings(df1, cfg.swing_left_1m, cfg.swing_right_1m)


def detect_1m_mss(
    df1: pd.DataFrame,
    direction: str,
    sweep: Dict[str, Any],
    cfg: V2Config,
) -> Optional[Dict[str, Any]]:
    """Find a confirmed 1M MSS after the sweep and within the expiry window.

    BUY requires a post-sweep lower-high sequence followed by a close above the
    lower high. SELL requires a higher-low sequence followed by a close below
    the higher low. The confirmation candle must occur *after* the candidate
    swing has actually formed and been confirmed.
    """
    df = _clean(df1)
    if len(df) < cfg.swing_left_1m + cfg.swing_right_1m + 5:
        return None

    sweep_time = pd.Timestamp(sweep["time"])
    deadline = sweep_time + pd.Timedelta(minutes=cfg.mss_window_minutes)
    post = df.loc[(df.index > sweep_time) & (df.index <= deadline)]
    if post.empty:
        return None

    # Use only data available by each candidate point. A confirmed swing is
    # inherently delayed by swing_right bars, which is intentional here.
    h, l = _confirmed_swings(df, cfg.swing_left_1m, cfg.swing_right_1m)
    highs = [x for x in h if sweep_time < pd.Timestamp(x["time"]) <= deadline]
    lows = [x for x in l if sweep_time < pd.Timestamp(x["time"]) <= deadline]
    atr_series = _atr(df, cfg.atr_period)

    if direction == "BUY":
        candidates = []
        for i in range(1, len(highs)):
            prior, cur = highs[i - 1], highs[i]
            if float(cur["price"]) < float(prior["price"]):
                candidates.append(cur)
        for candidate in candidates:
            ctime = pd.Timestamp(candidate["time"])
            confirmations = post.loc[post.index > ctime]
            for idx, row in confirmations.iterrows():
                a = _safe_float(atr_series.loc[idx]) if idx in atr_series.index else None
                if a is None:
                    continue
                if float(row["close"]) > float(candidate["price"]) and _directional_displacement(row, direction, a, cfg):
                    return {
                        "direction": direction, "time": idx, "price": float(row["close"]),
                        "broken_level": float(candidate["price"]), "atr1": a,
                        "displacement": True, "structure": "lower_high_broken",
                        "candidate_swing_time": ctime,
                    }
    else:
        candidates = []
        for i in range(1, len(lows)):
            prior, cur = lows[i - 1], lows[i]
            if float(cur["price"]) > float(prior["price"]):
                candidates.append(cur)
        for candidate in candidates:
            ctime = pd.Timestamp(candidate["time"])
            confirmations = post.loc[post.index > ctime]
            for idx, row in confirmations.iterrows():
                a = _safe_float(atr_series.loc[idx]) if idx in atr_series.index else None
                if a is None:
                    continue
                if float(row["close"]) < float(candidate["price"]) and _directional_displacement(row, direction, a, cfg):
                    return {
                        "direction": direction, "time": idx, "price": float(row["close"]),
                        "broken_level": float(candidate["price"]), "atr1": a,
                        "displacement": True, "structure": "higher_low_broken",
                        "candidate_swing_time": ctime,
                    }
    return None


# ---------------------------------------------------------------------------
# Market-condition gate and anti-chase
# ---------------------------------------------------------------------------

def market_condition_gate(df5: pd.DataFrame, df1: pd.DataFrame, cfg: V2Config) -> Dict[str, Any]:
    """Adaptive pass/fail market-condition gate.

    It rejects unusually dead M1/M5 conditions using each timeframe's own
    recent distribution. A 5M true-range floor is also required so the market
    is actually moving, not merely printing a normal ATR value after a lull.
    """
    d5 = _clean(df5)
    d1 = _clean(df1)
    a5 = _atr(d5, cfg.atr_period)
    a1 = _atr(d1, cfg.atr_period)
    recent5 = a5.dropna().tail(cfg.m5_volatility_lookback)
    recent1 = a1.dropna().tail(cfg.m1_volatility_lookback)
    ranges5 = (d5["high"] - d5["low"]).dropna().tail(cfg.m5_volatility_lookback)
    if recent5.empty or recent1.empty or ranges5.empty:
        return {"pass": False, "reason": "volatility_not_ready"}

    cur5 = float(recent5.iloc[-1])
    cur1 = float(recent1.iloc[-1])
    q5 = float(recent5.quantile(cfg.m5_volatility_floor_quantile))
    q1 = float(recent1.quantile(cfg.m1_volatility_floor_quantile))
    range_floor = float(ranges5.quantile(cfg.m5_range_floor_quantile))
    current_range5 = float(ranges5.iloc[-1])
    passed = cur5 >= q5 and cur1 >= q1 and current_range5 >= range_floor
    return {
        "pass": passed,
        "m5_atr": cur5,
        "m5_floor": q5,
        "m1_atr": cur1,
        "m1_floor": q1,
        "m5_range": current_range5,
        "m5_range_floor": range_floor,
        "reason": "healthy_volatility" if passed else "volatility_or_movement_too_low",
    }


def late_entry_check(mss: Dict[str, Any], sweep: Dict[str, Any], cfg: V2Config) -> Dict[str, Any]:
    distance = abs(float(mss["price"]) - float(sweep["reclaim_price"]))
    atr = float(mss["atr1"])
    extension = distance / atr if atr > 0 else float("inf")
    passed = extension <= cfg.max_extension_atr
    return {
        "pass": passed,
        "distance": distance,
        "extension_atr": extension,
        "max_extension_atr": cfg.max_extension_atr,
        "reason": "entry_not_chased" if passed else "move_already_extended",
    }


# ---------------------------------------------------------------------------
# Risk and targets
# ---------------------------------------------------------------------------

def calculate_structural_sl(direction: str, sweep: Dict[str, Any], atr1: float, cfg: V2Config) -> Dict[str, Any]:
    extreme = float(sweep["sweep_extreme"])
    buffer = cfg.sl_buffer_atr * atr1
    if direction == "BUY":
        sl = extreme - buffer
    else:
        sl = extreme + buffer
    return {"sl": sl, "buffer": buffer, "sweep_extreme": extreme}


def calculate_dynamic_targets(
    df15: pd.DataFrame,
    df5: pd.DataFrame,
    entry: float,
    sl: float,
    direction: str,
    cfg: V2Config,
    consumed_prices: Optional[Sequence[float]] = None,
) -> Dict[str, Any]:
    risk = abs(entry - sl)
    candidates = _target_levels(df15, df5, entry, direction, cfg, consumed_prices)
    # Freshness/structure are informational. Do not invent a TP if no real
    # opposing liquidity exists.
    valid = []
    for c in candidates:
        p = float(c["price"])
        rr = abs(p - entry) / risk if risk > 0 else 0
        if rr < cfg.min_tp1_risk_multiple:
            continue
        valid.append({**c, "rr": rr})

    if not valid:
        return {"tp1": None, "tp2": None, "risk": risk, "targets": []}

    tp1 = valid[0]
    tp2 = None
    for candidate in valid[1:]:
        if candidate["rr"] >= cfg.min_tp2_risk_multiple and abs(candidate["price"] - tp1["price"]) > 0:
            tp2 = candidate
            break

    return {
        "tp1": tp1["price"],
        "tp2": tp2["price"] if tp2 else None,
        "risk": risk,
        "tp1_rr": tp1["rr"],
        "tp2_rr": tp2["rr"] if tp2 else None,
        "targets": valid[:5],
    }


# ---------------------------------------------------------------------------
# Informational confidence — deliberately NOT a probability or gate
# ---------------------------------------------------------------------------

def calculate_informational_confidence(
    *,
    trend: Dict[str, Any],
    sweep: Dict[str, Any],
    mss: Dict[str, Any],
    late: Dict[str, Any],
    market: Dict[str, Any],
    targets: Dict[str, Any],
) -> Optional[int]:
    """Return a transparent 0-100 setup-quality descriptor.

    This value is calculated only AFTER all signal gates pass. It is never
    used to approve/reject a signal and is not a calibrated win probability.
    The formula intentionally stays simple until live/backtest data exists to
    calibrate it.
    """
    try:
        points = 0.0
        points += 20.0 if trend.get("bias") == sweep.get("direction") else 0.0
        points += min(20.0, max(0.0, float(sweep.get("rejection_strength", 0.0)) * 20.0))
        points += 20.0 if mss.get("displacement") else 0.0
        ext = float(late.get("extension_atr", 999.0))
        points += 15.0 * max(0.0, 1.0 - ext / max(float(late.get("max_extension_atr", 1.25)), 1e-9))
        points += 10.0 if market.get("pass") else 0.0
        if targets.get("tp2") is not None:
            points += 15.0
        elif targets.get("tp1") is not None:
            points += 8.0
        return int(round(max(0.0, min(100.0, points))))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Protected-profit / management plan
# ---------------------------------------------------------------------------

def build_management_plan(
    direction: str,
    entry: float,
    tp1: float,
    tp2: Optional[float],
    atr1: float,
    cfg: V2Config,
) -> Dict[str, Any]:
    """Describe the post-TP1 management rule.

    TP1 partial is 50%. The stop is NOT moved to exact entry merely because
    TP1 was touched. After TP1, wait for a confirmed 1M continuation swing and
    then place the protected stop beyond that swing with an adaptive buffer.
    """
    buffer = cfg.sl_buffer_atr * atr1
    return {
        "tp1_partial_percent": 50,
        "after_tp1": "wait_for_confirmed_1m_continuation",
        "protected_stop_rule": "BUY: below confirmed 1M higher low minus buffer; SELL: above confirmed 1M lower high plus buffer",
        "protected_buffer": buffer,
        "tp2": tp2,
        "break_even_mode": "protected_profit_not_exact_entry",
    }


def protected_stop_from_continuation(
    direction: str,
    continuation_swing: float,
    atr1: float,
    cfg: V2Config,
) -> float:
    buffer = cfg.sl_buffer_atr * atr1
    return continuation_swing - buffer if direction == "BUY" else continuation_swing + buffer


def setup_invalidated_before_entry(
    df1: pd.DataFrame,
    sweep: Dict[str, Any],
    direction: str,
    until_time: Optional[Any] = None,
) -> bool:
    """Check only the price path between sweep and entry/MSS confirmation."""
    df = _clean(df1)
    try:
        post = df.loc[df.index > pd.Timestamp(sweep["time"])]
        if until_time is not None:
            post = post.loc[post.index <= pd.Timestamp(until_time)]
    except Exception:
        post = df
    if post.empty:
        return False
    extreme = float(sweep["sweep_extreme"])
    for _, row in post.iterrows():
        if direction == "BUY" and float(row["close"]) < extreme:
            return True
        if direction == "SELL" and float(row["close"]) > extreme:
            return True
    return False


# ---------------------------------------------------------------------------
# Setup state / public analysis
# ---------------------------------------------------------------------------

def _result(status: str, reason: str, **kwargs) -> Dict[str, Any]:
    out = {"strategy_version": "V2", "market": "XAUUSD", "status": status, "reason": reason}
    out.update(kwargs)
    return out


def analyze_gold_v2(
    df15: pd.DataFrame,
    df5: pd.DataFrame,
    df1: pd.DataFrame,
    *,
    config: Optional[V2Config] = None,
    spread: Optional[float] = None,
    max_spread: Optional[float] = None,
) -> Dict[str, Any]:
    """Evaluate the complete frozen Gold V2 pipeline.

    Returns a serializable dictionary. ``status == 'SIGNAL'`` means every
    required strategy gate passed. ``confidence`` is informational only.
    """
    cfg = config or V2Config()
    for df, name in ((df15, "df15"), (df5, "df5"), (df1, "df1")):
        _validate_ohlc(df, name)
    d15, d5, d1 = _clean(df15), _clean(df5), _clean(df1)

    trend = determine_15m_bias(d15, cfg)
    if trend["bias"] == "NEUTRAL":
        return _result("NO_SIGNAL", "15m_trend_filter_not_aligned", trend=trend)

    market = market_condition_gate(d5, d1, cfg)
    if not market["pass"]:
        return _result("NO_SIGNAL", market["reason"], trend=trend, market_condition=market)

    if spread is not None and max_spread is not None:
        if not np.isfinite(spread) or spread < 0 or spread > max_spread:
            return _result("NO_SIGNAL", "spread_cost_gate_failed", trend=trend, market_condition=market, spread=spread, max_spread=max_spread)

    levels15 = build_15m_liquidity_levels(d15, cfg)
    if not levels15:
        return _result("NO_SIGNAL", "no_meaningful_15m_liquidity", trend=trend, market_condition=market)

    direction = trend["bias"]
    sweep = detect_5m_sweep_reclaim(d5, levels15, direction, cfg)
    if sweep is None:
        return _result("NO_SIGNAL", "no_valid_5m_sweep_reclaim", trend=trend, market_condition=market, liquidity_levels=levels15[:10])

    # First locate the MSS inside the expiry window; then validate that price
    # did not invalidate the sweep before that exact entry point. This avoids
    # allowing later candles (after a valid entry) to retroactively cancel it.
    mss = detect_1m_mss(d1, direction, sweep, cfg)
    if mss is None:
        return _result("NO_SIGNAL", "1m_mss_not_confirmed_within_window", trend=trend, market_condition=market, sweep=sweep)

    if setup_invalidated_before_entry(d1, sweep, direction, until_time=mss["time"]):
        return _result("NO_SIGNAL", "sweep_setup_invalidated_before_entry", trend=trend, market_condition=market, sweep=sweep, mss=mss)

    late = late_entry_check(mss, sweep, cfg)
    if not late["pass"]:
        return _result("NO_SIGNAL", late["reason"], trend=trend, market_condition=market, sweep=sweep, mss=mss, late_entry=late)

    sl_info = calculate_structural_sl(direction, sweep, float(mss["atr1"]), cfg)
    entry = float(mss["price"])
    sl = float(sl_info["sl"])
    risk = abs(entry - sl)
    if risk <= 0 or risk > cfg.max_sl_atr * float(mss["atr1"]):
        return _result("NO_SIGNAL", "stop_loss_too_large_or_invalid", trend=trend, market_condition=market, sweep=sweep, mss=mss, late_entry=late, sl=sl)

    targets = calculate_dynamic_targets(d15, d5, entry, sl, direction, cfg)
    if targets["tp1"] is None:
        return _result("NO_SIGNAL", "no_valid_dynamic_tp1", trend=trend, market_condition=market, sweep=sweep, mss=mss, late_entry=late, sl=sl, targets=targets)

    # If TP1 has already been reached by the current entry candle's range, the
    # setup is too late to enter. Entry must precede the first target.
    tp1 = float(targets["tp1"])
    candle = d1.iloc[-1]
    if direction == "BUY" and float(candle["high"]) >= tp1:
        return _result("NO_SIGNAL", "tp1_already_reached_before_entry", trend=trend, market_condition=market, sweep=sweep, mss=mss, late_entry=late, sl=sl, targets=targets)
    if direction == "SELL" and float(candle["low"]) <= tp1:
        return _result("NO_SIGNAL", "tp1_already_reached_before_entry", trend=trend, market_condition=market, sweep=sweep, mss=mss, late_entry=late, sl=sl, targets=targets)

    management = build_management_plan(direction, entry, tp1, targets.get("tp2"), float(mss["atr1"]), cfg)
    confidence = calculate_informational_confidence(
        trend=trend,
        sweep=sweep,
        mss=mss,
        late=late,
        market=market,
        targets=targets,
    )

    return _result(
        "SIGNAL",
        "all_required_gates_passed",
        direction=direction,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=targets.get("tp2"),
        risk=risk,
        tp1_rr=targets.get("tp1_rr"),
        tp2_rr=targets.get("tp2_rr"),
        confidence=confidence,
        confidence_is_informational=True,
        confidence_is_probability=False,
        trend=trend,
        market_condition=market,
        liquidity_levels=levels15[:10],
        sweep=sweep,
        mss=mss,
        late_entry=late,
        management=management,
        state_machine={
            "pre_entry": "CANDIDATE -> CONFIRMED -> ENTERED",
            "post_entry": "ENTERED -> TP1 (50%) -> PROTECTED -> TP2/SL",
            "expiry": f"MSS must confirm within {cfg.mss_window_minutes} minutes after sweep",
            "invalidation": "re-break and acceptance beyond sweep extreme before entry",
        },
        config=asdict(cfg),
    )


# Convenient alias for future app integration. It does not import app.py.
analyze_v2 = analyze_gold_v2


__all__ = [
    "V2Config",
    "analyze_gold_v2",
    "analyze_v2",
    "build_15m_liquidity_levels",
    "determine_15m_bias",
    "detect_5m_sweep_reclaim",
    "detect_1m_mss",
    "market_condition_gate",
    "late_entry_check",
    "calculate_structural_sl",
    "calculate_dynamic_targets",
    "calculate_informational_confidence",
    "build_management_plan",
    "protected_stop_from_continuation",
    "setup_invalidated_before_entry",
]
