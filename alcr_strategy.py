
"""
ALCR v1.0
Adaptive Liquidity Continuation & Reversal Strategy

Standalone research strategy module for SmartFS.

IMPORTANT:
- This module does NOT fetch market data.
- It does NOT modify V2/V3.
- It does NOT send Telegram messages.
- It does NOT write to Supabase.
- The caller supplies already-fetched/cached OHLCV DataFrames.
- It is designed to sit on top of the existing shared market-data/cache layer.

Supported universe:
    Forex:  EUR/USD, GBP/USD, USD/JPY
    Crypto: BTC/USDT, ETH/USDT, SOL/USDT

Signal threshold:
    70+ = eligible signal, provided all hard gates pass.
    60-69 = near-miss / research-only, never a signal.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import math

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FOREX_PAIRS = {"EUR/USD", "GBP/USD", "USD/JPY"}
CRYPTO_PAIRS = {"BTC/USDT", "ETH/USDT", "SOL/USDT"}
ALCR_PAIRS = FOREX_PAIRS | CRYPTO_PAIRS

SIGNAL_THRESHOLD = 70
NEAR_MISS_THRESHOLD = 60
MIN_RR = 1.50
MAX_EXTENSION_ATR = 1.25
LIQUIDITY_CLUSTER_ATR = 0.20
REVERSAL_CONFIRMATION_CANDLES = 4
CONTINUATION_PULLBACK_ATR = 0.25
COOLDOWN_MINUTES = 30

SETUP_TTL_MINUTES = {
    "CONTINUATION": 30,
    "LIQUIDITY_REVERSAL": 20,
    "BREAKOUT_RETEST": 45,
}

# Starting hypotheses. These are deliberately configurable and should be
# calibrated later with historical/out-of-sample testing.
@dataclass(frozen=True)
class ALCRConfig:
    ema_period: int = 50
    atr_period: int = 14

    forex_context_tf: str = "1h"
    forex_setup_tf: str = "15m"
    forex_trigger_tf: str = "5m"

    crypto_context_tf: str = "4h"
    crypto_structure_tf: str = "1h"
    crypto_setup_tf: str = "15m"
    crypto_trigger_tf: str = "5m"

    swing_left_context: int = 3
    swing_right_context: int = 3
    swing_left_setup: int = 3
    swing_right_setup: int = 3
    swing_left_trigger: int = 2
    swing_right_trigger: int = 2

    min_context_swing_atr: float = 0.35
    min_trigger_swing_atr: float = 0.20

    liquidity_cluster_atr: float = LIQUIDITY_CLUSTER_ATR
    sweep_min_atr: float = 0.05
    continuation_pullback_atr: float = CONTINUATION_PULLBACK_ATR
    max_extension_atr: float = MAX_EXTENSION_ATR
    min_rr: float = MIN_RR

    score_threshold: int = SIGNAL_THRESHOLD
    near_miss_threshold: int = NEAR_MISS_THRESHOLD

    momentum_score_threshold: float = 60.0

    cooldown_minutes: int = COOLDOWN_MINUTES
    one_active_trade_per_pair: bool = True

    # Volatility classification uses rolling ATR distribution.
    volatility_lookback: int = 100

    # How much recent history is used to build the liquidity map.
    liquidity_lookback: int = 300


# ---------------------------------------------------------------------------
# Public result objects
# ---------------------------------------------------------------------------

class SetupType(str, Enum):
    CONTINUATION = "CONTINUATION"
    LIQUIDITY_REVERSAL = "LIQUIDITY_REVERSAL"
    BREAKOUT_RETEST = "BREAKOUT_RETEST"


class Lifecycle(str, Enum):
    DETECTED = "DETECTED"
    VALIDATING = "VALIDATING"
    READY = "READY"
    TRIGGERED = "TRIGGERED"
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"


@dataclass
class ScoreBreakdown:
    environment: float = 0.0      # /20
    location: float = 0.0         # /20
    setup_quality: float = 0.0    # /15
    momentum: float = 0.0         # /15
    structure: float = 0.0        # /15
    risk_quality: float = 0.0     # /5
    target_quality: float = 0.0   # /10

    @property
    def total(self) -> float:
        return round(
            self.environment
            + self.location
            + self.setup_quality
            + self.momentum
            + self.structure
            + self.risk_quality
            + self.target_quality,
            2,
        )

    def as_dict(self) -> Dict[str, float]:
        return {
            "environment": round(self.environment, 2),
            "location": round(self.location, 2),
            "setup_quality": round(self.setup_quality, 2),
            "momentum": round(self.momentum, 2),
            "structure": round(self.structure, 2),
            "risk_quality": round(self.risk_quality, 2),
            "target_quality": round(self.target_quality, 2),
            "total": self.total,
        }


@dataclass
class SignalCandidate:
    pair: str
    market_type: str
    setup_type: str
    direction: str
    score: float
    score_breakdown: Dict[str, float]
    entry: float
    stop_loss: float
    tp1: float
    tp2: Optional[float]
    rr_to_tp1: float
    rr_to_tp2: Optional[float]
    lifecycle: str
    setup_detected_at: Optional[str]
    trigger_at: Optional[str]
    expires_at: Optional[str]
    regime: str
    volatility_state: str
    location: str
    reasons: List[str] = field(default_factory=list)
    hard_failures: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_signal(self) -> bool:
        return (
            self.score >= SIGNAL_THRESHOLD
            and not self.hard_failures
            and self.rr_to_tp1 >= MIN_RR
        )

    @property
    def classification(self) -> str:
        if self.is_signal:
            return "SIGNAL"
        if self.score >= NEAR_MISS_THRESHOLD:
            return "NEAR_MISS"
        return "REJECTED"

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["classification"] = self.classification
        d["is_signal"] = self.is_signal
        return d


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = ("open", "high", "low", "close")


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        raise ValueError("OHLC dataframe is empty.")

    out = df.copy()
    out.columns = [str(c).lower() for c in out.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in out.columns]
    if missing:
        raise ValueError(f"Missing OHLC columns: {missing}")

    for c in REQUIRED_COLUMNS:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=list(REQUIRED_COLUMNS)).copy()

    if not isinstance(out.index, pd.DatetimeIndex):
        if "timestamp" in out.columns:
            out.index = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
        elif "time" in out.columns:
            out.index = pd.to_datetime(out["time"], utc=True, errors="coerce")
        else:
            # Keep a usable index even for unit tests.
            out.index = pd.RangeIndex(len(out))

    if isinstance(out.index, pd.DatetimeIndex):
        if out.index.tz is None:
            out.index = out.index.tz_localize("UTC")
        else:
            out.index = out.index.tz_convert("UTC")
        out = out[~out.index.duplicated(keep="last")].sort_index()

    return out


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
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _ema(df: pd.DataFrame, period: int) -> pd.Series:
    return df["close"].ewm(span=period, adjust=False, min_periods=period).mean()


def _safe_last(series: pd.Series, default: float = 0.0) -> float:
    if series is None or len(series) == 0:
        return default
    value = series.iloc[-1]
    return default if pd.isna(value) else float(value)


def _timestamp_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


# ---------------------------------------------------------------------------
# Swing / structure helpers
# ---------------------------------------------------------------------------

def _confirmed_swings(
    df: pd.DataFrame,
    left: int,
    right: int,
) -> Tuple[pd.Series, pd.Series]:
    """
    Returns confirmed pivot-high / pivot-low boolean Series.

    A pivot is only marked after `right` candles have printed. This avoids
    treating an unconfirmed swing as known information.
    """
    n = len(df)
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()

    sh = np.zeros(n, dtype=bool)
    sl = np.zeros(n, dtype=bool)

    for i in range(left, n - right):
        h = high[i]
        l = low[i]

        left_highs = high[i - left:i]
        right_highs = high[i + 1:i + right + 1]
        left_lows = low[i - left:i]
        right_lows = low[i + 1:i + right + 1]

        if h >= np.max(left_highs) and h > np.max(right_highs):
            sh[i] = True
        if l <= np.min(left_lows) and l < np.min(right_lows):
            sl[i] = True

    return pd.Series(sh, index=df.index), pd.Series(sl, index=df.index)


def _swing_points(
    df: pd.DataFrame,
    left: int,
    right: int,
    min_displacement: float,
    atr: pd.Series,
) -> Tuple[List[Tuple[Any, float]], List[Tuple[Any, float]]]:
    sh, sl = _confirmed_swings(df, left, right)

    highs: List[Tuple[Any, float]] = []
    lows: List[Tuple[Any, float]] = []

    for i in np.flatnonzero(sh.to_numpy()):
        a = _safe_last(atr.iloc[max(0, i - 5):i + 1], 0.0)
        if a > 0:
            # Filter microscopic swings.
            if i > 0 and abs(float(df["high"].iloc[i]) - float(df["close"].iloc[max(0, i - 1)])) < min_displacement * a:
                continue
        highs.append((df.index[i], float(df["high"].iloc[i])))

    for i in np.flatnonzero(sl.to_numpy()):
        a = _safe_last(atr.iloc[max(0, i - 5):i + 1], 0.0)
        if a > 0:
            if i > 0 and abs(float(df["low"].iloc[i]) - float(df["close"].iloc[max(0, i - 1)])) < min_displacement * a:
                continue
        lows.append((df.index[i], float(df["low"].iloc[i])))

    return highs, lows


def _structure_bias(
    df: pd.DataFrame,
    left: int,
    right: int,
    min_displacement_atr: float,
    atr: pd.Series,
) -> str:
    highs, lows = _swing_points(
        df.tail(250),
        left,
        right,
        min_displacement_atr,
        atr.tail(250),
    )

    if len(highs) < 2 or len(lows) < 2:
        return "UNCLEAR"

    h1, h2 = highs[-2][1], highs[-1][1]
    l1, l2 = lows[-2][1], lows[-1][1]

    if h2 > h1 and l2 > l1:
        return "BULLISH"
    if h2 < h1 and l2 < l1:
        return "BEARISH"
    return "RANGE"


# ---------------------------------------------------------------------------
# Volatility / regime
# ---------------------------------------------------------------------------

def _volatility_state(atr: pd.Series, lookback: int) -> str:
    current = _safe_last(atr, 0.0)
    if current <= 0:
        return "UNKNOWN"

    hist = atr.dropna().tail(lookback)
    if len(hist) < max(20, lookback // 4):
        return "NORMAL"

    p20 = float(hist.quantile(0.20))
    p80 = float(hist.quantile(0.80))
    p95 = float(hist.quantile(0.95))

    if current < p20:
        return "LOW"
    if current <= p80:
        return "NORMAL"
    if current <= p95:
        return "HIGH"
    return "EXTREME"


def _market_regime(
    context_df: pd.DataFrame,
    setup_df: pd.DataFrame,
    cfg: ALCRConfig,
) -> Tuple[str, str, str]:
    c = _prepare(context_df)
    s = _prepare(setup_df)

    c_atr = _atr(c, cfg.atr_period)
    s_atr = _atr(s, cfg.atr_period)

    c_ema = _ema(c, cfg.ema_period)

    c_bias = _structure_bias(
        c,
        cfg.swing_left_context,
        cfg.swing_right_context,
        cfg.min_context_swing_atr,
        c_atr,
    )

    ema_now = _safe_last(c_ema)
    ema_prev = _safe_last(c_ema.iloc[:-3], ema_now) if len(c_ema) > 3 else ema_now
    close = float(c["close"].iloc[-1])

    ema_slope = ema_now - ema_prev
    atr_now = _safe_last(c_atr)

    if c_bias == "BULLISH" and close > ema_now and ema_slope >= 0:
        regime = "TREND_BULLISH"
    elif c_bias == "BEARISH" and close < ema_now and ema_slope <= 0:
        regime = "TREND_BEARISH"
    elif c_bias in {"BULLISH", "BEARISH"}:
        regime = "PULLBACK"
    elif c_bias == "RANGE":
        regime = "RANGE"
    else:
        # Compression is a watch state rather than an automatic rejection.
        if atr_now > 0 and _volatility_state(s_atr, cfg.volatility_lookback) == "LOW":
            regime = "COMPRESSION"
        else:
            regime = "UNCLEAR"

    return regime, c_bias, _volatility_state(s_atr, cfg.volatility_lookback)


# ---------------------------------------------------------------------------
# Liquidity map
# ---------------------------------------------------------------------------

@dataclass
class LiquidityLevel:
    price: float
    kind: str
    strength: float
    source: str


def _cluster_levels(
    raw: List[LiquidityLevel],
    cluster_distance: float,
) -> List[LiquidityLevel]:
    if not raw:
        return []

    raw = sorted(raw, key=lambda x: x.price)
    clusters: List[List[LiquidityLevel]] = [[raw[0]]]

    for level in raw[1:]:
        center = float(np.mean([x.price for x in clusters[-1]]))
        if abs(level.price - center) <= cluster_distance:
            clusters[-1].append(level)
        else:
            clusters.append([level])

    out: List[LiquidityLevel] = []
    for cluster in clusters:
        weights = np.array([max(0.1, x.strength) for x in cluster])
        prices = np.array([x.price for x in cluster])
        center = float(np.average(prices, weights=weights))
        strength = min(1.0, float(sum(x.strength for x in cluster)) / 3.0)
        kinds = {x.kind for x in cluster}
        kind = "MIXED" if len(kinds) > 1 else next(iter(kinds))
        source = "+".join(sorted({x.source for x in cluster}))
        out.append(LiquidityLevel(center, kind, strength, source))

    return out


def _liquidity_map(
    df: pd.DataFrame,
    cfg: ALCRConfig,
) -> List[LiquidityLevel]:
    d = _prepare(df).tail(cfg.liquidity_lookback)
    atr = _atr(d, cfg.atr_period)
    atr_now = _safe_last(atr, 0.0)
    cluster_distance = max(atr_now * cfg.liquidity_cluster_atr, 1e-12)

    levels: List[LiquidityLevel] = []

    # Confirmed swing liquidity.
    sh, sl = _confirmed_swings(d, cfg.swing_left_setup, cfg.swing_right_setup)
    for i in np.flatnonzero(sh.to_numpy()):
        age = len(d) - 1 - int(i)
        strength = 1.0 if age <= 100 else 0.7
        levels.append(LiquidityLevel(float(d["high"].iloc[i]), "HIGH", strength, "SWING"))

    for i in np.flatnonzero(sl.to_numpy()):
        age = len(d) - 1 - int(i)
        strength = 1.0 if age <= 100 else 0.7
        levels.append(LiquidityLevel(float(d["low"].iloc[i]), "LOW", strength, "SWING"))

    # Previous day high/low when a datetime index is available.
    if isinstance(d.index, pd.DatetimeIndex) and len(d) > 2:
        utc = d.copy()
        groups = utc.groupby(utc.index.date)
        dates = list(groups.groups.keys())
        if len(dates) >= 2:
            prev_date = dates[-2]
            prev = groups.get_group(prev_date)
            levels.append(
                LiquidityLevel(float(prev["high"].max()), "HIGH", 1.0, "PREVIOUS_DAY")
            )
            levels.append(
                LiquidityLevel(float(prev["low"].min()), "LOW", 1.0, "PREVIOUS_DAY")
            )

    return _cluster_levels(levels, cluster_distance)


def _nearest_liquidity(
    levels: List[LiquidityLevel],
    price: float,
    direction: str,
    minimum_distance: float = 0.0,
) -> List[LiquidityLevel]:
    if direction == "BUY":
        candidates = [
            x for x in levels
            if x.price > price + minimum_distance
            and x.kind in {"HIGH", "MIXED"}
        ]
        return sorted(candidates, key=lambda x: x.price)
    candidates = [
        x for x in levels
        if x.price < price - minimum_distance
        and x.kind in {"LOW", "MIXED"}
    ]
    return sorted(candidates, key=lambda x: x.price, reverse=True)


def _nearest_opposing_level(
    levels: List[LiquidityLevel],
    price: float,
    direction: str,
) -> Optional[LiquidityLevel]:
    candidates = _nearest_liquidity(levels, price, direction)
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# Price-action helpers
# ---------------------------------------------------------------------------

def _body_ratio(row: pd.Series) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0
    return abs(float(row["close"] - row["open"])) / rng


def _directional_close_strength(row: pd.Series, direction: str) -> float:
    rng = float(row["high"] - row["low"])
    if rng <= 0:
        return 0.0

    if direction == "BUY":
        return max(0.0, min(1.0, (float(row["close"]) - float(row["low"])) / rng))
    return max(0.0, min(1.0, (float(row["high"]) - float(row["close"])) / rng))


def _momentum_score(
    trigger: pd.DataFrame,
    direction: str,
    cfg: ALCRConfig,
) -> float:
    d = _prepare(trigger)
    atr = _atr(d, cfg.atr_period)

    if len(d) < 4:
        return 0.0

    recent = d.tail(3)
    atr_now = _safe_last(atr, 0.0)
    if atr_now <= 0:
        return 0.0

    last = recent.iloc[-1]
    body = abs(float(last["close"] - last["open"]))
    body_atr = body / atr_now
    close_strength = _directional_close_strength(last, direction)

    if direction == "BUY":
        directional = sum(
            1 for _, r in recent.iterrows()
            if float(r["close"]) > float(r["open"])
        )
    else:
        directional = sum(
            1 for _, r in recent.iterrows()
            if float(r["close"]) < float(r["open"])
        )

    # 0-100 internal momentum quality.
    score = (
        min(40.0, body_atr / 0.8 * 40.0)
        + close_strength * 35.0
        + (directional / 3.0) * 25.0
    )
    return max(0.0, min(100.0, score))


def _structure_confirmation(
    trigger: pd.DataFrame,
    direction: str,
    cfg: ALCRConfig,
) -> Tuple[float, bool, str]:
    d = _prepare(trigger)
    if len(d) < 8:
        return 0.0, False, "insufficient_trigger_history"

    atr = _atr(d, cfg.atr_period)
    highs, lows = _swing_points(
        d,
        cfg.swing_left_trigger,
        cfg.swing_right_trigger,
        cfg.min_trigger_swing_atr,
        atr,
    )

    if direction == "BUY":
        if not highs or not lows:
            return 0.0, False, "no_confirmed_trigger_swings"

        last_high = highs[-1][1]
        last_low = lows[-1][1]
        close = float(d["close"].iloc[-1])

        if close > last_high:
            return 100.0, True, "bullish_structure_break"
        if close > last_low:
            return 55.0, False, "partial_bullish_structure"
        return 20.0, False, "bullish_structure_not_confirmed"

    if not highs or not lows:
        return 0.0, False, "no_confirmed_trigger_swings"

    last_high = highs[-1][1]
    last_low = lows[-1][1]
    close = float(d["close"].iloc[-1])

    if close < last_low:
        return 100.0, True, "bearish_structure_break"
    if close < last_high:
        return 55.0, False, "partial_bearish_structure"
    return 20.0, False, "bearish_structure_not_confirmed"


def _extension_ok(
    entry: float,
    zone_price: float,
    atr: float,
    cfg: ALCRConfig,
) -> bool:
    if atr <= 0:
        return False
    return abs(entry - zone_price) <= cfg.max_extension_atr * atr


def _rr(entry: float, stop: float, target: float) -> float:
    risk = abs(entry - stop)
    if risk <= 0:
        return 0.0
    reward = abs(target - entry)
    return reward / risk


# ---------------------------------------------------------------------------
# Candidate construction
# ---------------------------------------------------------------------------

def _score(
    environment: float,
    location: float,
    setup_quality: float,
    momentum_100: float,
    structure_100: float,
    risk_quality: float,
    target_quality: float,
) -> ScoreBreakdown:
    return ScoreBreakdown(
        environment=max(0.0, min(20.0, environment)),
        location=max(0.0, min(20.0, location)),
        setup_quality=max(0.0, min(15.0, setup_quality)),
        momentum=max(0.0, min(15.0, momentum_100 * 0.15)),
        structure=max(0.0, min(15.0, structure_100 * 0.15)),
        risk_quality=max(0.0, min(5.0, risk_quality)),
        target_quality=max(0.0, min(10.0, target_quality)),
    )


def _candidate(
    *,
    pair: str,
    market_type: str,
    setup_type: str,
    direction: str,
    score: ScoreBreakdown,
    entry: float,
    stop: float,
    tp1: float,
    tp2: Optional[float],
    lifecycle: str,
    detected_at: Any,
    trigger_at: Any,
    regime: str,
    volatility_state: str,
    location: str,
    reasons: List[str],
    hard_failures: List[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> SignalCandidate:
    rr1 = _rr(entry, stop, tp1)
    rr2 = _rr(entry, stop, tp2) if tp2 is not None else None

    expires_at = None
    if isinstance(trigger_at, pd.Timestamp):
        ttl = SETUP_TTL_MINUTES[setup_type]
        expires_at = (trigger_at + pd.Timedelta(minutes=ttl)).isoformat()

    return SignalCandidate(
        pair=pair,
        market_type=market_type,
        setup_type=setup_type,
        direction=direction,
        score=score.total,
        score_breakdown=score.as_dict(),
        entry=entry,
        stop_loss=stop,
        tp1=tp1,
        tp2=tp2,
        rr_to_tp1=round(rr1, 3),
        rr_to_tp2=round(rr2, 3) if rr2 is not None else None,
        lifecycle=lifecycle,
        setup_detected_at=_timestamp_string(detected_at),
        trigger_at=_timestamp_string(trigger_at),
        expires_at=expires_at,
        regime=regime,
        volatility_state=volatility_state,
        location=location,
        reasons=reasons,
        hard_failures=hard_failures,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# Setup detectors
# ---------------------------------------------------------------------------

def _continuation(
    pair: str,
    market_type: str,
    context: pd.DataFrame,
    setup: pd.DataFrame,
    trigger: pd.DataFrame,
    levels: List[LiquidityLevel],
    regime: str,
    context_bias: str,
    volatility_state: str,
    cfg: ALCRConfig,
) -> Optional[SignalCandidate]:
    s = _prepare(setup)
    t = _prepare(trigger)

    if len(s) < 10 or len(t) < 10:
        return None

    atr_s = _atr(s, cfg.atr_period)
    atr_t = _atr(t, cfg.atr_period)
    atr_now = _safe_last(atr_t, 0.0)
    if atr_now <= 0:
        return None

    direction = "BUY" if context_bias == "BULLISH" else "SELL" if context_bias == "BEARISH" else None
    if direction is None:
        return None

    entry = float(t["close"].iloc[-1])
    s_high = float(s["high"].tail(8).max())
    s_low = float(s["low"].tail(8).min())

    # A pullback is meaningful when price has moved against the trend by
    # at least ~0.25 ATR but has not broken the broader setup structure.
    if direction == "BUY":
        impulse_high = float(s["high"].tail(20).max())
        pullback_low = float(s["low"].tail(5).min())
        pullback_depth = impulse_high - pullback_low
        if pullback_depth < cfg.continuation_pullback_atr * _safe_last(atr_s, atr_now):
            return None

        zone = _nearest_liquidity(levels, entry, "BUY")
        zone_price = zone[0].price if zone else pullback_low
        if entry < zone_price:
            zone_price = pullback_low

        structure_score, confirmed, structure_reason = _structure_confirmation(t, "BUY", cfg)
        momentum_100 = _momentum_score(t, "BUY", cfg)

        if not confirmed:
            return _candidate(
                pair=pair, market_type=market_type,
                setup_type=SetupType.CONTINUATION.value, direction="BUY",
                score=_score(14, 12, 10, momentum_100, structure_score, 4, 5),
                entry=entry, stop=pullback_low - 0.15 * atr_now,
                tp1=max(entry + 0.1 * atr_now, zone_price),
                tp2=None,
                lifecycle=Lifecycle.VALIDATING.value,
                detected_at=t.index[-1], trigger_at=None,
                regime=regime, volatility_state=volatility_state,
                location="pullback",
                reasons=[structure_reason],
                hard_failures=["continuation_trigger_not_confirmed"],
                metadata={"pullback_depth_atr": pullback_depth / max(_safe_last(atr_s, atr_now), 1e-12)},
            )

        stop = pullback_low - 0.15 * atr_now
        target = _nearest_opposing_level(levels, entry, "BUY")
        if target is None:
            return None

        tp1 = target.price
        candidates = _nearest_liquidity(levels, tp1, "BUY", minimum_distance=atr_now * 0.25)
        tp2 = candidates[0].price if candidates else None

        location_score = 17.0 if zone else 10.0
        env_score = 18.0 if regime == "TREND_BULLISH" else 15.0
        setup_score = 13.0
        risk = _rr(entry, stop, tp1)
        risk_score = 5.0 if risk >= 2.0 else 4.0 if risk >= 1.5 else 1.0
        target_score = 10.0 if tp2 is not None else 7.0

        score = _score(env_score, location_score, setup_score, momentum_100,
                       structure_score, risk_score, target_score)

        failures = []
        if risk < cfg.min_rr:
            failures.append(f"rr_below_{cfg.min_rr:.2f}")
        if not _extension_ok(entry, zone_price, atr_now, cfg):
            failures.append("entry_too_extended")

        return _candidate(
            pair=pair, market_type=market_type,
            setup_type=SetupType.CONTINUATION.value, direction="BUY",
            score=score, entry=entry, stop=stop, tp1=tp1, tp2=tp2,
            lifecycle=Lifecycle.TRIGGERED.value,
            detected_at=t.index[-1], trigger_at=t.index[-1],
            regime=regime, volatility_state=volatility_state,
            location="pullback",
            reasons=["trend continuation", "structure confirmed", "momentum confirmed"],
            hard_failures=failures,
            metadata={"structure_reason": structure_reason},
        )

    # SELL
    impulse_low = float(s["low"].tail(20).min())
    pullback_high = float(s["high"].tail(5).max())
    pullback_depth = pullback_high - impulse_low
    if pullback_depth < cfg.continuation_pullback_atr * _safe_last(atr_s, atr_now):
        return None

    zone = _nearest_liquidity(levels, entry, "SELL")
    zone_price = zone[0].price if zone else pullback_high
    if entry > zone_price:
        zone_price = pullback_high

    structure_score, confirmed, structure_reason = _structure_confirmation(t, "SELL", cfg)
    momentum_100 = _momentum_score(t, "SELL", cfg)

    if not confirmed:
        return _candidate(
            pair=pair, market_type=market_type,
            setup_type=SetupType.CONTINUATION.value, direction="SELL",
            score=_score(14, 12, 10, momentum_100, structure_score, 4, 5),
            entry=entry, stop=pullback_high + 0.15 * atr_now,
            tp1=min(entry - 0.1 * atr_now, zone_price),
            tp2=None,
            lifecycle=Lifecycle.VALIDATING.value,
            detected_at=t.index[-1], trigger_at=None,
            regime=regime, volatility_state=volatility_state,
            location="pullback",
            reasons=[structure_reason],
            hard_failures=["continuation_trigger_not_confirmed"],
            metadata={"pullback_depth_atr": pullback_depth / max(_safe_last(atr_s, atr_now), 1e-12)},
        )

    stop = pullback_high + 0.15 * atr_now
    target = _nearest_opposing_level(levels, entry, "SELL")
    if target is None:
        return None

    tp1 = target.price
    candidates = _nearest_liquidity(levels, tp1, "SELL", minimum_distance=atr_now * 0.25)
    tp2 = candidates[0].price if candidates else None

    location_score = 17.0 if zone else 10.0
    env_score = 18.0 if regime == "TREND_BEARISH" else 15.0
    setup_score = 13.0
    risk = _rr(entry, stop, tp1)
    risk_score = 5.0 if risk >= 2.0 else 4.0 if risk >= 1.5 else 1.0
    target_score = 10.0 if tp2 is not None else 7.0

    score = _score(env_score, location_score, setup_score, momentum_100,
                   structure_score, risk_score, target_score)

    failures = []
    if risk < cfg.min_rr:
        failures.append(f"rr_below_{cfg.min_rr:.2f}")
    if not _extension_ok(entry, zone_price, atr_now, cfg):
        failures.append("entry_too_extended")

    return _candidate(
        pair=pair, market_type=market_type,
        setup_type=SetupType.CONTINUATION.value, direction="SELL",
        score=score, entry=entry, stop=stop, tp1=tp1, tp2=tp2,
        lifecycle=Lifecycle.TRIGGERED.value,
        detected_at=t.index[-1], trigger_at=t.index[-1],
        regime=regime, volatility_state=volatility_state,
        location="pullback",
        reasons=["trend continuation", "structure confirmed", "momentum confirmed"],
        hard_failures=failures,
        metadata={"structure_reason": structure_reason},
    )


def _liquidity_reversal(
    pair: str,
    market_type: str,
    context: pd.DataFrame,
    setup: pd.DataFrame,
    trigger: pd.DataFrame,
    levels: List[LiquidityLevel],
    regime: str,
    volatility_state: str,
    cfg: ALCRConfig,
) -> Optional[SignalCandidate]:
    t = _prepare(trigger)
    if len(t) < 12:
        return None

    atr = _atr(t, cfg.atr_period)
    atr_now = _safe_last(atr, 0.0)
    if atr_now <= 0:
        return None

    last = t.iloc[-1]
    previous = t.iloc[-2]

    candidates = []
    for level in levels:
        distance = abs(float(last["high"] if level.kind == "HIGH" else last["low"]) - level.price)
        if distance <= 2.0 * atr_now:
            candidates.append(level)

    if not candidates:
        return None

    # Most recent candle can sweep a high and close back below it => SELL.
    # Or sweep a low and close back above it => BUY.
    best = None
    direction = None
    rejection = 0.0

    for level in candidates:
        if level.kind in {"HIGH", "MIXED"}:
            swept = float(last["high"]) >= level.price + cfg.sweep_min_atr * atr_now
            rejected = float(last["close"]) < level.price
            if swept and rejected:
                strength = (float(last["high"]) - float(last["close"])) / max(
                    float(last["high"] - last["low"]), 1e-12
                )
                if strength > rejection:
                    best, direction, rejection = level, "SELL", strength

        if level.kind in {"LOW", "MIXED"}:
            swept = float(last["low"]) <= level.price - cfg.sweep_min_atr * atr_now
            rejected = float(last["close"]) > level.price
            if swept and rejected:
                strength = (float(last["close"]) - float(last["low"])) / max(
                    float(last["high"] - last["low"]), 1e-12
                )
                if strength > rejection:
                    best, direction, rejection = level, "BUY", strength

    if best is None or direction is None:
        return None

    structure_score, confirmed, structure_reason = _structure_confirmation(t, direction, cfg)
    momentum_100 = _momentum_score(t, direction, cfg)

    entry = float(last["close"])
    sweep_extreme = float(last["low"] if direction == "BUY" else last["high"])

    stop_buffer = 0.15 * atr_now
    stop = sweep_extreme - stop_buffer if direction == "BUY" else sweep_extreme + stop_buffer

    target = _nearest_opposing_level(levels, entry, direction)
    if target is None:
        return None

    tp1 = target.price
    candidates2 = _nearest_liquidity(levels, tp1, direction, minimum_distance=0.25 * atr_now)
    tp2 = candidates2[0].price if candidates2 else None

    location_score = 19.0 if best.source in {"PREVIOUS_DAY", "SWING"} and best.strength >= 0.8 else 15.0
    setup_score = 14.0 if rejection >= 0.55 else 10.0
    env_score = 16.0 if regime not in {"CHAOTIC", "UNCLEAR"} else 10.0
    risk = _rr(entry, stop, tp1)
    risk_score = 5.0 if risk >= 2.0 else 4.0 if risk >= 1.5 else 1.0
    target_score = 10.0 if tp2 is not None else 7.0

    score = _score(
        env_score, location_score, setup_score,
        momentum_100, structure_score, risk_score, target_score
    )

    failures: List[str] = []
    if not confirmed:
        failures.append("reversal_structure_not_confirmed")
    if momentum_100 < cfg.momentum_score_threshold:
        failures.append("momentum_below_threshold")
    if risk < cfg.min_rr:
        failures.append(f"rr_below_{cfg.min_rr:.2f}")
    if not _extension_ok(entry, best.price, atr_now, cfg):
        failures.append("entry_too_extended")

    return _candidate(
        pair=pair, market_type=market_type,
        setup_type=SetupType.LIQUIDITY_REVERSAL.value,
        direction=direction, score=score,
        entry=entry, stop=stop, tp1=tp1, tp2=tp2,
        lifecycle=Lifecycle.TRIGGERED.value if confirmed else Lifecycle.VALIDATING.value,
        detected_at=t.index[-1], trigger_at=t.index[-1] if confirmed else None,
        regime=regime, volatility_state=volatility_state,
        location=f"{best.source.lower()}_{best.kind.lower()}",
        reasons=[
            "meaningful liquidity swept",
            "price reclaimed liquidity",
            f"rejection_strength={rejection:.2f}",
        ],
        hard_failures=failures,
        metadata={
            "liquidity_price": best.price,
            "liquidity_source": best.source,
            "rejection_strength": round(rejection, 4),
            "structure_reason": structure_reason,
        },
    )


def _breakout_retest(
    pair: str,
    market_type: str,
    context: pd.DataFrame,
    setup: pd.DataFrame,
    trigger: pd.DataFrame,
    levels: List[LiquidityLevel],
    regime: str,
    volatility_state: str,
    cfg: ALCRConfig,
) -> Optional[SignalCandidate]:
    s = _prepare(setup)
    t = _prepare(trigger)
    if len(s) < 30 or len(t) < 12:
        return None

    atr_s = _atr(s, cfg.atr_period)
    atr_t = _atr(t, cfg.atr_period)
    atr_now = _safe_last(atr_t, 0.0)
    if atr_now <= 0:
        return None

    # Compression proxy: relatively narrow recent range.
    box = s.iloc[-12:-3]
    if len(box) < 6:
        return None

    box_high = float(box["high"].max())
    box_low = float(box["low"].min())
    box_width = box_high - box_low

    avg_atr = float(atr_s.iloc[-12:-3].mean()) if atr_s.iloc[-12:-3].notna().any() else 0.0
    if avg_atr <= 0 or box_width > 4.0 * avg_atr:
        return None

    last_s = s.iloc[-1]
    direction = None
    breakout_level = None

    if float(last_s["close"]) > box_high + 0.05 * _safe_last(atr_s):
        direction = "BUY"
        breakout_level = box_high
    elif float(last_s["close"]) < box_low - 0.05 * _safe_last(atr_s):
        direction = "SELL"
        breakout_level = box_low
    else:
        return None

    # Retest is checked on the trigger timeframe.
    last = t.iloc[-1]
    if direction == "BUY":
        retested = float(last["low"]) <= breakout_level + 0.20 * atr_now
        held = float(last["close"]) > breakout_level
    else:
        retested = float(last["high"]) >= breakout_level - 0.20 * atr_now
        held = float(last["close"]) < breakout_level

    if not (retested and held):
        return _candidate(
            pair=pair, market_type=market_type,
            setup_type=SetupType.BREAKOUT_RETEST.value,
            direction=direction,
            score=_score(15, 15, 11, _momentum_score(t, direction, cfg), 45, 4, 5),
            entry=float(last["close"]),
            stop=(breakout_level - 0.75 * atr_now if direction == "BUY"
                  else breakout_level + 0.75 * atr_now),
            tp1=(breakout_level + atr_now if direction == "BUY"
                 else breakout_level - atr_now),
            tp2=None,
            lifecycle=Lifecycle.VALIDATING.value,
            detected_at=t.index[-1], trigger_at=None,
            regime=regime, volatility_state=volatility_state,
            location="compression_boundary",
            reasons=["breakout detected", "retest not yet confirmed"],
            hard_failures=["breakout_retest_not_confirmed"],
            metadata={"breakout_level": breakout_level},
        )

    entry = float(last["close"])
    structure_score, confirmed, structure_reason = _structure_confirmation(t, direction, cfg)
    momentum_100 = _momentum_score(t, direction, cfg)

    stop = (
        breakout_level - 0.75 * atr_now
        if direction == "BUY"
        else breakout_level + 0.75 * atr_now
    )

    target = _nearest_opposing_level(levels, entry, direction)
    if target is None:
        # If no mapped target exists, the setup is not tradeable.
        return _candidate(
            pair=pair, market_type=market_type,
            setup_type=SetupType.BREAKOUT_RETEST.value,
            direction=direction,
            score=_score(17, 17, 14, momentum_100, structure_score, 4, 0),
            entry=entry, stop=stop,
            tp1=entry, tp2=None,
            lifecycle=Lifecycle.INVALIDATED.value,
            detected_at=t.index[-1], trigger_at=None,
            regime=regime, volatility_state=volatility_state,
            location="compression_boundary",
            reasons=["breakout and retest confirmed"],
            hard_failures=["no_meaningful_target"],
            metadata={"breakout_level": breakout_level, "structure_reason": structure_reason},
        )

    tp1 = target.price
    next_levels = _nearest_liquidity(levels, tp1, direction, minimum_distance=0.25 * atr_now)
    tp2 = next_levels[0].price if next_levels else None

    risk = _rr(entry, stop, tp1)
    score = _score(
        18 if regime in {"COMPRESSION", "RANGE", "PULLBACK"} else 15,
        18,
        14,
        momentum_100,
        structure_score,
        5 if risk >= 2.0 else 4 if risk >= 1.5 else 1,
        10 if tp2 is not None else 7,
    )

    failures = []
    if not confirmed:
        failures.append("breakout_structure_not_confirmed")
    if momentum_100 < cfg.momentum_score_threshold:
        failures.append("momentum_below_threshold")
    if risk < cfg.min_rr:
        failures.append(f"rr_below_{cfg.min_rr:.2f}")
    if not _extension_ok(entry, breakout_level, atr_now, cfg):
        failures.append("entry_too_extended")

    return _candidate(
        pair=pair, market_type=market_type,
        setup_type=SetupType.BREAKOUT_RETEST.value,
        direction=direction, score=score,
        entry=entry, stop=stop, tp1=tp1, tp2=tp2,
        lifecycle=Lifecycle.TRIGGERED.value if confirmed else Lifecycle.VALIDATING.value,
        detected_at=t.index[-1], trigger_at=t.index[-1] if confirmed else None,
        regime=regime, volatility_state=volatility_state,
        location="compression_boundary",
        reasons=["compression", "breakout", "retest", "momentum"],
        hard_failures=failures,
        metadata={"breakout_level": breakout_level, "structure_reason": structure_reason},
    )


# ---------------------------------------------------------------------------
# Main analyzer
# ---------------------------------------------------------------------------

class ALCRAnalyzer:
    """
    Main ALCR engine.

    `analyze()` consumes already-fetched OHLC data. It never makes network
    requests, so it can reuse the existing SmartFS shared cache.

    Expected data keys:
        Forex:
            "1h", "15m", "5m"
        Crypto:
            "4h", "1h", "15m", "5m"

    Each value must be a pandas DataFrame containing:
        open, high, low, close
    and optionally volume.
    """

    def __init__(self, config: Optional[ALCRConfig] = None):
        self.cfg = config or ALCRConfig()

    def analyze(
        self,
        pair: str,
        frames: Dict[str, pd.DataFrame],
        *,
        active_trade: bool = False,
        last_completed_at: Optional[pd.Timestamp] = None,
        now: Optional[pd.Timestamp] = None,
    ) -> Dict[str, Any]:
        if pair not in ALCR_PAIRS:
            raise ValueError(
                f"ALCR does not support {pair}. Supported: {sorted(ALCR_PAIRS)}"
            )

        market_type = "CRYPTO" if pair in CRYPTO_PAIRS else "FOREX"

        if market_type == "FOREX":
            required = ("1h", "15m", "5m")
            for tf in required:
                if tf not in frames:
                    return self._no_data(pair, market_type, f"missing_{tf}")
            context = _prepare(frames["1h"])
            setup = _prepare(frames["15m"])
            trigger = _prepare(frames["5m"])
        else:
            required = ("4h", "1h", "15m", "5m")
            for tf in required:
                if tf not in frames:
                    return self._no_data(pair, market_type, f"missing_{tf}")
            context = _prepare(frames["4h"])
            structure = _prepare(frames["1h"])
            setup = _prepare(frames["15m"])
            trigger = _prepare(frames["5m"])

            # The crypto 1H structure becomes part of the context frame for
            # regime interpretation while the 4H frame remains the macro bias.
            # Keep the 4H frame as the primary context. The 1H structure is
            # evaluated separately below so it never mutates pandas attrs or
            # creates a second market-data path.
            crypto_structure_df = structure

        if min(len(context), len(setup), len(trigger)) < 30:
            return self._no_data(pair, market_type, "insufficient_candles")

        if active_trade and self.cfg.one_active_trade_per_pair:
            return self._blocked(pair, market_type, "active_trade_exists")

        if last_completed_at is not None:
            current_time = now or trigger.index[-1]
            if isinstance(current_time, pd.Timestamp) and isinstance(last_completed_at, pd.Timestamp):
                elapsed = (current_time - last_completed_at).total_seconds() / 60.0
                if elapsed < self.cfg.cooldown_minutes:
                    return self._blocked(
                        pair,
                        market_type,
                        f"cooldown_active_{elapsed:.1f}m",
                    )

        regime, context_bias, volatility_state = _market_regime(
            context, setup, self.cfg
        )

        # For crypto, require the 1H structure to be considered alongside the
        # 4H context. It does not replace the 4H context; it refines the state.
        if market_type == "CRYPTO":
            crypto_structure_bias = _structure_bias(
                crypto_structure_df,
                self.cfg.swing_left_context,
                self.cfg.swing_right_context,
                self.cfg.min_context_swing_atr,
                _atr(crypto_structure_df, self.cfg.atr_period),
            )
            if context_bias == "BULLISH" and crypto_structure_bias == "BEARISH":
                regime = "PULLBACK"
            elif context_bias == "BEARISH" and crypto_structure_bias == "BULLISH":
                regime = "PULLBACK"
            elif crypto_structure_bias in {"BULLISH", "BEARISH"} and context_bias == crypto_structure_bias:
                regime = "TREND_BULLISH" if crypto_structure_bias == "BULLISH" else "TREND_BEARISH"

        levels = _liquidity_map(setup, self.cfg)

        candidates: List[SignalCandidate] = []

        c = _continuation(
            pair, market_type, context, setup, trigger,
            levels, regime, context_bias, volatility_state, self.cfg
        )
        if c is not None:
            candidates.append(c)

        r = _liquidity_reversal(
            pair, market_type, context, setup, trigger,
            levels, regime, volatility_state, self.cfg
        )
        if r is not None:
            candidates.append(r)

        b = _breakout_retest(
            pair, market_type, context, setup, trigger,
            levels, regime, volatility_state, self.cfg
        )
        if b is not None:
            candidates.append(b)

        if not candidates:
            return self._no_setup(
                pair, market_type, regime, volatility_state, context_bias
            )

        # Best candidate is highest score; in a tie prefer one with no hard
        # failures, then highest R:R.
        candidates.sort(
            key=lambda x: (
                not bool(x.hard_failures),
                x.score,
                x.rr_to_tp1,
            ),
            reverse=True,
        )
        best = candidates[0]

        if best.score >= self.cfg.score_threshold and not best.hard_failures and best.rr_to_tp1 >= self.cfg.min_rr:
            classification = "SIGNAL"
        elif best.score >= self.cfg.near_miss_threshold:
            classification = "NEAR_MISS"
        else:
            classification = "REJECTED"

        return {
            "status": classification,
            "strategy_version": "ALCR_V1.0",
            "pair": pair,
            "market_type": market_type,
            "timeframe": "5m",
            "candidate_detected": True,
            "candidate_direction": best.direction,
            "outcome": best.classification,
            "failed_gate": best.hard_failures[0] if best.hard_failures else None,
            "reason": best.hard_failures[0] if best.hard_failures else "eligible_setup",
            "setup_type": best.setup_type,
            "regime": best.regime,
            "volatility_state": best.volatility_state,
            "score": best.score,
            "score_breakdown": best.score_breakdown,
            "entry": best.entry,
            "stop_loss": best.stop_loss,
            "tp1": best.tp1,
            "tp2": best.tp2,
            "rr_to_tp1": best.rr_to_tp1,
            "rr_to_tp2": best.rr_to_tp2,
            "lifecycle": best.lifecycle,
            "setup_detected_at": best.setup_detected_at,
            "trigger_at": best.trigger_at,
            "expires_at": best.expires_at,
            "location": best.location,
            "reasons": best.reasons,
            "hard_failures": best.hard_failures,
            "metadata": best.metadata,
            "all_candidates": [x.as_dict() for x in candidates],
        }

    @staticmethod
    def _no_data(pair: str, market_type: str, reason: str) -> Dict[str, Any]:
        return {
            "status": "NO_DATA",
            "strategy_version": "ALCR_V1.0",
            "pair": pair,
            "market_type": market_type,
            "timeframe": "5m",
            "candidate_detected": False,
            "candidate_direction": None,
            "outcome": "NO_DATA",
            "failed_gate": "DATA",
            "reason": reason,
        }

    @staticmethod
    def _blocked(pair: str, market_type: str, reason: str) -> Dict[str, Any]:
        return {
            "status": "BLOCKED",
            "strategy_version": "ALCR_V1.0",
            "pair": pair,
            "market_type": market_type,
            "timeframe": "5m",
            "candidate_detected": False,
            "candidate_direction": None,
            "outcome": "BLOCKED",
            "failed_gate": "RISK_CONTROL",
            "reason": reason,
        }

    @staticmethod
    def _no_setup(
        pair: str,
        market_type: str,
        regime: str,
        volatility_state: str,
        context_bias: str,
    ) -> Dict[str, Any]:
        return {
            "status": "NO_SIGNAL",
            "strategy_version": "ALCR_V1.0",
            "pair": pair,
            "market_type": market_type,
            "timeframe": "5m",
            "candidate_detected": False,
            "candidate_direction": None,
            "outcome": "NO_SIGNAL",
            "failed_gate": "SETUP",
            "reason": "no_valid_alcr_setup",
            "regime": regime,
            "volatility_state": volatility_state,
            "context_bias": context_bias,
        }


# ---------------------------------------------------------------------------
# Research / lifecycle helpers
# ---------------------------------------------------------------------------

@dataclass
class TradeResearchRecord:
    pair: str
    setup_type: str
    direction: str
    entry: float
    stop_loss: float
    tp1: float
    tp2: Optional[float]
    score: float
    setup_detected_at: Optional[str]
    trigger_at: Optional[str]
    result: Optional[str] = None
    r_multiple: Optional[float] = None
    mfe_r: Optional[float] = None
    mae_r: Optional[float] = None
    lifecycle: str = Lifecycle.TRIGGERED.value
    session: Optional[str] = None
    regime: Optional[str] = None
    volatility_state: Optional[str] = None
    rejection_reason: Optional[str] = None


def compute_excursion(
    entry: float,
    stop: float,
    direction: str,
    highs: pd.Series,
    lows: pd.Series,
) -> Tuple[float, float]:
    """
    Calculate MFE/MAE in R units over the supplied post-entry bars.
    """
    risk = abs(entry - stop)
    if risk <= 0:
        return 0.0, 0.0

    if direction == "BUY":
        mfe_price = max(0.0, float(highs.max()) - entry)
        mae_price = max(0.0, entry - float(lows.min()))
    else:
        mfe_price = max(0.0, entry - float(lows.min()))
        mae_price = max(0.0, float(highs.max()) - entry)

    return mfe_price / risk, mae_price / risk


def classify_session(ts: pd.Timestamp) -> str:
    """
    UTC session labels for research logging only.
    """
    hour = ts.hour
    if 7 <= hour < 12:
        return "LONDON"
    if 12 <= hour < 17:
        return "NEW_YORK"
    if 17 <= hour < 22:
        return "NEW_YORK_LATE"
    if 22 <= hour or hour < 1:
        return "ASIA_OPEN"
    return "ASIA"


def signal_to_scan_log(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert ALCR output into fields compatible with the existing
    strategy_scan_log concept. This function does NOT write to Supabase.
    """
    details = {
        "setup_type": result.get("setup_type"),
        "regime": result.get("regime"),
        "volatility_state": result.get("volatility_state"),
        "score": result.get("score"),
        "score_breakdown": result.get("score_breakdown"),
        "entry": result.get("entry"),
        "stop_loss": result.get("stop_loss"),
        "tp1": result.get("tp1"),
        "tp2": result.get("tp2"),
        "rr_to_tp1": result.get("rr_to_tp1"),
        "rr_to_tp2": result.get("rr_to_tp2"),
        "lifecycle": result.get("lifecycle"),
        "location": result.get("location"),
        "reasons": result.get("reasons"),
        "hard_failures": result.get("hard_failures"),
        "metadata": result.get("metadata"),
    }

    return {
        "pair": result.get("pair"),
        "market_type": result.get("market_type"),
        "strategy_version": "ALCR_V1.0",
        "timeframe": result.get("timeframe", "5m"),
        "candidate_direction": result.get("candidate_direction"),
        "candidate_detected": bool(result.get("candidate_detected")),
        "outcome": result.get("outcome"),
        "failed_gate": result.get("failed_gate"),
        "reason": result.get("reason"),
        "gate_results": result.get("score_breakdown"),
        "details": details,
    }


__all__ = [
    "ALCRAnalyzer",
    "ALCRConfig",
    "SignalCandidate",
    "TradeResearchRecord",
    "SetupType",
    "Lifecycle",
    "ALCR_PAIRS",
    "SIGNAL_THRESHOLD",
    "NEAR_MISS_THRESHOLD",
    "compute_excursion",
    "classify_session",
    "signal_to_scan_log",
]
