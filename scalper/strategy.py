"""
SmartFX Experimental Gold Reversal Scalper
Original reversal strategy engine.

Concept:

    5M direction
          +
    1M structure
          +
    momentum/displacement
          +
    volatility
          +
    anti-chase
          =
       CONFIRMED SIGNAL

The strategy is designed around direction changes.

Example:

    BUY
      ↓
    SELL reversal confirmed
      ↓
    BUY reversal confirmed
      ↓
    SELL reversal confirmed

A bullish candle by itself does NOT create another BUY.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .config import ScalperConfig
from .indicators import (
    clean_ohlc,
    prepare_indicators,
    recent_swing_high,
    recent_swing_low,
    directional_candle_count,
)


def _empty_result(
    reason: str,
) -> Dict[str, Any]:

    return {
        "status": "NO_SIGNAL",
        "direction": None,
        "reason": reason,
    }


def _atr_value(
    df: pd.DataFrame,
) -> Optional[float]:

    if df.empty:
        return None

    value = df["atr"].iloc[-1]

    try:
        value = float(value)
    except (
        TypeError,
        ValueError,
    ):
        return None

    if not np.isfinite(value):
        return None

    if value <= 0:
        return None

    return value


def _last(
    df: pd.DataFrame,
    column: str,
) -> Optional[float]:

    if df.empty:
        return None

    try:
        value = float(
            df[column].iloc[-1]
        )
    except (
        TypeError,
        ValueError,
        KeyError,
    ):
        return None

    if not np.isfinite(value):
        return None

    return value


def confirmation_direction(
    df5: pd.DataFrame,
    cfg: ScalperConfig,
) -> str:

    """
    Determine 5M directional context.

    This is intentionally not a signal by itself.
    """

    if len(df5) < max(
        cfg.confirmation_slow_ema,
        cfg.atr_period,
    ) + 5:
        return "NEUTRAL"

    last = df5.iloc[-1]

    fast = float(
        last["ema_fast"]
    )

    slow = float(
        last["ema_slow"]
    )

    close = float(
        last["close"]
    )

    if not np.isfinite(
        fast
    ) or not np.isfinite(
        slow
    ):
        return "NEUTRAL"

    if (
        close > fast
        and fast > slow
    ):
        return "BUY"

    if (
        close < fast
        and fast < slow
    ):
        return "SELL"

    return "NEUTRAL"


def volatility_gate(
    df1: pd.DataFrame,
    df5: pd.DataFrame,
    cfg: ScalperConfig,
) -> Dict[str, Any]:

    atr1 = _atr_value(df1)
    atr5 = _atr_value(df5)

    price = _last(
        df1,
        "close",
    )

    if (
        atr1 is None
        or atr5 is None
        or price is None
        or price <= 0
    ):
        return {
            "pass": False,
            "reason": "invalid_volatility_data",
        }

    atr_ratio = atr1 / price

    if atr_ratio < cfg.min_atr_ratio:
        return {
            "pass": False,
            "reason": "volatility_too_low",
            "atr_ratio": atr_ratio,
        }

    if atr_ratio > cfg.max_atr_ratio:
        return {
            "pass": False,
            "reason": "volatility_expansion_too_high",
            "atr_ratio": atr_ratio,
        }

    return {
        "pass": True,
        "atr1": atr1,
        "atr5": atr5,
        "atr_ratio": atr_ratio,
    }


def detect_reversal(
    df1: pd.DataFrame,
    cfg: ScalperConfig,
) -> Optional[Dict[str, Any]]:

    """
    Detect a genuine short-term reversal.

    BUY reversal:

        price was recently making lower lows
        ->
        current candle breaks recent swing high
        ->
        current candle has bullish displacement

    SELL reversal:

        price was recently making higher highs
        ->
        current candle breaks recent swing low
        ->
        current candle has bearish displacement
    """

    if len(df1) < 30:
        return None

    current = df1.iloc[-1]

    atr_value = _atr_value(df1)

    if atr_value is None:
        return None

    swing_high = recent_swing_high(
        df1,
        cfg.swing_lookback,
    )

    swing_low = recent_swing_low(
        df1,
        cfg.swing_lookback,
    )

    if (
        swing_high is None
        or swing_low is None
    ):
        return None

    close = float(
        current["close"]
    )

    high = float(
        current["high"]
    )

    low = float(
        current["low"]
    )

    body = abs(
        float(current["close"])
        -
        float(current["open"])
    )

    candle_range = max(
        high - low,
        0.0,
    )

    if candle_range <= 0:
        return None

    body_ratio = (
        body / candle_range
    )

    displacement_required = (
        body
        >=
        cfg.displacement_atr
        * atr_value
    )

    directional_body = (
        body_ratio
        >=
        cfg.displacement_ratio
    )

    if not (
        displacement_required
        and directional_body
    ):
        return None

    # ---------------------------------------------------------
    # BUY REVERSAL
    # ---------------------------------------------------------

    bullish_break = (
        close
        >
        swing_high
        +
        cfg.reversal_break_atr
        * atr_value
    )

    bullish_candle = (
        float(current["close"])
        >
        float(current["open"])
    )

    if (
        bullish_break
        and bullish_candle
    ):
        return {
            "direction": "BUY",
            "price": close,
            "swing_level": swing_high,
            "atr": atr_value,
            "break_distance": close - swing_high,
            "body_ratio": body_ratio,
        }

    # ---------------------------------------------------------
    # SELL REVERSAL
    # ---------------------------------------------------------

    bearish_break = (
        close
        <
        swing_low
        -
        cfg.reversal_break_atr
        * atr_value
    )

    bearish_candle = (
        float(current["close"])
        <
        float(current["open"])
    )

    if (
        bearish_break
        and bearish_candle
    ):
        return {
            "direction": "SELL",
            "price": close,
            "swing_level": swing_low,
            "atr": atr_value,
            "break_distance": swing_low - close,
            "body_ratio": body_ratio,
        }

    return None


def anti_chop_gate(
    df1: pd.DataFrame,
    cfg: ScalperConfig,
) -> Dict[str, Any]:

    if len(df1) < 50:
        return {
            "pass": False,
            "reason": "insufficient_1m_history",
        }

    current = df1.iloc[-1]

    atr_value = _atr_value(df1)

    if atr_value is None:
        return {
            "pass": False,
            "reason": "missing_atr",
        }

    fast = float(
        current["ema_fast"]
    )

    slow = float(
        current["ema_slow"]
    )

    separation = abs(
        fast - slow
    )

    minimum_separation = (
        cfg.min_ema_separation_atr
        * atr_value
    )

    if separation < minimum_separation:
        return {
            "pass": False,
            "reason": "ema_structure_too_flat",
            "separation": separation,
            "minimum": minimum_separation,
        }

    return {
        "pass": True,
        "ema_separation": separation,
    }


def entry_quality_gate(
    df1: pd.DataFrame,
    reversal: Dict[str, Any],
    cfg: ScalperConfig,
) -> Dict[str, Any]:

    price = float(
        reversal["price"]
    )

    atr_value = float(
        reversal["atr"]
    )

    fast_ema = float(
        df1["ema_fast"].iloc[-1]
    )

    ema_distance = abs(
        price - fast_ema
    )

    if (
        ema_distance
        >
        cfg.max_ema_distance_atr
        * atr_value
    ):
        return {
            "pass": False,
            "reason": "entry_too_far_from_fast_ema",
            "ema_distance": ema_distance,
        }

    extension = abs(
        price
        -
        float(reversal["swing_level"])
    )

    if (
        extension
        >
        cfg.max_entry_extension_atr
        * atr_value
    ):
        return {
            "pass": False,
            "reason": "entry_overextended",
            "extension": extension,
        }

    return {
        "pass": True,
        "ema_distance": ema_distance,
        "extension": extension,
    }


def calculate_trade_levels(
    df1: pd.DataFrame,
    direction: str,
    cfg: ScalperConfig,
) -> Dict[str, Any]:

    entry = float(
        df1["close"].iloc[-1]
    )

    atr_value = _atr_value(df1)

    if atr_value is None:
        raise ValueError(
            "Cannot calculate trade levels without ATR"
        )

    recent_high = float(
        df1["high"]
        .iloc[-6:-1]
        .max()
    )

    recent_low = float(
        df1["low"]
        .iloc[-6:-1]
        .min()
    )

    if direction == "BUY":

        structural_stop = (
            recent_low
            -
            cfg.stop_atr_multiplier
            * atr_value
            * 0.25
        )

        atr_stop = (
            entry
            -
            cfg.stop_atr_multiplier
            * atr_value
        )

        stop = min(
            structural_stop,
            atr_stop,
        )

        risk = entry - stop

        target = (
            entry
            +
            cfg.target_atr_multiplier
            * atr_value
        )

    else:

        structural_stop = (
            recent_high
            +
            cfg.stop_atr_multiplier
            * atr_value
            * 0.25
        )

        atr_stop = (
            entry
            +
            cfg.stop_atr_multiplier
            * atr_value
        )

        stop = max(
            structural_stop,
            atr_stop,
        )

        risk = stop - entry

        target = (
            entry
            -
            cfg.target_atr_multiplier
            * atr_value
        )

    risk_atr = risk / atr_value

    if (
        risk_atr < cfg.min_stop_atr
        or
        risk_atr > cfg.max_stop_atr
    ):
        return {
            "valid": False,
            "reason": "stop_distance_outside_allowed_range",
            "entry": entry,
            "sl": stop,
            "tp": target,
            "risk": risk,
            "risk_atr": risk_atr,
        }

    reward = abs(
        target - entry
    )

    rr = (
        reward / risk
        if risk > 0
        else 0
    )

    if rr < cfg.minimum_rr:
        return {
            "valid": False,
            "reason": "reward_risk_too_low",
            "entry": entry,
            "sl": stop,
            "tp": target,
            "risk": risk,
            "rr": rr,
        }

    return {
        "valid": True,
        "entry": entry,
        "sl": stop,
        "tp": target,
        "risk": risk,
        "risk_atr": risk_atr,
        "rr": rr,
    }


def evaluate(
    df1: pd.DataFrame,
    df5: pd.DataFrame,
    cfg: Optional[ScalperConfig] = None,
) -> Dict[str, Any]:

    """
    Main strategy evaluation.

    Returns:

        NO_SIGNAL
        or

        SIGNAL
        direction = BUY / SELL
    """

    cfg = cfg or ScalperConfig()

    try:
        df1 = clean_ohlc(df1)
        df5 = clean_ohlc(df5)

        df1 = prepare_indicators(
            df1,
            cfg.fast_ema,
            cfg.slow_ema,
            cfg.rsi_period,
            cfg.atr_period,
        )

        df5 = prepare_indicators(
            df5,
            cfg.confirmation_fast_ema,
            cfg.confirmation_slow_ema,
            cfg.rsi_period,
            cfg.atr_period,
        )

    except Exception as exc:

        return _empty_result(
            f"indicator_error:{exc}"
        )

    if len(df1) < cfg.minimum_1m_bars:
        return _empty_result(
            "insufficient_1m_history"
        )

    if len(df5) < cfg.minimum_5m_bars:
        return _empty_result(
            "insufficient_5m_history"
        )

    # ---------------------------------------------------------
    # 1. VOLATILITY
    # ---------------------------------------------------------

    volatility = volatility_gate(
        df1,
        df5,
        cfg,
    )

    if not volatility["pass"]:
        return _empty_result(
            volatility["reason"]
        )

    # ---------------------------------------------------------
    # 2. 5M CONFIRMATION
    # ---------------------------------------------------------

    confirmation = confirmation_direction(
        df5,
        cfg,
    )

    if (
        cfg.require_5m_confirmation
        and confirmation == "NEUTRAL"
    ):
        return _empty_result(
            "5m_confirmation_neutral"
        )

    # ---------------------------------------------------------
    # 3. REVERSAL
    # ---------------------------------------------------------

    reversal = detect_reversal(
        df1,
        cfg,
    )

    if reversal is None:
        return _empty_result(
            "no_confirmed_1m_reversal"
        )

    direction = reversal[
        "direction"
    ]

    # ---------------------------------------------------------
    # 4. 5M DIRECTION MUST AGREE
    # ---------------------------------------------------------

    if (
        cfg.confirmation_required
        and confirmation != direction
    ):
        return _empty_result(
            "5m_confirmation_disagrees"
        )

    # ---------------------------------------------------------
    # 5. ANTI-CHOP
    # ---------------------------------------------------------

    chop = anti_chop_gate(
        df1,
        cfg,
    )

    if not chop["pass"]:
        return _empty_result(
            chop["reason"]
        )

    # ---------------------------------------------------------
    # 6. MOMENTUM
    # ---------------------------------------------------------

    rsi_value = float(
        df1["rsi"].iloc[-1]
    )

    if direction == "BUY":

        if rsi_value < cfg.rsi_buy_min:
            return _empty_result(
                "buy_momentum_not_confirmed"
            )

    else:

        if rsi_value > cfg.rsi_sell_max:
            return _empty_result(
                "sell_momentum_not_confirmed"
            )

    # ---------------------------------------------------------
    # 7. DIRECTIONAL RECENT CANDLES
    # ---------------------------------------------------------

    directional_count = directional_candle_count(
        df1,
        direction,
        cfg.directional_window,
    )

    # We don't require every recent candle to agree.
    # This is intentionally permissive because reversal setups
    # naturally contain candles from the previous direction.
    if directional_count < 1:
        return _empty_result(
            "no_recent_directional_momentum"
        )

    # ---------------------------------------------------------
    # 8. ENTRY QUALITY
    # ---------------------------------------------------------

    quality = entry_quality_gate(
        df1,
        reversal,
        cfg,
    )

    if not quality["pass"]:
        return _empty_result(
            quality["reason"]
        )

    # ---------------------------------------------------------
    # 9. TRADE LEVELS
    # ---------------------------------------------------------

    levels = calculate_trade_levels(
        df1,
        direction,
        cfg,
    )

    if not levels["valid"]:
        return _empty_result(
            levels["reason"]
        )

    # ---------------------------------------------------------
    # SIGNAL
    # ---------------------------------------------------------

    return {
        "status": "SIGNAL",

        "strategy": "EXPERIMENTAL_GOLD_REVERSAL_SCALPER",

        "pair": cfg.pair,

        "direction": direction,

        "timeframe": cfg.entry_timeframe,

        "confirmation_timeframe":
            cfg.confirmation_timeframe,

        "entry": levels["entry"],

        "sl": levels["sl"],

        "tp": levels["tp"],

        "risk": levels["risk"],

        "risk_atr": levels["risk_atr"],

        "rr": levels["rr"],

        "rsi": rsi_value,

        "confirmation": confirmation,

        "atr": volatility["atr1"],

        "reversal": reversal,

        "entry_quality": quality,

        "anti_chop": chop,

        "directional_candles":
            directional_count,

        "reason":
            "confirmed_reversal",

    }


def should_reverse(
    current_position: Optional[str],
    new_signal: Dict[str, Any],
) -> bool:

    """
    Determines whether a new confirmed signal should reverse
    the current paper position.
    """

    if not current_position:
        return False

    if new_signal.get(
        "status"
    ) != "SIGNAL":
        return False

    new_direction = (
        new_signal.get("direction")
    )

    return (
        new_direction
        in {"BUY", "SELL"}
        and
        new_direction
        != current_position
)
