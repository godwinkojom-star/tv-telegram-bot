"""
==========================================================
 SMC ML Research Engine (PickMyTrade-style)
==========================================================

Research-only strategy module for SmartFX / tv-telegram-bot.

IMPORTANT:
- This module does NOT fetch market data itself.
- It does NOT send Telegram messages.
- It does NOT create live trading signals.
- The caller supplies already-fetched OHLCV DataFrames (1H preferred).
- Results are written only to Supabase research tables.
- Designed to share the same pairs and data layer as V2/V3/ALCR.

Timeframe focus: 1 Hour (as agreed)
Entry modes: break_close | adaptive
Timeout: trades that stay open too long without SL/TP are marked EXPIRED.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import math

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SMC_ML_PAIRS_FOREX = {"EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD"}
SMC_ML_PAIRS_CRYPTO = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
SMC_ML_PAIRS = SMC_ML_PAIRS_FOREX | SMC_ML_PAIRS_CRYPTO

DEFAULT_TIMEOUT_BARS = 100          # ~4 days on 1H
DEFAULT_SWING_LENGTH = 10
DEFAULT_RETEST_WINDOW = 20
DEFAULT_ATR_PERIOD = 14
DEFAULT_STOP_BUFFER_ATR = 0.20
DEFAULT_TP1_R = 1.0
DEFAULT_TP2_R = 2.0
DEFAULT_ADAPTIVE_THRESHOLD = 0.70
DEFAULT_ENTRY_MODE = "adaptive"     # "break_close" or "adaptive"


@dataclass
class SMCMLConfig:
    swing_length: int = DEFAULT_SWING_LENGTH
    retest_window: int = DEFAULT_RETEST_WINDOW
    atr_period: int = DEFAULT_ATR_PERIOD
    stop_buffer_atr: float = DEFAULT_STOP_BUFFER_ATR
    tp1_r: float = DEFAULT_TP1_R
    tp2_r: float = DEFAULT_TP2_R
    timeout_bars: int = DEFAULT_TIMEOUT_BARS
    entry_mode: str = DEFAULT_ENTRY_MODE
    adaptive_threshold: float = DEFAULT_ADAPTIVE_THRESHOLD
    move_be_after_tp1: bool = True
    max_risk_atr: float = 6.0       # skip if stop is wider than this


# ---------------------------------------------------------------------------
# Result objects
# ---------------------------------------------------------------------------

@dataclass
class SMCMLSignal:
    pair: str
    market_type: str
    direction: str                  # "BUY" or "SELL"
    entry: float
    stop_loss: float
    tp1: float
    tp2: float
    risk: float
    rr_to_tp1: float
    rr_to_tp2: float
    retest_probability: float
    structure_type: str             # "BOS" or "CHOCH"
    entry_mode_used: str
    timeframe: str = "1h"
    strategy_version: str = "SMC_ML_V1.0"
    detected_at: Optional[str] = None
    expires_at: Optional[str] = None
    status: str = "SIGNAL"          # SIGNAL / REJECTED / ERROR
    reason: str = ""
    analysis_details: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


# ---------------------------------------------------------------------------
# Helpers
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
    return tr.rolling(period).mean()


def _find_swings(df: pd.DataFrame, length: int) -> Tuple[pd.Series, pd.Series]:
    n = len(df)
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()

    sh = np.full(n, np.nan)
    sl = np.full(n, np.nan)

    for i in range(length, n - length):
        if high[i] == np.max(high[i - length : i + length + 1]):
            sh[i] = high[i]
        if low[i] == np.min(low[i - length : i + length + 1]):
            sl[i] = low[i]

    return pd.Series(sh, index=df.index), pd.Series(sl, index=df.index)


def _simple_retest_probability(distance: float, atr: float, window: int) -> float:
    if atr <= 0:
        return 0.5
    norm_dist = distance / atr
    prob = math.exp(-0.4 * norm_dist) * (window / 30.0)
    return float(max(0.15, min(0.90, prob)))


# ---------------------------------------------------------------------------
# Core analysis (single 1H dataframe)
# ---------------------------------------------------------------------------

def analyze_1h(
    df_1h: pd.DataFrame,
    pair: str,
    market_type: str,
    cfg: Optional[SMCMLConfig] = None,
) -> Optional[SMCMLSignal]:
    """
    Run the SMC ML logic on a 1H dataframe.
    Returns a SMCMLSignal if a valid setup is found on the latest bar, else None.
    """
    cfg = cfg or SMCMLConfig()
    df = _prepare(df_1h)

    if len(df) < cfg.swing_length * 3 + 20:
        return None

    atr_series = _atr(df, cfg.atr_period)
    swing_high, swing_low = _find_swings(df, cfg.swing_length)

    # Walk structure to detect the most recent BOS / CHoCH on the last bar
    last_sh = None
    last_sl = None
    structure = 0  # 1 bullish, -1 bearish

    bos_bull = False
    bos_bear = False
    choch_bull = False
    choch_bear = False

    for i in range(len(df)):
        if not pd.isna(swing_high.iloc[i]):
            last_sh = float(swing_high.iloc[i])
        if not pd.isna(swing_low.iloc[i]):
            last_sl = float(swing_low.iloc[i])

        close = float(df["close"].iloc[i])

        if last_sh is not None and close > last_sh:
            if structure == 1:
                if i == len(df) - 1:
                    bos_bull = True
            else:
                if i == len(df) - 1:
                    choch_bull = True
                structure = 1
            last_sh = None

        if last_sl is not None and close < last_sl:
            if structure == -1:
                if i == len(df) - 1:
                    bos_bear = True
            else:
                if i == len(df) - 1:
                    choch_bear = True
                structure = -1
            last_sl = None

    if not (bos_bull or bos_bear or choch_bull or choch_bear):
        return None

    direction = "BUY" if (bos_bull or choch_bull) else "SELL"
    structure_type = "BOS" if (bos_bull or bos_bear) else "CHOCH"

    row = df.iloc[-1]
    atr = float(atr_series.iloc[-1]) if not pd.isna(atr_series.iloc[-1]) else 0.0
    if atr <= 0:
        return None

    close = float(row["close"])

    # Protected swing for stop
    if direction == "BUY":
        recent_lows = swing_low.iloc[-40:].dropna()
        if len(recent_lows) > 0:
            stop = float(recent_lows.iloc[-1]) - (atr * cfg.stop_buffer_atr)
        else:
            stop = close - (atr * 1.5)
        recent_highs = swing_high.iloc[-50:].dropna()
        broken_level = float(recent_highs.iloc[-1]) if len(recent_highs) > 0 else close
    else:
        recent_highs = swing_high.iloc[-40:].dropna()
        if len(recent_highs) > 0:
            stop = float(recent_highs.iloc[-1]) + (atr * cfg.stop_buffer_atr)
        else:
            stop = close + (atr * 1.5)
        recent_lows = swing_low.iloc[-50:].dropna()
        broken_level = float(recent_lows.iloc[-1]) if len(recent_lows) > 0 else close

    risk = abs(close - stop)
    if risk <= 0 or risk > atr * cfg.max_risk_atr:
        return None

    if direction == "BUY":
        tp1 = close + risk * cfg.tp1_r
        tp2 = close + risk * cfg.tp2_r
    else:
        tp1 = close - risk * cfg.tp1_r
        tp2 = close - risk * cfg.tp2_r

    distance = abs(close - broken_level)
    retest_prob = _simple_retest_probability(distance, atr, cfg.retest_window)

    take_trade = False
    if cfg.entry_mode == "break_close":
        take_trade = True
    elif cfg.entry_mode == "adaptive":
        # Enter immediately only when retest probability is relatively low
        take_trade = retest_prob < cfg.adaptive_threshold
    else:
        take_trade = True

    if not take_trade:
        return SMCMLSignal(
            pair=pair,
            market_type=market_type,
            direction=direction,
            entry=close,
            stop_loss=stop,
            tp1=tp1,
            tp2=tp2,
            risk=risk,
            rr_to_tp1=cfg.tp1_r,
            rr_to_tp2=cfg.tp2_r,
            retest_probability=retest_prob,
            structure_type=structure_type,
            entry_mode_used=cfg.entry_mode,
            status="REJECTED",
            reason=f"Adaptive filter: retest_prob={retest_prob:.2f} >= {cfg.adaptive_threshold}",
            analysis_details={
                "broken_level": broken_level,
                "atr": atr,
                "structure": structure,
            },
        )

    now = datetime.utcnow()
    expires = now + timedelta(hours=cfg.timeout_bars)  # approximate for 1H bars

    return SMCMLSignal(
        pair=pair,
        market_type=market_type,
        direction=direction,
        entry=close,
        stop_loss=stop,
        tp1=tp1,
        tp2=tp2,
        risk=risk,
        rr_to_tp1=cfg.tp1_r,
        rr_to_tp2=cfg.tp2_r,
        retest_probability=retest_prob,
        structure_type=structure_type,
        entry_mode_used=cfg.entry_mode,
        detected_at=now.isoformat() + "Z",
        expires_at=expires.isoformat() + "Z",
        status="SIGNAL",
        reason=f"{structure_type} {direction} on 1H",
        analysis_details={
            "broken_level": broken_level,
            "atr": atr,
            "structure": structure,
            "retest_prob": retest_prob,
        },
    )


# ---------------------------------------------------------------------------
# Simple historical backtest helper (for offline testing)
# ---------------------------------------------------------------------------

def run_historical_backtest(
    df_1h: pd.DataFrame,
    pair: str = "UNKNOWN",
    market_type: str = "forex",
    cfg: Optional[SMCMLConfig] = None,
) -> Dict[str, Any]:
    """
    Walk-forward style backtest on a 1H dataframe.
    Returns summary statistics + list of closed trades.
    """
    cfg = cfg or SMCMLConfig()
    df = _prepare(df_1h)

    trades: List[Dict[str, Any]] = []
    open_trade: Optional[Dict[str, Any]] = None

    min_start = cfg.swing_length * 3 + 30

    for i in range(min_start, len(df)):
        window = df.iloc[: i + 1]
        row = df.iloc[i]
        ts = df.index[i]

        # Manage open trade
        if open_trade is not None:
            open_trade["bars_held"] += 1
            direction = open_trade["direction"]
            hit_sl = hit_tp1 = hit_tp2 = False

            if direction == "BUY":
                if float(row["low"]) <= open_trade["stop_loss"]:
                    hit_sl = True
                if float(row["high"]) >= open_trade["tp1"]:
                    hit_tp1 = True
                if float(row["high"]) >= open_trade["tp2"]:
                    hit_tp2 = True
            else:
                if float(row["high"]) >= open_trade["stop_loss"]:
                    hit_sl = True
                if float(row["low"]) <= open_trade["tp1"]:
                    hit_tp1 = True
                if float(row["low"]) <= open_trade["tp2"]:
                    hit_tp2 = True

            exit_price = None
            reason = None

            if hit_sl:
                exit_price = open_trade["stop_loss"]
                reason = "sl"
            elif hit_tp2:
                exit_price = open_trade["tp2"]
                reason = "tp2"
            elif hit_tp1 and cfg.move_be_after_tp1 and not open_trade.get("be_moved"):
                open_trade["stop_loss"] = open_trade["entry"]
                open_trade["be_moved"] = True
            elif open_trade["bars_held"] >= cfg.timeout_bars:
                exit_price = float(row["close"])
                reason = "expired"

            if exit_price is not None and reason is not None:
                risk = open_trade["risk"]
                if direction == "BUY":
                    r_mult = (exit_price - open_trade["entry"]) / risk
                else:
                    r_mult = (open_trade["entry"] - exit_price) / risk

                trades.append({
                    **open_trade,
                    "exit_time": str(ts),
                    "exit_price": exit_price,
                    "exit_reason": reason,
                    "r_multiple": round(r_mult, 3),
                })
                open_trade = None

        # New signal only if flat
        if open_trade is None:
            sig = analyze_1h(window, pair, market_type, cfg)
            if sig is not None and sig.status == "SIGNAL":
                open_trade = {
                    "entry_time": str(ts),
                    "direction": sig.direction,
                    "entry": sig.entry,
                    "stop_loss": sig.stop_loss,
                    "tp1": sig.tp1,
                    "tp2": sig.tp2,
                    "risk": sig.risk,
                    "structure_type": sig.structure_type,
                    "retest_probability": sig.retest_probability,
                    "bars_held": 0,
                    "be_moved": False,
                }

    # Force close any remaining trade
    if open_trade is not None:
        last = df.iloc[-1]
        exit_price = float(last["close"])
        risk = open_trade["risk"]
        if open_trade["direction"] == "BUY":
            r_mult = (exit_price - open_trade["entry"]) / risk
        else:
            r_mult = (open_trade["entry"] - exit_price) / risk
        trades.append({
            **open_trade,
            "exit_time": str(df.index[-1]),
            "exit_price": exit_price,
            "exit_reason": "end_of_data",
            "r_multiple": round(r_mult, 3),
        })

    # Summary
    total = len(trades)
    wins = sum(1 for t in trades if t["r_multiple"] > 0.05)
    losses = sum(1 for t in trades if t["r_multiple"] < -0.05)
    be = total - wins - losses
    total_r = sum(t["r_multiple"] for t in trades)
    sl_hits = sum(1 for t in trades if t["exit_reason"] == "sl")
    tp2_hits = sum(1 for t in trades if t["exit_reason"] == "tp2")
    expired = sum(1 for t in trades if t["exit_reason"] == "expired")

    return {
        "pair": pair,
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "breakevens": be,
        "win_rate": round(wins / total * 100, 2) if total else 0.0,
        "average_r": round(total_r / total, 3) if total else 0.0,
        "total_r": round(total_r, 2),
        "sl_hits": sl_hits,
        "tp2_hits": tp2_hits,
        "expired": expired,
        "trades": trades,
    }
