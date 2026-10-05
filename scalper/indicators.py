"""
SmartFX Experimental Gold Reversal Scalper
Indicator calculations.

No Telegram.
No database.
No MT5.
No app.py.
No trade state.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
)


def clean_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize an OHLC dataframe.

    Expected columns:
        open
        high
        low
        close

    Additional columns are preserved.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("OHLC data must be a pandas DataFrame")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]

    if missing:
        raise ValueError(
            f"OHLC dataframe missing columns: {missing}"
        )

    out = df.copy()

    for column in REQUIRED_COLUMNS:
        out[column] = pd.to_numeric(
            out[column],
            errors="coerce",
        )

    out = out.dropna(
        subset=list(REQUIRED_COLUMNS)
    )

    out = out.sort_index()

    return out


def ema(
    series: pd.Series,
    period: int,
) -> pd.Series:
    return series.ewm(
        span=period,
        adjust=False,
        min_periods=period,
    ).mean()


def atr(
    df: pd.DataFrame,
    period: int = 14,
) -> pd.Series:

    previous_close = df["close"].shift(1)

    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return true_range.rolling(
        period,
        min_periods=period,
    ).mean()


def rsi(
    series: pd.Series,
    period: int = 14,
) -> pd.Series:

    delta = series.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    average_gain = gain.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    average_loss = loss.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    rs = average_gain / average_loss.replace(
        0,
        np.nan,
    )

    result = 100 - (
        100 / (1 + rs)
    )

    # If loss is zero, RSI should behave as fully bullish.
    result = result.where(
        average_loss != 0,
        100.0,
    )

    return result


def candle_body(
    df: pd.DataFrame,
) -> pd.Series:
    return (
        df["close"] -
        df["open"]
    ).abs()


def candle_range(
    df: pd.DataFrame,
) -> pd.Series:
    return (
        df["high"] -
        df["low"]
    ).clip(lower=0)


def body_ratio(
    df: pd.DataFrame,
) -> pd.Series:

    rng = candle_range(df)

    body = candle_body(df)

    return body.div(
        rng.replace(0, np.nan)
    )


def bullish_candle(
    df: pd.DataFrame,
) -> pd.Series:
    return df["close"] > df["open"]


def bearish_candle(
    df: pd.DataFrame,
) -> pd.Series:
    return df["close"] < df["open"]


def recent_swing_high(
    df: pd.DataFrame,
    lookback: int,
) -> Optional[float]:

    if len(df) < lookback + 1:
        return None

    window = df.iloc[
        -(lookback + 1):-1
    ]

    if window.empty:
        return None

    return float(
        window["high"].max()
    )


def recent_swing_low(
    df: pd.DataFrame,
    lookback: int,
) -> Optional[float]:

    if len(df) < lookback + 1:
        return None

    window = df.iloc[
        -(lookback + 1):-1
    ]

    if window.empty:
        return None

    return float(
        window["low"].min()
    )


def directional_candle_count(
    df: pd.DataFrame,
    direction: str,
    window: int,
) -> int:

    if len(df) < window:
        return 0

    recent = df.iloc[-window:]

    if direction == "BUY":
        return int(
            (recent["close"] > recent["open"]).sum()
        )

    if direction == "SELL":
        return int(
            (recent["close"] < recent["open"]).sum()
        )

    return 0


def prepare_indicators(
    df: pd.DataFrame,
    fast_ema: int,
    slow_ema: int,
    rsi_period: int,
    atr_period: int,
) -> pd.DataFrame:

    out = clean_ohlc(df)

    out["ema_fast"] = ema(
        out["close"],
        fast_ema,
    )

    out["ema_slow"] = ema(
        out["close"],
        slow_ema,
    )

    out["rsi"] = rsi(
        out["close"],
        rsi_period,
    )

    out["atr"] = atr(
        out,
        atr_period,
    )

    out["body"] = candle_body(out)

    out["range"] = candle_range(out)

    out["body_ratio"] = body_ratio(out)

    out["bullish"] = bullish_candle(out)

    out["bearish"] = bearish_candle(out)

    return out
