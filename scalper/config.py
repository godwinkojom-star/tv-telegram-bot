"""
SmartFX Experimental Gold Reversal Scalper
Configuration only.

This module is intentionally independent from:
- V2
- V3
- ALCR
- Telegram
- Supabase
- MT5
- app.py scanner timing

The values below are TEST PARAMETERS, not claims of profitability.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ScalperConfig:
    # ---------------------------------------------------------
    # MARKET
    # ---------------------------------------------------------

    pair: str = "XAU/USD"
    market_type: str = "forex"

    # Primary entry timeframe and confirmation timeframe.
    entry_timeframe: str = "1m"
    confirmation_timeframe: str = "5m"

    # ---------------------------------------------------------
    # TREND / MOMENTUM
    # ---------------------------------------------------------

    fast_ema: int = 9
    slow_ema: int = 21

    # Higher timeframe directional EMA.
    confirmation_fast_ema: int = 9
    confirmation_slow_ema: int = 21

    rsi_period: int = 14

    # RSI does NOT mean:
    # RSI > 50 = automatically BUY.
    # It is used as momentum confirmation.
    rsi_buy_min: float = 52.0
    rsi_sell_max: float = 48.0

    # ---------------------------------------------------------
    # ATR / VOLATILITY
    # ---------------------------------------------------------

    atr_period: int = 14

    # Avoid dead markets.
    min_atr_ratio: float = 0.00008

    # Don't enter during extreme expansion.
    max_atr_ratio: float = 0.0035

    # ---------------------------------------------------------
    # REVERSAL DETECTION
    # ---------------------------------------------------------

    swing_lookback: int = 5

    # A reversal must exceed the recent swing by this ATR fraction.
    reversal_break_atr: float = 0.10

    # Candle body must have enough directional strength.
    displacement_atr: float = 0.30

    # Minimum body/range ratio.
    displacement_ratio: float = 0.55

    # ---------------------------------------------------------
    # ANTI-CHOP
    # ---------------------------------------------------------

    min_ema_separation_atr: float = 0.05

    # Require recent directional candles before accepting reversal.
    directional_window: int = 4

    # ---------------------------------------------------------
    # ENTRY QUALITY
    # ---------------------------------------------------------

    max_entry_extension_atr: float = 0.90

    # Maximum distance from fast EMA at entry.
    max_ema_distance_atr: float = 1.00

    # ---------------------------------------------------------
    # RISK
    # ---------------------------------------------------------

    stop_atr_multiplier: float = 1.20

    # Minimum and maximum structural stop distance.
    min_stop_atr: float = 0.35
    max_stop_atr: float = 2.00

    # Targets are primarily for paper-test accounting.
    target_atr_multiplier: float = 1.50

    # Minimum reward/risk accepted.
    minimum_rr: float = 1.00

    # ---------------------------------------------------------
    # POSITION / REVERSAL
    # ---------------------------------------------------------

    # Opposite confirmed signal closes current position and
    # immediately opens the new direction.
    allow_immediate_reversal: bool = True

    # Do not repeatedly enter the same direction.
    one_position_per_direction: bool = True

    # Maximum number of bars a position can remain open.
    # This protects the experiment from dead trades.
    max_hold_bars: int = 30

    # ---------------------------------------------------------
    # SIGNAL COOLDOWN
    # ---------------------------------------------------------

    # Minimum bars between same-direction entries.
    same_direction_cooldown_bars: int = 3

    # ---------------------------------------------------------
    # CONFIRMATION
    # ---------------------------------------------------------

    require_5m_confirmation: bool = True

    # If enabled:
    # BUY requires 5m bullish structure.
    # SELL requires 5m bearish structure.
    confirmation_required: bool = True

    # ---------------------------------------------------------
    # PAPER TRADING
    # ---------------------------------------------------------

    starting_balance: float = 1000.0

    # Risk used only for paper calculations.
    risk_percent: float = 1.0

    # Gold point value is broker-dependent.
    # We deliberately keep this abstract for now.
    point_value: float = 1.0

    # ---------------------------------------------------------
    # DATA
    # ---------------------------------------------------------

    minimum_1m_bars: int = 100
    minimum_5m_bars: int = 100
