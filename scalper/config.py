"""Gold reversal scalper settings copied from the reviewed repository defaults."""
from dataclasses import dataclass
@dataclass(frozen=True)
class ScalperConfig:
    pair: str = "XAU/USD"
    market_type: str = "forex"
    entry_timeframe: str = "1m"
    confirmation_timeframe: str = "5m"
    fast_ema: int = 9
    slow_ema: int = 21
    confirmation_fast_ema: int = 9
    confirmation_slow_ema: int = 21
    rsi_period: int = 14
    rsi_buy_min: float = 52.0
    rsi_sell_max: float = 48.0
    atr_period: int = 14
    min_atr_ratio: float = 0.00008
    max_atr_ratio: float = 0.0035
    swing_lookback: int = 5
    reversal_break_atr: float = 0.10
    displacement_atr: float = 0.30
    displacement_ratio: float = 0.55
    min_ema_separation_atr: float = 0.05
    directional_window: int = 4
    max_entry_extension_atr: float = 0.90
    max_ema_distance_atr: float = 1.00
    stop_atr_multiplier: float = 1.20
    min_stop_atr: float = 0.35
    max_stop_atr: float = 2.00
    target_atr_multiplier: float = 1.50
    minimum_rr: float = 1.00
    allow_immediate_reversal: bool = True
    one_position_per_direction: bool = True
    max_hold_bars: int = 30
    same_direction_cooldown_bars: int = 3
    require_5m_confirmation: bool = True
    confirmation_required: bool = True
    starting_balance: float = 1000.0
    risk_percent: float = 1.0
    # Broker/contract-specific. Keep at 1.0 only for abstract test P/L until verified.
    point_value: float = 1.0
    minimum_1m_bars: int = 100
    minimum_5m_bars: int = 100
