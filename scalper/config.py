"""Gold reversal scalper settings.

Risk percent is configurable at 1%, 2%, or 5% for paper trading.
point_value is abstract paper units by default; document real-contract
multipliers before any live use.
"""
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
    allow_immediate_reversal: bool = True
    one_position_per_direction: bool = True
    same_direction_cooldown_bars: int = 3
    require_5m_confirmation: bool = True
    confirmation_required: bool = True
    starting_balance: float = 1000.0
    # Allowed risk options for Gold paper trading: 1.0, 2.0, 5.0
    risk_percent: float = 1.0
    # Broker/contract-specific multiplier. Keep at 1.0 for abstract paper P/L
    # until a real XAU contract specification is verified.
    point_value: float = 1.0
    minimum_1m_bars: int = 100
    minimum_5m_bars: int = 100

    def with_risk(self, risk_percent: float) -> "ScalperConfig":
        """Return a new config with the given risk percent (1, 2, or 5)."""
        allowed = {1.0, 2.0, 5.0}
        rp = float(risk_percent)
        if rp not in allowed:
            raise ValueError(f"Gold paper risk_percent must be one of {sorted(allowed)}, got {rp}")
        # frozen dataclass: construct a new instance
        return ScalperConfig(
            pair=self.pair,
            market_type=self.market_type,
            entry_timeframe=self.entry_timeframe,
            confirmation_timeframe=self.confirmation_timeframe,
            fast_ema=self.fast_ema,
            slow_ema=self.slow_ema,
            confirmation_fast_ema=self.confirmation_fast_ema,
            confirmation_slow_ema=self.confirmation_slow_ema,
            rsi_period=self.rsi_period,
            rsi_buy_min=self.rsi_buy_min,
            rsi_sell_max=self.rsi_sell_max,
            atr_period=self.atr_period,
            min_atr_ratio=self.min_atr_ratio,
            max_atr_ratio=self.max_atr_ratio,
            swing_lookback=self.swing_lookback,
            reversal_break_atr=self.reversal_break_atr,
            displacement_atr=self.displacement_atr,
            displacement_ratio=self.displacement_ratio,
            min_ema_separation_atr=self.min_ema_separation_atr,
            directional_window=self.directional_window,
            max_entry_extension_atr=self.max_entry_extension_atr,
            max_ema_distance_atr=self.max_ema_distance_atr,
            stop_atr_multiplier=self.stop_atr_multiplier,
            min_stop_atr=self.min_stop_atr,
            max_stop_atr=self.max_stop_atr,
            allow_immediate_reversal=self.allow_immediate_reversal,
            one_position_per_direction=self.one_position_per_direction,
            same_direction_cooldown_bars=self.same_direction_cooldown_bars,
            require_5m_confirmation=self.require_5m_confirmation,
            confirmation_required=self.confirmation_required,
            starting_balance=self.starting_balance,
            risk_percent=rp,
            point_value=self.point_value,
            minimum_1m_bars=self.minimum_1m_bars,
            minimum_5m_bars=self.minimum_5m_bars,
        )
