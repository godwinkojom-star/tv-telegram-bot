import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# ============================================================
# SMART MONEY CONCEPT STRATEGY - 1 HOUR BACKTEST VERSION
# ============================================================

@dataclass
class Trade:
    entry_time: datetime
    exit_time: Optional[datetime] = None
    direction: str = ""          # "long" or "short"
    entry_price: float = 0.0
    stop_loss: float = 0.0
    tp1: float = 0.0
    tp2: float = 0.0
    exit_price: float = 0.0
    exit_reason: str = ""        # "tp1", "tp2", "sl", "timeout", "opposite", "breakeven"
    r_multiple: float = 0.0
    bars_held: int = 0
    risk: float = 0.0

@dataclass
class StrategyStats:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    breakevens: int = 0
    tp1_hits: int = 0
    tp2_hits: int = 0
    sl_hits: int = 0
    timeouts: int = 0
    opposite_closes: int = 0
    total_r: float = 0.0
    max_drawdown_r: float = 0.0
    long_trades: int = 0
    short_trades: int = 0
    long_wins: int = 0
    short_wins: int = 0
    trades: List[Trade] = field(default_factory=list)

class SmartMoneyConceptBacktest:
    def __init__(
        self,
        swing_length: int = 10,
        retest_window: int = 20,
        atr_period: int = 14,
        stop_buffer_atr: float = 0.2,
        tp1_r: float = 1.0,
        tp2_r: float = 2.0,
        timeout_bars: int = 150,
        entry_mode: str = "adaptive",      # "break_close", "retest", "adaptive"
        adaptive_threshold: float = 0.70,
        move_be_after_tp1: bool = True,
        risk_per_trade: float = 1.0
    ):
        self.swing_length = swing_length
        self.retest_window = retest_window
        self.atr_period = atr_period
        self.stop_buffer_atr = stop_buffer_atr
        self.tp1_r = tp1_r
        self.tp2_r = tp2_r
        self.timeout_bars = timeout_bars
        self.entry_mode = entry_mode
        self.adaptive_threshold = adaptive_threshold
        self.move_be_after_tp1 = move_be_after_tp1
        self.risk_per_trade = risk_per_trade

        self.stats = StrategyStats()
        self.current_trade: Optional[Trade] = None
        self.equity_curve = []
        self.peak_equity = 0.0

    def calculate_atr(self, df: pd.DataFrame) -> pd.Series:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        return true_range.rolling(self.atr_period).mean()

    def find_swings(self, df: pd.DataFrame):
        """Find confirmed swing highs and lows"""
        df = df.copy()
        df['swing_high'] = np.nan
        df['swing_low'] = np.nan

        for i in range(self.swing_length, len(df) - self.swing_length):
            # Swing High
            if df['high'].iloc[i] == df['high'].iloc[i - self.swing_length:i + self.swing_length + 1].max():
                df.loc[df.index[i], 'swing_high'] = df['high'].iloc[i]
            # Swing Low
            if df['low'].iloc[i] == df['low'].iloc[i - self.swing_length:i + self.swing_length + 1].min():
                df.loc[df.index[i], 'swing_low'] = df['low'].iloc[i]

        return df

    def detect_structure(self, df: pd.DataFrame):
        """Detect BOS and CHoCH"""
        df = df.copy()
        df['bos_bull'] = False
        df['bos_bear'] = False
        df['choch_bull'] = False
        df['choch_bear'] = False
        df['structure'] = 0          # 1 = bullish, -1 = bearish

        last_swing_high = None
        last_swing_low = None
        structure = 0

        for i in range(len(df)):
            if not pd.isna(df['swing_high'].iloc[i]):
                last_swing_high = df['swing_high'].iloc[i]
            if not pd.isna(df['swing_low'].iloc[i]):
                last_swing_low = df['swing_low'].iloc[i]

            close = df['close'].iloc[i]

            # Bullish BOS / CHoCH
            if last_swing_high is not None and close > last_swing_high:
                if structure == 1:
                    df.loc[df.index[i], 'bos_bull'] = True
                else:
                    df.loc[df.index[i], 'choch_bull'] = True
                    structure = 1
                last_swing_high = None

            # Bearish BOS / CHoCH
            if last_swing_low is not None and close < last_swing_low:
                if structure == -1:
                    df.loc[df.index[i], 'bos_bear'] = True
                else:
                    df.loc[df.index[i], 'choch_bear'] = True
                    structure = -1
                last_swing_low = None

            df.loc[df.index[i], 'structure'] = structure

        return df

    def simple_retest_probability(self, distance: float, atr: float, window: int) -> float:
        """
        Simplified practical version of retest probability.
        Higher distance → lower probability of retest.
        """
        if atr == 0:
            return 0.5
        norm_dist = distance / atr
        prob = np.exp(-0.4 * norm_dist) * (window / 30)
        return float(np.clip(prob, 0.15, 0.90))

    def run(self, df: pd.DataFrame) -> StrategyStats:
        """
        Main backtest function.
        df must have columns: open, high, low, close, volume
        and DatetimeIndex
        """
        df = df.copy()
        df = self.find_swings(df)
        df = self.detect_structure(df)
        df['atr'] = self.calculate_atr(df)

        self.stats = StrategyStats()
        self.current_trade = None
        equity = 0.0
        self.peak_equity = 0.0
        self.equity_curve = []

        for i in range(self.swing_length * 2, len(df)):
            row = df.iloc[i]
            current_time = df.index[i]
            atr = row['atr']

            # ========== MANAGE OPEN TRADE ==========
            if self.current_trade is not None:
                trade = self.current_trade
                trade.bars_held += 1

                hit_sl = False
                hit_tp1 = False
                hit_tp2 = False

                if trade.direction == "long":
                    if row['low'] <= trade.stop_loss:
                        hit_sl = True
                    if row['high'] >= trade.tp1:
                        hit_tp1 = True
                    if row['high'] >= trade.tp2:
                        hit_tp2 = True
                else:
                    if row['high'] >= trade.stop_loss:
                        hit_sl = True
                    if row['low'] <= trade.tp1:
                        hit_tp1 = True
                    if row['low'] <= trade.tp2:
                        hit_tp2 = True

                # Exit logic priority
                if hit_sl:
                    self._close_trade(trade, trade.stop_loss, "sl", current_time)
                elif hit_tp2:
                    self._close_trade(trade, trade.tp2, "tp2", current_time)
                elif hit_tp1:
                    if self.move_be_after_tp1 and trade.exit_reason == "":
                        trade.stop_loss = trade.entry_price  # move to breakeven
                        trade.exit_reason = "tp1_hit"
                elif trade.bars_held >= self.timeout_bars:
                    self._close_trade(trade, row['close'], "timeout", current_time)

                # Opposite signal close
                if self.current_trade is not None:
                    if (trade.direction == "long" and (row['bos_bear'] or row['choch_bear'])) or \
                       (trade.direction == "short" and (row['bos_bull'] or row['choch_bull'])):
                        self._close_trade(trade, row['close'], "opposite", current_time)

            # ========== NEW ENTRY LOGIC ==========
            if self.current_trade is None and not pd.isna(atr):
                bull_signal = row['bos_bull'] or row['choch_bull']
                bear_signal = row['bos_bear'] or row['choch_bear']

                if bull_signal or bear_signal:
                    direction = "long" if bull_signal else "short"

                    # Find the broken level
                    if direction == "long":
                        recent_swings = df['swing_high'].iloc[max(0, i-50):i].dropna()
                        broken_level = recent_swings.iloc[-1] if len(recent_swings) > 0 else row['close']
                    else:
                        recent_swings = df['swing_low'].iloc[max(0, i-50):i].dropna()
                        broken_level = recent_swings.iloc[-1] if len(recent_swings) > 0 else row['close']

                    # Better stop placement: beyond the protected swing
                    if direction == "long":
                        recent_lows = df['swing_low'].iloc[max(0, i-30):i].dropna()
                        if len(recent_lows) > 0:
                            stop = recent_lows.iloc[-1] - (atr * self.stop_buffer_atr)
                        else:
                            stop = row['close'] - (atr * 1.5)
                    else:
                        recent_highs = df['swing_high'].iloc[max(0, i-30):i].dropna()
                        if len(recent_highs) > 0:
                            stop = recent_highs.iloc[-1] + (atr * self.stop_buffer_atr)
                        else:
                            stop = row['close'] + (atr * 1.5)

                    risk = abs(row['close'] - stop)
                    if risk == 0 or risk > atr * 6:  # skip unusually wide stops
                        continue

                    tp1 = row['close'] + (risk * self.tp1_r) if direction == "long" else row['close'] - (risk * self.tp1_r)
                    tp2 = row['close'] + (risk * self.tp2_r) if direction == "long" else row['close'] - (risk * self.tp2_r)

                    # Simple retest probability
                    distance = abs(row['close'] - broken_level)
                    retest_prob = self.simple_retest_probability(distance, atr, self.retest_window)

                    take_trade = False
                    if self.entry_mode == "break_close":
                        take_trade = True
                    elif self.entry_mode == "retest":
                        take_trade = True
                    elif self.entry_mode == "adaptive":
                        if retest_prob < self.adaptive_threshold:
                            take_trade = True
                        else:
                            take_trade = False

                    if take_trade:
                        self.current_trade = Trade(
                            entry_time=current_time,
                            direction=direction,
                            entry_price=row['close'],
                            stop_loss=stop,
                            tp1=tp1,
                            tp2=tp2,
                            risk=risk
                        )

            # Equity tracking
            self.equity_curve.append(equity)

        # Close any remaining trade at the end
        if self.current_trade is not None:
            self._close_trade(self.current_trade, df['close'].iloc[-1], "end_of_data", df.index[-1])

        return self.stats

    def _close_trade(self, trade: Trade, exit_price: float, reason: str, exit_time: datetime):
        trade.exit_time = exit_time
        trade.exit_price = exit_price
        trade.exit_reason = reason

        if trade.direction == "long":
            raw_r = (exit_price - trade.entry_price) / trade.risk
        else:
            raw_r = (trade.entry_price - exit_price) / trade.risk

        trade.r_multiple = raw_r

        self.stats.trades.append(trade)
        self.stats.total_trades += 1
        self.stats.total_r += trade.r_multiple

        if trade.direction == "long":
            self.stats.long_trades += 1
        else:
            self.stats.short_trades += 1

        if trade.r_multiple > 0.05:
            self.stats.wins += 1
            if trade.direction == "long":
                self.stats.long_wins += 1
            else:
                self.stats.short_wins += 1
        elif trade.r_multiple < -0.05:
            self.stats.losses += 1
        else:
            self.stats.breakevens += 1

        if "tp1" in reason:
            self.stats.tp1_hits += 1
        if reason == "tp2":
            self.stats.tp2_hits += 1
        if reason == "sl":
            self.stats.sl_hits += 1
        if reason == "timeout":
            self.stats.timeouts += 1
        if reason == "opposite":
            self.stats.opposite_closes += 1

        self.current_trade = None

    def print_report(self):
        s = self.stats
        print("\n" + "="*60)
        print("SMART MONEY CONCEPT - BACKTEST REPORT (1 HOUR)")
        print("="*60)
        print(f"Total Trades        : {s.total_trades}")
        print(f"Wins                : {s.wins}")
        print(f"Losses              : {s.losses}")
        print(f"Breakevens          : {s.breakevens}")
        win_rate = (s.wins / s.total_trades * 100) if s.total_trades > 0 else 0
        print(f"Win Rate            : {win_rate:.2f}%")
        avg_r = s.total_r / s.total_trades if s.total_trades > 0 else 0
        print(f"Average R           : {avg_r:.3f}")
        print(f"Total R             : {s.total_r:.2f}")
        print("-"*60)
        print(f"TP1 Hits            : {s.tp1_hits}")
        print(f"TP2 Hits            : {s.tp2_hits}")
        print(f"Stop Loss Hits      : {s.sl_hits}")
        print(f"Timeouts            : {s.timeouts}")
        print(f"Opposite Signal     : {s.opposite_closes}")
        print("-"*60)
        print(f"Long Trades         : {s.long_trades} (Wins: {s.long_wins})")
        print(f"Short Trades        : {s.short_trades} (Wins: {s.short_wins})")
        print("="*60)

        print("\nLast 10 Trades:")
        print("-"*60)
        for t in s.trades[-10:]:
            print(f"{t.entry_time} | {t.direction.upper():5} | R: {t.r_multiple:+.2f} | {t.exit_reason}")


# ============================================================
# HOW TO USE
# ============================================================
"""
Example usage:

# 1. Load your 1H data (must have columns: open, high, low, close, volume)
#    Index must be DatetimeIndex
df = pd.read_csv("your_1h_data.csv", parse_dates=True, index_col=0)

# Make sure column names are lowercase
df.columns = [c.lower() for c in df.columns]

# 2. Run backtest
strategy = SmartMoneyConceptBacktest(
    entry_mode="adaptive",          # options: "break_close", "retest", "adaptive"
    swing_length=10,
    retest_window=20,
    timeout_bars=100,
    adaptive_threshold=0.70
)

stats = strategy.run(df)
strategy.print_report()

# 3. Access full trade list if needed:
# for trade in stats.trades:
#     print(trade.entry_time, trade.direction, trade.r_multiple, trade.exit_reason)
"""
