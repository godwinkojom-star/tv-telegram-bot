"""Backtester for the SmartFX experimental Gold reversal scalper.

Signals are evaluated on completed bars. By default an entry/reversal is filled
at the NEXT 1m candle open, which avoids look-ahead bias.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional
import math
import pandas as pd

from .config import ScalperConfig
from .strategy import evaluate


@dataclass
class Trade:
    trade_id: int
    direction: str
    entry_time: object
    entry_price: float
    exit_time: Optional[object] = None
    exit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0
    pnl_pct: float = 0.0
    hold_bars: int = 0

    def to_dict(self):
        return asdict(self)


@dataclass
class BacktestResult:
    trades: list[Trade]
    equity_curve: pd.DataFrame
    starting_balance: float
    ending_balance: float
    net_pnl: float
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    profit_factor: float
    max_drawdown: float
    max_drawdown_pct: float


class GoldScalperBacktester:
    """Sequential 1m backtester using 5m confirmation and the existing strategy."""

    def __init__(self, config: Optional[ScalperConfig] = None, spread: float = 0.0,
                 slippage: float = 0.0):
        self.cfg = config or ScalperConfig()
        self.spread = float(spread)
        self.slippage = float(slippage)

    def _fill_price(self, direction: str, raw_price: float) -> float:
        # Conservative fixed spread + slippage model.
        if direction == "BUY":
            return raw_price + self.spread / 2 + self.slippage
        return raw_price - self.spread / 2 - self.slippage

    def _pnl(self, direction: str, entry: float, exit_: float) -> float:
        if direction == "BUY":
            return exit_ - entry
        return entry - exit_

    def run(self, df1: pd.DataFrame, df5: pd.DataFrame) -> BacktestResult:
        if df1.empty or df5.empty:
            return self._empty_result()

        one = df1.copy().sort_index()
        five = df5.copy().sort_index()
        if not isinstance(one.index, pd.DatetimeIndex):
            one.index = pd.to_datetime(one.index)
        if not isinstance(five.index, pd.DatetimeIndex):
            five.index = pd.to_datetime(five.index)

        balance = float(self.cfg.starting_balance)
        position: Optional[Trade] = None
        trades: list[Trade] = []
        equity_rows = []
        next_trade_id = 1

        # Only completed 1m candles through i are available to the strategy.
        for i in range(len(one) - 1):
            timestamp = one.index[i]
            bar = one.iloc[i]
            next_bar = one.iloc[i + 1]

            # Use only 5m candles that closed no later than this 1m bar.
            five_available = five.loc[five.index <= timestamp]
            one_available = one.iloc[: i + 1]

            # First resolve an existing position against this completed bar.
            if position is not None:
                position.hold_bars += 1
                exit_price = None
                reason = None

                if position.direction == "BUY":
                    if float(bar["low"]) <= position.stop_loss:
                        exit_price = position.stop_loss
                        reason = "SL"
                    elif float(bar["high"]) >= position.take_profit:
                        exit_price = position.take_profit
                        reason = "TP"
                else:
                    if float(bar["high"]) >= position.stop_loss:
                        exit_price = position.stop_loss
                        reason = "SL"
                    elif float(bar["low"]) <= position.take_profit:
                        exit_price = position.take_profit
                        reason = "TP"

                if reason is None and position.hold_bars >= self.cfg.max_hold_bars:
                    exit_price = float(bar["close"])
                    reason = "TIMEOUT"

                if reason is not None:
                    fill = self._fill_price("SELL" if position.direction == "BUY" else "BUY", exit_price)
                    pnl = self._pnl(position.direction, position.entry_price, fill)
                    position.exit_time = timestamp
                    position.exit_price = fill
                    position.exit_reason = reason
                    position.pnl = pnl
                    position.pnl_pct = (pnl / position.entry_price) * 100 if position.entry_price else 0.0
                    balance += pnl
                    trades.append(position)
                    position = None

            # Evaluate a fresh signal only when flat. Signal is filled next bar open.
            if position is None and len(one_available) >= self.cfg.minimum_1m_bars and len(five_available) >= self.cfg.minimum_5m_bars:
                signal = evaluate(one_available, five_available, self.cfg)
                if signal.get("status") == "SIGNAL":
                    direction = signal["direction"]
                    entry = self._fill_price(direction, float(next_bar["open"]))
                    position = Trade(
                        trade_id=next_trade_id,
                        direction=direction,
                        entry_time=one.index[i + 1],
                        entry_price=entry,
                        stop_loss=float(signal["stop_loss"]),
                        take_profit=float(signal["take_profit"]),
                    )
                    next_trade_id += 1

            mark = float(bar["close"])
            unrealized = 0.0
            if position is not None:
                unrealized = self._pnl(position.direction, position.entry_price, mark)
            equity_rows.append({"timestamp": timestamp, "equity": balance + unrealized})

        # Close any remaining position at the final close.
        if position is not None:
            timestamp = one.index[-1]
            fill = self._fill_price("SELL" if position.direction == "BUY" else "BUY", float(one.iloc[-1]["close"]))
            pnl = self._pnl(position.direction, position.entry_price, fill)
            position.exit_time = timestamp
            position.exit_price = fill
            position.exit_reason = "END_OF_DATA"
            position.pnl = pnl
            position.pnl_pct = (pnl / position.entry_price) * 100 if position.entry_price else 0.0
            position.hold_bars += 1
            balance += pnl
            trades.append(position)

        curve = pd.DataFrame(equity_rows)
        if not curve.empty:
            running_max = curve["equity"].cummax()
            dd = curve["equity"] - running_max
            dd_pct = dd / running_max.replace(0, pd.NA) * 100
            max_dd = float(dd.min())
            max_dd_pct = float(dd_pct.min()) if len(dd_pct.dropna()) else 0.0
        else:
            max_dd = max_dd_pct = 0.0

        wins = sum(1 for t in trades if t.pnl > 0)
        losses = sum(1 for t in trades if t.pnl < 0)
        gross_win = sum(t.pnl for t in trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        pf = gross_win / gross_loss if gross_loss else (math.inf if gross_win else 0.0)
        total = len(trades)

        return BacktestResult(
            trades=trades,
            equity_curve=curve,
            starting_balance=self.cfg.starting_balance,
            ending_balance=balance,
            net_pnl=balance - self.cfg.starting_balance,
            total_trades=total,
            wins=wins,
            losses=losses,
            win_rate=(wins / total * 100) if total else 0.0,
            profit_factor=pf,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
        )

    def _empty_result(self):
        return BacktestResult([], pd.DataFrame(columns=["timestamp", "equity"]), self.cfg.starting_balance,
                              self.cfg.starting_balance, 0.0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0)
