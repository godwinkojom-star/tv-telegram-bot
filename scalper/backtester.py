"""Sequential Gold scalper backtester with risk-based sizing and no look-ahead."""
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
    stop_loss: float
    take_profit: float
    risk_amount: float
    quantity: float
    rr: float
    exit_time: Optional[object] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0
    pnl_pct: float = 0.0
    pnl_r: float = 0.0
    hold_bars: int = 0
    balance_after: Optional[float] = None
    def to_dict(self): return asdict(self)

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
    def __init__(self, config: Optional[ScalperConfig] = None, spread: float = 0.0, slippage: float = 0.0):
        self.cfg = config or ScalperConfig()
        self.spread, self.slippage = max(0.0, float(spread)), max(0.0, float(slippage))

    def _fill_price(self, direction: str, raw: float) -> float:
        cost = self.spread / 2 + self.slippage
        return raw + cost if direction == "BUY" else raw - cost

    @staticmethod
    def _move(direction: str, entry: float, exit_: float) -> float:
        return exit_ - entry if direction == "BUY" else entry - exit_

    def run(self, df1: pd.DataFrame, df5: pd.DataFrame) -> BacktestResult:
        if df1 is None or df5 is None or df1.empty or df5.empty: return self._empty_result()
        one, five = df1.copy().sort_index(), df5.copy().sort_index()
        if not isinstance(one.index, pd.DatetimeIndex): one.index = pd.to_datetime(one.index)
        if not isinstance(five.index, pd.DatetimeIndex): five.index = pd.to_datetime(five.index)
        balance = float(self.cfg.starting_balance)
        position: Optional[Trade] = None
        trades: list[Trade] = []
        curve = []
        last_entry_bar: dict[str, int] = {}
        trade_id = 1

        # Iterate all bars except last for entries; every signal fills at next bar open.
        for i in range(len(one) - 1):
            ts, bar, next_bar = one.index[i], one.iloc[i], one.iloc[i + 1]
            one_seen = one.iloc[:i + 1]
            five_seen = five.loc[five.index <= ts]
            if position is not None:
                position.hold_bars += 1
                exit_price, reason = None, None
                high, low = float(bar["high"]), float(bar["low"])
                if position.direction == "BUY":
                    if low <= position.stop_loss: exit_price, reason = position.stop_loss, "SL"
                    elif high >= position.take_profit: exit_price, reason = position.take_profit, "TP"
                else:
                    if high >= position.stop_loss: exit_price, reason = position.stop_loss, "SL"
                    elif low <= position.take_profit: exit_price, reason = position.take_profit, "TP"
                if reason is None and position.hold_bars >= max(1, int(self.cfg.max_hold_bars)):
                    exit_price, reason = float(bar["close"]), "TIMEOUT"
                if reason:
                    self._close_position(position, self._fill_price("SELL" if position.direction == "BUY" else "BUY", float(exit_price)), ts, reason, balance)
                    pnl = position.pnl
                    balance += pnl
                    position.balance_after = balance
                    trades.append(position)
                    last_entry_bar.setdefault(position.direction, i)
                    position = None

            signal = None
            if len(one_seen) >= self.cfg.minimum_1m_bars and len(five_seen) >= self.cfg.minimum_5m_bars:
                signal = evaluate(one_seen, five_seen, self.cfg)
            if signal and signal.get("status") == "SIGNAL":
                direction = signal.get("direction")
                # Avoid repeated same-direction entries; allow confirmed opposite reversal.
                cooldown = max(0, int(self.cfg.same_direction_cooldown_bars))
                in_cooldown = direction in last_entry_bar and i - last_entry_bar[direction] <= cooldown
                can_enter = not in_cooldown
                if position is not None and direction == position.direction and self.cfg.one_position_per_direction:
                    can_enter = False
                if position is not None and direction != position.direction and self.cfg.allow_immediate_reversal:
                    # Close at current close; entry fills at next bar open.
                    close_px = self._fill_price("SELL" if position.direction == "BUY" else "BUY", float(bar["close"]))
                    self._close_position(position, close_px, ts, "REVERSAL", balance)
                    balance += position.pnl
                    position.balance_after = balance
                    trades.append(position)
                    last_entry_bar[position.direction] = i
                    position = None
                    can_enter = not in_cooldown
                if position is None and can_enter and direction in ("BUY", "SELL"):
                    entry = self._fill_price(direction, float(next_bar["open"]))
                    sl = float(signal.get("sl", signal.get("stop_loss", float("nan"))))
                    tp = float(signal.get("tp", signal.get("take_profit", float("nan"))))
                    risk_per_unit = abs(entry - sl) * float(self.cfg.point_value)
                    reward_per_unit = abs(tp - entry) * float(self.cfg.point_value)
                    risk_amount = max(0.0, balance * float(self.cfg.risk_percent) / 100.0)
                    if all(math.isfinite(x) for x in (entry, sl, tp, risk_per_unit, reward_per_unit)) and risk_per_unit > 0 and reward_per_unit > 0 and risk_amount > 0:
                        qty = risk_amount / risk_per_unit
                        rr = reward_per_unit / risk_per_unit
                        position = Trade(trade_id, direction, one.index[i+1], entry, sl, tp, risk_amount, qty, rr)
                        trade_id += 1
                        last_entry_bar[direction] = i
            mark = float(bar["close"])
            unrealized = self._move(position.direction, position.entry_price, mark) * position.quantity * float(self.cfg.point_value) if position else 0.0
            curve.append({"timestamp": ts, "balance": balance, "equity": balance + unrealized})

        if position is not None:
            ts = one.index[-1]
            exit_px = self._fill_price("SELL" if position.direction == "BUY" else "BUY", float(one.iloc[-1]["close"]))
            self._close_position(position, exit_px, ts, "END_OF_DATA", balance)
            balance += position.pnl
            position.balance_after = balance
            trades.append(position)
        eq = pd.DataFrame(curve)
        if not eq.empty:
            peak = eq["equity"].cummax()
            dd = eq["equity"] - peak
            dd_pct = dd / peak.replace(0, float("nan")) * 100
            max_dd = float(dd.min())
            max_dd_pct = float(dd_pct.min()) if dd_pct.notna().any() else 0.0
        else: max_dd = max_dd_pct = 0.0
        wins = sum(t.pnl > 0 for t in trades); losses = sum(t.pnl < 0 for t in trades)
        gross_win = sum(t.pnl for t in trades if t.pnl > 0); gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        pf = gross_win / gross_loss if gross_loss else (math.inf if gross_win else 0.0)
        total = len(trades)
        return BacktestResult(trades, eq, float(self.cfg.starting_balance), balance, balance-float(self.cfg.starting_balance), total, wins, losses, wins/total*100 if total else 0.0, pf, max_dd, max_dd_pct)

    def _close_position(self, p: Trade, exit_price: float, timestamp, reason: str, balance: float) -> None:
        p.exit_time, p.exit_price, p.exit_reason = timestamp, float(exit_price), reason
        p.pnl = self._move(p.direction, p.entry_price, p.exit_price) * p.quantity * float(self.cfg.point_value)
        p.pnl_pct = p.pnl / balance * 100 if balance else 0.0
        p.pnl_r = p.pnl / p.risk_amount if p.risk_amount else 0.0

    def _empty_result(self):
        return BacktestResult([], pd.DataFrame(columns=["timestamp", "balance", "equity"]), float(self.cfg.starting_balance), float(self.cfg.starting_balance), 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)
