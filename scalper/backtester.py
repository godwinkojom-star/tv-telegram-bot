"""Sequential Gold scalper backtester with risk-based sizing and no look-ahead."""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Optional

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
    risk_amount: float
    quantity: float
    rr: float = 0.0
    max_favorable_r: float = 0.0
    reached_milestones: list[int] = field(default_factory=list)
    exit_time: Optional[object] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0
    pnl_pct: float = 0.0
    pnl_r: float = 0.0
    hold_bars: int = 0
    balance_after: Optional[float] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class BacktestResult:
    trades: list
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
    def __init__(
        self,
        config: Optional[ScalperConfig] = None,
        spread: float = 0.0,
        slippage: float = 0.0,
    ):
        self.cfg = config or ScalperConfig()
        self.spread = max(0.0, float(spread))
        self.slippage = max(0.0, float(slippage))

    def _fill_price(self, direction: str, raw: float) -> float:
        cost = self.spread / 2 + self.slippage
        return raw + cost if direction == "BUY" else raw - cost

    def _move(self, direction: str, entry: float, exit_: float) -> float:
        return exit_ - entry if direction == "BUY" else entry - exit_

    def _validate_levels(self, direction: str, entry: float, sl: float) -> bool:
        if not all(math.isfinite(v) for v in (entry, sl)):
            return False
        if direction == "BUY":
            return sl < entry
        if direction == "SELL":
            return sl > entry
        return False

    def run(self, df1: pd.DataFrame, df5: pd.DataFrame) -> BacktestResult:
        if df1 is None or df5 is None or df1.empty or df5.empty:
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
        curve = []
        last_entry_bar: dict[str, int] = {}
        trade_id = 1

        for i in range(len(one) - 1):
            ts = one.index[i]
            bar = one.iloc[i]
            next_bar = one.iloc[i + 1]
            one_seen = one.iloc[: i + 1]
            five_seen = five.loc[five.index <= ts]

            if position is not None:
                position.hold_bars += 1
                exit_price, reason = None, None
                high, low = float(bar["high"]), float(bar["low"])
                if position.direction == "BUY":
                    if low <= position.stop_loss:
                        exit_price, reason = position.stop_loss, "SL"
                    favourable = max(0.0, float(high) - position.entry_price)
                else:
                    if high >= position.stop_loss:
                        exit_price, reason = position.stop_loss, "SL"
                    favourable = max(0.0, position.entry_price - float(low))
                risk_per_unit = abs(position.entry_price - position.stop_loss)
                if risk_per_unit > 0:
                    position.max_favorable_r = max(position.max_favorable_r, favourable / risk_per_unit)
                    position.reached_milestones = [m for m in (1, 2, 3, 4, 5) if position.max_favorable_r >= m]
                if reason:
                    fill = self._fill_price(
                        "SELL" if position.direction == "BUY" else "BUY", float(exit_price)
                    )
                    self._close_position(position, fill, ts, reason, balance)
                    balance += position.pnl
                    position.balance_after = balance
                    trades.append(position)
                    last_entry_bar[position.direction] = i
                    position = None

            signal = None
            if len(one_seen) >= self.cfg.minimum_1m_bars and len(five_seen) >= self.cfg.minimum_5m_bars:
                signal = evaluate(one_seen, five_seen, self.cfg)

            if signal and signal.get("status") == "SIGNAL":
                direction = signal.get("direction")
                cooldown = max(0, int(self.cfg.same_direction_cooldown_bars))
                in_cooldown = direction in last_entry_bar and i - last_entry_bar[direction] <= cooldown
                can_enter = not in_cooldown
                if position is not None and direction == position.direction and self.cfg.one_position_per_direction:
                    can_enter = False
                if position is not None and direction != position.direction:
                    close_px = self._fill_price(
                        "SELL" if position.direction == "BUY" else "BUY", float(bar["close"])
                    )
                    self._close_position(position, close_px, ts, "REVERSAL", balance)
                    balance += position.pnl
                    position.balance_after = balance
                    trades.append(position)
                    last_entry_bar[position.direction] = i
                    position = None
                    can_enter = self.cfg.allow_immediate_reversal and not in_cooldown

                if position is None and can_enter and direction in ("BUY", "SELL"):
                    entry = self._fill_price(direction, float(next_bar["open"]))
                    sl = float(signal.get("sl", float("nan")))
                    if self._validate_levels(direction, entry, sl):
                        risk_per_unit = abs(entry - sl) * float(self.cfg.point_value)
                        risk_amount = max(0.0, balance * float(self.cfg.risk_percent) / 100.0)
                        if math.isfinite(risk_per_unit) and risk_per_unit > 0 and risk_amount > 0:
                            qty = risk_amount / risk_per_unit
                            position = Trade(
                                trade_id, direction, one.index[i + 1], entry, sl, risk_amount, qty, 0.0,
                                reached_milestones=[]
                            )
                            trade_id += 1
                            last_entry_bar[direction] = i

            mark = float(bar["close"])
            unrealized = (
                self._move(position.direction, position.entry_price, mark)
                * position.quantity
                * float(self.cfg.point_value)
                if position
                else 0.0
            )
            curve.append({"timestamp": ts, "balance": balance, "equity": balance + unrealized})

        if position is not None:
            ts = one.index[-1]
            exit_px = self._fill_price(
                "SELL" if position.direction == "BUY" else "BUY", float(one.iloc[-1]["close"])
            )
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
        else:
            max_dd = max_dd_pct = 0.0

        wins = sum(1 for t in trades if t.pnl > 0)
        losses = sum(1 for t in trades if t.pnl < 0)
        gross_win = sum(t.pnl for t in trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        pf = gross_win / gross_loss if gross_loss else (math.inf if gross_win else 0.0)
        total = len(trades)
        return BacktestResult(
            trades,
            eq,
            float(self.cfg.starting_balance),
            balance,
            balance - float(self.cfg.starting_balance),
            total,
            wins,
            losses,
            wins / total * 100 if total else 0.0,
            pf,
            max_dd,
            max_dd_pct,
        )

    def _close_position(self, p: Trade, exit_price: float, timestamp, reason: str, balance: float) -> None:
        p.exit_time = timestamp
        p.exit_price = float(exit_price)
        p.exit_reason = reason
        p.pnl = (
            self._move(p.direction, p.entry_price, p.exit_price)
            * p.quantity
            * float(self.cfg.point_value)
        )
        p.pnl_pct = p.pnl / balance * 100 if balance else 0.0
        p.pnl_r = p.pnl / p.risk_amount if p.risk_amount else 0.0

    def _empty_result(self) -> BacktestResult:
        return BacktestResult(
            [],
            pd.DataFrame(columns=["timestamp", "balance", "equity"]),
            float(self.cfg.starting_balance),
            float(self.cfg.starting_balance),
            0.0,
            0,
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        )
