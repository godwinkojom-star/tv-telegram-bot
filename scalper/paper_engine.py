"""Risk-sized, duplicate-safe paper engine for the SmartFS Gold scalper.

This module never fetches market data and never writes to shared portfolios.
Feed it completed OHLC bars from the configured Gold data path.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional, Any
import math
import pandas as pd
from .config import ScalperConfig
from .strategy import evaluate

@dataclass
class PaperPosition:
    direction: str
    entry_price: float
    entry_time: Any
    stop_loss: float
    take_profit: float
    risk_amount: float
    quantity: float
    initial_risk_per_unit: float
    initial_balance: float
    hold_bars: int = 0
    unrealized_pnl: float = 0.0
    rr: float = 0.0

class PaperEngine:
    def __init__(self, config: Optional[ScalperConfig] = None):
        self.cfg = config or ScalperConfig()
        self.balance = float(self.cfg.starting_balance)
        self.position: Optional[PaperPosition] = None
        self.closed_trades: list[dict] = []
        self.last_signal: Optional[dict] = None
        self.last_processed_bar: Any = None
        self.last_entry_by_direction: dict[str, int] = {}
        self.bars_processed = 0

    def on_bar(self, df1: pd.DataFrame, df5: pd.DataFrame) -> dict:
        """Process the latest completed 1m candle once; duplicate timestamps are ignored."""
        if df1 is None or df1.empty:
            return self.status("NO_DATA")
        now = df1.index[-1]
        if self.last_processed_bar is not None and now <= self.last_processed_bar:
            return self.status("DUPLICATE_BAR_IGNORED")
        self.last_processed_bar = now
        self.bars_processed += 1
        bar = df1.iloc[-1]
        price = float(bar["close"])

        # Existing position management always runs even if no new entry is permitted.
        if self.position is not None:
            p = self.position
            p.hold_bars += 1
            p.unrealized_pnl = self._pnl(p.direction, p.entry_price, price, p.quantity)
            exit_reason, exit_price = self._bar_exit(p, bar)
            if exit_reason is None and p.hold_bars >= max(1, int(self.cfg.max_hold_bars)):
                exit_reason, exit_price = "TIMEOUT", price
            if exit_reason:
                self._close(float(exit_price), now, exit_reason)

        signal = evaluate(df1, df5, self.cfg) if df5 is not None and not df5.empty else {"status": "NO_SIGNAL", "reason": "no_5m_data"}
        self.last_signal = signal
        if signal.get("status") != "SIGNAL":
            return self.status("UPDATED")

        direction = signal.get("direction")
        if direction not in ("BUY", "SELL"):
            return self.status("INVALID_SIGNAL")

        # Do not re-enter the same direction merely because evaluate() repeats a signal.
        last_idx = self.last_entry_by_direction.get(direction)
        cooldown = max(0, int(self.cfg.same_direction_cooldown_bars))
        if last_idx is not None and self.bars_processed - last_idx <= cooldown:
            if self.position is None or self.position.direction == direction:
                return self.status("COOLDOWN")

        if self.position is not None:
            if self.position.direction == direction:
                if self.cfg.one_position_per_direction:
                    return self.status("SAME_DIRECTION_POSITION_EXISTS")
                return self.status("UPDATED")
            if not self.cfg.allow_immediate_reversal:
                return self.status("REVERSAL_DISABLED")
            # An opposite confirmed signal closes at the completed candle close, then opens.
            self._close(price, now, "REVERSAL")

        self._open(direction, now, signal)
        return self.status("UPDATED")

    def _bar_exit(self, p: PaperPosition, bar) -> tuple[Optional[str], Optional[float]]:
        high, low = float(bar["high"]), float(bar["low"])
        # Conservative ambiguity rule: if SL and TP are both in the same 1m candle, SL wins.
        if p.direction == "BUY":
            if low <= p.stop_loss: return "SL", p.stop_loss
            if high >= p.take_profit: return "TP", p.take_profit
        else:
            if high >= p.stop_loss: return "SL", p.stop_loss
            if low <= p.take_profit: return "TP", p.take_profit
        return None, None

    def _open(self, direction: str, timestamp, signal: dict) -> None:
        entry = float(signal.get("entry", signal.get("price", float("nan"))))
        sl = float(signal.get("sl", signal.get("stop_loss", float("nan"))))
        tp = float(signal.get("tp", signal.get("take_profit", float("nan"))))
        if not all(math.isfinite(v) for v in (entry, sl, tp)):
            self.last_signal = {**signal, "engine_error": "invalid_trade_levels"}
            return
        risk_per_unit = abs(entry - sl) * float(self.cfg.point_value)
        reward_per_unit = abs(tp - entry) * float(self.cfg.point_value)
        if risk_per_unit <= 0 or reward_per_unit <= 0:
            self.last_signal = {**signal, "engine_error": "non_positive_risk_or_reward"}
            return
        risk_amount = max(0.0, self.balance * float(self.cfg.risk_percent) / 100.0)
        quantity = risk_amount / risk_per_unit
        if not math.isfinite(quantity) or quantity <= 0:
            self.last_signal = {**signal, "engine_error": "invalid_position_size"}
            return
        rr = reward_per_unit / risk_per_unit
        self.position = PaperPosition(direction, entry, timestamp, sl, tp, risk_amount, quantity,
                                      abs(entry - sl), self.balance, rr=rr)
        self.last_entry_by_direction[direction] = self.bars_processed

    def _close(self, price: float, timestamp, reason: str) -> None:
        p = self.position
        if p is None: return
        pnl = self._pnl(p.direction, p.entry_price, price, p.quantity)
        self.balance += pnl
        risk_amount = p.risk_amount
        self.closed_trades.append({
            "pair": self.cfg.pair, "strategy": "EXPERIMENTAL_GOLD_REVERSAL_SCALPER",
            "direction": p.direction, "entry_time": p.entry_time, "exit_time": timestamp,
            "entry_price": p.entry_price, "exit_price": float(price),
            "stop_loss": p.stop_loss, "take_profit": p.take_profit,
            "risk_amount": risk_amount, "quantity": p.quantity, "rr": p.rr,
            "hold_bars": p.hold_bars, "pnl": pnl, "pnl_r": pnl / risk_amount if risk_amount else 0.0,
            "balance_after": self.balance, "exit_reason": reason,
            "result": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "BREAKEVEN",
        })
        self.position = None

    @staticmethod
    def _pnl(direction: str, entry: float, exit_price: float, quantity: float) -> float:
        move = exit_price - entry if direction == "BUY" else entry - exit_price
        return move * quantity

    def status(self, state: str = "UPDATED") -> dict:
        trades = self.closed_trades
        wins = sum(t["pnl"] > 0 for t in trades)
        losses = sum(t["pnl"] < 0 for t in trades)
        return {
            "state": state, "pair": self.cfg.pair, "balance": self.balance,
            "starting_balance": float(self.cfg.starting_balance),
            "net_pnl": self.balance - float(self.cfg.starting_balance),
            "position": asdict(self.position) if self.position else None,
            "last_signal": self.last_signal, "closed_trades": len(trades),
            "wins": wins, "losses": losses,
            "win_rate": (wins / len(trades) * 100) if trades else 0.0,
        }
