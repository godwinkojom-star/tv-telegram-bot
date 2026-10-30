"""Paper-only Gold Reversal Scalper engine.

Positions exit at the original stop loss or after a fully confirmed opposite
strategy signal. There is no fixed take-profit and no bar-count timeout.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

import pandas as pd

from .config import ScalperConfig
from .strategy import evaluate


@dataclass
class PaperPosition:
    direction: str
    entry_price: float
    entry_time: Any
    stop_loss: float
    risk_amount: float
    quantity: float
    initial_risk_per_unit: float
    initial_balance: float
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    hold_bars: int = 0
    unrealized_pnl: float = 0.0
    current_rr: float = 0.0
    max_favorable_r: float = 0.0
    reached_milestones: list[int] = field(default_factory=list)


def _validate_levels(direction: str, entry: float, sl: float) -> Optional[str]:
    if not all(math.isfinite(v) for v in (entry, sl)):
        return "non_finite_levels"
    if direction == "BUY" and sl >= entry:
        return "buy_stop_must_be_below_entry"
    if direction == "SELL" and sl <= entry:
        return "sell_stop_must_be_above_entry"
    if direction not in ("BUY", "SELL"):
        return "invalid_direction"
    return None


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

    def set_risk_percent(self, risk_percent: float) -> None:
        self.cfg = self.cfg.with_risk(risk_percent)

    def restore_state(self, state: dict) -> None:
        """Restore the latest persisted balance/open position after a restart."""
        if not state:
            return
        self.balance = float(state.get("balance", self.balance))
        raw = state.get("position")
        self.last_processed_bar = state.get("last_processed_bar") or (raw or {}).get("last_processed_bar")
        if raw:
            self.position = PaperPosition(
                direction=str(raw["direction"]).upper(),
                entry_price=float(raw["entry_price"]),
                entry_time=raw.get("entry_time"),
                stop_loss=float(raw["stop_loss"]),
                risk_amount=float(raw["risk_amount"]),
                quantity=float(raw["quantity"]),
                initial_risk_per_unit=float(raw["initial_risk_per_unit"]),
                initial_balance=float(raw.get("initial_balance", self.balance)),
                trade_id=str(raw["trade_id"]),
                hold_bars=int(raw.get("hold_bars", 0)),
                unrealized_pnl=float(raw.get("unrealized_pnl", 0.0)),
                current_rr=float(raw.get("current_rr", 0.0)),
                max_favorable_r=float(raw.get("max_favorable_r", 0.0)),
                reached_milestones=list(raw.get("reached_milestones") or []),
            )

    def on_bar(self, df1: pd.DataFrame, df5: pd.DataFrame) -> dict:
        if df1 is None or df1.empty:
            return self.status("NO_DATA")
        now = df1.index[-1]
        if self.last_processed_bar is not None and now <= self.last_processed_bar:
            return self.status("DUPLICATE_BAR_IGNORED")
        self.last_processed_bar = now
        self.bars_processed += 1
        bar = df1.iloc[-1]
        price = float(bar["close"])

        if self.position is not None:
            p = self.position
            p.hold_bars += 1
            p.unrealized_pnl = self._pnl(p.direction, p.entry_price, price, p.quantity)
            p.current_rr = p.unrealized_pnl / p.risk_amount if p.risk_amount else 0.0
            self._update_excursion(p, bar)
            exit_reason, exit_price = self._bar_exit(p, bar)
            if exit_reason:
                self._close(float(exit_price), now, exit_reason)

        signal = (
            evaluate(df1, df5, self.cfg)
            if df5 is not None and not df5.empty
            else {"status": "NO_SIGNAL", "reason": "no_5m_data"}
        )
        self.last_signal = signal
        if signal.get("status") != "SIGNAL":
            return self.status("UPDATED")

        direction = str(signal.get("direction") or "").upper()
        if direction not in ("BUY", "SELL"):
            return self.status("INVALID_SIGNAL")

        # A fully confirmed opposite signal closes the old position. The same
        # signal may open the opposite position if immediate reversal is enabled.
        if self.position is not None:
            if self.position.direction == direction:
                return self.status("SAME_DIRECTION_POSITION_EXISTS")
            self._close(price, now, "REVERSAL")
            if not self.cfg.allow_immediate_reversal:
                return self.status("CLOSED_ON_CONFIRMED_REVERSAL")

        last_idx = self.last_entry_by_direction.get(direction)
        cooldown = max(0, int(self.cfg.same_direction_cooldown_bars))
        if last_idx is not None and self.bars_processed - last_idx <= cooldown:
            return self.status("COOLDOWN")

        self._open(direction, now, signal)
        return self.status("UPDATED")

    def _update_excursion(self, p: PaperPosition, bar) -> None:
        high, low = float(bar["high"]), float(bar["low"])
        if p.direction == "BUY":
            favourable_move = max(0.0, high - p.entry_price)
        else:
            favourable_move = max(0.0, p.entry_price - low)
        current_favourable_r = favourable_move / p.initial_risk_per_unit if p.initial_risk_per_unit else 0.0
        p.max_favorable_r = max(p.max_favorable_r, current_favourable_r)
        for milestone in (1, 2, 3, 4, 5):
            if p.max_favorable_r >= milestone and milestone not in p.reached_milestones:
                p.reached_milestones.append(milestone)

    def _bar_exit(self, p: PaperPosition, bar) -> tuple[Optional[str], Optional[float]]:
        high, low = float(bar["high"]), float(bar["low"])
        if p.direction == "BUY" and low <= p.stop_loss:
            return "SL", p.stop_loss
        if p.direction == "SELL" and high >= p.stop_loss:
            return "SL", p.stop_loss
        return None, None

    def _open(self, direction: str, timestamp, signal: dict) -> None:
        entry = float(signal.get("entry", signal.get("price", float("nan"))))
        sl = float(signal.get("sl", signal.get("stop_loss", float("nan"))))
        level_err = _validate_levels(direction, entry, sl)
        if level_err:
            self.last_signal = {**signal, "engine_error": level_err}
            return
        risk_per_unit = abs(entry - sl) * float(self.cfg.point_value)
        risk_amount = max(0.0, self.balance * float(self.cfg.risk_percent) / 100.0)
        quantity = risk_amount / risk_per_unit if risk_per_unit > 0 else 0.0
        if not math.isfinite(quantity) or quantity <= 0:
            self.last_signal = {**signal, "engine_error": "invalid_position_size"}
            return
        self.position = PaperPosition(
            direction=direction,
            entry_price=entry,
            entry_time=timestamp,
            stop_loss=sl,
            risk_amount=risk_amount,
            quantity=quantity,
            initial_risk_per_unit=abs(entry - sl),
            initial_balance=self.balance,
        )
        self.last_entry_by_direction[direction] = self.bars_processed

    def _close(self, price: float, timestamp, reason: str) -> None:
        p = self.position
        if p is None:
            return
        pnl = self._pnl(p.direction, p.entry_price, price, p.quantity)
        self.balance += pnl
        realized_r = pnl / p.risk_amount if p.risk_amount else 0.0
        self.closed_trades.append({
            "trade_id": p.trade_id,
            "pair": self.cfg.pair,
            "strategy": "GOLD_REVERSAL_SCALPER",
            "strategy_name": "Gold Reversal Scalper",
            "direction": p.direction,
            "entry_time": p.entry_time,
            "exit_time": timestamp,
            "entry_price": p.entry_price,
            "exit_price": float(price),
            "stop_loss": p.stop_loss,
            "take_profit": None,
            "exit_method": "STOP_LOSS_OR_CONFIRMED_OPPOSITE_REVERSAL",
            "risk_amount": p.risk_amount,
            "risk_percent": float(self.cfg.risk_percent),
            "quantity": p.quantity,
            "point_value": float(self.cfg.point_value),
            "initial_risk_per_unit": p.initial_risk_per_unit,
            "realized_rr": realized_r,
            "max_favorable_r": p.max_favorable_r,
            "reached_milestones": list(p.reached_milestones),
            "hold_bars": p.hold_bars,
            "pnl": pnl,
            "pnl_r": realized_r,
            "balance_after": self.balance,
            "exit_reason": reason,
            "result": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "BREAKEVEN",
        })
        self.position = None

    def _pnl(self, direction: str, entry: float, exit_price: float, quantity: float) -> float:
        move = exit_price - entry if direction == "BUY" else entry - exit_price
        return move * quantity * float(self.cfg.point_value)

    def status(self, state: str = "UPDATED") -> dict:
        trades = self.closed_trades
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        return {
            "state": state,
            "pair": self.cfg.pair,
            "strategy": "GOLD_REVERSAL_SCALPER",
            "strategy_name": "Gold Reversal Scalper",
            "balance": self.balance,
            "starting_balance": float(self.cfg.starting_balance),
            "net_pnl": self.balance - float(self.cfg.starting_balance),
            "risk_percent": float(self.cfg.risk_percent),
            "point_value": float(self.cfg.point_value),
            "position": asdict(self.position) if self.position else None,
            "last_signal": self.last_signal,
            "closed_trades": len(trades),
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / len(trades) * 100) if trades else 0.0,
            "bars_processed": self.bars_processed,
        }
